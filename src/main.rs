mod app_run_config;
mod app_run_image;
mod apply_diff;
mod config_image;
mod filter;
mod generate_diff;
mod kubernetes_io;
mod kubernetes_rendering;
mod object_policy;
mod output;
mod registry_clients;
mod sisyphus_yaml;
mod starlark;

use crate::{
    app_run_config::{run_config, RunConfigArgs},
    app_run_image::{run_image, RunImageArgs},
    apply_diff::{apply_diff, namespace_or_default},
    filter::{
        key_matches_filter, namespace_key_retained, required_namespace_identities, PartialKey,
    },
    generate_diff::{generate_diff, DiffAction, ResourceDiff},
    kubernetes_io::{
        get_kubernetes_api, get_kubernetes_clients, make_comparable, munge_secrets, KubernetesKey,
        KubernetesResources, MANAGER,
    },
    kubernetes_rendering::render_sisyphus_resource,
    object_policy::{
        get_status, list_history, load_pauses, pause_object, resume_object, ObjectPause,
        ObjectRef, GLOBAL_NAMESPACE,
    },
    output::{
        print_diff, report_applied, report_diffs, report_history, report_paused, report_status,
        ActionLabels,
    },
    registry_clients::{resolve_image_tag, RegistryClients},
    sisyphus_yaml::{HasConfigImage, HasKind, SisyphusResource},
};
use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use console::style;
use indicatif::{ProgressBar, ProgressStyle};
use k8s_openapi::api::core::v1::Namespace;
use kube::{
    api::{DynamicObject, ObjectMeta, Patch, PatchParams},
    core::ErrorResponse,
    Error, ResourceExt,
};
use serde::Deserialize;
use sqlx::{any::AnyPoolOptions, AnyPool, Row};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::{self, File},
    io::Write,
    path::Path,
};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct SisyphusArgs {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    App {
        #[command(subcommand)]
        app_command: AppCommands,
    },
    Diff {
        #[command(flatten)]
        args: PushArgs,
    },
    Forget {
        #[command(flatten)]
        database: Database,

        #[command(flatten)]
        key: FullKey,

        #[command(flatten)]
        yes: Consent,
    },
    Import {
        #[command(flatten)]
        database: Database,

        #[command(flatten)]
        key: FullKey,

        #[command(flatten)]
        yes: Consent,
    },
    Push {
        #[command(flatten)]
        args: PushArgs,

        #[command(flatten)]
        yes: Consent,
    },
    Refresh {
        #[command(flatten)]
        database: Database,

        #[command(flatten)]
        yes: Consent,
    },
    /// Examine and control one Sisyphus object.
    Object {
        #[command(subcommand)]
        object_command: ObjectCommands,
    },
}

#[derive(Debug, Subcommand)]
enum AppCommands {
    RunConfig {
        #[command(flatten)]
        args: RunConfigArgs,
    },
    RunImage {
        #[command(flatten)]
        args: RunImageArgs,
    },
}

#[derive(Debug, Subcommand)]
enum ObjectCommands {
    /// Show each config image that Sisyphus pushed for this object, most recent first.
    History {
        #[command(flatten)]
        database: Database,

        #[command(flatten)]
        object: ObjectKey,
    },
    /// Keep an object out of all pushes. The object continues to run with the configuration from
    /// its last push.
    Pause {
        #[command(flatten)]
        database: Database,

        #[command(flatten)]
        object: ObjectKey,

        #[arg(long)]
        reason: String,
    },
    /// Push one object with a config image that you give. Use this command to do a rollback. The
    /// `history` command shows the versions that did run, and this command installs one of them
    /// again.
    ///
    /// This command renders and applies no other resource. No other resource can change, and no
    /// other registry must reply. This command also deletes nothing. It does not change the pause.
    /// A paused object stays paused. An object that is not paused moves forward again at the next
    /// usual push.
    Push {
        #[command(flatten)]
        database: Database,

        /// A full image reference, for example registry/repository@sha256:...
        #[arg(long)]
        image: String,

        #[arg(long, env = "MONITOR_DIRECTORY")]
        monitor_directory: String,

        #[command(flatten)]
        object: ObjectKey,

        #[command(flatten)]
        yes: Consent,
    },
    /// Let Sisyphus push an object again.
    Resume {
        #[command(flatten)]
        database: Database,

        #[command(flatten)]
        object: ObjectKey,
    },
    /// Show the version that one object runs, and the pause that holds it at that version.
    Status {
        #[command(flatten)]
        database: Database,

        #[command(flatten)]
        object: ObjectKey,
    },
}

#[derive(Args, Debug)]
struct Database {
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
}

/// Identifies a Sisyphus object as the yaml writes it. This is not a rendered Kubernetes object.
#[derive(Args, Debug)]
struct ObjectKey {
    /// The kind as your Sisyphus yaml writes it: Deployment, CronJob, or KubernetesYaml. A pause
    /// applies to each Kubernetes object that the resource renders. Only a Deployment and a CronJob
    /// have a config image, and `object push` accepts these two kinds only.
    #[arg(long)]
    kind: String,

    #[arg(long)]
    name: String,

    /// The directory that contains the resource. For a cluster-level resource, this is `global`.
    #[arg(long)]
    namespace: String,
}

impl From<&ObjectKey> for ObjectRef {
    fn from(key: &ObjectKey) -> Self {
        ObjectRef::new(&key.kind, &key.namespace, &key.name)
    }
}

#[derive(Args, Debug)]
struct FullKey {
    #[arg(long)]
    api_version: String,

    #[arg(long)]
    cluster: String,

    #[arg(long)]
    kind: String,

    #[arg(long)]
    name: String,

    #[arg(long)]
    namespace: Option<String>,
}

impl Into<KubernetesKey> for FullKey {
    fn into(self) -> KubernetesKey {
        KubernetesKey {
            api_version: self.api_version,
            cluster: self.cluster,
            kind: self.kind,
            name: self.name,
            namespace: self.namespace,
        }
    }
}

#[derive(Args, Debug)]
struct PushArgs {
    #[command(flatten)]
    database: Database,

    // The filters to consider
    #[command(flatten)]
    filter: PartialKey,

    // The path to the directory of configuration files to monitor
    #[arg(long, env = "MONITOR_DIRECTORY")]
    monitor_directory: String,
}

#[derive(Args, Debug)]
struct Consent {
    /// Do not show the confirmation prompt. Each command that reads an answer from stdin accepts
    /// this flag, and a server can then run the command without a wait for an answer.
    #[arg(long, short = 'y')]
    yes: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    env_logger::init();
    sqlx::any::install_default_drivers();

    let args = SisyphusArgs::parse();
    match run(args.command).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Error: {:#}", error);
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run(command: Commands) -> Result<()> {
    match command {
        Commands::App { app_command } => match app_command {
            AppCommands::RunConfig { args } => run_config(args).await?,
            AppCommands::RunImage { args } => run_image(args).await?,
        },
        Commands::Diff {
            args: PushArgs {
                database,
                filter,
                monitor_directory,
            }
        } => {
            let pool = connect(&database).await?;
            let pauses = load_pauses(&pool).await?;
            let changed = diff(
                &filter,
                &monitor_directory,
                Scope::Everything(&pauses),
                &pool,
            )
            .await?;
            report_changes(&changed);
        }
        Commands::Forget { database, key, yes } => {
            let pool = connect(&database).await?;
            forget(key.into(), yes.yes, &pool).await?
        }
        Commands::Import { database, key, yes } => {
            let pool = connect(&database).await?;
            import(key.into(), yes.yes, &pool).await?
        }
        Commands::Push {
            args: PushArgs {
                database,
                filter,
                monitor_directory,
            },
            yes,
        } => {
            let pool = connect(&database).await?;
            push(&filter, &monitor_directory, yes.yes, &pool).await?
        }
        Commands::Refresh { database, yes } => {
            let pool = connect(&database).await?;
            refresh(yes.yes, &pool).await?
        }
        Commands::Object { object_command } => object(object_command).await?,
    };
    Ok(())
}

/// Connects to the database and gives it the name of the user.
///
/// The audit tables from `create_audit_table` record the `app.username` and `app.user_id` session
/// settings with each change. This function reads these two values from the environment, and not
/// from flags. The server that starts this CLI knows the user, and it sets these values for each
/// command from a session that it authenticated. Do not set these values from user input.
///
/// The pool holds one connection only, and then these settings apply to each query. This code runs
/// one query at a time on a single thread, and more connections give no advantage.
async fn connect(database: &Database) -> Result<AnyPool> {
    // `set_config`, `current_setting`, and the audit tables that read them are Postgres features.
    // Sqlite and MySQL have no equivalent, and a query for one makes the connection fail.
    let attributable = database.database_url.starts_with("postgres");
    let username = attributable
        .then(|| std::env::var("SISYPHUS_USERNAME").ok())
        .flatten();
    let user_id = attributable
        .then(|| std::env::var("SISYPHUS_USER_ID").ok())
        .flatten();
    AnyPoolOptions::new()
        .max_connections(1)
        .after_connect(move |connection, _| {
            let username = username.clone();
            let user_id = user_id.clone();
            Box::pin(async move {
                // Bind these values. Do not put them into the query text. The server sends
                // data, and not SQL.
                for (setting, value) in [("app.username", username), ("app.user_id", user_id)] {
                    let Some(value) = value else { continue };
                    sqlx::query("SELECT set_config($1, $2, false)")
                        .bind(setting)
                        .bind(value)
                        .execute(&mut *connection)
                        .await?;
                }
                Ok(())
            })
        })
        .connect(&database.database_url)
        .await
        .map_err(Into::into)
}

async fn object(command: ObjectCommands) -> Result<()> {
    match command {
        ObjectCommands::History { database, object } => {
            let pool = connect(&database).await?;
            report_history(&list_history(&(&object).into(), &pool).await?);
        }
        ObjectCommands::Pause {
            database,
            object,
            reason,
        } => {
            let pool = connect(&database).await?;
            let reference = ObjectRef::from(&object);
            pause_object(&reference, &reason, &pool).await?;
            println!("Paused {}", reference);
        }
        ObjectCommands::Push {
            database,
            image,
            monitor_directory,
            object,
            yes,
        } => {
            let pool = connect(&database).await?;
            let reference = ObjectRef::from(&object);
            push_one_object(&reference, &image, &monitor_directory, yes.yes, &pool).await?;
        }
        ObjectCommands::Resume { database, object } => {
            let pool = connect(&database).await?;
            let reference = ObjectRef::from(&object);
            resume_object(&reference, &pool).await?;
            println!("Resumed {}", reference);
        }
        ObjectCommands::Status { database, object } => {
            let pool = connect(&database).await?;
            report_status(&get_status(&(&object).into(), &pool).await?);
        }
    }
    Ok(())
}

async fn forget(key: KubernetesKey, yes: bool, pool: &AnyPool) -> Result<()> {
    let result = sqlx::query(
        r#"
        SELECT yaml
        FROM kubernetes_objects
        WHERE
          api_version = $1
          AND cluster = $2
          AND kind = $3
          AND name = $4
          AND namespace = $5
        "#,
    )
    .bind(key.api_version.clone())
    .bind(key.cluster.clone())
    .bind(key.kind.clone())
    .bind(key.name.clone())
    .bind(namespace_or_default(key.namespace.clone()))
    .fetch_all(pool)
    .await?;
    let Some(first) = result.iter().next() else {
        bail!("No such object")
    };
    let as_yaml: String = first.get("yaml");
    println!("• {} {}\n", style("forget").red(), key);
    print_diff(&as_yaml, "");
    println!("");

    if !ask_for_user_permission("forgetting", yes)? {
        return Ok(());
    }

    let result = sqlx::query(
        r#"
        DELETE FROM kubernetes_objects
        WHERE
          api_version = $1
          AND cluster = $2
          AND kind = $3
          AND name = $4
          AND namespace = $5
        "#,
    )
    .bind(key.api_version.clone())
    .bind(key.cluster.clone())
    .bind(key.kind.clone())
    .bind(key.name.clone())
    .bind(namespace_or_default(key.namespace.clone()))
    .execute(pool)
    .await?;
    if result.rows_affected() == 0 {
        bail!("Unable to find object {}", key);
    } else {
        println!("Forgot {}", key);
    }
    Ok(())
}

async fn import(key: KubernetesKey, yes: bool, pool: &AnyPool) -> Result<()> {
    let result = sqlx::query(
        r#"
        SELECT name
        FROM kubernetes_objects
        WHERE
          api_version = $1
          AND cluster = $2
          AND kind = $3
          AND name = $4
          AND namespace = $5
        "#,
    )
    .bind(key.api_version.clone())
    .bind(key.cluster.clone())
    .bind(key.kind.clone())
    .bind(key.name.clone())
    .bind(namespace_or_default(key.namespace.clone()))
    .fetch_all(pool)
    .await?;
    if result.len() > 0 {
        bail!("Object {} already exists", key);
    }

    let (clients, types) = get_kubernetes_clients([&key]).await?;
    let api = get_kubernetes_api(&key, &clients, &types)?;
    if let (Some(_), None) = (&key.namespace, api.namespace()) {
        bail!("Resource type {} is cluster scoped", key.kind);
    }
    let mut object = api.get(&key.name).await?;
    munge_secrets(None, &mut object)?;
    let as_yaml = serde_yaml::to_string(&object)?;
    println!("• {} {}\n", style("import").green(), key);
    print_diff("", &as_yaml);
    println!("");

    if !ask_for_user_permission("importing", yes)? {
        return Ok(());
    }

    object.metadata.managed_fields = None;
    let (clients, types) = get_kubernetes_clients([&key]).await?;
    let api = get_kubernetes_api(&key, &clients, &types)?;
    let result = api
        .patch(
            &key.name,
            &PatchParams::apply(MANAGER).force(),
            &Patch::Apply(object),
        )
        .await
        .with_context(|| format!("while imporing {}", key))?;

    sqlx::query(
        r#"
        INSERT INTO kubernetes_objects (api_version, cluster, kind, name, namespace, yaml)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(key.api_version.clone())
    .bind(key.cluster.clone())
    .bind(key.kind.clone())
    .bind(key.name.clone())
    .bind(namespace_or_default(key.namespace.clone()))
    .bind(serde_yaml::to_string(&result)?)
    .execute(pool)
    .await?;
    println!("Imported {}", key);

    Ok(())
}

/// The resources that a push can change.
#[derive(Clone, Copy)]
enum Scope<'a> {
    /// Each resource in the configuration files, but not a paused resource. This is the usual push.
    Everything(&'a BTreeMap<ObjectRef, ObjectPause>),
    /// One resource. Sisyphus renders it with a config image that the user gives, and not with the
    /// image from its tag. Sisyphus renders no other resource. Then no other resource can change,
    /// and no other registry must reply. This is the rollback.
    OneObject {
        image: &'a str,
        reference: &'a ObjectRef,
    },
}

async fn diff(
    filter: &PartialKey,
    monitor_directory: &str,
    scope: Scope<'_>,
    pool: &AnyPool,
) -> Result<Vec<ResourceDiff>> {
    let mut registries = RegistryClients::new();
    let mut from_files = KubernetesResources {
        by_key: BTreeMap::new(),
        namespaces: BTreeMap::new(),
    };
    // The objects that a paused resource rendered. Sisyphus must also ignore these objects on the
    // database side.
    let mut held = BTreeSet::new();
    {
        let resources = get_sisyphus_resources_from_files(Path::new(&monitor_directory))?;
        render_sisyphus_resources(
            &resources.global_by_key,
            /* allow_any_namespace= */ true,
            /* maybe_namespace= */ None,
            &mut from_files.by_key,
            &mut registries,
            scope,
            &mut held,
        )
        .await?;
        from_files.by_key.retain(|k, v| {
            if k.api_version == "v1" && k.kind == "Namespace" {
                from_files.namespaces.insert(k.clone(), v.clone());
                false
            } else {
                true
            }
        });
        for (namespace, objects) in resources.by_namespace_by_key {
            render_sisyphus_resources(
                &objects,
                /* allow_any_namespace= */ false,
                Some(namespace.to_string()),
                &mut from_files.by_key,
                &mut registries,
                scope,
                &mut held,
            )
            .await?;
        }

        for key in from_files.by_key.keys() {
            let Some(namespace) = key.namespace.clone() else {
                continue;
            };
            from_files
                .namespaces
                .entry(KubernetesKey {
                    name: namespace.clone(),
                    kind: "Namespace".to_string(),
                    api_version: "v1".to_string(),
                    namespace: None,
                    cluster: key.cluster.clone(),
                })
                .or_insert_with(|| {
                    let mut metadata = ObjectMeta::default();
                    metadata.name = Some(namespace);
                    let as_namespace = Namespace {
                        metadata,
                        spec: None,
                        status: None,
                    };
                    serde_yaml::from_str(&serde_yaml::to_string(&as_namespace).unwrap()).unwrap()
                });
        }
    }

    if let Scope::OneObject { reference, .. } = scope {
        if from_files.by_key.is_empty() {
            bail!(
                "No such resource: {}. `--namespace` is the directory the resource lives in, and \
                 `--kind` is the kind as written in its Sisyphus yaml.",
                reference
            );
        }
    }

    let mut from_database = get_objects_from_database(&pool).await?;
    match scope {
        // Sisyphus rendered the objects of the paused resource and then kept them back, and they
        // are not on the files side. Remove them from the database side also. If you do not, the
        // diff shows them as removed from the configuration, and the push deletes them.
        Scope::Everything(_) => drop_held(&mut from_database, &from_files, &held),
        // Sisyphus rendered one resource only, and each other object in the database shows as
        // deleted. Remove those objects, and then an `object push` can create and patch, but it
        // cannot delete. This is safe when you use the command during an incident.
        Scope::OneObject { .. } => {
            from_database
                .by_key
                .retain(|k, _| from_files.by_key.contains_key(k));
            from_database
                .namespaces
                .retain(|k, _| from_files.namespaces.contains_key(k));
        }
    }

    for (k, to) in &mut from_files.by_key {
        let from = from_database.by_key.get(&k);
        if let Some(f) = from {
            to.metadata.resource_version = f.metadata.resource_version.clone();
            to.metadata.uid = f.metadata.uid.clone();
        }
        munge_secrets(from, to)?;
    }

    from_files
        .by_key
        .retain(|k, _| key_matches_filter(k, filter));
    from_database
        .by_key
        .retain(|k, _| key_matches_filter(k, filter));

    // Keep the namespaces holding any resource we're pushing, even when the
    // filter (e.g. `--name`) doesn't match the Namespace object itself.
    // Otherwise a scoped push into a not-yet-created namespace drops the
    // namespace and the resource fails to create.
    let required_namespaces =
        required_namespace_identities(from_files.by_key.keys().chain(from_database.by_key.keys()));
    from_files
        .namespaces
        .retain(|k, _| namespace_key_retained(k, filter, &required_namespaces));
    from_database
        .namespaces
        .retain(|k, _| namespace_key_retained(k, filter, &required_namespaces));

    let (comparable_database, comparable_files) =
        make_comparable(from_database.clone(), from_files.clone())?;
    generate_diff(comparable_database, comparable_files)
}

async fn push(
    filter: &PartialKey,
    monitor_directory: &str,
    yes: bool,
    pool: &AnyPool,
) -> Result<()> {
    // Read the pauses one time here, and not for each object during the render. Then the render
    // code does not use the database.
    let pauses = load_pauses(pool).await?;
    let changed = diff(filter, monitor_directory, Scope::Everything(&pauses), pool).await?;
    report_changes(&changed);
    if changed.is_empty() {
        return Ok(())
    }
    if !ask_for_user_permission("pushing", yes)? {
        return Ok(());
    }
    apply_diff(changed, &pool).await?;
    Ok(())
}

/// Sets one object to a config image that the user gives.
///
/// This function does not change the pause. For a paused object, this is the second step of a
/// rollback, and the object stays paused. For an object that is not paused, the image applies only
/// until the next usual push resolves the tag again.
async fn push_one_object(
    reference: &ObjectRef,
    image: &str,
    monitor_directory: &str,
    yes: bool,
    pool: &AnyPool,
) -> Result<()> {
    let changed = diff(
        &PartialKey::default(),
        monitor_directory,
        Scope::OneObject { image, reference },
        pool,
    )
    .await?;
    report_changes(&changed);
    if changed.is_empty() {
        return Ok(());
    }
    if !ask_for_user_permission("pushing", yes)? {
        return Ok(());
    }
    apply_diff(changed, &pool).await?;
    Ok(())
}

async fn refresh(yes: bool, pool: &AnyPool) -> Result<()> {
    let from_database = get_objects_from_database(&pool).await?;
    let mut from_kubernetes = get_objects_from_kubernetes(&from_database).await?;
    for (k, to) in &mut from_kubernetes.by_key {
        munge_secrets(from_database.by_key.get(k), to)?;
    }
    let changed = generate_diff(from_database, from_kubernetes)?;
    report_changes(&changed);
    if changed.is_empty() {
        return Ok(());
    }

    if !ask_for_user_permission("refreshing", yes)? {
        return Ok(());
    }

    apply_refresh(changed, &pool).await?;
    Ok(())
}

/// Removes each object of a paused resource from the database side. Then the diff shows no change
/// for these objects.
fn drop_held(
    from_database: &mut KubernetesResources,
    from_files: &KubernetesResources,
    held: &BTreeSet<KubernetesKey>,
) {
    for key in held {
        from_database.by_key.remove(key);
        from_database.namespaces.remove(key);

        // Sisyphus deletes a namespace when no tracked object stays in it. If the only object in
        // a namespace is paused, that pause causes the deletion of the namespace. Keep the
        // namespace, unless an object that is not paused also needs it. Then let the usual diff
        // decide.
        let Some(namespace) = &key.namespace else {
            continue;
        };
        let namespace_key = KubernetesKey {
            name: namespace.clone(),
            kind: "Namespace".to_string(),
            api_version: "v1".to_string(),
            namespace: None,
            cluster: key.cluster.clone(),
        };
        if !from_files.namespaces.contains_key(&namespace_key) {
            from_database.namespaces.remove(&namespace_key);
        }
    }
}

fn report_changes(changed: &[ResourceDiff]) {
    if changed.is_empty() {
        println!("Nothing to do");
    } else {
        report_diffs(changed);
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct SisyphusKey {
    pub api_version: String,
    pub kind: String,
    pub name: String,
}

#[derive(Debug)]
struct SisyphusResources {
    by_namespace_by_key: HashMap<String, HashMap<SisyphusKey, SisyphusResource>>,
    global_by_key: HashMap<SisyphusKey, SisyphusResource>,
}

async fn apply_refresh(changed: Vec<ResourceDiff>, pool: &AnyPool) -> Result<()> {
    refresh_group(changed, &pool).await?;
    Ok(())
}

async fn refresh_group(changed: Vec<ResourceDiff>, pool: &AnyPool) -> Result<()> {
    for ResourceDiff { action, key, .. } in changed {
        let labels = ActionLabels::from(&action);
        match &action {
            DiffAction::Create(w)
            | DiffAction::Patch { after: w, .. }
            | DiffAction::Recreate(w) => {
                sqlx::query(
                    r#"
                    UPDATE kubernetes_objects
                    SET last_updated = CURRENT_TIMESTAMP, yaml = $1
                    WHERE
                        api_version = $2
                        AND cluster = $3
                        AND kind = $4
                        AND name = $5
                        AND namespace = $6
                    "#,
                )
                .bind(serde_yaml::to_string(&w)?)
                .bind(key.api_version.clone())
                .bind(key.cluster.clone())
                .bind(key.kind.clone())
                .bind(key.name.clone())
                .bind(namespace_or_default(key.namespace.clone()))
                .execute(pool)
                .await?;
            }
            DiffAction::Delete => {
                sqlx::query(
                    r#"
                    DELETE FROM kubernetes_objects
                    WHERE
                        api_version = $1
                        AND cluster = $2
                        AND kind = $3
                        AND name = $4
                        AND namespace = $5
                    "#,
                )
                .bind(key.api_version.clone())
                .bind(key.cluster.clone())
                .bind(key.kind.clone())
                .bind(key.name.clone())
                .bind(namespace_or_default(key.namespace.clone()))
                .execute(pool)
                .await?;
            }
        };
        report_applied(&key, labels);
    }
    Ok(())
}

async fn get_objects_from_database(pool: &AnyPool) -> Result<KubernetesResources> {
    let recs = sqlx::query(
        r#"SELECT api_version, cluster, kind, namespace, name, yaml FROM kubernetes_objects"#,
    )
    .fetch_all(pool)
    .await?;

    let mut resources = KubernetesResources {
        by_key: BTreeMap::new(),
        namespaces: BTreeMap::new(),
    };
    for rec in recs {
        //let created: DecodableOffsetDateTime = rec.get::<DecodableOffsetDateTime, &str>("created");
        //let last_updated: DecodableOffsetDateTime = rec.get("last_updated");
        let key = KubernetesKey {
            name: rec.get("name"),
            kind: rec.get("kind"),
            api_version: rec.get("api_version"),
            namespace: match rec.get("namespace") {
                "" => None,
                v => Some(v.to_string()),
            },
            cluster: rec.get("cluster"),
        };
        let object: DynamicObject = serde_yaml::from_str(rec.get("yaml"))
            .with_context(|| format!("Failed to parse stored yaml for {:?}", key))?;
        if key.api_version == "v1" && key.kind == "Namespace" {
            resources.namespaces.insert(key, object);
        } else {
            resources.by_key.insert(key, object);
        };
    }
    Ok(resources)
}

async fn get_objects_from_kubernetes(
    from_database: &KubernetesResources,
) -> Result<KubernetesResources> {
    let mut resources = KubernetesResources {
        by_key: BTreeMap::new(),
        namespaces: BTreeMap::new(),
    };
    let (clients, types) = get_kubernetes_clients(
        from_database
            .by_key
            .keys()
            .chain(from_database.namespaces.keys()),
    )
    .await?;
    let bar =
        ProgressBar::new((from_database.by_key.len() + from_database.namespaces.len()) as u64)
            .with_style(ProgressStyle::with_template(
            "Comparing resources... {wide_bar:.magenta/dim} {pos:>7}/{len:7} {elapsed}/{duration}",
        )?);
    for (source, destination) in [
        (&from_database.by_key, &mut resources.by_key),
        (&from_database.namespaces, &mut resources.namespaces),
    ] {
        for key in source.keys() {
            let api = get_kubernetes_api(key, &clients, &types)?;
            match api.get(&key.name).await {
                Ok(o) => {
                    destination.insert(key.clone(), o);
                }
                Err(Error::Api(ErrorResponse { code: 404, .. })) => { /* deletions are fine */ }
                Err(e) => bail!("Unable to fetch item, caused by: {:?}", e),
            };
            bar.inc(1);
        }
    }
    bar.finish();
    Ok(resources)
}

fn get_sisyphus_resources_from_files(directory: &Path) -> Result<SisyphusResources> {
    let mut resources = SisyphusResources {
        by_namespace_by_key: HashMap::new(),
        global_by_key: HashMap::new(),
    };
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.is_dir() {
            let (resources, allow_any_namespace, namespace) =
                match path.file_name().map(|s| s.to_str()).flatten() {
                    Some("global") => (&mut resources.global_by_key, true, None),
                    Some(namespace) => (
                        resources
                            .by_namespace_by_key
                            .entry(namespace.to_string())
                            .or_insert_with(|| HashMap::new()),
                        false,
                        Some(namespace.to_string()),
                    ),
                    None => bail!("Path has no filename"),
                };
            get_objects_from_namespace(&path, resources, allow_any_namespace, &namespace)?;
        }
    }
    Ok(resources)
}

fn get_objects_from_namespace(
    directory: &Path,
    resources: &mut HashMap<SisyphusKey, SisyphusResource>,
    allow_any_namespace: bool,
    namespace: &Option<String>,
) -> Result<()> {
    let index_path = directory.join("index.yaml");
    if !index_path.exists() {
        return Ok(());
    }
    get_objects_from_file(&index_path, resources, allow_any_namespace, &namespace)
}

fn get_objects_from_file(
    path: &Path,
    resources: &mut HashMap<SisyphusKey, SisyphusResource>,
    allow_any_namespace: bool,
    namespace: &Option<String>,
) -> Result<()> {
    let directory = path
        .parent()
        .ok_or_else(|| anyhow!("Expected to be in a child folder"))?;
    let reader = File::open(&path)?;
    for document in serde_yaml::Deserializer::from_reader(&reader) {
        let mut object: SisyphusResource = SisyphusResource::deserialize(document)
            .with_context(|| format!("in file {:?}", path))?;

        if let SisyphusResource::KubernetesYaml(v) = &mut object {
            let mut extra_objects = Vec::new();
            for source_path in &v.sources {
                load_objects_from_kubernetes_yaml(&directory.join(source_path), &mut extra_objects)
                    .with_context(|| {
                        format!("reading file {:?} referenced by {:?}", source_path, path)
                    })?;
            }
            v.sources.clear();
            v.objects.append(&mut extra_objects);

            for object in &mut v.objects {
                if let Some(namespace) = object.metadata.namespace.as_ref() {
                    if !allow_any_namespace {
                        let types = object
                            .types
                            .as_ref()
                            .map(|t| format!("{}/{}", t.api_version, t.kind))
                            .unwrap_or_else(|| "unknown".to_string());
                        bail!(
                            "{}/{} referenced by {} in {:?} should not specify namespace {:?}",
                            types,
                            object.name_any(),
                            v.metadata.name,
                            path,
                            namespace
                        );
                    }
                } else {
                    object.metadata.namespace = namespace.clone();
                }
            }
            insert_sisyphus_resource(object, resources)?;
        } else if let SisyphusResource::SisyphusYaml(v) = &mut object {
            for source_path in &v.sources {
                get_objects_from_file(
                    &directory.join(source_path),
                    resources,
                    allow_any_namespace,
                    namespace,
                )?;
            }
        } else {
            insert_sisyphus_resource(object, resources)?;
        }
    }

    Ok(())
}

fn load_objects_from_kubernetes_yaml(path: &Path, into: &mut Vec<DynamicObject>) -> Result<()> {
    let reader = File::open(&path)?;
    for document in serde_yaml::Deserializer::from_reader(&reader) {
        let object: DynamicObject = DynamicObject::deserialize(document)?;
        if object.types.is_none() && object.metadata == ObjectMeta::default() {
            // kubectl tolerates these, so we do too
            continue;
        }
        into.push(object);
    }

    Ok(())
}

fn insert_sisyphus_resource(
    object: SisyphusResource,
    resources: &mut HashMap<SisyphusKey, SisyphusResource>,
) -> Result<()> {
    let (api_version, kind, name) = match &object {
        SisyphusResource::KubernetesYaml(v) => (&v.api_version, v.kind(), &v.metadata.name),
        SisyphusResource::SisyphusCronJob(v) => (&v.api_version, v.kind(), &v.metadata.name),
        SisyphusResource::SisyphusDeployment(v) => (&v.api_version, v.kind(), &v.metadata.name),
        SisyphusResource::SisyphusYaml(_) => unreachable!("These should already have been loaded"),
    };
    let key = SisyphusKey {
        api_version: api_version.clone(),
        kind: kind.to_string(),
        name: name.clone(),
    };
    if resources.contains_key(&key) {
        bail!("Key {:?} already exists", key);
    }
    resources.insert(key, object);
    Ok(())
}

async fn render_sisyphus_resources(
    objects: &HashMap<SisyphusKey, SisyphusResource>,
    allow_any_namespace: bool,
    maybe_namespace: Option<String>,
    by_key: &mut BTreeMap<KubernetesKey, DynamicObject>,
    registries: &mut RegistryClients,
    scope: Scope<'_>,
    held: &mut BTreeSet<KubernetesKey>,
) -> Result<()> {
    for (key, object) in objects {
        // The address of the object on the command line: the kind as its yaml writes it, and the
        // directory that contains it. A resource in `global` has no namespace of its own, and the
        // name of that directory is its address.
        let reference = ObjectRef::new(
            match object {
                SisyphusResource::KubernetesYaml(_) => "KubernetesYaml",
                SisyphusResource::SisyphusCronJob(_) => "CronJob",
                SisyphusResource::SisyphusDeployment(_) => "Deployment",
                SisyphusResource::SisyphusYaml(_) => {
                    unreachable!("These should already have been loaded")
                }
            },
            maybe_namespace.as_deref().unwrap_or(GLOBAL_NAMESPACE),
            &key.name,
        );

        // An object push ignores each other resource. A render of a resource gets its config image
        // from a registry. If Sisyphus rendered all the resources, a rollback would then need each
        // registry to reply and each other resource to render correctly.
        let config_image = match scope {
            Scope::Everything(_) => None,
            Scope::OneObject { image, reference: target } => {
                if &reference != target {
                    continue;
                }
                Some(image)
            }
        };

        let mut copy = object.clone();
        match &mut copy {
            SisyphusResource::SisyphusCronJob(v) => {
                resolve_sisyphus_config_image(v, config_image, registries).await?
            }
            SisyphusResource::SisyphusDeployment(v) => {
                resolve_sisyphus_config_image(v, config_image, registries).await?
            }
            SisyphusResource::KubernetesYaml(_) | SisyphusResource::SisyphusYaml(_) => {
                if config_image.is_some() {
                    bail!(
                        "{} renders no config image, so there's no version for `object push` to \
                         put back. Edit its yaml and push normally.",
                        reference
                    );
                }
            }
        };

        // Render into a new map, and then a pause can hold the objects of this resource only. A
        // Deployment also renders a Service, and raw yaml renders the objects in its manifests.
        // The list is not known before the render.
        let mut rendered = BTreeMap::new();
        render_sisyphus_resource(
            &copy,
            allow_any_namespace,
            &maybe_namespace,
            &mut rendered,
            registries,
        )
        .await
        .with_context(|| match paused_by(scope, &reference) {
            // Without this text, the error shows a paused object that stopped a push of other
            // objects.
            Some(_) => format!(
                "while rendering {}, which is paused. A pause still renders, because that's how we \
                 learn which objects it covers",
                reference
            ),
            None => format!("while rendering {:?}", key),
        })?;

        match paused_by(scope, &reference) {
            Some(pause) => {
                let keys: Vec<_> = rendered.into_keys().collect();
                report_paused(pause, &keys);
                held.extend(keys);
            }
            None => by_key.extend(rendered),
        }
    }
    Ok(())
}

/// The pause that keeps this resource out of the push, if a pause exists. An object push applies
/// to one resource that the user selected, and it holds back no object.
fn paused_by<'a>(scope: Scope<'a>, reference: &ObjectRef) -> Option<&'a ObjectPause> {
    match scope {
        Scope::Everything(pauses) => pauses.get(reference),
        Scope::OneObject { .. } => None,
    }
}

/// Resolves the config image tag. If the caller gives an image, this function uses that image.
async fn resolve_sisyphus_config_image(
    object: &mut impl HasConfigImage,
    config_image: Option<&str>,
    registries: &mut RegistryClients,
) -> Result<()> {
    let image = match config_image {
        Some(image) => image.to_string(),
        None => resolve_image_tag(object.config_image(), registries)
            .await?
            .to_string(),
    };
    object.set_config_image(image);
    Ok(())
}

fn ask_for_user_permission(verb: &str, yes: bool) -> Result<bool> {
    if yes {
        return Ok(true);
    }

    // The prompt is a message to the user, and not output data. On stdout it would go into the
    // JSON stream that a `--output json` caller reads.
    eprint!("Continue {}? y/(n): ", verb);
    std::io::stderr().flush()?;
    let mut response = String::new();
    std::io::stdin().read_line(&mut response)?;
    Ok(match response.trim().to_lowercase().as_str() {
        "y" => true,
        _ => {
            eprintln!("Canceled");
            false
        }
    })
}
