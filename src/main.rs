mod app_run_config;
mod app_run_image;
mod apply_diff;
mod config_image;
mod filter;
mod generate_diff;
mod kubernetes_io;
mod kubernetes_rendering;
mod registry_clients;
mod sisyphus_yaml;
mod starlark;

use crate::{
    app_run_config::{run_config, RunConfigArgs},
    app_run_image::{run_image, RunImageArgs},
    apply_diff::{apply_diff, namespace_or_default},
    filter::{key_matches_filter, PartialKey},
    generate_diff::{generate_diff, print_diff, DiffAction},
    kubernetes_io::{
        get_kubernetes_api, get_kubernetes_clients, make_comparable, munge_secrets, KubernetesKey,
        KubernetesResources, MANAGER,
    },
    kubernetes_rendering::render_sisyphus_resource,
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
use similar::TextDiff;
use sqlx::{AnyPool, Row};
use std::{
    collections::{BTreeMap, HashMap},
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
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,

        #[command(flatten)]
        key: FullKey,
    },
    Import {
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,

        #[command(flatten)]
        key: FullKey,
    },
    Push {
        #[command(flatten)]
        args: PushArgs,
    },
    Refresh {
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
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
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    // The filters to consider
    #[command(flatten)]
    filter: PartialKey,

    // The namespace to label resources with
    #[arg(long, env = "LABEL_NAMESPACE", default_value = "april.dev")]
    label_namespace: String,

    // The path to the directory of configuration files to monitor
    #[arg(long, env = "MONITOR_DIRECTORY")]
    monitor_directory: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();
    sqlx::any::install_default_drivers();

    let args = SisyphusArgs::parse();
    match args.command {
        Commands::App { app_command } => match app_command {
            AppCommands::RunConfig { args } => run_config(args).await?,
            AppCommands::RunImage { args } => run_image(args).await?,
        },
        Commands::Diff {
            args: PushArgs {
                database_url,
                filter,
                label_namespace,
                monitor_directory,
            }
        } => {
            let pool = AnyPool::connect(&database_url).await?;
            diff(&filter, &label_namespace, &monitor_directory, &pool).await?;
        }
        Commands::Forget { database_url, key } => {
            let pool = AnyPool::connect(&database_url).await?;
            forget(key.into(), &pool).await?
        }
        Commands::Import { database_url, key } => {
            let pool = AnyPool::connect(&database_url).await?;
            import(key.into(), &pool).await?
        }
        Commands::Push {
            args: PushArgs {
                database_url,
                filter,
                label_namespace,
                monitor_directory,
            }
        } => {
            let pool = AnyPool::connect(&database_url).await?;
            push(&filter, &label_namespace, &monitor_directory, &pool).await?
        }
        Commands::Refresh { database_url } => {
            let pool = AnyPool::connect(&database_url).await?;
            refresh(&pool).await?
        }
    };
    Ok(())
}

async fn forget(key: KubernetesKey, pool: &AnyPool) -> Result<()> {
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
    let diff = TextDiff::from_lines(as_yaml.as_str(), "");
    println!("• {} {}\n", style("forget").red(), key);
    print_diff(&diff);
    println!("");

    if !ask_for_user_permission("forgetting")? {
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

async fn import(key: KubernetesKey, pool: &AnyPool) -> Result<()> {
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
    let diff = TextDiff::from_lines("", &as_yaml);
    println!("• {} {}\n", style("import").green(), key);
    print_diff(&diff);
    println!("");

    if !ask_for_user_permission("importing")? {
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

async fn diff(
    filter: &PartialKey,
    label_namespace: &str,
    monitor_directory: &str,
    pool: &AnyPool,
) -> Result<Vec<(KubernetesKey, DiffAction)>> {
    let mut registries = RegistryClients::new();
    let mut from_files = KubernetesResources {
        by_key: BTreeMap::new(),
        namespaces: BTreeMap::new(),
    };
    {
        let resources = get_sisyphus_resources_from_files(Path::new(&monitor_directory))?;
        render_sisyphus_resources(
            &resources.global_by_key,
            /* allow_any_namespace= */ true,
            label_namespace,
            /* maybe_namespace= */ None,
            &mut from_files.by_key,
            &mut registries,
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
                &label_namespace,
                Some(namespace.to_string()),
                &mut from_files.by_key,
                &mut registries,
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

    let mut from_database = get_objects_from_database(&pool).await?;
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
    from_files
        .namespaces
        .retain(|k, _| key_matches_filter(k, filter));
    from_database
        .by_key
        .retain(|k, _| key_matches_filter(k, filter));
    from_database
        .namespaces
        .retain(|k, _| key_matches_filter(k, filter));

    let (comparable_database, comparable_files) =
        make_comparable(from_database.clone(), from_files.clone())?;
    let changed = generate_diff(comparable_database, comparable_files)?;
    if changed.len() == 0 {
        println!("Nothing to do");
    }
    Ok(changed)
}

async fn push(
    filter: &PartialKey,
    label_namespace: &str,
    monitor_directory: &str,
    pool: &AnyPool,
) -> Result<()> {
    let changed = diff(filter, label_namespace, monitor_directory, pool).await?;
    if changed.len() == 0 {
        return Ok(())
    }
    if !ask_for_user_permission("pushing")? {
        return Ok(());
    }
    apply_diff(changed, &pool).await?;
    Ok(())
}

async fn refresh(pool: &AnyPool) -> Result<()> {
    let from_database = get_objects_from_database(&pool).await?;
    let mut from_kubernetes = get_objects_from_kubernetes(&from_database).await?;
    for (k, to) in &mut from_kubernetes.by_key {
        munge_secrets(from_database.by_key.get(k), to)?;
    }
    let changed = generate_diff(from_database, from_kubernetes)?;
    if changed.len() == 0 {
        println!("Nothing to do");
        return Ok(());
    }

    if !ask_for_user_permission("refreshing")? {
        return Ok(());
    }

    apply_refresh(changed, &pool).await?;
    Ok(())
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

async fn apply_refresh(changed: Vec<(KubernetesKey, DiffAction)>, pool: &AnyPool) -> Result<()> {
    refresh_group(changed, &pool).await?;
    Ok(())
}

async fn refresh_group(changed: Vec<(KubernetesKey, DiffAction)>, pool: &AnyPool) -> Result<()> {
    for (key, action) in changed {
        match action {
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
                println!("Updated {}", key);
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
                println!("Deleted {}", key);
            }
        };
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
        let object: DynamicObject = serde_yaml::from_str(rec.get("yaml"))?;
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
    label_namespace: &str,
    maybe_namespace: Option<String>,
    by_key: &mut BTreeMap<KubernetesKey, DynamicObject>,
    registries: &mut RegistryClients,
) -> Result<()> {
    for (key, object) in objects {
        let mut copy = object.clone();
        match &mut copy {
            SisyphusResource::KubernetesYaml(_) => {}
            SisyphusResource::SisyphusCronJob(v) => {
                resolve_sisyphus_config_image(v, registries).await?
            }
            SisyphusResource::SisyphusDeployment(v) => {
                resolve_sisyphus_config_image(v, registries).await?
            }
            SisyphusResource::SisyphusYaml(_) => {}
        };

        render_sisyphus_resource(
            &copy,
            allow_any_namespace,
            label_namespace,
            &maybe_namespace,
            by_key,
            registries,
        )
        .await
        .with_context(|| format!("while rendering {:?}", key))?;
    }
    Ok(())
}

async fn resolve_sisyphus_config_image(
    object: &mut impl HasConfigImage,
    registries: &mut RegistryClients,
) -> Result<()> {
    let reference = resolve_image_tag(object.config_image(), registries).await?;
    object.set_config_image(reference.to_string());
    Ok(())
}

fn ask_for_user_permission(verb: &str) -> Result<bool> {
    print!("Continue {}? y/(n): ", verb);
    std::io::stdout().flush()?;
    let mut response = String::new();
    std::io::stdin().read_line(&mut response)?;
    Ok(match response.trim().to_lowercase().as_str() {
        "y" => true,
        _ => {
            println!("Canceled");
            false
        }
    })
}
