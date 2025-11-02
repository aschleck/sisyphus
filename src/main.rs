mod config_image;
mod kubernetes;
mod registry_clients;
mod rendering;
mod sisyphus_yaml;

use crate::{
    kubernetes::{
        get_kubernetes_api, get_kubernetes_clients, make_comparable, munge_secrets, KubernetesKey,
        KubernetesResources, MANAGER,
    },
    registry_clients::RegistryClients,
    rendering::render_sisyphus_resource,
    sisyphus_yaml::{HasKind, SisyphusDeployment, SisyphusResource},
};
use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use console::{style, Style};
use docker_registry::reference::{Reference as RegistryReference, Version as RegistryVersion};
use indicatif::{ProgressBar, ProgressStyle};
use k8s_openapi::api::core::v1::Namespace;
use kube::{
    api::{DeleteParams, DynamicObject, ObjectMeta, Patch, PatchParams},
    core::ErrorResponse,
    Error, ResourceExt,
};
use serde::Deserialize;
use similar::{ChangeTag, TextDiff};
use sqlx::{AnyPool, Row};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::{self, File},
    io::Write,
    path::Path,
    str::FromStr,
};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct SisyphusArgs {
    #[command(subcommand)]
    command: Commands,

    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
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
struct PartialKey {
    #[arg(long)]
    api_version: Option<String>,

    #[arg(long)]
    cluster: Option<String>,

    #[arg(long)]
    kind: Option<String>,

    #[arg(long)]
    name: Option<String>,

    #[arg(long)]
    namespace: Option<String>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Forget {
        #[command(flatten)]
        key: FullKey,
    },
    Import {
        #[command(flatten)]
        key: FullKey,
    },
    Push {
        // The filters to consider
        #[command(flatten)]
        filter: PartialKey,

        // The namespace to label resources with
        #[arg(long, env = "LABEL_NAMESPACE", default_value = "april.dev")]
        label_namespace: String,

        // The path to the directory of configuration files to monitor
        #[arg(long, env = "MONITOR_DIRECTORY")]
        monitor_directory: String,
    },
    Refresh,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();
    sqlx::any::install_default_drivers();
    let args = SisyphusArgs::parse();
    let pool = AnyPool::connect(&args.database_url).await?;

    match args.command {
        Commands::Forget { key } => forget(key.into(), &pool).await?,
        Commands::Import { key } => import(key.into(), &pool).await?,
        Commands::Push {
            filter,
            label_namespace,
            monitor_directory,
        } => push(&filter, &label_namespace, &monitor_directory, &pool).await?,
        Commands::Refresh => refresh(&pool).await?,
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

async fn push(
    filter: &PartialKey,
    label_namespace: &str,
    monitor_directory: &str,
    pool: &AnyPool,
) -> Result<()> {
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
        return Ok(());
    }

    if !ask_for_user_permission("pushing")? {
        return Ok(());
    }

    apply_diff(changed, &from_database, &from_files, &pool).await?;
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

fn generate_diff(
    mut have: KubernetesResources,
    want: KubernetesResources,
) -> Result<Vec<(KubernetesKey, DiffAction)>> {
    let mut changed = Vec::new();
    let mut after = HashSet::new();
    for (key, w) in want.namespaces {
        let h = have.namespaces.remove(&key);
        if h.as_ref() == Some(&w) {
            continue;
        }
        changed.push((key.clone(), generate_single_diff(&key, h, Some(w))?));
        after.insert(key);
    }

    for (key, w) in want.by_key {
        let h = have.by_key.remove(&key);
        if h.as_ref() == Some(&w) {
            continue;
        }
        changed.push((key.clone(), generate_single_diff(&key, h, Some(w))?));
        after.insert(key);
    }

    for (key, h) in have.by_key {
        if !after.contains(&key) {
            changed.push((key.clone(), generate_single_diff(&key, Some(h), None)?));
        }
    }

    for (key, h) in have.namespaces {
        if !after.contains(&key) {
            changed.push((key.clone(), generate_single_diff(&key, Some(h), None)?));
        }
    }

    Ok(changed)
}

async fn apply_diff(
    changed: Vec<(KubernetesKey, DiffAction)>,
    have: &KubernetesResources,
    want: &KubernetesResources,
    pool: &AnyPool,
) -> Result<()> {
    let (clients, types) =
        get_kubernetes_clients(have.by_key.keys().chain(want.by_key.keys())).await?;
    for (key, action) in changed {
        let api = get_kubernetes_api(&key, &clients, &types)?;
        apply_single_diff(action, &key, &api, pool).await?;
    }
    Ok(())
}

async fn apply_single_diff(
    action: DiffAction,
    key: &KubernetesKey,
    api: &kube::Api<DynamicObject>,
    pool: &AnyPool,
) -> Result<()> {
    match action {
        DiffAction::Create(v) => {
            let result = api
                .patch(
                    &key.name,
                    &PatchParams::apply(MANAGER).force(),
                    &Patch::Apply(v),
                )
                .await
                .with_context(|| format!("while creating {}", key))?;
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
            println!("Created {}", key);
        }
        DiffAction::Delete => {
            api.delete(&key.name, &DeleteParams::default())
                .await
                .with_context(|| format!("while deleting {}", key))?;
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
        DiffAction::Patch { patch, .. } => {
            let result = api
                .patch(
                    &key.name,
                    &PatchParams::apply(MANAGER),
                    &Patch::<()>::Json(patch),
                )
                .await
                .with_context(|| format!("while updating {}", key))?;
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
            .bind(serde_yaml::to_string(&result)?)
            .bind(key.api_version.clone())
            .bind(key.cluster.clone())
            .bind(key.kind.clone())
            .bind(key.name.clone())
            .bind(namespace_or_default(key.namespace.clone()))
            .execute(pool)
            .await?;
            println!("Updated {}", key);
        }
        DiffAction::Recreate(v) => {
            api.delete(&key.name, &DeleteParams::default())
                .await
                .with_context(|| format!("while replacing {}", key))?;
            println!("Deleting prior to recreate {}", key);
            let result = api
                .patch(
                    &key.name,
                    &PatchParams::apply(MANAGER).force(),
                    &Patch::Apply(v),
                )
                .await
                .with_context(|| format!("while replacing {}", key))?;
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
            .bind(serde_yaml::to_string(&result)?)
            .bind(key.api_version.clone())
            .bind(key.cluster.clone())
            .bind(key.kind.clone())
            .bind(key.name.clone())
            .bind(namespace_or_default(key.namespace.clone()))
            .execute(pool)
            .await?;
            println!("Recreated {}", key);
        }
    }
    Ok(())
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
            SisyphusResource::SisyphusDeployment(v) => {
                resolve_sisyphus_deployment_image(v, registries).await?;
            }
            _ => {}
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

async fn resolve_sisyphus_deployment_image(
    object: &mut SisyphusDeployment,
    registries: &mut RegistryClients,
) -> Result<()> {
    let (image, registry) = registries
        .get_reference_and_registry(&object.config.image)
        .await?;
    let manifest = registry
        .get_manifest(image.repository().as_ref(), image.version().as_ref())
        .await
        .with_context(|| format!("while resolving {}", object.config.image))?;
    let digests = manifest.layers_digests(None)?;
    object.config.image = RegistryReference::new(
        Some(image.registry()),
        image.repository(),
        Some(RegistryVersion::from_str(
            format!("@{}", digests[0]).as_ref(),
        )?),
    )
    .to_string();
    Ok(())
}

enum DiffAction {
    Delete,
    Create(DynamicObject),
    Recreate(DynamicObject),
    Patch {
        after: DynamicObject,
        patch: json_patch::Patch,
    },
}

fn generate_single_diff<'a>(
    key: &KubernetesKey,
    have: Option<DynamicObject>,
    want: Option<DynamicObject>,
) -> Result<DiffAction> {
    let hs = if let Some(h) = &have {
        serde_yaml::to_string(&h)?
    } else {
        "".to_string()
    };
    let ws = if let Some(w) = &want {
        serde_yaml::to_string(&w)?
    } else {
        "".to_string()
    };
    let action = match (have, want) {
        (Some(h), Some(mut w)) => {
            let patch = json_patch::diff(&serde_json::to_value(&h)?, &serde_json::to_value(&w)?);
            let types = w.types.as_ref().ok_or_else(|| anyhow!("Expected types"))?;
            let action = match (types.api_version.as_str(), types.kind.as_str()) {
                ("apps/v1", "Deployment") => {
                    let mut recreate = false;
                    for modification in &patch.0 {
                        match modification {
                            json_patch::PatchOperation::Add(o) => {
                                let path = o.path.to_string();
                                if path.starts_with("/spec/selector/") {
                                    recreate = true;
                                }
                            }
                            json_patch::PatchOperation::Remove(o) => {
                                let path = o.path.to_string();
                                if path.starts_with("/spec/selector/") {
                                    recreate = true;
                                }
                            }
                            json_patch::PatchOperation::Replace(o) => {
                                let path = o.path.to_string();
                                if path.starts_with("/spec/selector/") {
                                    recreate = true;
                                }
                            }
                            _ => {}
                        }
                    }
                    match recreate {
                        true => {
                            w.metadata.resource_version = None;
                            w.metadata.uid = None;
                            DiffAction::Recreate(w)
                        }
                        false => DiffAction::Patch { after: w, patch },
                    }
                }
                _ => DiffAction::Patch { after: w, patch },
            };

            action
        }
        (Some(_), None) => DiffAction::Delete,
        (None, Some(w)) => DiffAction::Create(w),
        (None, None) => bail!("Expected a difference"),
    };

    let verb = match &action {
        DiffAction::Create(_) => style("create").green(),
        DiffAction::Delete => style("delete").red(),
        DiffAction::Patch { .. } => style("patch").yellow(),
        DiffAction::Recreate(_) => style("delete and recreate").red(),
    };

    let diff = TextDiff::from_lines(&hs, &ws);
    println!("• {} {}\n", verb, key);
    print_diff(&diff);
    println!("");
    Ok(action)
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

fn key_matches_filter(key: &KubernetesKey, filter: &PartialKey) -> bool {
    if let Some(v) = &filter.api_version {
        if &key.api_version != v {
            return false;
        }
    }
    if let Some(v) = &filter.cluster {
        if &key.cluster != v {
            return false;
        }
    }
    if let Some(v) = &filter.kind {
        if &key.kind != v {
            return false;
        }
    }
    if let Some(v) = &filter.name {
        if &key.name != v {
            return false;
        }
    }
    if filter.namespace.is_some() {
        if key.namespace != filter.namespace {
            return false;
        }
    }
    true
}

fn print_diff<'a>(diff: &TextDiff<'a, 'a, 'a, str>) -> () {
    for change in diff.iter_all_changes() {
        let (sign, style) = match change.tag() {
            ChangeTag::Delete => ("-", Style::new().red()),
            ChangeTag::Insert => ("+", Style::new().green()),
            ChangeTag::Equal => (" ", Style::new()),
        };
        print!("{}{}", style.apply_to(sign).bold(), style.apply_to(change));
    }
}

fn namespace_or_default(namespace: Option<String>) -> String {
    namespace.unwrap_or_else(|| "".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::api::TypeMeta;
    use serde_json::json;

    // Tests for key_matches_filter
    #[test]
    fn test_key_matches_filter_empty_filter() {
        let key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "Pod".to_string(),
            name: "my-pod".to_string(),
            namespace: Some("default".to_string()),
        };
        let filter = PartialKey {
            api_version: None,
            cluster: None,
            kind: None,
            name: None,
            namespace: None,
        };

        assert!(key_matches_filter(&key, &filter));
    }

    #[test]
    fn test_key_matches_filter_api_version_mismatch() {
        let key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "Pod".to_string(),
            name: "my-pod".to_string(),
            namespace: Some("default".to_string()),
        };
        let filter = PartialKey {
            api_version: Some("apps/v1".to_string()),
            cluster: None,
            kind: None,
            name: None,
            namespace: None,
        };

        assert!(!key_matches_filter(&key, &filter));
    }

    #[test]
    fn test_key_matches_filter_cluster_mismatch() {
        let key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "Pod".to_string(),
            name: "my-pod".to_string(),
            namespace: Some("default".to_string()),
        };
        let filter = PartialKey {
            api_version: None,
            cluster: Some("dev".to_string()),
            kind: None,
            name: None,
            namespace: None,
        };

        assert!(!key_matches_filter(&key, &filter));
    }

    #[test]
    fn test_key_matches_filter_kind_mismatch() {
        let key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "Pod".to_string(),
            name: "my-pod".to_string(),
            namespace: Some("default".to_string()),
        };
        let filter = PartialKey {
            api_version: None,
            cluster: None,
            kind: Some("Deployment".to_string()),
            name: None,
            namespace: None,
        };

        assert!(!key_matches_filter(&key, &filter));
    }

    #[test]
    fn test_key_matches_filter_name_mismatch() {
        let key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "Pod".to_string(),
            name: "my-pod".to_string(),
            namespace: Some("default".to_string()),
        };
        let filter = PartialKey {
            api_version: None,
            cluster: None,
            kind: None,
            name: Some("other-pod".to_string()),
            namespace: None,
        };

        assert!(!key_matches_filter(&key, &filter));
    }

    #[test]
    fn test_key_matches_filter_namespace_mismatch() {
        let key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "Pod".to_string(),
            name: "my-pod".to_string(),
            namespace: Some("default".to_string()),
        };
        let filter = PartialKey {
            api_version: None,
            cluster: None,
            kind: None,
            name: None,
            namespace: Some("production".to_string()),
        };

        assert!(!key_matches_filter(&key, &filter));
    }

    #[test]
    fn test_key_matches_filter_partial_match() {
        let key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "Pod".to_string(),
            name: "my-pod".to_string(),
            namespace: Some("default".to_string()),
        };
        let filter = PartialKey {
            api_version: Some("v1".to_string()),
            cluster: Some("prod".to_string()),
            kind: None,
            name: None,
            namespace: None,
        };

        assert!(key_matches_filter(&key, &filter));
    }

    #[test]
    fn test_key_matches_filter_none_namespace_key() {
        let key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "Namespace".to_string(),
            name: "default".to_string(),
            namespace: None,
        };
        let filter = PartialKey {
            api_version: None,
            cluster: None,
            kind: None,
            name: None,
            namespace: None,
        };

        assert!(key_matches_filter(&key, &filter));
    }

    #[test]
    fn test_generate_diff_no_changes() -> Result<()> {
        let key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "ConfigMap".to_string(),
            name: "my-config".to_string(),
            namespace: Some("default".to_string()),
        };

        let object = DynamicObject {
            types: Some(TypeMeta {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
            }),
            metadata: ObjectMeta::default(),
            data: json!({"key": "value"}),
        };

        let have = KubernetesResources {
            by_key: BTreeMap::from([(key.clone(), object.clone())]),
            namespaces: BTreeMap::new(),
        };
        let want = KubernetesResources {
            by_key: BTreeMap::from([(key, object)]),
            namespaces: BTreeMap::new(),
        };
        let diff = generate_diff(have, want)?;

        assert_eq!(diff.len(), 0);

        Ok(())
    }

    #[test]
    fn test_generate_diff_create_object() -> Result<()> {
        let key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "ConfigMap".to_string(),
            name: "new-config".to_string(),
            namespace: Some("default".to_string()),
        };

        let object = DynamicObject {
            types: Some(TypeMeta {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
            }),
            metadata: ObjectMeta::default(),
            data: json!({"key": "value"}),
        };

        let have = KubernetesResources {
            by_key: BTreeMap::new(),
            namespaces: BTreeMap::new(),
        };
        let want = KubernetesResources {
            by_key: BTreeMap::from([(key.clone(), object)]),
            namespaces: BTreeMap::new(),
        };
        let diff = generate_diff(have, want)?;

        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].0, key);
        assert!(matches!(diff[0].1, DiffAction::Create(_)));

        Ok(())
    }

    #[test]
    fn test_generate_diff_delete_object() -> Result<()> {
        let key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "ConfigMap".to_string(),
            name: "old-config".to_string(),
            namespace: Some("default".to_string()),
        };

        let object = DynamicObject {
            types: Some(TypeMeta {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
            }),
            metadata: ObjectMeta::default(),
            data: json!({"key": "value"}),
        };

        let have = KubernetesResources {
            by_key: BTreeMap::from([(key.clone(), object)]),
            namespaces: BTreeMap::new(),
        };
        let want = KubernetesResources {
            by_key: BTreeMap::new(),
            namespaces: BTreeMap::new(),
        };
        let diff = generate_diff(have, want)?;

        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].0, key);
        assert!(matches!(diff[0].1, DiffAction::Delete));

        Ok(())
    }

    #[test]
    fn test_generate_diff_update_object() -> Result<()> {
        let key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "ConfigMap".to_string(),
            name: "my-config".to_string(),
            namespace: Some("default".to_string()),
        };

        let old_object = DynamicObject {
            types: Some(TypeMeta {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
            }),
            metadata: ObjectMeta::default(),
            data: json!({"key": "old-value"}),
        };

        let new_object = DynamicObject {
            types: Some(TypeMeta {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
            }),
            metadata: ObjectMeta::default(),
            data: json!({"key": "new-value"}),
        };

        let have = KubernetesResources {
            by_key: BTreeMap::from([(key.clone(), old_object)]),
            namespaces: BTreeMap::new(),
        };
        let want = KubernetesResources {
            by_key: BTreeMap::from([(key.clone(), new_object)]),
            namespaces: BTreeMap::new(),
        };
        let diff = generate_diff(have, want)?;

        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].0, key);
        assert!(matches!(diff[0].1, DiffAction::Patch { .. }));

        Ok(())
    }

    #[test]
    fn test_generate_diff_mixed_operations() -> Result<()> {
        // Key for object to keep (no change)
        let keep_key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "ConfigMap".to_string(),
            name: "keep-config".to_string(),
            namespace: Some("default".to_string()),
        };

        // Key for object to delete
        let delete_key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "ConfigMap".to_string(),
            name: "delete-config".to_string(),
            namespace: Some("default".to_string()),
        };

        // Key for object to create
        let create_key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "ConfigMap".to_string(),
            name: "create-config".to_string(),
            namespace: Some("default".to_string()),
        };

        // Key for object to update
        let update_key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "ConfigMap".to_string(),
            name: "update-config".to_string(),
            namespace: Some("default".to_string()),
        };

        let keep_object = DynamicObject {
            types: Some(TypeMeta {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
            }),
            metadata: ObjectMeta::default(),
            data: json!({"key": "value"}),
        };

        let delete_object = DynamicObject {
            types: Some(TypeMeta {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
            }),
            metadata: ObjectMeta::default(),
            data: json!({"key": "delete-me"}),
        };

        let update_object_old = DynamicObject {
            types: Some(TypeMeta {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
            }),
            metadata: ObjectMeta::default(),
            data: json!({"key": "old"}),
        };

        let update_object_new = DynamicObject {
            types: Some(TypeMeta {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
            }),
            metadata: ObjectMeta::default(),
            data: json!({"key": "new"}),
        };

        let create_object = DynamicObject {
            types: Some(TypeMeta {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
            }),
            metadata: ObjectMeta::default(),
            data: json!({"key": "created"}),
        };

        let mut have = KubernetesResources {
            by_key: BTreeMap::new(),
            namespaces: BTreeMap::new(),
        };
        have.by_key.insert(keep_key.clone(), keep_object.clone());
        have.by_key.insert(delete_key.clone(), delete_object);
        have.by_key.insert(update_key.clone(), update_object_old);

        let mut want = KubernetesResources {
            by_key: BTreeMap::new(),
            namespaces: BTreeMap::new(),
        };
        want.by_key.insert(keep_key, keep_object);
        want.by_key.insert(create_key.clone(), create_object);
        want.by_key.insert(update_key.clone(), update_object_new);

        let diff = generate_diff(have, want)?;

        // Should have 3 changes: create, delete, update (keep is not in diff)
        assert_eq!(diff.len(), 3);

        // Verify we have one of each action type
        let mut has_create = false;
        let mut has_delete = false;
        let mut has_update = false;

        for (key, action) in &diff {
            match action {
                DiffAction::Create(_) => {
                    assert_eq!(key, &create_key);
                    has_create = true;
                }
                DiffAction::Delete => {
                    assert_eq!(key, &delete_key);
                    has_delete = true;
                }
                DiffAction::Patch { .. } => {
                    assert_eq!(key, &update_key);
                    has_update = true;
                }
                _ => {}
            }
        }

        assert!(has_create);
        assert!(has_delete);
        assert!(has_update);

        Ok(())
    }

    #[test]
    fn test_generate_diff_namespace_operations() -> Result<()> {
        let ns_key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "Namespace".to_string(),
            name: "my-namespace".to_string(),
            namespace: None,
        };

        let ns_object = DynamicObject {
            types: Some(TypeMeta {
                api_version: "v1".to_string(),
                kind: "Namespace".to_string(),
            }),
            metadata: ObjectMeta::default(),
            data: json!({}),
        };

        let have = KubernetesResources {
            by_key: BTreeMap::new(),
            namespaces: BTreeMap::new(),
        };
        let want = KubernetesResources {
            by_key: BTreeMap::from([(ns_key.clone(), ns_object)]),
            namespaces: BTreeMap::new(),
        };
        let diff = generate_diff(have, want)?;

        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].0, ns_key);
        assert!(matches!(diff[0].1, DiffAction::Create(_)));

        Ok(())
    }
}
