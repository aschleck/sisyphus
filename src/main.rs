mod config_image;
mod kubernetes;
mod registry_clients;
mod sisyphus_yaml;

use crate::{
    config_image::{get_config, Argument, ArgumentValues},
    kubernetes::{
        get_kubernetes_api, get_kubernetes_clients, munge_ignored_fields, KubernetesKey,
        KubernetesResources, MungeOptions, MANAGER,
    },
    registry_clients::RegistryClients,
    sisyphus_yaml::{Deployment as SisyphusDeployment, HasKind, SisyphusResource, VariableSource},
};
use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand};
use console::{style, Style};
use docker_registry::{
    reference::{Reference as RegistryReference, Version as RegistryVersion},
    render as containerRender,
};
use futures::future::try_join_all;
use k8s_openapi::api::{
    apps::v1::{Deployment, DeploymentSpec},
    core::v1::{
        Container, ContainerPort, EnvVar, EnvVarSource, KeyToPath, Namespace, PodSpec,
        SecretKeySelector, SecretVolumeSource, Volume, VolumeMount,
    },
};
use kube::{
    api::{DeleteParams, DynamicObject, ObjectMeta, Patch, PatchParams},
    core::ErrorResponse,
    Error, ResourceExt,
};
use serde::Deserialize;
use similar::{ChangeTag, TextDiff};
use sqlx::{AnyPool, Row};
use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File},
    io::Write,
    path::Path,
    str::FromStr,
};
use tempfile::TempDir;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,

    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Push {
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
    let args = Args::parse();
    let pool = AnyPool::connect(&args.database_url).await?;

    match args.command {
        Commands::Push {
            label_namespace,
            monitor_directory,
        } => push(&label_namespace, &monitor_directory, &pool).await?,
        Commands::Refresh => refresh(&pool).await?,
    };
    Ok(())
}

async fn push(label_namespace: &str, monitor_directory: &str, pool: &AnyPool) -> Result<()> {
    let mut registries = RegistryClients::new();
    let mut from_files = KubernetesResources {
        by_key: BTreeMap::new(),
        namespaces: BTreeMap::new(),
    };
    {
        let resources = get_sisyphus_resources_from_files(Path::new(&monitor_directory))?;
        render_sisyphus_resources(
            &resources.global_by_key,
            label_namespace,
            /* maybe_namespace= */ None,
            &mut from_files.by_key,
            &mut registries,
        )
        .await?;
        for (namespace, objects) in resources.by_namespace_by_key {
            render_sisyphus_resources(
                &objects,
                &label_namespace,
                Some(&namespace),
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
    munge_ignored_fields(
        &mut from_database,
        &mut from_files,
        MungeOptions {
            munge_managed_fields: true,
            munge_secret_data: true,
        },
    )?;
    let changed = generate_diff(&from_database, &from_files)?;
    if !changed {
        println!("Nothing to do");
        return Ok(());
    }

    print!("Continue pushing? y/(n): ");
    std::io::stdout().flush()?;
    let mut response = String::new();
    std::io::stdin().read_line(&mut response)?;
    match response.trim().to_lowercase().as_str() {
        "y" => {
            apply_diff(&from_database, &from_files, &pool).await?;
        }
        _ => {
            println!("Canceled");
        }
    }

    Ok(())
}

async fn refresh(pool: &AnyPool) -> Result<()> {
    let mut from_database = get_objects_from_database(&pool).await?;
    let mut from_kubernetes = get_objects_from_kubernetes(&from_database).await?;
    munge_ignored_fields(
        &mut from_database,
        &mut from_kubernetes,
        MungeOptions {
            munge_managed_fields: false,
            munge_secret_data: true,
        },
    )?;
    let changed = generate_diff(&from_database, &from_kubernetes)?;
    if !changed {
        println!("Nothing to do");
        return Ok(());
    }

    print!("Continue refreshing? y/(n): ");
    std::io::stdout().flush()?;
    let mut response = String::new();
    std::io::stdin().read_line(&mut response)?;
    match response.trim().to_lowercase().as_str() {
        "y" => {
            apply_refresh(&from_database, &from_kubernetes, &pool).await?;
        }
        _ => {
            println!("Canceled");
        }
    }

    Ok(())
}

async fn apply_refresh(
    from_database: &KubernetesResources,
    from_kubernetes: &KubernetesResources,
    pool: &AnyPool,
) -> Result<()> {
    refresh_group(&from_database.by_key, &from_kubernetes.by_key, &pool).await?;
    refresh_group(
        &from_database.namespaces,
        &from_kubernetes.namespaces,
        &pool,
    )
    .await?;
    Ok(())
}

async fn refresh_group(
    have: &BTreeMap<KubernetesKey, DynamicObject>,
    want: &BTreeMap<KubernetesKey, DynamicObject>,
    pool: &AnyPool,
) -> Result<()> {
    for (key, h) in have {
        match want.get(key) {
            Some(w) => {
                if h == w {
                    continue;
                }
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
            None => {
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

fn generate_diff(have: &KubernetesResources, want: &KubernetesResources) -> Result<bool> {
    let mut changes = 0;
    for (key, w) in &want.namespaces {
        let h = have.namespaces.get(&key);
        if h == Some(w) {
            continue;
        }
        generate_single_diff(key, h, Some(w))?;
        changes += 1;
    }

    for (key, w) in &want.by_key {
        let h = have.by_key.get(&key);
        if h == Some(w) {
            continue;
        }
        generate_single_diff(key, h, Some(w))?;
        changes += 1;
    }

    for (key, h) in &have.by_key {
        if !want.by_key.contains_key(&key) {
            generate_single_diff(key, Some(h), None)?;
            changes += 1;
        }
    }

    for (key, h) in &have.namespaces {
        if !want.namespaces.contains_key(&key) {
            generate_single_diff(key, Some(h), None)?;
            changes += 1;
        }
    }

    Ok(changes > 0)
}

async fn apply_diff(
    have: &KubernetesResources,
    want: &KubernetesResources,
    pool: &AnyPool,
) -> Result<()> {
    let (clients, types) =
        get_kubernetes_clients(have.by_key.keys().chain(want.by_key.keys())).await?;

    for (key, w) in &want.namespaces {
        let api = get_kubernetes_api(key, &clients, &types)?;
        apply_single_diff(&key, have.namespaces.get(&key), Some(w), &api, pool).await?;
    }
    for (key, w) in &want.by_key {
        let api = get_kubernetes_api(key, &clients, &types)?;
        apply_single_diff(&key, have.by_key.get(&key), Some(w), &api, pool).await?;
    }
    for (key, h) in &have.by_key {
        if !want.by_key.contains_key(&key) {
            let api = get_kubernetes_api(key, &clients, &types)?;
            apply_single_diff(&key, Some(h), None, &api, pool).await?;
        }
    }
    for (key, h) in &have.namespaces {
        if !want.namespaces.contains_key(&key) {
            let api = get_kubernetes_api(key, &clients, &types)?;
            apply_single_diff(&key, Some(h), None, &api, pool).await?;
        }
    }
    Ok(())
}

async fn apply_single_diff(
    key: &KubernetesKey,
    have: Option<&DynamicObject>,
    want: Option<&DynamicObject>,
    api: &kube::Api<DynamicObject>,
    pool: &AnyPool,
) -> Result<()> {
    if have == want {
        return Ok(());
    }

    match (have, want) {
        (Some(_), Some(w)) => {
            let result = api
                .patch(
                    &w.name_any(),
                    &PatchParams::apply(MANAGER),
                    &Patch::Apply(w),
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
        (Some(h), None) => {
            api.delete(&h.name_any(), &DeleteParams::default())
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
        (None, Some(w)) => {
            let result = api
                .patch(
                    &w.name_any(),
                    &PatchParams::apply(MANAGER),
                    &Patch::Apply(w),
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
        (None, None) => bail!("Expected some type of object"),
    }
    Ok(())
}

async fn get_objects_from_database(pool: &AnyPool) -> Result<KubernetesResources> {
    let mut tx = pool.begin().await?;
    let recs = sqlx::query(
        r#"SELECT api_version, cluster, kind, namespace, name, yaml FROM kubernetes_objects"#,
    )
    .fetch_all(&mut *tx)
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
    for (source, destination) in vec![
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
        }
    }
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
            let in_namespace = match path.file_name().map(|s| s.to_str()).flatten() {
                Some("global") => &mut resources.global_by_key,
                Some(namespace) => resources
                    .by_namespace_by_key
                    .entry(namespace.to_string())
                    .or_insert_with(|| HashMap::new()),
                None => bail!("Path has no filename"),
            };
            get_objects_from_namespace(&path, in_namespace)?;
        }
    }
    Ok(resources)
}

fn get_objects_from_namespace(
    directory: &Path,
    resources: &mut HashMap<SisyphusKey, SisyphusResource>,
) -> Result<()> {
    let index_path = directory.join("index.yaml");
    let reader = File::open(&index_path)?;

    for document in serde_yaml::Deserializer::from_reader(&reader) {
        let mut object: SisyphusResource = SisyphusResource::deserialize(document)
            .with_context(|| format!("in file {:?}", index_path))?;

        if let SisyphusResource::KubernetesYaml(v) = &mut object {
            let mut extra_objects = Vec::new();
            if let Some(sources) = &mut v.sources {
                for path in &*sources {
                    load_objects_from_kubernetes_yaml(&directory.join(path), &mut extra_objects)?;
                }
                sources.clear();
            }
            if let Some(objects) = &mut v.objects {
                objects.append(&mut extra_objects);
            } else {
                v.objects = Some(extra_objects);
            }
        }
        insert_sisyphus_resource(object, resources)?;
    }

    Ok(())
}

fn load_objects_from_kubernetes_yaml(path: &Path, into: &mut Vec<DynamicObject>) -> Result<()> {
    let reader = File::open(&path)?;
    for document in serde_yaml::Deserializer::from_reader(&reader) {
        let object: DynamicObject = DynamicObject::deserialize(document)?;
        let name = object
            .metadata
            .name
            .as_ref()
            .ok_or_else(|| anyhow!("Object in {:?} is missing a name", path))?;
        if object.metadata.namespace.is_some() {
            bail!(
                "Object {} in {:?} should not specify a namespace",
                name,
                path
            );
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
        SisyphusResource::Deployment(v) => (&v.api_version, v.kind(), &v.metadata.name),
        SisyphusResource::KubernetesYaml(v) => (&v.api_version, v.kind(), &v.metadata.name),
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
    label_namespace: &str,
    maybe_namespace: Option<&str>,
    by_key: &mut BTreeMap<KubernetesKey, DynamicObject>,
    registries: &mut RegistryClients,
) -> Result<()> {
    for (key, object) in objects {
        let mut copy = object.clone();
        match &mut copy {
            SisyphusResource::Deployment(v) => {
                resolve_sisyphus_deployment_image(v, registries).await?;
            }
            SisyphusResource::KubernetesYaml(v) => {
                if let Some(objects) = &mut v.objects {
                    for object in objects {
                        object.metadata.namespace = maybe_namespace.map(|n| n.to_string());
                    }
                }
            }
        };

        render_sisyphus_resource(&copy, label_namespace, &maybe_namespace, by_key, registries)
            .await
            .with_context(|| format!("while rendering {:?}", key))?;
    }
    Ok(())
}

async fn render_sisyphus_resource(
    object: &SisyphusResource,
    label_namespace: &str,
    maybe_namespace: &Option<&str>,
    by_key: &mut BTreeMap<KubernetesKey, DynamicObject>,
    registries: &mut RegistryClients,
) -> Result<()> {
    match object {
        SisyphusResource::Deployment(v) => {
            let image = RegistryReference::from_str(&v.config.image)
                .map_err(|e| anyhow!("Unable to parse image url: {}", e))?;
            let registry = registries.get_client(image.registry())?;
            let repository = image.repository();
            let manifest = registry
                .get_manifest(&repository, image.version().as_ref())
                .await?;
            let layers_digests = manifest.layers_digests(None)?;
            let blob_futures = layers_digests
                .iter()
                .map(|layer_digest| registry.get_blob(&repository, layer_digest))
                .collect::<Vec<_>>();
            let blobs = try_join_all(blob_futures).await?;
            let path = TempDir::new()?;
            containerRender::unpack(&blobs, path.path())?;
            let (index, application) = get_config(path.path()).await?;

            let mut labels = v.metadata.labels.clone().unwrap_or_else(|| BTreeMap::new());
            labels.insert(format!("{}/app", label_namespace), v.metadata.name.clone());
            let mut metadata = ObjectMeta::default();
            metadata.labels = Some(labels.clone());
            metadata.name = Some(v.metadata.name.clone());
            if let Some(n) = maybe_namespace {
                metadata.namespace = Some(n.to_string());
            }

            let mut independent_spec = DeploymentSpec::default();
            independent_spec.selector.match_labels = Some(labels.clone());
            let mut template_metadata = ObjectMeta::default();
            template_metadata.labels = Some(labels);
            independent_spec.template.metadata = Some(template_metadata);
            let mut container = Container::default();
            container.name = v.metadata.name.clone();
            container.image = Some(
                RegistryReference::new(
                    Some(image.registry()),
                    index.binary_image,
                    Some(RegistryVersion::from_str(&format!(
                        "@{}",
                        index.binary_digest
                    ))?),
                )
                .to_string(),
            );
            let mut ports = Vec::new();
            let mut volumes = Vec::new();
            let mut volume_mounts = Vec::new();
            let mut args = Vec::new();
            for arg in &application.args {
                args.push(
                    match render_argument(
                        &arg,
                        &v.config.env,
                        &mut ports,
                        &v.config.variables,
                        &mut volumes,
                        &mut volume_mounts,
                    )? {
                        RenderedArgument::String(v) => v,
                        u => bail!("Unexpected non-string argument {:?}", u),
                    },
                );
            }
            let mut env_vars = Vec::new();
            for (key, value) in &application.env {
                let rendered = render_argument(
                    &value,
                    &v.config.env,
                    &mut ports,
                    &v.config.variables,
                    &mut volumes,
                    &mut volume_mounts,
                )?;
                let mut var = EnvVar::default();
                var.name = key.clone();
                match rendered {
                    RenderedArgument::String(v) => {
                        var.value = Some(v);
                    }
                    RenderedArgument::ValueFrom(v) => {
                        var.value_from = Some(v);
                    }
                };
                env_vars.push(var);
            }
            container.args = Some(args);
            container.env = Some(env_vars);
            container.ports = Some(ports);
            container.volume_mounts = Some(volume_mounts);

            let mut pod_spec = PodSpec::default();
            pod_spec.containers.push(container);
            pod_spec.volumes = Some(volumes);
            independent_spec.template.spec = Some(pod_spec);

            for (cluster, cluster_spec) in &v.footprint {
                let mut spec = independent_spec.clone();
                spec.replicas = Some(cluster_spec.replicas);
                let serialized = serde_yaml::to_string(&Deployment {
                    metadata: metadata.clone(),
                    spec: Some(spec),
                    status: None,
                })?;
                let converted =
                    DynamicObject::deserialize(serde_yaml::Deserializer::from_str(&serialized))?;
                let types = converted
                    .types
                    .clone()
                    .ok_or_else(|| anyhow!("Object is type-free"))?;
                if types.api_version == "v1" && types.kind == "Namespace" {
                    bail!("Cannot specify a namespace");
                }
                let key = KubernetesKey {
                    api_version: types.api_version,
                    cluster: cluster.clone(),
                    kind: types.kind,
                    name: v.metadata.name.clone(),
                    namespace: maybe_namespace.map(|s| s.to_string()),
                };
                by_key.insert(key, converted);
            }
        }
        SisyphusResource::KubernetesYaml(v) => {
            if let Some(objects) = &v.objects {
                for object in objects {
                    let types = object
                        .types
                        .clone()
                        .ok_or_else(|| anyhow!("Object is type-free"))?;
                    for cluster in &v.clusters {
                        let key = KubernetesKey {
                            api_version: types.api_version.clone(),
                            cluster: cluster.clone(),
                            kind: types.kind.clone(),
                            name: object.name_any(),
                            namespace: maybe_namespace.map(|s| s.to_string()),
                        };
                        by_key.insert(key, object.clone());
                    }
                }
            }
        }
    };
    Ok(())
}

#[derive(Debug)]
enum RenderedArgument {
    String(String),
    ValueFrom(EnvVarSource),
}

fn render_argument(
    arg: &ArgumentValues,
    selector: &str,
    ports: &mut Vec<ContainerPort>,
    variables: &BTreeMap<String, VariableSource>,
    volumes: &mut Vec<Volume>,
    volume_mounts: &mut Vec<VolumeMount>,
) -> Result<RenderedArgument> {
    let single = match arg {
        ArgumentValues::Varying(a) => a
            .get(selector)
            .ok_or_else(|| anyhow!("No selector {} in {:?}", selector, a.keys()))?,
        ArgumentValues::Uniform(a) => a,
    };
    Ok(match single {
        Argument::FileVariable(v) => {
            let mut mount = VolumeMount::default();
            mount.name = v.name.clone();
            let path = Path::new(&v.path);
            mount.mount_path = String::from(
                path.parent()
                    .ok_or_else(|| anyhow!("Variable path has no parent"))?
                    .to_string_lossy(),
            );
            volume_mounts.push(mount);

            let mut volume = Volume::default();
            volume.name = v.name.clone();
            let variable = variables
                .get(&v.name)
                .ok_or_else(|| anyhow!("Variable {} isn't set", v.name))?;
            match variable {
                VariableSource::SecretKeyRef(v) => {
                    let mut secret = SecretVolumeSource::default();
                    secret.secret_name = Some(v.name.clone());
                    secret.items = Some(vec![KeyToPath {
                        key: v.key.clone(),
                        mode: None,
                        path: String::from(
                            path.file_name()
                                .ok_or_else(|| anyhow!("Unable to get file name"))?
                                .to_string_lossy(),
                        ),
                    }]);
                    volume.secret = Some(secret);
                }
            };
            volumes.push(volume);
            RenderedArgument::String(v.path.clone())
        }
        Argument::Port(v) => {
            let mut port = ContainerPort::default();
            port.name = Some(v.name.clone());
            port.container_port = v.number.into();
            ports.push(port);
            RenderedArgument::String(v.number.to_string())
        }
        Argument::String(v) => RenderedArgument::String(v.clone()),
        Argument::StringVariable(v) => {
            let mut source = EnvVarSource::default();
            let variable = variables
                .get(&v.name)
                .ok_or_else(|| anyhow!("Variable {} isn't set", v.name))?;
            match variable {
                VariableSource::SecretKeyRef(v) => {
                    source.secret_key_ref = Some(SecretKeySelector {
                        name: v.name.clone(),
                        key: v.key.clone(),
                        optional: None,
                    });
                }
            };
            RenderedArgument::ValueFrom(source)
        }
    })
}

async fn resolve_sisyphus_deployment_image(
    object: &mut SisyphusDeployment,
    registries: &mut RegistryClients,
) -> Result<()> {
    let image = RegistryReference::from_str(&object.config.image)
        .map_err(|e| anyhow!("Unable to parse image url: {}", e))?;
    let registry = registries.get_client(image.registry())?;
    let manifest = registry
        .get_manifest(image.repository().as_ref(), image.version().as_ref())
        .await?;
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

fn generate_single_diff<T: serde::Serialize>(
    key: &KubernetesKey,
    have: Option<&T>,
    want: Option<&T>,
) -> Result<()> {
    let (verb, hs, ws) = match (have, want) {
        (Some(h), Some(w)) => (
            style("patch").yellow(),
            serde_yaml::to_string(h)?,
            serde_yaml::to_string(w)?,
        ),
        (Some(h), None) => (
            style("delete").red(),
            serde_yaml::to_string(h)?,
            "".to_string(),
        ),
        (None, Some(w)) => (
            style("create").green(),
            "".to_string(),
            serde_yaml::to_string(w)?,
        ),
        (None, None) => bail!("Expected a difference"),
    };

    let diff = TextDiff::from_lines(&hs, &ws);
    println!("• {} {}\n", verb, key);
    print_diff(&diff);
    println!("");
    Ok(())
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
