use crate::{
    config_image::{
        get_config, Application, Argument, ArgumentValues, ConfigImageIndex, FileVariable,
    },
    kubernetes_io::KubernetesKey,
    registry_clients::RegistryClients,
    sisyphus_yaml::{DeploymentServiceConfig, SisyphusResource, VariableSource},
};
use anyhow::{anyhow, bail, Result};
use docker_registry::render as containerRender;
use futures::future::try_join_all;
use json_patch::jsonptr::{Assign, Pointer};
use k8s_openapi::{
    api::{
        apps::v1::{Deployment, DeploymentSpec, DeploymentStrategy, RollingUpdateDeployment},
        batch::v1::{CronJob, CronJobSpec, JobSpec, JobTemplateSpec},
        core::v1::{
            Container, ContainerPort, EnvVar, EnvVarSource, KeyToPath, PodSecurityContext, PodSpec,
            PodTemplateSpec, ResourceRequirements, SecretKeySelector, SecretVolumeSource, Service,
            ServicePort, ServiceSpec, Volume, VolumeMount,
        },
    },
    apimachinery::pkg::{api::resource::Quantity, util::intstr::IntOrString},
};
use kube::{
    api::{DynamicObject, ObjectMeta},
    ResourceExt,
};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::{collections::BTreeMap, path::Path};
use tempfile::TempDir;

#[cfg(test)]
mod tests;

pub(crate) async fn render_sisyphus_resource(
    object: &SisyphusResource,
    allow_any_namespace: bool,
    label_namespace: &str,
    maybe_namespace: &Option<String>,
    by_key: &mut BTreeMap<KubernetesKey, DynamicObject>,
    registries: &mut RegistryClients,
) -> Result<()> {
    match object {
        SisyphusResource::KubernetesYaml(v) => {
            handle_kubernetes_yaml_resource(v, allow_any_namespace, maybe_namespace, by_key)?;
        }
        SisyphusResource::SisyphusCronJob(v) => {
            let (index, application) =
                prepare_image_config(&v.config.image, registries, maybe_namespace.as_deref())
                    .await?;

            let metadata = render_deployment_metadata(
                &v.metadata.name,
                label_namespace,
                &v.metadata.labels,
                &v.metadata.annotations,
                maybe_namespace,
            )?;

            let (container, _, volumes) = build_container_config(
                &v.metadata.name,
                &index,
                &application,
                &v.config.env,
                &v.config.variables,
            )?;

            let pod_spec = build_pod_spec(container, volumes);

            let namespace = maybe_namespace
                .as_ref()
                .ok_or_else(|| anyhow!("Namespace must be explicit"))?;

            process_cronjob_footprint(
                v,
                &metadata,
                &v.config.concurrency_policy,
                &v.config.schedule,
                &pod_spec,
                namespace,
                by_key,
            )?;
        }
        SisyphusResource::SisyphusDeployment(v) => {
            let (index, application) =
                prepare_image_config(&v.config.image, registries, maybe_namespace.as_deref())
                    .await?;

            let metadata = render_deployment_metadata(
                &v.metadata.name,
                label_namespace,
                &v.metadata.labels,
                &v.metadata.annotations,
                maybe_namespace,
            )?;
            let labels = metadata.labels.clone().unwrap_or_default();

            let mut independent_spec = build_base_deployment_spec(labels.clone());

            let (container, ports, volumes) = build_container_config(
                &v.metadata.name,
                &index,
                &application,
                &v.config.env,
                &v.config.variables,
            )?;

            independent_spec.template.spec = Some(build_pod_spec(container, volumes));

            let service_spec_option =
                build_service_spec(&v.config.service, &ports, labels.clone())?;

            let namespace = maybe_namespace
                .as_ref()
                .ok_or_else(|| anyhow!("Namespace must be explicit"))?;

            process_deployment_footprint(
                v,
                &metadata,
                &independent_spec,
                &service_spec_option,
                namespace,
                by_key,
            )?;
        }
        SisyphusResource::SisyphusYaml(_) => {
            unreachable!("These should already have been resolved")
        }
    };
    Ok(())
}

fn handle_kubernetes_yaml_resource(
    v: &crate::sisyphus_yaml::KubernetesYaml,
    allow_any_namespace: bool,
    maybe_namespace: &Option<String>,
    by_key: &mut BTreeMap<KubernetesKey, DynamicObject>,
) -> Result<()> {
    for object in &v.objects {
        let types = object
            .types
            .clone()
            .ok_or_else(|| anyhow!("Object {} is type-free", object.name_any()))?;
        if !allow_any_namespace && types.api_version == "v1" && types.kind == "Namespace" {
            bail!("Cannot specify a namespace");
        }
        for cluster in &v.clusters {
            let key = KubernetesKey {
                api_version: types.api_version.clone(),
                cluster: cluster.clone(),
                kind: types.kind.clone(),
                name: object.name_any(),
                namespace: object
                    .metadata
                    .namespace
                    .as_ref()
                    .or(maybe_namespace.as_ref())
                    .cloned(),
            };
            by_key.insert(key, object.clone());
        }
    }
    Ok(())
}

pub(crate) async fn prepare_image_config(
    image_config: &String,
    registries: &mut RegistryClients,
    namespace: Option<&str>,
) -> Result<(ConfigImageIndex, Application)> {
    let (image, registry) = registries.get_reference_and_registry(image_config).await?;
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
    let (index, application) = get_config(path.path(), namespace).await?;
    Ok((index, application))
}

fn render_deployment_metadata(
    deployment_name: &str,
    label_namespace: &str,
    deployment_labels: &BTreeMap<String, String>,
    deployment_annotations: &BTreeMap<String, String>,
    maybe_namespace: &Option<String>,
) -> Result<ObjectMeta> {
    let mut labels = deployment_labels.clone();
    labels.insert(
        format!("{}/app", label_namespace),
        deployment_name.to_string(),
    );

    let mut metadata = ObjectMeta::default();
    if deployment_annotations.len() > 0 {
        metadata.annotations = Some(deployment_annotations.clone());
    }
    if labels.len() > 0 {
        metadata.labels = Some(labels);
    }
    metadata.name = Some(deployment_name.to_string());
    metadata.namespace = Some(
        maybe_namespace
            .as_ref()
            .ok_or_else(|| anyhow!("Namespace must be explicit"))?
            .clone(),
    );
    Ok(metadata)
}

fn build_base_deployment_spec(labels: BTreeMap<String, String>) -> DeploymentSpec {
    let mut independent_spec = DeploymentSpec::default();
    independent_spec.selector.match_labels = Some(labels.clone());
    independent_spec.progress_deadline_seconds = Some(600);
    independent_spec.revision_history_limit = Some(10);
    independent_spec.strategy = Some(DeploymentStrategy {
        type_: Some("RollingUpdate".to_string()),
        rolling_update: Some(RollingUpdateDeployment {
            max_surge: Some(IntOrString::String("25%".to_string())),
            max_unavailable: Some(IntOrString::String("25%".to_string())),
        }),
    });
    let mut template_metadata = ObjectMeta::default();
    template_metadata.labels = Some(labels);
    independent_spec.template.metadata = Some(template_metadata);
    independent_spec
}

fn render_container_args(
    application_args: &[ArgumentValues],
    config_env: &str,
    ports: &mut BTreeMap<String, ContainerPort>,
    config_vars: &BTreeMap<String, VariableSource>,
    volumes: &mut Vec<Volume>,
    volume_mounts: &mut Vec<VolumeMount>,
) -> Result<Vec<String>> {
    let mut args = Vec::new();
    for arg in application_args {
        let maybe = render_argument(arg, config_env, ports, config_vars, volumes, volume_mounts)?;
        let Some(rendered) = maybe else {
            continue;
        };
        args.push(match rendered {
            RenderedArgument::String(v) => v,
            u => bail!("Unexpected non-string argument {:?}", u),
        });
    }
    Ok(args)
}

fn render_container_env_vars(
    application_env: &BTreeMap<String, ArgumentValues>,
    config_env: &str,
    ports: &mut BTreeMap<String, ContainerPort>,
    config_vars: &BTreeMap<String, VariableSource>,
    volumes: &mut Vec<Volume>,
    volume_mounts: &mut Vec<VolumeMount>,
) -> Result<Vec<EnvVar>> {
    let mut env_vars = Vec::new();
    for (key, value) in application_env {
        let maybe = render_argument(
            value,
            config_env,
            ports,
            config_vars,
            volumes,
            volume_mounts,
        )?;
        let Some(rendered) = maybe else {
            continue;
        };
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
    Ok(env_vars)
}

fn render_resource_requirements_map(
    resource_map: &BTreeMap<String, ArgumentValues>,
    config_env: &str,
    ports: &mut BTreeMap<String, ContainerPort>,
    config_vars: &BTreeMap<String, VariableSource>,
    volumes: &mut Vec<Volume>,
    volume_mounts: &mut Vec<VolumeMount>,
) -> Result<BTreeMap<String, Quantity>> {
    let mut copy = BTreeMap::new();
    for (key, value) in resource_map {
        let maybe = render_argument(
            value,
            config_env,
            ports,
            config_vars,
            volumes,
            volume_mounts,
        )?;
        let Some(rendered) = maybe else {
            continue;
        };
        let quantity = Quantity(match rendered {
            RenderedArgument::String(v) => v,
            v => bail!("Unexpected resource request type {:?}", v),
        });
        copy.insert(key.clone(), quantity);
    }
    Ok(copy)
}

fn build_container_config(
    deployment_name: &str,
    index: &ConfigImageIndex,
    application: &Application,
    config_env: &str,
    config_vars: &BTreeMap<String, VariableSource>,
) -> Result<(Container, BTreeMap<String, ContainerPort>, Vec<Volume>)> {
    let mut container = Container::default();
    container.name = deployment_name.to_string();
    container.image = Some(format!(
        "{}@{}",
        index.binary_repository, index.binary_digest
    ));

    let mut ports = BTreeMap::new();
    let mut volumes = Vec::new();
    let mut volume_mounts = Vec::new();

    let args = render_container_args(
        &application.args,
        config_env,
        &mut ports,
        config_vars,
        &mut volumes,
        &mut volume_mounts,
    )?;
    if args.len() > 0 {
        container.args = Some(args);
    }

    let env_vars = render_container_env_vars(
        &application.env,
        config_env,
        &mut ports,
        config_vars,
        &mut volumes,
        &mut volume_mounts,
    )?;
    if env_vars.len() > 0 {
        container.env = Some(env_vars);
    }

    let mut resources = ResourceRequirements::default();
    if application.resources.requests.len() > 0 {
        resources.requests = Some(render_resource_requirements_map(
            &application.resources.requests,
            config_env,
            &mut ports,
            config_vars,
            &mut volumes,
            &mut volume_mounts,
        )?);
    }
    if application.resources.limits.len() > 0 {
        resources.limits = Some(render_resource_requirements_map(
            &application.resources.limits,
            config_env,
            &mut ports,
            config_vars,
            &mut volumes,
            &mut volume_mounts,
        )?);
    }
    container.resources = Some(resources);

    if ports.len() > 0 {
        container.ports = Some(ports.iter().map(|(_, v)| v.clone()).collect());
    }
    if volume_mounts.len() > 0 {
        container.volume_mounts = Some(volume_mounts);
    }

    // Set some defaults
    container.image_pull_policy = Some("IfNotPresent".to_string());
    container.termination_message_path = Some("/dev/termination-log".to_string());
    container.termination_message_policy = Some("File".to_string());

    Ok((container, ports, volumes))
}

fn build_pod_spec(container: Container, volumes: Vec<Volume>) -> PodSpec {
    let mut pod_spec = PodSpec::default();
    pod_spec.containers.push(container);
    if volumes.len() > 0 {
        pod_spec.volumes = Some(volumes);
    }
    // Set some defaults
    pod_spec.dns_policy = Some("ClusterFirst".to_string());
    pod_spec.restart_policy = Some("Always".to_string());
    // TODO(april): this won't work when there's another scheduler
    pod_spec.scheduler_name = Some("default-scheduler".to_string());
    pod_spec.security_context = Some(PodSecurityContext::default());
    pod_spec.termination_grace_period_seconds = Some(30);
    pod_spec
}

fn build_service_spec(
    config_service: &Option<DeploymentServiceConfig>,
    ports: &BTreeMap<String, ContainerPort>,
    labels: BTreeMap<String, String>,
) -> Result<Option<ServiceSpec>> {
    let mut service_spec = ServiceSpec::default();
    service_spec.selector = Some(labels);
    service_spec.ports = config_service
        .as_ref()
        .map(|p| {
            p.ports
                .iter()
                .map(|(k, v)| -> Result<ServicePort> {
                    let target = k;
                    let references = ports.get(target).ok_or_else(|| {
                        anyhow!("The config doesn't define a port named {}", target)
                    })?;
                    let mut sp = ServicePort::default();
                    sp.name = Some(v.name.as_ref().unwrap_or(k).clone());
                    sp.port = v.number;
                    sp.protocol = references.protocol.clone();
                    sp.target_port = Some(IntOrString::String(k.clone()));
                    Ok(sp)
                })
                .collect::<Result<Vec<ServicePort>>>()
        })
        .transpose()?;

    if service_spec.ports.as_ref().map_or(true, |p| p.is_empty()) {
        Ok(None)
    } else {
        Ok(Some(service_spec))
    }
}

fn process_cronjob_footprint(
    sisyphus_cronjob: &crate::sisyphus_yaml::SisyphusCronJob,
    metadata: &ObjectMeta,
    concurrency_policy: &Option<String>,
    schedule: &str,
    pod_spec: &PodSpec,
    namespace: &str,
    by_key: &mut BTreeMap<KubernetesKey, DynamicObject>,
) -> Result<()> {
    for (cluster, _) in &sisyphus_cronjob.footprint {
        let cronjob_spec = CronJobSpec {
            concurrency_policy: concurrency_policy.clone(),
            schedule: schedule.to_string(),
            job_template: JobTemplateSpec {
                metadata: None,
                spec: Some(JobSpec {
                    template: PodTemplateSpec {
                        metadata: None,
                        spec: Some(pod_spec.clone()),
                    },
                    ..Default::default()
                }),
            },
            ..Default::default()
        };

        let serialized = serde_yaml::to_string(&CronJob {
            metadata: metadata.clone(),
            spec: Some(cronjob_spec),
            status: None,
        })?;
        let mut converted =
            DynamicObject::deserialize(serde_yaml::Deserializer::from_str(&serialized))?;
        converted.data.assign(
            Pointer::parse("/spec/jobTemplate/metadata/creationTimestamp")?,
            JsonValue::Null,
        )?;
        converted.data.assign(
            Pointer::parse("/spec/jobTemplate/spec/template/metadata/creationTimestamp")?,
            JsonValue::Null,
        )?;
        let types = converted
            .types
            .clone()
            .ok_or_else(|| anyhow!("Object {} is type-free", converted.name_any()))?;
        let key = KubernetesKey {
            api_version: types.api_version,
            cluster: cluster.clone(),
            kind: types.kind,
            name: sisyphus_cronjob.metadata.name.clone(),
            namespace: Some(namespace.to_string()),
        };
        by_key.insert(key, converted);
    }
    Ok(())
}

fn process_deployment_footprint(
    sisyphus_deployment: &crate::sisyphus_yaml::SisyphusDeployment,
    metadata: &ObjectMeta,
    independent_spec: &DeploymentSpec,
    service_spec_option: &Option<ServiceSpec>,
    namespace: &str,
    by_key: &mut BTreeMap<KubernetesKey, DynamicObject>,
) -> Result<()> {
    for (cluster, cluster_spec) in &sisyphus_deployment.footprint {
        {
            let mut spec = independent_spec.clone();
            spec.replicas = Some(cluster_spec.replicas);
            let serialized = serde_yaml::to_string(&Deployment {
                metadata: metadata.clone(),
                spec: Some(spec),
                status: None,
            })?;
            let mut converted =
                DynamicObject::deserialize(serde_yaml::Deserializer::from_str(&serialized))?;
            converted.data.assign(
                Pointer::parse("/spec/template/metadata/creationTimestamp")?,
                JsonValue::Null,
            )?;
            let types = converted
                .types
                .clone()
                .ok_or_else(|| anyhow!("Object {} is type-free", converted.name_any()))?;
            let key = KubernetesKey {
                api_version: types.api_version,
                cluster: cluster.clone(),
                kind: types.kind,
                name: sisyphus_deployment.metadata.name.clone(),
                namespace: Some(namespace.to_string()),
            };
            by_key.insert(key, converted);
        }

        if let Some(service_spec) = service_spec_option {
            if service_spec.ports.as_ref().map_or(false, |p| !p.is_empty()) {
                let serialized = serde_yaml::to_string(&Service {
                    metadata: metadata.clone(),
                    spec: Some(service_spec.clone()),
                    status: None,
                })?;
                let converted =
                    DynamicObject::deserialize(serde_yaml::Deserializer::from_str(&serialized))?;
                let types = converted
                    .types
                    .clone()
                    .ok_or_else(|| anyhow!("Object {} is type-free", converted.name_any()))?;
                let key = KubernetesKey {
                    api_version: types.api_version,
                    cluster: cluster.clone(),
                    kind: types.kind,
                    name: sisyphus_deployment.metadata.name.clone(),
                    namespace: Some(namespace.to_string()),
                };
                by_key.insert(key, converted);
            }
        }
    }
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
    ports: &mut BTreeMap<String, ContainerPort>,
    variables: &BTreeMap<String, VariableSource>,
    volumes: &mut Vec<Volume>,
    volume_mounts: &mut Vec<VolumeMount>,
) -> Result<Option<RenderedArgument>> {
    let maybe = match arg {
        ArgumentValues::Varying(a) => a.get(selector),
        ArgumentValues::Uniform(a) => Some(a),
    };
    let Some(single) = maybe else {
        return Ok(None);
    };
    Ok(Some(match single {
        Argument::FileVariable(var) => {
            let source = variables
                .get(&var.name)
                .ok_or_else(|| anyhow!("Variable {} isn't set", var.name))?;
            render_file_variable(var, source, volumes, volume_mounts)?
        }
        Argument::Port(v) => {
            let mut port = ContainerPort::default();
            port.name = Some(v.name.clone());
            port.container_port = v.number.into();
            port.protocol = Some(format!("{}", v.protocol));
            ports.insert(v.name.clone(), port);
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
    }))
}

fn render_file_variable(
    variable: &FileVariable,
    source: &VariableSource,
    volumes: &mut Vec<Volume>,
    volume_mounts: &mut Vec<VolumeMount>,
) -> Result<RenderedArgument> {
    let path = Path::new(&variable.path);
    let filename = path
        .file_name()
        .ok_or_else(|| anyhow!("Unable to get file name"))?
        .to_string_lossy();
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("Variable path has no parent"))?
        .to_string_lossy();

    let volume = match source {
        VariableSource::SecretKeyRef(secret_source) => {
            let existing_volume = volumes.iter_mut().find(|volume| {
                volume
                    .secret
                    .as_ref()
                    .map(|secret| secret.secret_name.as_ref() == Some(&secret_source.name))
                    .unwrap_or(false)
            });
            match existing_volume {
                Some(v) => v,
                None => {
                    let mut volume = Volume::default();
                    volume.name = variable.name.clone();
                    let mut secret = SecretVolumeSource::default();
                    // TODO(april): the following 420 is the default from Kubernetes but it's
                    // confusing. Why does the group have write? We set read_only below, what does
                    // this even mean?
                    secret.default_mode = Some(420);
                    secret.secret_name = Some(secret_source.name.clone());
                    secret.items = Some(Vec::new());
                    volume.secret = Some(secret);
                    volumes.push(volume);
                    volumes.last_mut().unwrap()
                }
            }
        }
    };

    match source {
        VariableSource::SecretKeyRef(_) => {
            let existing_mount = volume_mounts
                .iter()
                .find(|mount| mount.name == volume.name && mount.mount_path == parent);
            match existing_mount {
                Some(m) => m,
                None => {
                    // TODO(april): can we mount the same volume multiple times?
                    let mut mount = VolumeMount::default();
                    mount.name = volume.name.clone();
                    mount.read_only = Some(true);
                    mount.mount_path = String::from(parent);
                    volume_mounts.push(mount);
                    volume_mounts.last().unwrap()
                }
            }
        }
    };

    match source {
        VariableSource::SecretKeyRef(secret_source) => {
            let Some(secret) = volume.secret.as_mut() else {
                unreachable!("Expected secret");
            };
            let Some(items) = secret.items.as_mut() else {
                unreachable!("Expected items");
            };
            let existing_item = items
                .iter()
                .find(|i| secret_source.key == i.key && filename == i.path);
            match existing_item {
                Some(_) => (),
                None => {
                    items.push(KeyToPath {
                        key: secret_source.key.clone(),
                        mode: None,
                        path: String::from(filename),
                    });
                }
            }
        }
    };

    Ok(RenderedArgument::String(variable.path.clone()))
}
