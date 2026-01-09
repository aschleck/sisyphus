use super::*;
use crate::config_image::{Port, Protocol};
use crate::sisyphus_yaml::ServicePort as SisyphusServicePort;

#[test]
fn test_process_cronjob_footprint() -> Result<()> {
    use crate::sisyphus_yaml::{CronJobConfig, CronJobFootprintEntry, Metadata, SisyphusCronJob};

    let cronjob = SisyphusCronJob {
        api_version: "sisyphus/v1".to_string(),
        metadata: Metadata {
            name: "test-cronjob".to_string(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
        },
        config: CronJobConfig {
            concurrency_policy: None,
            env: "prod".to_string(),
            image: "test-image".to_string(),
            schedule: "0 0 * * *".to_string(),
            variables: BTreeMap::new(),
        },
        footprint: BTreeMap::from([
            ("cluster1".to_string(), CronJobFootprintEntry {}),
            ("cluster2".to_string(), CronJobFootprintEntry {}),
        ]),
    };

    let metadata = ObjectMeta {
        name: Some("test-cronjob".to_string()),
        namespace: Some("default".to_string()),
        labels: Some(BTreeMap::from([(
            "app".to_string(),
            "test-cronjob".to_string(),
        )])),
        ..Default::default()
    };

    let mut container = Container::default();
    container.name = "test-cronjob".to_string();
    container.image = Some("test-image:latest".to_string());

    let pod_spec = build_pod_spec(container, Vec::new());

    let mut by_key = BTreeMap::new();

    process_cronjob_footprint(
        &cronjob,
        &metadata,
        &None,
        "0 0 * * *",
        &pod_spec,
        "default",
        &mut by_key,
    )?;

    // Verify two CronJobs were created (one per cluster)
    assert_eq!(by_key.len(), 2);

    // Verify both clusters have their CronJobs
    let cluster1_keys: Vec<_> = by_key.keys().filter(|k| k.cluster == "cluster1").collect();
    assert_eq!(cluster1_keys.len(), 1);
    assert_eq!(cluster1_keys[0].kind, "CronJob");
    assert_eq!(cluster1_keys[0].api_version, "batch/v1");

    let cluster2_keys: Vec<_> = by_key.keys().filter(|k| k.cluster == "cluster2").collect();
    assert_eq!(cluster2_keys.len(), 1);
    assert_eq!(cluster2_keys[0].kind, "CronJob");

    Ok(())
}

#[test]
fn test_cronjob_spec_structure() -> Result<()> {
    use crate::sisyphus_yaml::{CronJobConfig, CronJobFootprintEntry, Metadata, SisyphusCronJob};

    let cronjob = SisyphusCronJob {
        api_version: "sisyphus/v1".to_string(),
        metadata: Metadata {
            name: "test-cronjob".to_string(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
        },
        config: CronJobConfig {
            concurrency_policy: None,
            env: "prod".to_string(),
            image: "test-image".to_string(),
            schedule: "*/5 * * * *".to_string(),
            variables: BTreeMap::new(),
        },
        footprint: BTreeMap::from([("cluster1".to_string(), CronJobFootprintEntry {})]),
    };

    let metadata = ObjectMeta {
        name: Some("test-cronjob".to_string()),
        namespace: Some("default".to_string()),
        ..Default::default()
    };

    let mut container = Container::default();
    container.name = "test-cronjob".to_string();
    container.image = Some("test-image:latest".to_string());

    let pod_spec = build_pod_spec(container, Vec::new());

    let mut by_key = BTreeMap::new();

    process_cronjob_footprint(
        &cronjob,
        &metadata,
        &None,
        "*/5 * * * *",
        &pod_spec,
        "default",
        &mut by_key,
    )?;

    // Get the created CronJob
    let cronjob_obj = by_key.values().next().unwrap();

    // Verify the schedule is set correctly
    let spec = cronjob_obj.data.get("spec").unwrap();
    let schedule = spec.get("schedule").unwrap().as_str().unwrap();
    assert_eq!(schedule, "*/5 * * * *");

    // Verify jobTemplate structure exists
    assert!(spec.get("jobTemplate").is_some());
    let job_template = spec.get("jobTemplate").unwrap();
    assert!(job_template.get("spec").is_some());

    // Verify pod template structure
    let job_spec = job_template.get("spec").unwrap();
    assert!(job_spec.get("template").is_some());
    let pod_template = job_spec.get("template").unwrap();
    assert!(pod_template.get("spec").is_some());

    Ok(())
}

#[test]
fn test_cronjob_concurrency_policy() -> Result<()> {
    use crate::sisyphus_yaml::{CronJobConfig, CronJobFootprintEntry, Metadata, SisyphusCronJob};

    let cronjob = SisyphusCronJob {
        api_version: "sisyphus/v1".to_string(),
        metadata: Metadata {
            name: "test-cronjob".to_string(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
        },
        config: CronJobConfig {
            concurrency_policy: Some("Forbid".to_string()),
            env: "prod".to_string(),
            image: "test-image".to_string(),
            schedule: "0 * * * *".to_string(),
            variables: BTreeMap::new(),
        },
        footprint: BTreeMap::from([("cluster1".to_string(), CronJobFootprintEntry {})]),
    };

    let metadata = ObjectMeta {
        name: Some("test-cronjob".to_string()),
        namespace: Some("default".to_string()),
        ..Default::default()
    };

    let mut container = Container::default();
    container.name = "test-cronjob".to_string();
    container.image = Some("test-image:latest".to_string());

    let pod_spec = build_pod_spec(container, Vec::new());

    let mut by_key = BTreeMap::new();

    process_cronjob_footprint(
        &cronjob,
        &metadata,
        &Some("Forbid".to_string()),
        "0 * * * *",
        &pod_spec,
        "default",
        &mut by_key,
    )?;

    let cronjob_obj = by_key.values().next().unwrap();
    let spec = cronjob_obj.data.get("spec").unwrap();
    let concurrency_policy = spec.get("concurrencyPolicy").unwrap().as_str().unwrap();
    assert_eq!(concurrency_policy, "Forbid");

    Ok(())
}

#[test]
fn test_process_deployment_footprint() -> Result<()> {
    use crate::sisyphus_yaml::{
        DeploymentConfig, DeploymentFootprintEntry, Metadata, SisyphusDeployment,
    };

    let deployment = SisyphusDeployment {
        api_version: "sisyphus/v1".to_string(),
        metadata: Metadata {
            name: "test-deployment".to_string(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
        },
        config: DeploymentConfig {
            env: "prod".to_string(),
            image: "test-image".to_string(),
            service: None,
            variables: BTreeMap::new(),
        },
        footprint: BTreeMap::from([
            (
                "cluster1".to_string(),
                DeploymentFootprintEntry { replicas: 3 },
            ),
            (
                "cluster2".to_string(),
                DeploymentFootprintEntry { replicas: 5 },
            ),
        ]),
    };

    let metadata = ObjectMeta {
        name: Some("test-deployment".to_string()),
        namespace: Some("default".to_string()),
        labels: Some(BTreeMap::from([(
            "app".to_string(),
            "test-deployment".to_string(),
        )])),
        ..Default::default()
    };

    let labels = BTreeMap::from([("app".to_string(), "test-deployment".to_string())]);
    let deployment_spec = build_base_deployment_spec(labels);

    let mut by_key = BTreeMap::new();

    process_deployment_footprint(
        &deployment,
        &metadata,
        &deployment_spec,
        &None,
        "default",
        &mut by_key,
    )?;

    // Verify two Deployments were created (one per cluster)
    assert_eq!(by_key.len(), 2);

    // Verify cluster1 has correct replicas
    let cluster1_keys: Vec<_> = by_key.keys().filter(|k| k.cluster == "cluster1").collect();
    assert_eq!(cluster1_keys.len(), 1);
    assert_eq!(cluster1_keys[0].kind, "Deployment");
    assert_eq!(cluster1_keys[0].api_version, "apps/v1");

    let cluster1_obj = by_key.get(cluster1_keys[0]).unwrap();
    let cluster1_replicas = cluster1_obj
        .data
        .get("spec")
        .and_then(|s| s.get("replicas"))
        .and_then(|r| r.as_i64())
        .unwrap();
    assert_eq!(cluster1_replicas, 3);

    // Verify cluster2 has correct replicas
    let cluster2_keys: Vec<_> = by_key.keys().filter(|k| k.cluster == "cluster2").collect();
    assert_eq!(cluster2_keys.len(), 1);
    assert_eq!(cluster2_keys[0].kind, "Deployment");

    let cluster2_obj = by_key.get(cluster2_keys[0]).unwrap();
    let cluster2_replicas = cluster2_obj
        .data
        .get("spec")
        .and_then(|s| s.get("replicas"))
        .and_then(|r| r.as_i64())
        .unwrap();
    assert_eq!(cluster2_replicas, 5);

    Ok(())
}

#[test]
fn test_process_deployment_footprint_with_service() -> Result<()> {
    use crate::sisyphus_yaml::{
        DeploymentConfig, DeploymentFootprintEntry, Metadata, SisyphusDeployment,
    };

    let deployment = SisyphusDeployment {
        api_version: "sisyphus/v1".to_string(),
        metadata: Metadata {
            name: "test-deployment".to_string(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
        },
        config: DeploymentConfig {
            env: "prod".to_string(),
            image: "test-image".to_string(),
            service: None,
            variables: BTreeMap::new(),
        },
        footprint: BTreeMap::from([(
            "cluster1".to_string(),
            DeploymentFootprintEntry { replicas: 2 },
        )]),
    };

    let metadata = ObjectMeta {
        name: Some("test-deployment".to_string()),
        namespace: Some("default".to_string()),
        ..Default::default()
    };

    let labels = BTreeMap::from([("app".to_string(), "test-deployment".to_string())]);
    let deployment_spec = build_base_deployment_spec(labels.clone());

    // Create a service spec
    let mut service_spec = ServiceSpec::default();
    service_spec.selector = Some(labels);
    service_spec.ports = Some(vec![ServicePort {
        name: Some("http".to_string()),
        port: 80,
        ..Default::default()
    }]);

    let mut by_key = BTreeMap::new();

    process_deployment_footprint(
        &deployment,
        &metadata,
        &deployment_spec,
        &Some(service_spec),
        "default",
        &mut by_key,
    )?;

    // Verify both Deployment and Service were created
    assert_eq!(by_key.len(), 2);

    // Verify Deployment exists
    let deployment_keys: Vec<_> = by_key.keys().filter(|k| k.kind == "Deployment").collect();
    assert_eq!(deployment_keys.len(), 1);

    // Verify Service exists
    let service_keys: Vec<_> = by_key.keys().filter(|k| k.kind == "Service").collect();
    assert_eq!(service_keys.len(), 1);
    assert_eq!(service_keys[0].api_version, "v1");

    Ok(())
}

#[test]
fn test_build_base_deployment_spec() {
    let labels = BTreeMap::from([
        ("app".to_string(), "test-app".to_string()),
        ("env".to_string(), "prod".to_string()),
    ]);

    let spec = build_base_deployment_spec(labels.clone());

    // Verify selector
    assert_eq!(spec.selector.match_labels, Some(labels.clone()));

    // Verify defaults
    assert_eq!(spec.progress_deadline_seconds, Some(600));
    assert_eq!(spec.revision_history_limit, Some(10));

    // Verify strategy
    assert!(spec.strategy.is_some());
    let strategy = spec.strategy.unwrap();
    assert_eq!(strategy.type_, Some("RollingUpdate".to_string()));
    assert!(strategy.rolling_update.is_some());

    // Verify template metadata
    let Some(template_metadata) = spec.template.metadata else {
        panic!("Expected metadata")
    };
    assert_eq!(template_metadata.labels, Some(labels));
}

#[test]
fn test_build_pod_spec() {
    let mut container = Container::default();
    container.name = "test-container".to_string();
    container.image = Some("test-image:latest".to_string());

    let mut volume = Volume::default();
    volume.name = "test-volume".to_string();
    let volumes = vec![volume.clone()];

    let pod_spec = build_pod_spec(container.clone(), volumes.clone());

    // Verify container
    assert_eq!(pod_spec.containers.len(), 1);
    assert_eq!(pod_spec.containers[0].name, "test-container");

    // Verify volumes
    assert_eq!(pod_spec.volumes, Some(volumes));

    // Verify defaults
    assert_eq!(pod_spec.dns_policy, Some("ClusterFirst".to_string()));
    assert_eq!(pod_spec.restart_policy, Some("Always".to_string()));
    assert_eq!(
        pod_spec.scheduler_name,
        Some("default-scheduler".to_string())
    );
    assert!(pod_spec.security_context.is_some());
    assert_eq!(pod_spec.termination_grace_period_seconds, Some(30));
}

#[test]
fn test_build_pod_spec_empty_volumes() {
    let mut container = Container::default();
    container.name = "test-container".to_string();

    let pod_spec = build_pod_spec(container, Vec::new());
    assert_eq!(pod_spec.volumes, None);
}

#[test]
fn test_render_deployment_metadata() -> Result<()> {
    let deployment_name = "my-deployment".to_string();
    let label_namespace = "myapp.io";
    let deployment_labels = BTreeMap::from([("custom".to_string(), "label".to_string())]);
    let deployment_annotations =
        BTreeMap::from([("annotation-key".to_string(), "annotation-value".to_string())]);
    let namespace = Some("production".to_string());

    let metadata = render_deployment_metadata(
        &deployment_name,
        label_namespace,
        &deployment_labels,
        &deployment_annotations,
        &namespace,
    )?;

    assert_eq!(metadata.name, Some(deployment_name.clone()));
    assert_eq!(metadata.namespace, namespace);

    let labels = metadata.labels.unwrap();
    assert_eq!(
        labels,
        BTreeMap::from([
            ("custom".to_string(), "label".to_string()),
            ("myapp.io/app".to_string(), deployment_name),
        ])
    );

    let annotations = metadata.annotations.unwrap();
    assert_eq!(annotations, deployment_annotations);

    Ok(())
}

#[test]
fn test_render_deployment_metadata_no_namespace() {
    let result = render_deployment_metadata(
        "my-deployment",
        "myapp.io",
        &BTreeMap::new(),
        &BTreeMap::new(),
        &None,
    );

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Namespace must be explicit"));
}

#[test]
fn test_build_service_spec_no_ports() -> Result<()> {
    let config_service = None;
    let ports = BTreeMap::new();
    let labels = BTreeMap::new();

    let result = build_service_spec(&config_service, &ports, labels)?;

    assert!(result.is_none());

    Ok(())
}

#[test]
fn test_build_service_spec_with_ports() -> Result<()> {
    let service_ports = BTreeMap::from([(
        "http".to_string(),
        SisyphusServicePort {
            name: Some("web".to_string()),
            number: 80,
        },
    )]);

    let config_service = Some(DeploymentServiceConfig {
        ports: service_ports,
    });

    let mut container_port = ContainerPort::default();
    container_port.name = Some("http".to_string());
    container_port.container_port = 8080;
    container_port.protocol = Some("TCP".to_string());
    let container_ports = BTreeMap::from([("http".to_string(), container_port)]);

    let labels = BTreeMap::from([("app".to_string(), "test".to_string())]);

    let result = build_service_spec(&config_service, &container_ports, labels.clone())?;

    assert!(result.is_some());
    let service_spec = result.unwrap();

    // Verify selector
    assert_eq!(service_spec.selector, Some(labels));

    // Verify ports
    assert!(service_spec.ports.is_some());
    let ports = service_spec.ports.unwrap();
    assert_eq!(ports.len(), 1);
    assert_eq!(ports[0].name, Some("web".to_string()));
    assert_eq!(ports[0].port, 80);
    assert_eq!(ports[0].protocol, Some("TCP".to_string()));

    Ok(())
}

#[test]
fn test_render_argument_string() -> Result<()> {
    let arg = ArgumentValues::Uniform(Argument::String("test-value".to_string()));
    let selector = "prod";
    let mut ports = BTreeMap::new();
    let variables = BTreeMap::new();
    let mut volumes = Vec::new();
    let mut volume_mounts = Vec::new();

    let result = render_argument(
        &arg,
        selector,
        &mut ports,
        &variables,
        &mut volumes,
        &mut volume_mounts,
    )?;

    let Some(RenderedArgument::String(s)) = result else {
        panic!("Expected String variant");
    };
    assert_eq!(s, "test-value");
    Ok(())
}

#[test]
fn test_render_argument_port() -> Result<()> {
    let port = Port {
        name: "http".to_string(),
        number: 8080,
        protocol: Protocol::TCP,
    };
    let arg = ArgumentValues::Uniform(Argument::Port(port));
    let selector = "prod";
    let mut ports = BTreeMap::new();
    let variables = BTreeMap::new();
    let mut volumes = Vec::new();
    let mut volume_mounts = Vec::new();

    let result = render_argument(
        &arg,
        selector,
        &mut ports,
        &variables,
        &mut volumes,
        &mut volume_mounts,
    )?;

    // Verify port was added to ports map
    assert_eq!(ports.len(), 1);
    assert!(ports.contains_key("http"));

    let Some(RenderedArgument::String(s)) = result else {
        panic!("Expected String variant");
    };
    assert_eq!(s, "8080");
    Ok(())
}

#[test]
fn test_render_argument_varying() -> Result<()> {
    let varying_map = BTreeMap::from([
        (
            "prod".to_string(),
            Argument::String("prod-value".to_string()),
        ),
        ("dev".to_string(), Argument::String("dev-value".to_string())),
    ]);

    let arg = ArgumentValues::Varying(varying_map);
    let selector = "prod";
    let mut ports = BTreeMap::new();
    let variables = BTreeMap::new();
    let mut volumes = Vec::new();
    let mut volume_mounts = Vec::new();

    let result = render_argument(
        &arg,
        selector,
        &mut ports,
        &variables,
        &mut volumes,
        &mut volume_mounts,
    )?;

    let Some(RenderedArgument::String(s)) = result else {
        panic!("Expected String variant");
    };
    assert_eq!(s, "prod-value");
    Ok(())
}

#[test]
fn test_render_argument_varying_not_found() -> Result<()> {
    let varying_map = BTreeMap::from([(
        "prod".to_string(),
        Argument::String("prod-value".to_string()),
    )]);

    let arg = ArgumentValues::Varying(varying_map);
    let selector = "dev"; // Not in the map
    let mut ports = BTreeMap::new();
    let variables = BTreeMap::new();
    let mut volumes = Vec::new();
    let mut volume_mounts = Vec::new();

    let result = render_argument(
        &arg,
        selector,
        &mut ports,
        &variables,
        &mut volumes,
        &mut volume_mounts,
    )?;

    assert!(result.is_none());
    Ok(())
}
