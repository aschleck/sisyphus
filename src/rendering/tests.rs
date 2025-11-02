use super::*;
use crate::config_image::{Port, Protocol};
use crate::sisyphus_yaml::ServicePort as SisyphusServicePort;

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
