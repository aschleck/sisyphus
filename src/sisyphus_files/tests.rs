use super::*;
use anyhow::Result;
use std::{fs, path::Path};
use tempfile::TempDir;

const ROOT_VARIABLES: &str = r#"
apiVersion: sisyphus/v1
variables:
  credentials:
    secretKeyRef:
      name: root
      key: tls.crt
  trust-bundle:
    clusterTrustBundleRef:
      name: root-bundle
"#;

fn write(root: &Path, path: &str, contents: &str) {
    let path = root.join(path);
    fs::create_dir_all(path.parent().expect("Expected a parent directory"))
        .expect("Unable to create the directory");
    fs::write(path, contents).expect("Unable to write the file");
}

fn deployment(name: &str, variables: &str) -> String {
    format!(
        r#"
apiVersion: sisyphus/v1
kind: Deployment
metadata:
  name: {name}
config:
  env: prod
  image: example/{name}:latest
  variables: {variables}
footprint:
  prod:
    replicas: 1
"#
    )
}

fn index(sources: &[&str], variables: &str) -> String {
    format!(
        r#"
apiVersion: sisyphus/v1
kind: SisyphusYaml
metadata:
  name: yaml
sources: [{}]
variables: {variables}
"#,
        sources.join(", ")
    )
}

/// What `variable` of the Deployment `name` reads: the name of its secret, bundle, or signer, its
/// field, or its audience.
fn source_name(
    resources: &SisyphusResources,
    namespace: &str,
    name: &str,
    variable: &str,
) -> Option<String> {
    let key = SisyphusKey {
        api_version: "sisyphus/v1".to_string(),
        kind: "SisyphusDeployment".to_string(),
        name: name.to_string(),
    };
    let SisyphusResource::SisyphusDeployment(deployment) =
        &resources.by_namespace_by_key[namespace][&key]
    else {
        panic!("Expected {} to be a Deployment", name);
    };
    deployment
        .config
        .variables
        .get(variable)
        .and_then(|source| match source {
            VariableSource::ClusterTrustBundleRef(r) => r.name.clone().or(r.signer_name.clone()),
            VariableSource::ConfigMapKeyRef(r) | VariableSource::SecretKeyRef(r) => {
                Some(r.name.clone())
            }
            VariableSource::FieldRef(r) => Some(r.field_path.clone()),
            VariableSource::PodCertificate(c) => Some(c.signer_name.clone()),
            VariableSource::ResourceFieldRef(r) => Some(r.resource.clone()),
            VariableSource::ServiceAccountToken(t) => t.audience.clone(),
        })
}

#[test]
fn test_root_variables_reach_each_namespace() -> Result<()> {
    let root = TempDir::new()?;
    write(root.path(), "variables.yaml", ROOT_VARIABLES);
    write(root.path(), "april/index.yaml", &deployment("echo", "{}"));
    write(root.path(), "joe/index.yaml", &deployment("echo", "{}"));

    let resources = get_sisyphus_resources_from_files(root.path())?;

    for namespace in ["april", "joe"] {
        assert_eq!(
            source_name(&resources, namespace, "echo", "credentials").as_deref(),
            Some("root")
        );
        assert_eq!(
            source_name(&resources, namespace, "echo", "trust-bundle").as_deref(),
            Some("root-bundle")
        );
    }
    Ok(())
}

#[test]
fn test_directories_load_without_variables_yaml() -> Result<()> {
    let root = TempDir::new()?;
    write(root.path(), "april/index.yaml", &deployment("echo", "{}"));

    let resources = get_sisyphus_resources_from_files(root.path())?;

    assert_eq!(
        source_name(&resources, "april", "echo", "credentials"),
        None
    );
    Ok(())
}

#[test]
fn test_namespace_index_overrides_the_root() -> Result<()> {
    let root = TempDir::new()?;
    write(root.path(), "variables.yaml", ROOT_VARIABLES);
    write(
        root.path(),
        "april/index.yaml",
        &index(
            &["echo.yaml"],
            "{credentials: {secretKeyRef: {name: april, key: tls.crt}}}",
        ),
    );
    write(root.path(), "april/echo.yaml", &deployment("echo", "{}"));

    let resources = get_sisyphus_resources_from_files(root.path())?;

    assert_eq!(
        source_name(&resources, "april", "echo", "credentials").as_deref(),
        Some("april")
    );
    assert_eq!(
        source_name(&resources, "april", "echo", "trust-bundle").as_deref(),
        Some("root-bundle")
    );
    Ok(())
}

#[test]
fn test_deployment_overrides_what_it_inherits() -> Result<()> {
    let root = TempDir::new()?;
    write(root.path(), "variables.yaml", ROOT_VARIABLES);
    write(
        root.path(),
        "april/index.yaml",
        &deployment(
            "echo",
            "{credentials: {secretKeyRef: {name: echo, key: tls.crt}}}",
        ),
    );

    let resources = get_sisyphus_resources_from_files(root.path())?;

    assert_eq!(
        source_name(&resources, "april", "echo", "credentials").as_deref(),
        Some("echo")
    );
    Ok(())
}

/// A nested index's variables reach the files it lists and none of their siblings.
#[test]
fn test_nested_index_variables_stay_in_their_sources() -> Result<()> {
    let root = TempDir::new()?;
    write(
        root.path(),
        "april/index.yaml",
        &index(&["nested.yaml", "sibling.yaml"], "{}"),
    );
    write(
        root.path(),
        "april/nested.yaml",
        &index(
            &["inner.yaml"],
            "{credentials: {secretKeyRef: {name: nested, key: tls.crt}}}",
        ),
    );
    write(root.path(), "april/inner.yaml", &deployment("inner", "{}"));
    write(
        root.path(),
        "april/sibling.yaml",
        &deployment("sibling", "{}"),
    );

    let resources = get_sisyphus_resources_from_files(root.path())?;

    assert_eq!(
        source_name(&resources, "april", "inner", "credentials").as_deref(),
        Some("nested")
    );
    assert_eq!(
        source_name(&resources, "april", "sibling", "credentials"),
        None
    );
    Ok(())
}

/// Both ways that Sisyphus reads variables, from `variables.yaml` and from an object, read each
/// kind of source.
#[test]
fn test_variables_read_each_kind_of_source() -> Result<()> {
    let sources = r#"{
        credentials: {podCertificate: {keyType: ECDSAP256, signerName: escalante.bio/workload}},
        memory: {resourceFieldRef: {resource: limits.memory, divisor: 1Mi}},
        namespace: {fieldRef: {fieldPath: metadata.namespace}},
        sts-token: {serviceAccountToken: {audience: sts.escalante.bio, expirationSeconds: 3600}},
        trust-bundle: {clusterTrustBundleRef: {
            signerName: escalante.bio/workload,
            labelSelector: {matchLabels: {escalante.bio/trust: root}},
        }},
    }"#;
    let root = TempDir::new()?;
    write(
        root.path(),
        "variables.yaml",
        &format!("apiVersion: sisyphus/v1\nvariables: {}\n", sources),
    );
    write(root.path(), "april/index.yaml", &deployment("echo", "{}"));
    write(root.path(), "joe/index.yaml", &deployment("echo", sources));

    let resources = get_sisyphus_resources_from_files(root.path())?;

    for namespace in ["april", "joe"] {
        for (variable, expected) in [
            ("credentials", "escalante.bio/workload"),
            ("memory", "limits.memory"),
            ("namespace", "metadata.namespace"),
            ("sts-token", "sts.escalante.bio"),
            ("trust-bundle", "escalante.bio/workload"),
        ] {
            assert_eq!(
                source_name(&resources, namespace, "echo", variable).as_deref(),
                Some(expected),
                "{} in {}",
                variable,
                namespace
            );
        }
    }
    Ok(())
}

#[test]
fn test_variables_yaml_refuses_other_fields() -> Result<()> {
    let root = TempDir::new()?;
    write(
        root.path(),
        "variables.yaml",
        "apiVersion: sisyphus/v1\nsources: [april/index.yaml]\n",
    );

    let error = get_sisyphus_resources_from_files(root.path())
        .expect_err("Expected variables.yaml to refuse sources");
    assert!(
        format!("{:#}", error).contains("unknown field `sources`"),
        "unexpected error: {:#}",
        error
    );
    Ok(())
}
