use kube::api::DynamicObject;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum SisyphusResource {
    KubernetesYaml(KubernetesYaml),
    SisyphusDeployment(SisyphusDeployment),
    SisyphusYaml(SisyphusYaml),
}

pub trait HasKind {
    fn kind(&self) -> &'static str;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SisyphusDeployment {
    pub api_version: String,
    pub metadata: Metadata,
    pub config: DeploymentConfig,
    pub footprint: BTreeMap<String, FootprintEntry>,
}

impl HasKind for SisyphusDeployment {
    fn kind(&self) -> &'static str {
        "SisyphusDeployment"
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct KubernetesYaml {
    pub api_version: String,
    pub metadata: Metadata,
    pub clusters: Vec<String>,
    #[serde(default)]
    pub objects: Vec<DynamicObject>,
    #[serde(default)]
    pub sources: Vec<String>,
}

impl HasKind for KubernetesYaml {
    fn kind(&self) -> &'static str {
        "KubernetesYaml"
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SisyphusYaml {
    pub api_version: String,
    pub metadata: Metadata,
    #[serde(default)]
    pub sources: Vec<String>,
}

impl HasKind for SisyphusYaml {
    fn kind(&self) -> &'static str {
        "SisyphusYaml"
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Metadata {
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,
    pub name: String,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DeploymentConfig {
    pub env: String,
    pub image: String,
    pub variables: BTreeMap<String, VariableSource>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum VariableSource {
    SecretKeyRef(KubernetesSecretKeyRef),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct KubernetesSecretKeyRef {
    pub name: String,
    pub key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FootprintEntry {
    pub replicas: i32,
}
