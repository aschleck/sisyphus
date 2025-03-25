use kube::api::DynamicObject;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum SisyphusResource {
    Deployment(Deployment),
    KubernetesYaml(KubernetesYaml),
}

pub trait HasKind {
    fn kind(&self) -> &'static str;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Deployment {
    pub api_version: String,
    pub metadata: Metadata,
    pub config: DeploymentConfig,
    pub footprint: BTreeMap<String, FootprintEntry>,
}

impl HasKind for Deployment {
    fn kind(&self) -> &'static str {
        "Deployment"
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct KubernetesYaml {
    pub api_version: String,
    pub metadata: Metadata,
    pub clusters: Vec<String>,
    pub objects: Option<Vec<DynamicObject>>,
    pub sources: Option<Vec<String>>,
}

impl HasKind for KubernetesYaml {
    fn kind(&self) -> &'static str {
        "KubernetesYaml"
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Metadata {
    pub annotations: Option<BTreeMap<String, String>>,
    pub name: String,
    pub labels: Option<BTreeMap<String, String>>,
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
