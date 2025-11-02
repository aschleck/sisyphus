use kube::api::DynamicObject;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum SisyphusResource {
    KubernetesYaml(KubernetesYaml),
    #[serde(rename = "CronJob")]
    SisyphusCronJob(SisyphusCronJob),
    #[serde(rename = "Deployment")]
    SisyphusDeployment(SisyphusDeployment),
    SisyphusYaml(SisyphusYaml),
}

pub trait HasKind {
    fn kind(&self) -> &'static str;
}

pub trait HasConfigImage {
    fn config_image<'a>(&'a self) -> &'a String;
    fn set_config_image(&mut self, image: String) -> ();
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
pub struct SisyphusCronJob {
    pub api_version: String,
    pub metadata: Metadata,
    pub config: CronJobConfig,
    pub footprint: BTreeMap<String, CronJobFootprintEntry>,
}

impl HasConfigImage for SisyphusCronJob {
    fn config_image<'a>(&'a self) -> &'a String {
        &self.config.image
    }

    fn set_config_image(&mut self, image: String) -> () {
        self.config.image = image
    }
}

impl HasKind for SisyphusCronJob {
    fn kind(&self) -> &'static str {
        "SisyphusCronJob"
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SisyphusDeployment {
    pub api_version: String,
    pub metadata: Metadata,
    pub config: DeploymentConfig,
    pub footprint: BTreeMap<String, DeploymentFootprintEntry>,
}

impl HasConfigImage for SisyphusDeployment {
    fn config_image<'a>(&'a self) -> &'a String {
        &self.config.image
    }

    fn set_config_image(&mut self, image: String) -> () {
        self.config.image = image
    }
}

impl HasKind for SisyphusDeployment {
    fn kind(&self) -> &'static str {
        "SisyphusDeployment"
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
pub struct CronJobConfig {
    pub env: String,
    pub image: String,
    pub schedule: String,
    #[serde(default)]
    pub variables: BTreeMap<String, VariableSource>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CronJobFootprintEntry {}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DeploymentConfig {
    pub env: String,
    pub image: String,
    pub service: Option<DeploymentServiceConfig>,
    #[serde(default)]
    pub variables: BTreeMap<String, VariableSource>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DeploymentFootprintEntry {
    pub replicas: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DeploymentServiceConfig {
    pub ports: BTreeMap<String, ServicePort>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ServicePort {
    pub name: Option<String>,
    pub number: i32,
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
