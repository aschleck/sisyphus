use k8s_openapi::apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::LabelSelector};
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
    #[serde(default)]
    pub variables: BTreeMap<String, VariableSource>,
}

impl HasKind for SisyphusYaml {
    fn kind(&self) -> &'static str {
        "SisyphusYaml"
    }
}

/// The variables that every directory inherits, from the `variables.yaml` beside the directories.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct VariablesYaml {
    pub api_version: String,
    // The other objects read sources such as `secretKeyRef:` as maps because serde buffers them for
    // `kind`, but serde_yaml reads an enum from a file directly as a `!tag` unless told otherwise.
    #[serde(default, with = "serde_yaml::with::singleton_map_recursive")]
    pub variables: BTreeMap<String, VariableSource>,
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
    pub concurrency_policy: Option<String>,
    pub env: String,
    pub image: String,
    pub restart_policy: Option<String>,
    pub schedule: String,
    pub service_account: Option<String>,
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
    pub service_account: Option<String>,
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
    ClusterTrustBundleRef(ClusterTrustBundleRef),
    ConfigMapKeyRef(KubernetesKeyRef),
    FieldRef(FieldRef),
    PodCertificate(PodCertificate),
    ResourceFieldRef(ResourceFieldRef),
    SecretKeyRef(KubernetesKeyRef),
    ServiceAccountToken(ServiceAccountToken),
}

/// A cluster trust bundle carries one blob of trust anchors, so a reference to one names no key. It
/// names either one bundle, or a signer and a selector of the signer's bundles.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ClusterTrustBundleRef {
    pub label_selector: Option<LabelSelector>,
    pub name: Option<String>,
    pub signer_name: Option<String>,
}

/// A field of the pod itself, such as `metadata.namespace` or `status.podIP`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FieldRef {
    pub field_path: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct KubernetesKeyRef {
    pub key: String,
    pub name: String,
}

/// A key that kubelet generates for the pod, and the certificate chain that the signer issues for
/// it, in one file.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PodCertificate {
    pub key_type: String,
    pub max_expiration_seconds: Option<i32>,
    pub signer_name: String,
}

/// A request or a limit of the container itself, such as `limits.memory`, in units of `divisor`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ResourceFieldRef {
    pub divisor: Option<Quantity>,
    pub resource: String,
}

/// A token for the pod's service account that kubelet requests for `audience` and rotates.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ServiceAccountToken {
    pub audience: Option<String>,
    pub expiration_seconds: Option<i64>,
}
