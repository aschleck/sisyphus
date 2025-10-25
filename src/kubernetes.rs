use anyhow::{anyhow, bail, Result};
use kube::{
    api::{ApiResource, DynamicObject},
    config::KubeConfigOptions,
    discovery::{ApiCapabilities, Scope},
    Discovery, ResourceExt,
};
use serde_json::Value as JsonValue;
use std::{
    collections::{BTreeMap, HashMap},
    fmt,
};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct KubernetesKey {
    pub name: String,
    pub kind: String,
    pub api_version: String,
    pub namespace: Option<String>,
    pub cluster: String,
}

impl fmt::Display for KubernetesKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(namespace) = &self.namespace {
            write!(
                f,
                "{} {}/{} ({})",
                self.kind, namespace, self.name, self.cluster
            )
        } else {
            write!(f, "{} {} ({})", self.kind, self.name, self.cluster)
        }
    }
}

#[derive(Debug)]
pub(crate) struct KubernetesResources {
    pub by_key: BTreeMap<KubernetesKey, DynamicObject>,
    pub namespaces: BTreeMap<KubernetesKey, DynamicObject>,
}

pub(crate) const MANAGER: &str = "sisyphus";

pub(crate) fn clear_unmanaged_fields(value: &mut JsonValue, managed: &JsonValue) -> Result<()> {
    match (value, managed) {
        (JsonValue::Array(value), JsonValue::Object(managed)) => {
            if managed.len() == 0 {
                // We own the entire object, check nothing
                return Ok(());
            }

            let mut selectors = Vec::new();
            for (k, v) in managed {
                let Some((t, s)) = k.split_once(":") else {
                    bail!("Unknown selector {}", k);
                };
                if t != "k" {
                    bail!("Unknown type of selector {}", t);
                }
                selectors.push((
                    serde_json::from_str::<serde_json::Map<String, JsonValue>>(s)?,
                    v,
                ));
            }

            let mut managers = Vec::new();
            value.retain_mut(|item| {
                for (selector, managed) in &selectors {
                    let mut matches = true;
                    for (k, v) in selector {
                        if item.get(k) != Some(v) {
                            matches = false;
                            break;
                        }
                    }

                    if matches {
                        managers.push(managed);
                        return true;
                    }
                }
                false
            });

            for i in 0..value.len() {
                clear_unmanaged_fields(value.get_mut(i).unwrap(), managers.get(i).unwrap())?;
            }
        }
        (JsonValue::Object(value), JsonValue::Object(managed)) => {
            if managed.len() == 0 {
                // We own the entire object, check nothing
                return Ok(());
            }

            let keys = value
                .keys()
                .into_iter()
                .map(|k| k.clone())
                .collect::<Vec<_>>();
            for k in keys {
                match managed.get(&format!("f:{}", k)) {
                    Some(m) => {
                        let v = value.get_mut(&k).unwrap();
                        clear_unmanaged_fields(v, m)?;
                    }
                    None => {
                        value.remove(&k);
                    }
                }
            }
        }
        _ => {}
    }
    Ok(())
}

pub(crate) async fn get_kubernetes_clients(
    keys: impl IntoIterator<Item = &KubernetesKey>,
) -> Result<(
    HashMap<String, kube::Client>,
    HashMap<(String, String), (ApiResource, ApiCapabilities)>,
)> {
    let mut clients = HashMap::new();
    for key in keys.into_iter() {
        let config = kube::Config::from_kubeconfig(&KubeConfigOptions {
            context: Some(key.cluster.to_string()),
            cluster: None,
            user: None,
        })
        .await?;
        clients.insert(key.cluster.to_string(), kube::Client::try_from(config)?);
    }
    if clients.len() == 0 {
        return Ok((HashMap::new(), HashMap::new()));
    }

    let first = clients.values().next().unwrap().clone();
    let discovery = Discovery::new(first).run().await?;
    let mut types = HashMap::new();
    for group in discovery.groups() {
        for (ar, caps) in group.recommended_resources() {
            types.insert((ar.api_version.clone(), ar.kind.clone()), (ar, caps));
        }
    }
    Ok((clients, types))
}

pub(crate) fn get_kubernetes_api(
    key: &KubernetesKey,
    clients: &HashMap<String, kube::Client>,
    types: &HashMap<(String, String), (ApiResource, ApiCapabilities)>,
) -> Result<kube::Api<DynamicObject>> {
    let client = clients
        .get(&key.cluster)
        .ok_or_else(|| anyhow!("No client defined for {}", key.cluster))?
        .clone();
    let Some((ar, caps)) = types.get(&(key.api_version.clone(), key.kind.clone())) else {
        bail!("Unable to find type {} in {}", key.kind, key.api_version);
    };
    Ok(match caps.scope {
        Scope::Cluster => kube::Api::all_with(client, ar),
        Scope::Namespaced => {
            kube::Api::namespaced_with(client, key.namespace.as_ref().map_or("default", |v| v), ar)
        }
    })
}

pub(crate) struct MungeOptions {
    pub munge_managed_fields: bool,
    pub munge_secret_data: bool,
}

pub(crate) fn munge_ignored_fields(
    have: &mut KubernetesResources,
    want: &mut KubernetesResources,
    options: MungeOptions,
) -> Result<()> {
    let us = Some(MANAGER.to_string());

    for (key, w) in &mut want.by_key {
        munge_single_ignored_fields(&key, have.by_key.get_mut(&key), w, &options, &us)?;
    }
    for (key, w) in &mut want.namespaces {
        munge_single_ignored_fields(&key, have.namespaces.get_mut(&key), w, &options, &us)?;
    }
    Ok(())
}

fn munge_single_ignored_fields(
    key: &KubernetesKey,
    have: Option<&mut DynamicObject>,
    want: &mut DynamicObject,
    options: &MungeOptions,
    us: &Option<String>,
) -> Result<()> {
    if let Some(h) = have {
        if options.munge_secret_data && key.api_version == "v1" && key.kind == "Secret" {
            let hd = h
                .data
                .as_object()
                .ok_or_else(|| anyhow!("data must be an object"))?;
            let wd = want
                .data
                .as_object_mut()
                .ok_or_else(|| anyhow!("data must be an object"))?;
            wd.remove("stringData");
            wd.insert(
                "data".to_string(),
                hd.get("data").map_or(JsonValue::Null, |v| v.clone()),
            );

            if options.munge_managed_fields {
                let hm = h
                    .managed_fields_mut()
                    .iter_mut()
                    .find(|m| m.manager == *us)
                    .map(|m| m.fields_v1.as_mut())
                    .flatten()
                    .map(|m| m.0.as_object_mut())
                    .flatten();
                if let Some(hmo) = hm {
                    hmo.remove("f:stringData")
                        .map(|v| hmo.insert("f:data".to_string(), v));
                }
            }
        }

        if options.munge_managed_fields {
            let hm = h
                .managed_fields()
                .iter()
                .find(|m| m.manager == *us)
                .map(|m| m.fields_v1.as_ref().map(|m| m.0.clone()))
                .flatten()
                .unwrap_or(JsonValue::Object(serde_json::Map::new()));
            let mut hv = serde_json::to_value(&mut *h)?;
            clear_unmanaged_fields(&mut hv, &hm)?;
            *h = serde_json::from_value(hv)?;
        }

        h.metadata.name = want.metadata.name.clone();
        h.metadata.namespace = want.metadata.namespace.clone();
        h.types = want.types.clone();
    }
    Ok(())
}
