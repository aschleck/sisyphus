use anyhow::{Result, anyhow, bail};
use kube::{
    Discovery, ResourceExt,
    api::{ApiResource, DynamicObject},
    config::KubeConfigOptions,
    discovery::{ApiCapabilities, Scope},
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

#[derive(Clone, Debug)]
pub(crate) struct KubernetesResources {
    pub by_key: BTreeMap<KubernetesKey, DynamicObject>,
    pub namespaces: BTreeMap<KubernetesKey, DynamicObject>,
}

pub(crate) const MANAGER: &str = "sisyphus";

struct Selector<'a> {
    data: &'a JsonValue,
    matcher: serde_json::Map<String, JsonValue>,
    used: bool,
}

pub(crate) fn copy_unmanaged_fields(
    have: &JsonValue,
    want: &JsonValue,
    managed: &JsonValue,
    path: &mut Vec<String>,
    remove_patches: &mut Vec<String>,
) -> Result<JsonValue> {
    match (have, want, managed) {
        (JsonValue::Array(h), JsonValue::Array(w), JsonValue::Object(m)) => {
            // Try to find the matching rules for every key
            let mut selectors = Vec::new();
            for (k, v) in m {
                let Some((t, s)) = k.split_once(":") else {
                    bail!("Unknown selector {}", k);
                };
                if t != "k" {
                    bail!("Unknown type of selector {}", t);
                }
                selectors.push(Selector {
                    data: v,
                    matcher: serde_json::from_str::<serde_json::Map<String, JsonValue>>(s)?,
                    used: false,
                });
            }

            let mut copy = Vec::new();
            for i in 0..w.len() {
                let new_value = if i < h.len() {
                    let hv = h.get(i).unwrap();
                    let wv = w.get(i).unwrap();
                    let mv = selectors.iter_mut().find_map(|selector| {
                        if selector.used {
                            return None;
                        }
                        let mut matches = true;
                        for (k, v) in &selector.matcher {
                            if wv.get(k) != Some(v) {
                                matches = false;
                                break;
                            }
                        }

                        if matches {
                            selector.used = true;
                            return Some(selector.data);
                        } else {
                            return None;
                        }
                    });
                    if let JsonValue::Null = wv {
                        // If we're explicitly setting a value to null and it used to have a
                        // value that we didn't own, we're probably trying to clear it
                        match (hv, mv) {
                            (JsonValue::Null, _) => {}
                            (_, None) => {
                                path.push(i.to_string());
                                remove_patches.push(path.join("/"));
                                path.pop();
                                continue;
                            }
                            _ => {}
                        }
                        JsonValue::Null
                    } else {
                        path.push(i.to_string());
                        let result = copy_unmanaged_fields(
                            hv,
                            wv,
                            mv.unwrap_or(&JsonValue::Null),
                            path,
                            remove_patches,
                        )?;
                        path.pop();
                        result
                    }
                } else {
                    w.get(i).unwrap().clone()
                };
                copy.push(new_value);
            }
            Ok(JsonValue::Array(copy))
        }
        (JsonValue::Array(h), JsonValue::Array(w), JsonValue::Null) => {
            // If we don't already own anything, merge the keys half-heartedly
            let mut copy = Vec::new();
            for i in 0..w.len() {
                let new_value = if i < h.len() {
                    match (h.get(i).unwrap(), w.get(i).unwrap()) {
                        // Are we explicitly setting a value to null? If the previous value here
                        // *isn't* null then we're probably trying to delete an element we don't own
                        (hv @ _, JsonValue::Null) => {
                            if let JsonValue::Null = hv {
                            } else {
                                path.push(i.to_string());
                                remove_patches.push(path.join("/"));
                                path.pop();
                                continue;
                            }
                            JsonValue::Null
                        }
                        (hv, wv) => {
                            path.push(i.to_string());
                            let result = copy_unmanaged_fields(
                                hv,
                                wv,
                                &JsonValue::Null,
                                path,
                                remove_patches,
                            )?;
                            path.pop();
                            result
                        }
                    }
                } else {
                    w.get(i).unwrap().clone()
                };
                copy.push(new_value);
            }
            Ok(JsonValue::Array(copy))
        }
        (JsonValue::Object(h), JsonValue::Object(w), JsonValue::Object(managed)) => {
            // When we are adding keys but don't own anything currently, merge all the existing
            // keys according to our merge instructions and then plop our remaining ones on top
            let mut copy = serde_json::Map::new();
            let mut remaining = w.clone();
            for (k, v) in h {
                path.push(k.clone());
                let new_value = copy_unmanaged_fields(
                    v,
                    &remaining.remove(k).unwrap_or(JsonValue::Null),
                    managed.get(&format!("f:{}", k)).unwrap_or(&JsonValue::Null),
                    path,
                    remove_patches,
                )?;
                path.pop();
                copy.insert(k.clone(), new_value);
            }
            for (k, v) in remaining {
                copy.insert(k.clone(), v.clone());
            }
            Ok(JsonValue::Object(copy))
        }
        (JsonValue::Object(h), JsonValue::Object(w), JsonValue::Null) => {
            // When we are adding keys but don't own anything currently, merge all the existing
            // keys and then plop our remaining ones on top
            let mut copy = serde_json::Map::new();
            let mut remaining = w.clone();
            for (k, v) in h {
                path.push(k.clone());
                let new_value = copy_unmanaged_fields(
                    v,
                    &remaining.remove(k).unwrap_or(JsonValue::Null),
                    &JsonValue::Null,
                    path,
                    remove_patches,
                )?;
                path.pop();
                copy.insert(k.clone(), new_value);
            }
            for (k, v) in remaining {
                copy.insert(k, v);
            }
            Ok(JsonValue::Object(copy))
        }
        // If something is already a string, and we put a number, convert it to a string so it
        // doesn't generate a diff
        (JsonValue::String(_), JsonValue::Number(n), _) => Ok(JsonValue::String(n.to_string())),
        (_, JsonValue::Null, JsonValue::Object(_)) => Ok(want.clone()),
        (_, JsonValue::Null, JsonValue::Null) => Ok(have.clone()),
        _ => Ok(want.clone()),
    }
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

pub(crate) fn make_comparable(
    mut from: KubernetesResources,
    mut to: KubernetesResources,
) -> Result<(
    KubernetesResources,
    KubernetesResources,
    HashMap<KubernetesKey, Vec<String>>,
)> {
    let mut path = vec!["".to_string()];
    let mut remove_patches = HashMap::new();
    let us = Some(MANAGER.to_string());
    for (key, w) in &mut to.by_key {
        let mut patches = Vec::new();
        copy_single_unspecified_data(from.by_key.get_mut(&key), w, &mut path, &mut patches, &us)?;
        if patches.len() > 0 {
            remove_patches.insert(key.clone(), patches);
        }
    }
    for (key, w) in &mut to.namespaces {
        let mut patches = Vec::new();
        copy_single_unspecified_data(
            from.namespaces.get_mut(&key),
            w,
            &mut path,
            &mut patches,
            &us,
        )?;
        if patches.len() > 0 {
            remove_patches.insert(key.clone(), patches);
        }
    }

    Ok((from, to, remove_patches))
}

fn copy_single_unspecified_data(
    have: Option<&mut DynamicObject>,
    want: &mut DynamicObject,
    path: &mut Vec<String>,
    remove_patches: &mut Vec<String>,
    us: &Option<String>,
) -> Result<()> {
    if let Some(h) = have {
        let hm = h
            .managed_fields()
            .iter()
            .find(|m| m.manager == *us)
            .map(|m| m.fields_v1.as_ref().map(|m| m.0.clone()))
            .flatten()
            .unwrap_or(JsonValue::Null);
        let copied = copy_unmanaged_fields(
            &serde_json::to_value(&mut *h)?,
            &serde_json::to_value(&mut *want)?,
            &hm,
            path,
            remove_patches,
        )?;
        *want = serde_json::from_value(copied)?;

        h.metadata.managed_fields = None;
        want.metadata.managed_fields = None;

        // In case of generateName, copy the name
        h.metadata.name = want.metadata.name.clone();
        // We don't expect a namespace to be set, so copy the old one
        h.metadata.namespace = want.metadata.namespace.clone();
    }
    Ok(())
}

pub(crate) fn munge_secrets(from: Option<&DynamicObject>, to: &mut DynamicObject) -> Result<()> {
    let is_secret = to
        .types
        .as_ref()
        .map(|t| t.api_version == "v1" && t.kind == "Secret")
        .unwrap_or(false);
    if !is_secret {
        return Ok(());
    }

    let fd = from
        .map(|v| v.data.as_object())
        .flatten()
        .map(|v| v.get("data"))
        .flatten()
        .map(|v| v.as_object())
        .flatten();
    let t = to
        .data
        .as_object_mut()
        .ok_or_else(|| anyhow!("data must be an object"))?;
    if let Some(fdd) = fd {
        // This is the case where we are refreshing or pushing an existing object
        let tdd = t
            .entry("data")
            .or_insert_with(|| JsonValue::Object(serde_json::Map::new()))
            .as_object_mut()
            .ok_or_else(|| anyhow!("data must be an object"))?;
        for (k, v) in fdd {
            tdd.insert(k.clone(), v.clone());
        }
        for (k, v) in tdd.iter_mut() {
            if !fdd.contains_key(k) {
                *v = JsonValue::String("c29tZSBzdHVmZg==".to_string()); // "replace-me"
            }
        }
    } else if let Some(tdd) = t.get_mut("data").map(|v| v.as_object_mut()).flatten() {
        // This is the case where we are importing or pushing a new object. We replace all data
        // because we don't think the user is putting actual secrets in anyway.
        for v in tdd.values_mut() {
            *v = JsonValue::String("c29tZSBzdHVmZg==".to_string()); // "replace-me"
        }
    }

    if let Some(tsd) = t.get_mut("stringData").map(|v| v.as_object_mut()).flatten() {
        for k in fd.unwrap_or(&serde_json::Map::new()).keys() {
            tsd.remove(k);
        }
        if tsd.len() == 0 {
            t.remove("stringData");
        }
    }
    Ok(())
}
