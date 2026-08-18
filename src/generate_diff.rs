use crate::kubernetes_io::{KubernetesKey, KubernetesResources};
use anyhow::{anyhow, bail, Result};
use kube::api::{DynamicObject, TypeMeta};
use std::collections::HashSet;

#[cfg(test)]
mod tests;

pub(crate) enum DiffAction {
    Delete,
    Create(DynamicObject),
    Recreate(DynamicObject),
    Patch {
        after: DynamicObject,
        patch: json_patch::Patch,
    },
}

pub(crate) struct ResourceDiff {
    pub action: DiffAction,
    pub after: String,
    pub before: String,
    pub key: KubernetesKey,
}

pub(crate) fn generate_diff(
    mut have: KubernetesResources,
    want: KubernetesResources,
) -> Result<Vec<ResourceDiff>> {
    let mut changed = Vec::new();
    let mut after = HashSet::new();
    for (key, w) in want.namespaces {
        let h = have.namespaces.remove(&key);
        if h.as_ref() == Some(&w) {
            continue;
        }
        changed.push(generate_single_diff(&key, h, Some(w))?);
        after.insert(key);
    }

    for (key, w) in want.by_key {
        let h = have.by_key.remove(&key);
        if h.as_ref() == Some(&w) {
            continue;
        }
        changed.push(generate_single_diff(&key, h, Some(w))?);
        after.insert(key);
    }

    for (key, h) in have.by_key {
        if !after.contains(&key) {
            changed.push(generate_single_diff(&key, Some(h), None)?);
        }
    }

    for (key, h) in have.namespaces {
        if !after.contains(&key) {
            changed.push(generate_single_diff(&key, Some(h), None)?);
        }
    }

    Ok(changed)
}

fn generate_single_diff(
    key: &KubernetesKey,
    have: Option<DynamicObject>,
    want: Option<DynamicObject>,
) -> Result<ResourceDiff> {
    let before = if let Some(h) = &have {
        serde_yaml::to_string(&h)?
    } else {
        "".to_string()
    };
    let after = if let Some(w) = &want {
        serde_yaml::to_string(&w)?
    } else {
        "".to_string()
    };
    let action = match (have, want) {
        (Some(h), Some(mut w)) => {
            let patch = json_patch::diff(&serde_json::to_value(&h)?, &serde_json::to_value(&w)?);
            let types = w.types.as_ref().ok_or_else(|| anyhow!("Expected types"))?;
            if requires_recreate(types, &patch) {
                w.metadata.resource_version = None;
                w.metadata.uid = None;
                DiffAction::Recreate(w)
            } else {
                DiffAction::Patch { after: w, patch }
            }
        }
        (Some(_), None) => DiffAction::Delete,
        (None, Some(w)) => DiffAction::Create(w),
        (None, None) => bail!("Expected a difference"),
    };

    Ok(ResourceDiff {
        action,
        after,
        before,
        key: key.clone(),
    })
}

fn requires_recreate(types: &TypeMeta, patch: &json_patch::Patch) -> bool {
    match (types.api_version.as_str(), types.kind.as_str()) {
        ("apps/v1", "Deployment") => {
            for modification in &patch.0 {
                match modification {
                    json_patch::PatchOperation::Add(o) => {
                        let path = o.path.to_string();
                        if path.starts_with("/spec/selector/") {
                            return true;
                        }
                    }
                    json_patch::PatchOperation::Remove(o) => {
                        let path = o.path.to_string();
                        if path.starts_with("/spec/selector/") {
                            return true;
                        }
                    }
                    json_patch::PatchOperation::Replace(o) => {
                        let path = o.path.to_string();
                        if path.starts_with("/spec/selector/") {
                            return true;
                        }
                    }
                    _ => {}
                }
            }
        }
        ("batch/v1", "Job") => {
            let prefixes: Vec<&str> = vec![
                "/spec/selector",
                "/spec/template",
            ];
            for modification in &patch.0 {
                match modification {
                    json_patch::PatchOperation::Add(o) => {
                        let path = o.path.to_string();
                        for prefix in &prefixes {
                            if path.starts_with(prefix) {
                                return true;
                            }
                        }
                    }
                    json_patch::PatchOperation::Remove(o) => {
                        let path = o.path.to_string();
                        for prefix in &prefixes {
                            if path.starts_with(prefix) {
                                return true;
                            }
                        }
                    }
                    json_patch::PatchOperation::Replace(o) => {
                        let path = o.path.to_string();
                        for prefix in &prefixes {
                            if path.starts_with(prefix) {
                                return true;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        _ => {},
    };
    false
}
