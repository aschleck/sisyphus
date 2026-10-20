use crate::kubernetes_io::{
    get_kubernetes_api, KubernetesClients, KubernetesKey, KubernetesResources, MANAGER,
};
use anyhow::{anyhow, bail, Context, Result};
use futures::{stream, StreamExt, TryStreamExt};
use kube::api::{DynamicObject, Patch, PatchParams, TypeMeta};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashSet;

#[cfg(test)]
mod tests;

// How many patches to try on the server at the same time
const DRY_RUN_CONCURRENCY: usize = 16;

#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum DiffAction {
    Delete,
    Create(DynamicObject),
    Recreate(DynamicObject),
    Patch {
        after: DynamicObject,
        patch: json_patch::Patch,
    },
}

#[derive(Debug, Deserialize, Serialize)]
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

/// Asks the server whether each patch changes the object and drops the ones that do not. The server
/// fills in defaults and rewrites values, so the diff shows changes that the server would undo.
pub(crate) async fn drop_noop_patches(
    changed: Vec<ResourceDiff>,
    clients: &KubernetesClients,
) -> Result<Vec<ResourceDiff>> {
    let checks = changed
        .into_iter()
        .map(|diff| keep_if_patch_changes_object(diff, clients));
    let checked: Vec<Option<ResourceDiff>> = stream::iter(checks)
        .buffered(DRY_RUN_CONCURRENCY)
        .try_collect()
        .await?;
    Ok(checked.into_iter().flatten().collect())
}

/// Gives back the diff unless it is a patch that leaves the object as it is. The diff it gives back
/// compares the live object with what the server makes of the patch, so the server's own defaults
/// and rewrites do not show as changes.
async fn keep_if_patch_changes_object(
    mut diff: ResourceDiff,
    clients: &KubernetesClients,
) -> Result<Option<ResourceDiff>> {
    let DiffAction::Patch { patch, .. } = &diff.action else {
        return Ok(Some(diff));
    };
    let api = get_kubernetes_api(&diff.key, clients)?;
    let params = PatchParams::apply(MANAGER).dry_run();
    let trial = Patch::<()>::Json(patch.clone());
    let (live, patched) = tokio::join!(
        api.get_opt(&diff.key.name),
        api.patch(&diff.key.name, &params, &trial),
    );
    let Some(live) = live.with_context(|| format!("while reading {}", diff.key))? else {
        // The object is gone from the cluster. Keep the diff, and the push reports it.
        return Ok(Some(diff));
    };
    let patched = patched.with_context(|| format!("while trying the patch to {}", diff.key))?;

    let before = without_write_metadata(live)?;
    let after = without_write_metadata(patched)?;
    if before == after {
        return Ok(None);
    }
    diff.before = serde_yaml::to_string(&before)?;
    diff.after = serde_yaml::to_string(&after)?;
    Ok(Some(diff))
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

/// Removes what every write changes even when the object stays the same.
fn without_write_metadata(object: DynamicObject) -> Result<JsonValue> {
    let mut value = serde_json::to_value(object)?;
    if let Some(metadata) = value.get_mut("metadata").and_then(JsonValue::as_object_mut) {
        metadata.remove("managedFields");
        metadata.remove("resourceVersion");
    }
    Ok(value)
}
