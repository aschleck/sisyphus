use std::collections::BTreeMap;

use kube::api::{ObjectMeta, TypeMeta};
use serde_json::json;

use super::*;

#[test]
fn test_generate_diff_no_changes() -> Result<()> {
    let key = KubernetesKey {
        api_version: "v1".to_string(),
        cluster: "prod".to_string(),
        kind: "ConfigMap".to_string(),
        name: "my-config".to_string(),
        namespace: Some("default".to_string()),
    };

    let object = DynamicObject {
        types: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({"key": "value"}),
    };

    let have = KubernetesResources {
        by_key: BTreeMap::from([(key.clone(), object.clone())]),
        namespaces: BTreeMap::new(),
    };
    let want = KubernetesResources {
        by_key: BTreeMap::from([(key, object)]),
        namespaces: BTreeMap::new(),
    };
    let diff = generate_diff(have, want)?;

    assert_eq!(diff.len(), 0);

    Ok(())
}

#[test]
fn test_generate_diff_create_object() -> Result<()> {
    let key = KubernetesKey {
        api_version: "v1".to_string(),
        cluster: "prod".to_string(),
        kind: "ConfigMap".to_string(),
        name: "new-config".to_string(),
        namespace: Some("default".to_string()),
    };

    let object = DynamicObject {
        types: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({"key": "value"}),
    };

    let have = KubernetesResources {
        by_key: BTreeMap::new(),
        namespaces: BTreeMap::new(),
    };
    let want = KubernetesResources {
        by_key: BTreeMap::from([(key.clone(), object)]),
        namespaces: BTreeMap::new(),
    };
    let diff = generate_diff(have, want)?;

    assert_eq!(diff.len(), 1);
    assert_eq!(diff[0].0, key);
    assert!(matches!(diff[0].1, DiffAction::Create(_)));

    Ok(())
}

#[test]
fn test_generate_diff_delete_object() -> Result<()> {
    let key = KubernetesKey {
        api_version: "v1".to_string(),
        cluster: "prod".to_string(),
        kind: "ConfigMap".to_string(),
        name: "old-config".to_string(),
        namespace: Some("default".to_string()),
    };

    let object = DynamicObject {
        types: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({"key": "value"}),
    };

    let have = KubernetesResources {
        by_key: BTreeMap::from([(key.clone(), object)]),
        namespaces: BTreeMap::new(),
    };
    let want = KubernetesResources {
        by_key: BTreeMap::new(),
        namespaces: BTreeMap::new(),
    };
    let diff = generate_diff(have, want)?;

    assert_eq!(diff.len(), 1);
    assert_eq!(diff[0].0, key);
    assert!(matches!(diff[0].1, DiffAction::Delete));

    Ok(())
}

#[test]
fn test_generate_diff_update_object() -> Result<()> {
    let key = KubernetesKey {
        api_version: "v1".to_string(),
        cluster: "prod".to_string(),
        kind: "ConfigMap".to_string(),
        name: "my-config".to_string(),
        namespace: Some("default".to_string()),
    };

    let old_object = DynamicObject {
        types: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({"key": "old-value"}),
    };

    let new_object = DynamicObject {
        types: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({"key": "new-value"}),
    };

    let have = KubernetesResources {
        by_key: BTreeMap::from([(key.clone(), old_object)]),
        namespaces: BTreeMap::new(),
    };
    let want = KubernetesResources {
        by_key: BTreeMap::from([(key.clone(), new_object)]),
        namespaces: BTreeMap::new(),
    };
    let diff = generate_diff(have, want)?;

    assert_eq!(diff.len(), 1);
    assert_eq!(diff[0].0, key);
    assert!(matches!(diff[0].1, DiffAction::Patch { .. }));

    Ok(())
}

#[test]
fn test_generate_diff_mixed_operations() -> Result<()> {
    // Key for object to keep (no change)
    let keep_key = KubernetesKey {
        api_version: "v1".to_string(),
        cluster: "prod".to_string(),
        kind: "ConfigMap".to_string(),
        name: "keep-config".to_string(),
        namespace: Some("default".to_string()),
    };

    // Key for object to delete
    let delete_key = KubernetesKey {
        api_version: "v1".to_string(),
        cluster: "prod".to_string(),
        kind: "ConfigMap".to_string(),
        name: "delete-config".to_string(),
        namespace: Some("default".to_string()),
    };

    // Key for object to create
    let create_key = KubernetesKey {
        api_version: "v1".to_string(),
        cluster: "prod".to_string(),
        kind: "ConfigMap".to_string(),
        name: "create-config".to_string(),
        namespace: Some("default".to_string()),
    };

    // Key for object to update
    let update_key = KubernetesKey {
        api_version: "v1".to_string(),
        cluster: "prod".to_string(),
        kind: "ConfigMap".to_string(),
        name: "update-config".to_string(),
        namespace: Some("default".to_string()),
    };

    let keep_object = DynamicObject {
        types: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({"key": "value"}),
    };

    let delete_object = DynamicObject {
        types: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({"key": "delete-me"}),
    };

    let update_object_old = DynamicObject {
        types: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({"key": "old"}),
    };

    let update_object_new = DynamicObject {
        types: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({"key": "new"}),
    };

    let create_object = DynamicObject {
        types: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({"key": "created"}),
    };

    let mut have = KubernetesResources {
        by_key: BTreeMap::new(),
        namespaces: BTreeMap::new(),
    };
    have.by_key.insert(keep_key.clone(), keep_object.clone());
    have.by_key.insert(delete_key.clone(), delete_object);
    have.by_key.insert(update_key.clone(), update_object_old);

    let mut want = KubernetesResources {
        by_key: BTreeMap::new(),
        namespaces: BTreeMap::new(),
    };
    want.by_key.insert(keep_key, keep_object);
    want.by_key.insert(create_key.clone(), create_object);
    want.by_key.insert(update_key.clone(), update_object_new);

    let diff = generate_diff(have, want)?;

    // Should have 3 changes: create, delete, update (keep is not in diff)
    assert_eq!(diff.len(), 3);

    // Verify we have one of each action type
    let mut has_create = false;
    let mut has_delete = false;
    let mut has_update = false;

    for (key, action) in &diff {
        match action {
            DiffAction::Create(_) => {
                assert_eq!(key, &create_key);
                has_create = true;
            }
            DiffAction::Delete => {
                assert_eq!(key, &delete_key);
                has_delete = true;
            }
            DiffAction::Patch { .. } => {
                assert_eq!(key, &update_key);
                has_update = true;
            }
            _ => {}
        }
    }

    assert!(has_create);
    assert!(has_delete);
    assert!(has_update);

    Ok(())
}

#[test]
fn test_generate_diff_namespace_operations() -> Result<()> {
    let ns_key = KubernetesKey {
        api_version: "v1".to_string(),
        cluster: "prod".to_string(),
        kind: "Namespace".to_string(),
        name: "my-namespace".to_string(),
        namespace: None,
    };

    let ns_object = DynamicObject {
        types: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "Namespace".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({}),
    };

    let have = KubernetesResources {
        by_key: BTreeMap::new(),
        namespaces: BTreeMap::new(),
    };
    let want = KubernetesResources {
        by_key: BTreeMap::from([(ns_key.clone(), ns_object)]),
        namespaces: BTreeMap::new(),
    };
    let diff = generate_diff(have, want)?;

    assert_eq!(diff.len(), 1);
    assert_eq!(diff[0].0, ns_key);
    assert!(matches!(diff[0].1, DiffAction::Create(_)));

    Ok(())
}

#[test]
fn test_deployment_selector_change_triggers_recreate() -> Result<()> {
    let key = KubernetesKey {
        api_version: "apps/v1".to_string(),
        cluster: "prod".to_string(),
        kind: "Deployment".to_string(),
        name: "my-deployment".to_string(),
        namespace: Some("default".to_string()),
    };

    let old_object = DynamicObject {
        types: Some(TypeMeta {
            api_version: "apps/v1".to_string(),
            kind: "Deployment".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({
            "spec": {
                "selector": {
                    "matchLabels": {
                        "app": "old-app"
                    }
                }
            }
        }),
    };

    let new_object = DynamicObject {
        types: Some(TypeMeta {
            api_version: "apps/v1".to_string(),
            kind: "Deployment".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({
            "spec": {
                "selector": {
                    "matchLabels": {
                        "app": "new-app"
                    }
                }
            }
        }),
    };

    let have = KubernetesResources {
        by_key: BTreeMap::from([(key.clone(), old_object)]),
        namespaces: BTreeMap::new(),
    };
    let want = KubernetesResources {
        by_key: BTreeMap::from([(key.clone(), new_object)]),
        namespaces: BTreeMap::new(),
    };
    let diff = generate_diff(have, want)?;

    assert_eq!(diff.len(), 1);
    assert_eq!(diff[0].0, key);
    assert!(matches!(diff[0].1, DiffAction::Recreate(_)));

    Ok(())
}

#[test]
fn test_deployment_non_selector_change_triggers_patch() -> Result<()> {
    let key = KubernetesKey {
        api_version: "apps/v1".to_string(),
        cluster: "prod".to_string(),
        kind: "Deployment".to_string(),
        name: "my-deployment".to_string(),
        namespace: Some("default".to_string()),
    };

    let old_object = DynamicObject {
        types: Some(TypeMeta {
            api_version: "apps/v1".to_string(),
            kind: "Deployment".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({
            "spec": {
                "replicas": 1,
                "selector": {
                    "matchLabels": {
                        "app": "my-app"
                    }
                }
            }
        }),
    };

    let new_object = DynamicObject {
        types: Some(TypeMeta {
            api_version: "apps/v1".to_string(),
            kind: "Deployment".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({
            "spec": {
                "replicas": 3,
                "selector": {
                    "matchLabels": {
                        "app": "my-app"
                    }
                }
            }
        }),
    };

    let have = KubernetesResources {
        by_key: BTreeMap::from([(key.clone(), old_object)]),
        namespaces: BTreeMap::new(),
    };
    let want = KubernetesResources {
        by_key: BTreeMap::from([(key.clone(), new_object)]),
        namespaces: BTreeMap::new(),
    };
    let diff = generate_diff(have, want)?;

    assert_eq!(diff.len(), 1);
    assert_eq!(diff[0].0, key);
    assert!(matches!(diff[0].1, DiffAction::Patch { .. }));

    Ok(())
}

#[test]
fn test_job_template_change_triggers_recreate() -> Result<()> {
    let key = KubernetesKey {
        api_version: "batch/v1".to_string(),
        cluster: "prod".to_string(),
        kind: "Job".to_string(),
        name: "my-job".to_string(),
        namespace: Some("default".to_string()),
    };

    let old_object = DynamicObject {
        types: Some(TypeMeta {
            api_version: "batch/v1".to_string(),
            kind: "Job".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({
            "spec": {
                "template": {
                    "spec": {
                        "containers": [{
                            "name": "job",
                            "image": "busybox:1.0"
                        }]
                    }
                }
            }
        }),
    };

    let new_object = DynamicObject {
        types: Some(TypeMeta {
            api_version: "batch/v1".to_string(),
            kind: "Job".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({
            "spec": {
                "template": {
                    "spec": {
                        "containers": [{
                            "name": "job",
                            "image": "busybox:2.0"
                        }]
                    }
                }
            }
        }),
    };

    let have = KubernetesResources {
        by_key: BTreeMap::from([(key.clone(), old_object)]),
        namespaces: BTreeMap::new(),
    };
    let want = KubernetesResources {
        by_key: BTreeMap::from([(key.clone(), new_object)]),
        namespaces: BTreeMap::new(),
    };
    let diff = generate_diff(have, want)?;

    assert_eq!(diff.len(), 1);
    assert_eq!(diff[0].0, key);
    assert!(matches!(diff[0].1, DiffAction::Recreate(_)));

    Ok(())
}

#[test]
fn test_job_non_template_change_triggers_patch() -> Result<()> {
    let key = KubernetesKey {
        api_version: "batch/v1".to_string(),
        cluster: "prod".to_string(),
        kind: "Job".to_string(),
        name: "my-job".to_string(),
        namespace: Some("default".to_string()),
    };

    let old_object = DynamicObject {
        types: Some(TypeMeta {
            api_version: "batch/v1".to_string(),
            kind: "Job".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({
            "meta": {
                "labels": {
                    "dog": "red",
                },
                "template": {
                    "spec": {
                        "containers": [{
                            "name": "job",
                            "image": "busybox:1.0"
                        }]
                    }
                }
            }
        }),
    };

    let new_object = DynamicObject {
        types: Some(TypeMeta {
            api_version: "batch/v1".to_string(),
            kind: "Job".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({
            "spec": {
                "labels": {
                    "dog": "blue",
                },
                "template": {
                    "spec": {
                        "containers": [{
                            "name": "job",
                            "image": "busybox:1.0"
                        }]
                    }
                }
            }
        }),
    };

    let have = KubernetesResources {
        by_key: BTreeMap::from([(key.clone(), old_object)]),
        namespaces: BTreeMap::new(),
    };
    let want = KubernetesResources {
        by_key: BTreeMap::from([(key.clone(), new_object)]),
        namespaces: BTreeMap::new(),
    };
    let diff = generate_diff(have, want)?;

    assert_eq!(diff.len(), 1);
    assert_eq!(diff[0].0, key);
    assert!(matches!(diff[0].1, DiffAction::Patch { .. }));

    Ok(())
}
