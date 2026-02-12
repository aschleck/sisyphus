use super::*;
use anyhow::Result;
use kube::api::{DynamicObject, ObjectMeta, TypeMeta};
use serde_json::{json, Value as JsonValue};

// Test simple scalar values
#[test]
fn test_copy_unmanaged_fields_scalar_bool() -> Result<()> {
    let merged = copy_unmanaged_fields(&json!(true), &json!(false), &JsonValue::Null)?;
    assert_eq!(merged, json!(false));
    Ok(())
}

#[test]
fn test_copy_unmanaged_fields_scalar_number() -> Result<()> {
    let merged = copy_unmanaged_fields(&json!(42), &json!(100), &JsonValue::Null)?;
    assert_eq!(merged, json!(100));
    Ok(())
}

#[test]
fn test_copy_unmanaged_fields_scalar_string() -> Result<()> {
    let merged = copy_unmanaged_fields(&json!("old"), &json!("new"), &JsonValue::Null)?;
    assert_eq!(merged, json!("new"));
    Ok(())
}

// Test String to Number conversion
#[test]
fn test_copy_unmanaged_fields_string_to_number_conversion() -> Result<()> {
    let merged = copy_unmanaged_fields(&json!("123"), &json!(456), &JsonValue::Null)?;
    assert_eq!(merged, json!("456"));
    Ok(())
}

// Test null want value scenarios
#[test]
fn test_copy_unmanaged_fields_null_want_with_object_managed() -> Result<()> {
    let merged = copy_unmanaged_fields(
        &json!({"key": "value"}),
        &JsonValue::Null,
        &json!({"f:key": {}}),
    )?;
    assert_eq!(merged, JsonValue::Null);
    Ok(())
}

#[test]
fn test_copy_unmanaged_fields_null_want_with_null_managed() -> Result<()> {
    let merged =
        copy_unmanaged_fields(&json!({"key": "value"}), &JsonValue::Null, &JsonValue::Null)?;
    assert_eq!(merged, json!({"key": "value"}));
    Ok(())
}

// Test Object scenarios
#[test]
fn test_copy_unmanaged_fields_object_empty_managed() -> Result<()> {
    let have = json!({"old_key": "old_value"});
    let want = json!({"new_key": "new_value"});
    let managed = json!({});

    let merged = copy_unmanaged_fields(&have, &want, &managed)?;
    // When managed is empty object, we own everything, so just return want
    assert_eq!(merged, json!({"new_key": "new_value"}));
    Ok(())
}

#[test]
fn test_copy_unmanaged_fields_object_with_managed_fields() -> Result<()> {
    let have = json!({"unmanaged_key": "old_value", "managed_key": "old_managed"});
    let want = json!({"managed_key": "new_managed"});
    let managed = json!({"f:managed_key": {}});

    let merged = copy_unmanaged_fields(&have, &want, &managed)?;
    // Should preserve unmanaged_key from have and add managed_key from want
    assert_eq!(
        merged,
        json!({"unmanaged_key": "old_value", "managed_key": "new_managed"})
    );
    Ok(())
}

#[test]
fn test_copy_unmanaged_fields_object_null_managed() -> Result<()> {
    let have = json!({"key1": "value1"});
    let want = json!({"key1": "updated", "key2": "new"});
    let managed = JsonValue::Null;

    let merged = copy_unmanaged_fields(&have, &want, &managed)?;
    // Should merge existing keys and add new ones
    assert_eq!(merged, json!({"key1": "updated", "key2": "new"}));
    Ok(())
}

#[test]
fn test_copy_unmanaged_fields_object_nested_merge() -> Result<()> {
    let have = json!({"outer": {"inner": "old", "unmanaged": "keep"}});
    let want = json!({"outer": {"inner": "new"}});
    let managed = json!({"f:outer": {"f:inner": {}}});

    let merged = copy_unmanaged_fields(&have, &want, &managed)?;
    // Should preserve unmanaged nested field
    assert_eq!(
        merged,
        json!({"outer": {"inner": "new", "unmanaged": "keep"}})
    );
    Ok(())
}

#[test]
fn test_copy_unmanaged_fields_object_multiple_keys() -> Result<()> {
    let have = json!({
        "key1": "value1",
        "key2": "value2",
        "key3": "value3"
    });
    let want = json!({
        "key2": "updated2",
        "key4": "new4"
    });
    let managed = json!({"f:key2": {}});

    let merged = copy_unmanaged_fields(&have, &want, &managed)?;
    // Should keep unmanaged keys (key1, key3), update managed (key2), add new (key4)
    assert_eq!(
        merged,
        json!({
            "key1": "value1",
            "key3": "value3",
            "key2": "updated2",
            "key4": "new4"
        })
    );
    Ok(())
}

// Test Array scenarios
#[test]
fn test_copy_unmanaged_fields_array_null_managed() -> Result<()> {
    let have = json!([1, 2, 3]);
    let want = json!([4, 5, 6, 7]);
    let managed = JsonValue::Null;

    let merged = copy_unmanaged_fields(&have, &want, &managed)?;
    // Should merge arrays by index when managed is null
    assert_eq!(merged, json!([4, 5, 6, 7]));
    Ok(())
}

#[test]
fn test_copy_unmanaged_fields_array_null_managed_shorter_want() -> Result<()> {
    let have = json!([1, 2, 3, 4]);
    let want = json!([5, 6]);
    let managed = JsonValue::Null;

    let merged = copy_unmanaged_fields(&have, &want, &managed)?;
    assert_eq!(merged, json!([5, 6]));
    Ok(())
}

#[test]
fn test_copy_unmanaged_fields_array_with_selectors() -> Result<()> {
    let have = json!([
        {"name": "item1", "value": "old1"},
        {"name": "item2", "value": "old2"},
    ]);
    let want = json!([
        {"name": "item1", "value": "new1"},
        {"name": "item2", "value": "new2"},
    ]);
    let managed = json!({
        r#"k:{"name":"item1"}"#: {"f:value": {}},
        r#"k:{"name":"item2"}"#: {"f:value": {}}
    });

    let merged = copy_unmanaged_fields(&have, &want, &managed)?;
    assert_eq!(
        merged,
        json!([
            {"name": "item1", "value": "new1"},
            {"name": "item2", "value": "new2"}
        ])
    );
    Ok(())
}

#[test]
fn test_copy_unmanaged_fields_array_with_selectors_preserve_unmanaged() -> Result<()> {
    let have = json!([
        {"name": "item1", "value": "old1", "extra": "keep"}
    ]);
    let want = json!([
        {"name": "item1", "value": "new1"}
    ]);
    let managed = json!({
        r#"k:{"name":"item1"}"#: {"f:value": {}}
    });

    let merged = copy_unmanaged_fields(&have, &want, &managed)?;
    // Should preserve "extra" field as it's not managed
    assert_eq!(
        merged,
        json!([
            {"name": "item1", "value": "new1", "extra": "keep"}
        ])
    );
    Ok(())
}

#[test]
fn test_copy_unmanaged_fields_array_with_selectors_reordering() -> Result<()> {
    let have = json!([
        {"name": "item1", "value": "old1"},
        {"name": "item2", "value": "old2"}
    ]);
    let want = json!([
        {"name": "item2", "value": "new2"},
        {"name": "item1", "value": "new1"}
    ]);
    let managed = json!({
        r#"k:{"name":"item1"}"#: {"f:value": {}},
        r#"k:{"name":"item2"}"#: {"f:value": {}}
    });

    let merged = copy_unmanaged_fields(&have, &want, &managed)?;
    // Should match items by selector, so order from want is preserved
    assert_eq!(
        merged,
        json!([
            {"name": "item2", "value": "new2"},
            {"name": "item1", "value": "new1"}
        ])
    );
    Ok(())
}

#[test]
fn test_copy_unmanaged_fields_array_with_selectors_new_item() -> Result<()> {
    let have = json!([
        {"name": "item1", "value": "old1"}
    ]);
    let want = json!([
        {"name": "item1", "value": "new1"},
        {"name": "item2", "value": "new2"}
    ]);
    let managed = json!({
        r#"k:{"name":"item1"}"#: {"f:value": {}}
    });

    let merged = copy_unmanaged_fields(&have, &want, &managed)?;
    // New item (item2) should be added as-is
    assert_eq!(
        merged,
        json!([
            {"name": "item1", "value": "new1"},
            {"name": "item2", "value": "new2"}
        ])
    );
    Ok(())
}

#[test]
fn test_copy_unmanaged_fields_array_with_selectors_remove_item() -> Result<()> {
    let have = json!([
        {"name": "item1", "value": "old1"},
        {"name": "item2", "value": "old2"}
    ]);
    let want = json!([
        {"name": "item1", "value": "new1"}
    ]);
    let managed = json!({
        r#"k:{"name":"item1"}"#: {"f:value": {}}
    });

    let merged = copy_unmanaged_fields(&have, &want, &managed)?;
    // item2 should be removed since it's not in want
    assert_eq!(
        merged,
        json!([
            {"name": "item1", "value": "new1"}
        ])
    );
    Ok(())
}

#[test]
fn test_copy_unmanaged_fields_array_with_selectors_no_match() -> Result<()> {
    let have = json!([
        {"name": "item1", "value": "old1"}
    ]);
    let want = json!([
        {"name": "item2", "value": "new2"}
    ]);
    let managed = json!({
        r#"k:{"name":"item2"}"#: {"f:value": {}}
    });

    let merged = copy_unmanaged_fields(&have, &want, &managed)?;
    // item2 is new and managed, so it should just appear
    assert_eq!(
        merged,
        json!([
            {"name": "item2", "value": "new2"}
        ])
    );
    Ok(())
}

#[test]
fn test_copy_unmanaged_fields_array_with_dot_selector() -> Result<()> {
    let have = json!([1, 2, 3]);
    let want = json!([4, 5, 6]);
    let managed = json!({".": {}});

    let merged = copy_unmanaged_fields(&have, &want, &managed)?;
    // Dot selector means we own the whole array
    assert_eq!(merged, json!([4, 5, 6]));
    Ok(())
}

// Test complex nested scenarios
#[test]
fn test_copy_unmanaged_fields_nested_object_in_array() -> Result<()> {
    let have = json!([
        {"obj": {"nested": "old"}}
    ]);
    let want = json!([
        {"obj": {"nested": "new"}}
    ]);
    let managed = json!({
        r#"k:{"obj":{"nested":"new"}}"#: {"f:obj": {"f:nested": {}}}
    });

    let merged = copy_unmanaged_fields(&have, &want, &managed)?;
    assert_eq!(
        merged,
        json!([
            {"obj": {"nested": "new"}}
        ])
    );
    Ok(())
}

#[test]
fn test_copy_unmanaged_fields_array_in_object() -> Result<()> {
    let have = json!({"list": [1, 2, 3]});
    let want = json!({"list": [4, 5]});
    let managed = json!({"f:list": {}});

    let merged = copy_unmanaged_fields(&have, &want, &managed)?;
    // Should replace the array since we manage the list field
    assert_eq!(merged, json!({"list": [4, 5]}));
    Ok(())
}

#[test]
fn test_copy_unmanaged_fields_mixed_types_object_to_array() -> Result<()> {
    let have = json!({"key": "value"});
    let want = json!([1, 2, 3]);
    let managed = JsonValue::Null;

    let merged = copy_unmanaged_fields(&have, &want, &managed)?;
    // Type mismatch should return want
    assert_eq!(merged, json!([1, 2, 3]));
    Ok(())
}

#[test]
fn test_copy_unmanaged_fields_mixed_types_array_to_object() -> Result<()> {
    let have = json!([1, 2, 3]);
    let want = json!({"key": "value"});
    let managed = JsonValue::Null;

    let merged = copy_unmanaged_fields(&have, &want, &managed)?;
    // Type mismatch should return want
    assert_eq!(merged, json!({"key": "value"}));
    Ok(())
}

// Test that replacing an env var (changing name and switching value -> valueFrom)
// doesn't leak the old "value" field into the new entry
#[test]
fn test_copy_unmanaged_fields_replace_env_var_value_to_value_from() -> Result<()> {
    let have = json!([
        {"name": "ANTHROPIC_VERTEX_CHAT_MODEL", "value": "claude-opus-4-6"},
        {"name": "ANTHROPIC_VERTEX_REGION", "value": "us-east5"},
    ]);
    let want = json!([
        {"name": "ANTHROPIC_API_KEY", "valueFrom": {"secretKeyRef": {"key": "anthropic-api-key", "name": "frontend"}}},
        {"name": "ANTHROPIC_CHAT_MODEL", "value": "claude-opus-4-6"},
        {"name": "ANTHROPIC_VERTEX_REGION", "value": "us-east5"},
    ]);
    let managed = json!({
        r#"k:{"name":"ANTHROPIC_VERTEX_CHAT_MODEL"}"#: {".":{}, "f:name": {}, "f:value": {}},
        r#"k:{"name":"ANTHROPIC_VERTEX_REGION"}"#: {".":{}, "f:name": {}, "f:value": {}}
    });

    let merged = copy_unmanaged_fields(&have, &want, &managed)?;
    // ANTHROPIC_API_KEY should only have valueFrom, not the old "value" from ANTHROPIC_VERTEX_CHAT_MODEL
    let arr = merged.as_array().unwrap();
    let api_key_entry = arr.iter().find(|v| v["name"] == "ANTHROPIC_API_KEY").unwrap();
    assert_eq!(api_key_entry.get("value"), None);
    assert!(api_key_entry.get("valueFrom").is_some());
    Ok(())
}

// Test that unowned keys from have are preserved when managed fields are null
// (e.g. Service fields like clusterIP, status set by other controllers)
#[test]
fn test_copy_unmanaged_fields_object_null_managed_preserves_have_keys() -> Result<()> {
    let have = json!({"name": "foo", "clusterIP": "10.0.0.1", "type": "ClusterIP"});
    let want = json!({"name": "foo"});
    let managed = JsonValue::Null;

    let merged = copy_unmanaged_fields(&have, &want, &managed)?;
    assert_eq!(
        merged,
        json!({"name": "foo", "clusterIP": "10.0.0.1", "type": "ClusterIP"})
    );
    Ok(())
}

// Tests for munge_secrets
#[test]
fn test_munge_secrets_non_secret_resource() -> Result<()> {
    let mut to = DynamicObject {
        types: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({"key": "value"}),
    };

    munge_secrets(None, &mut to)?;

    // Non-secret resources should be unchanged
    assert_eq!(to.data, json!({"key": "value"}));
    Ok(())
}

#[test]
fn test_munge_secrets_new_secret_with_data() -> Result<()> {
    let mut to = DynamicObject {
        types: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "Secret".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({
            "data": {
                "password": "c2VjcmV0",
                "username": "YWRtaW4="
            }
        }),
    };

    munge_secrets(None, &mut to)?;

    // All secret data should be replaced with placeholder
    assert_eq!(
        to.data,
        json!({
            "data": {
                "password": "c29tZSBzdHVmZg==",
                "username": "c29tZSBzdHVmZg=="
            }
        })
    );
    Ok(())
}

#[test]
fn test_munge_secrets_refresh_existing_secret() -> Result<()> {
    let from = DynamicObject {
        types: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "Secret".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({
            "data": {
                "password": "b2xkX3NlY3JldA==",
                "username": "b2xkX3VzZXI="
            }
        }),
    };

    let mut to = DynamicObject {
        types: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "Secret".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({
            "data": {
                "password": "bmV3X3NlY3JldA==",
                "api_key": "bmV3X2FwaV9rZXk="
            }
        }),
    };

    munge_secrets(Some(&from), &mut to)?;

    // All keys from 'from' should be copied, and keys in 'to' not in 'from' get placeholder
    assert_eq!(
        to.data,
        json!({
            "data": {
                "password": "b2xkX3NlY3JldA==",
                "username": "b2xkX3VzZXI=",
                "api_key": "c29tZSBzdHVmZg=="
            }
        })
    );
    Ok(())
}

#[test]
fn test_munge_secrets_with_string_data() -> Result<()> {
    let from = DynamicObject {
        types: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "Secret".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({
            "data": {
                "password": "b2xkX3NlY3JldA=="
            }
        }),
    };

    let mut to = DynamicObject {
        types: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "Secret".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({
            "data": {
                "password": "bmV3X3NlY3JldA=="
            },
            "stringData": {
                "password": "plain_text_password",
                "new_key": "plain_text_value"
            }
        }),
    };

    munge_secrets(Some(&from), &mut to)?;

    // stringData keys that exist in data should be removed
    let result_data = to.data.as_object().unwrap();
    assert_eq!(
        result_data.get("data").unwrap(),
        &json!({"password": "b2xkX3NlY3JldA=="})
    );
    assert_eq!(
        result_data.get("stringData").unwrap(),
        &json!({"new_key": "plain_text_value"})
    );
    Ok(())
}

#[test]
fn test_munge_secrets_empty_string_data_removed() -> Result<()> {
    let from = DynamicObject {
        types: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "Secret".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({
            "data": {
                "password": "b2xkX3NlY3JldA=="
            }
        }),
    };

    let mut to = DynamicObject {
        types: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "Secret".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({
            "data": {
                "password": "bmV3X3NlY3JldA=="
            },
            "stringData": {
                "password": "plain_text_password"
            }
        }),
    };

    munge_secrets(Some(&from), &mut to)?;

    // stringData should be removed entirely if empty after removing overlapping keys
    let result_data = to.data.as_object().unwrap();
    assert!(!result_data.contains_key("stringData"));
    Ok(())
}

#[test]
fn test_munge_secrets_no_from_data() -> Result<()> {
    let mut to = DynamicObject {
        types: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "Secret".to_string(),
        }),
        metadata: ObjectMeta::default(),
        data: json!({
            "data": {
                "password": "c2VjcmV0",
                "username": "YWRtaW4="
            }
        }),
    };

    munge_secrets(None, &mut to)?;

    // All data should be replaced with placeholder when there's no 'from'
    assert_eq!(
        to.data,
        json!({
            "data": {
                "password": "c29tZSBzdHVmZg==",
                "username": "c29tZSBzdHVmZg=="
            }
        })
    );
    Ok(())
}
