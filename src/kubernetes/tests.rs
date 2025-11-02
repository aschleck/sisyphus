use anyhow::Result;
use serde_json::Value as JsonValue;
use super::*;

#[test]
fn test_some_function() -> Result<()> {
    let merged =
        copy_unmanaged_fields(&JsonValue::Bool(true), &JsonValue::Bool(false), &JsonValue::Null)?;
    Ok(assert_eq!(merged, JsonValue::Bool(false)))
}
