use super::*;
use crate::generate_diff::DiffAction;
use crate::kubernetes_io::KubernetesKey;
use kube::api::{DynamicObject, ObjectMeta, TypeMeta};
use serde_json::json;
use std::collections::BTreeMap;

fn key(name: &str) -> KubernetesKey {
    KubernetesKey {
        api_version: "v1".to_string(),
        cluster: "prod".to_string(),
        kind: "ConfigMap".to_string(),
        name: name.to_string(),
        namespace: Some("echo".to_string()),
    }
}

fn namespace_key(name: &str) -> KubernetesKey {
    KubernetesKey {
        api_version: "v1".to_string(),
        cluster: "prod".to_string(),
        kind: "Namespace".to_string(),
        name: name.to_string(),
        namespace: None,
    }
}

fn object(value: &str) -> DynamicObject {
    DynamicObject {
        types: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
        }),
        metadata: ObjectMeta {
            name: Some("my-config".to_string()),
            namespace: Some("echo".to_string()),
            ..ObjectMeta::default()
        },
        data: json!({"data": {"key": value}}),
    }
}

fn diff(action: DiffAction, name: &str) -> ResourceDiff {
    ResourceDiff {
        action,
        after: "after".to_string(),
        before: "before".to_string(),
        key: key(name),
    }
}

/// A database that holds each given object. `plan` and `push --plan` both compare against one of
/// these, and the tests build them by hand instead of with a database.
fn database(objects: Vec<(KubernetesKey, DynamicObject)>) -> KubernetesResources {
    let mut resources = KubernetesResources {
        by_key: BTreeMap::new(),
        namespaces: BTreeMap::new(),
    };
    for (key, object) in objects {
        if key.api_version == "v1" && key.kind == "Namespace" {
            resources.namespaces.insert(key, object);
        } else {
            resources.by_key.insert(key, object);
        }
    }
    resources
}

fn no_database() -> KubernetesResources {
    database(Vec::new())
}

/// Each action must survive the trip through the file. A `DynamicObject` holds its type and its
/// body in flattened fields, and a mistake there loses the body without an error.
#[test]
fn test_plan_round_trip_keeps_each_action() -> Result<()> {
    let patch = json_patch::diff(
        &serde_json::to_value(object("old"))?,
        &serde_json::to_value(object("new"))?,
    );
    let changes = vec![
        diff(DiffAction::Create(object("created")), "created"),
        diff(DiffAction::Delete, "deleted"),
        diff(DiffAction::Recreate(object("recreated")), "recreated"),
        diff(
            DiffAction::Patch {
                after: object("new"),
                patch: patch.clone(),
            },
            "patched",
        ),
    ];

    let file = tempfile::NamedTempFile::new()?;
    write_plan(changes, &no_database(), file.path())?;
    let read = read_plan(file.path())?;

    assert_eq!(read.len(), 4);
    assert_eq!(read[0].change.key, key("created"));
    assert_eq!(read[0].change.before, "before");
    assert_eq!(read[0].change.after, "after");
    match &read[0].change.action {
        DiffAction::Create(v) => assert_eq!(v, &object("created")),
        other => panic!("expected a create, and the file gave {:?}", other),
    }
    match &read[1].change.action {
        DiffAction::Delete => {}
        other => panic!("expected a delete, and the file gave {:?}", other),
    }
    match &read[2].change.action {
        DiffAction::Recreate(v) => assert_eq!(v, &object("recreated")),
        other => panic!("expected a recreate, and the file gave {:?}", other),
    }
    match &read[3].change.action {
        DiffAction::Patch { after, patch: p } => {
            assert_eq!(after, &object("new"));
            assert_eq!(p, &patch);
        }
        other => panic!("expected a patch, and the file gave {:?}", other),
    }

    Ok(())
}

/// The plan keeps the database row of each change, and `check_plan_is_current` has nothing to
/// compare without it. A `Namespace` lives in its own map, and the plan must find it there too.
#[test]
fn test_plan_keeps_the_database_state_of_each_change() -> Result<()> {
    let mut namespace_diff = diff(DiffAction::Delete, "echo");
    namespace_diff.key = namespace_key("echo");
    let changes = vec![
        diff(DiffAction::Delete, "tracked"),
        diff(DiffAction::Create(object("created")), "untracked"),
        namespace_diff,
    ];
    let db = database(vec![
        (key("tracked"), object("in the database")),
        (namespace_key("echo"), object("the namespace")),
    ]);

    let file = tempfile::NamedTempFile::new()?;
    write_plan(changes, &db, file.path())?;
    let read = read_plan(file.path())?;

    assert_eq!(
        read[0].database,
        Some(serde_yaml::to_string(&object("in the database"))?),
        "a change of a tracked object must keep the row it came from"
    );
    assert_eq!(
        read[1].database, None,
        "a create comes from no row, and the plan says so"
    );
    assert_eq!(
        read[2].database,
        Some(serde_yaml::to_string(&object("the namespace"))?),
        "a Namespace lives in the namespaces map, and the plan must read it there"
    );

    // The plan holds what the database held, and the same database still matches it.
    check_plan_is_current(&read, &db)?;
    Ok(())
}

/// An empty plan is a valid plan. A push of it changes nothing.
#[test]
fn test_plan_round_trip_with_no_changes() -> Result<()> {
    let file = tempfile::NamedTempFile::new()?;
    write_plan(Vec::new(), &no_database(), file.path())?;
    assert!(read_plan(file.path())?.is_empty());
    Ok(())
}

/// The version comes first, and the rest of the file may hold anything. A file of another version
/// must give the version error, and not a serde error about the shape of a field.
#[test]
fn test_read_plan_rejects_another_version() -> Result<()> {
    let file = tempfile::NamedTempFile::new()?;
    std::fs::write(
        file.path(),
        format!(
            r#"{{"changes": "not the changes of this version", "version": {}}}"#,
            LATEST_VERSION + 1
        ),
    )?;
    let error = read_plan(file.path()).expect_err("another version must not apply");
    assert!(
        error
            .to_string()
            .contains(&format!("version {} plan", LATEST_VERSION + 1)),
        "the error must name the version of the file, and it said {}",
        error
    );
    Ok(())
}

/// A file with no version at all is not a plan, and the error must say what it read.
#[test]
fn test_read_plan_rejects_a_file_with_no_version() -> Result<()> {
    let file = tempfile::NamedTempFile::new()?;
    std::fs::write(file.path(), "{}")?;
    let error = read_plan(file.path()).expect_err("a file with no version must not apply");
    assert!(
        format!("{:#}", error).contains("version"),
        "the error must name the missing version, and it said {:#}",
        error
    );
    Ok(())
}

/// The object changed after the plan. The plan holds a JSON patch of absolute paths, and applying
/// it to another object can change a field that nobody reviewed.
#[test]
fn test_check_plan_refuses_a_changed_object() -> Result<()> {
    let file = tempfile::NamedTempFile::new()?;
    write_plan(
        vec![diff(DiffAction::Delete, "tracked")],
        &database(vec![(key("tracked"), object("as planned"))]),
        file.path(),
    )?;
    let planned = read_plan(file.path())?;

    let error = check_plan_is_current(
        &planned,
        &database(vec![(key("tracked"), object("somebody pushed this"))]),
    )
    .expect_err("a changed object must not apply");
    assert!(
        error.to_string().contains("ConfigMap echo/tracked"),
        "the error must name the object, and it said {}",
        error
    );
    Ok(())
}

/// Sisyphus no longer tracks the object. A `forget` or another push took the row away, and the
/// plan describes a state that is gone.
#[test]
fn test_check_plan_refuses_a_dropped_object() -> Result<()> {
    let file = tempfile::NamedTempFile::new()?;
    write_plan(
        vec![diff(DiffAction::Delete, "tracked")],
        &database(vec![(key("tracked"), object("as planned"))]),
        file.path(),
    )?;
    let planned = read_plan(file.path())?;

    let error = check_plan_is_current(&planned, &no_database())
        .expect_err("a dropped object must not apply");
    assert!(
        error.to_string().contains("ConfigMap echo/tracked")
            && error.to_string().contains("is not tracked by Sisyphus"),
        "the error must say the row is gone, and it said {}",
        error
    );
    Ok(())
}

/// The plan creates an object that the database now holds. The apply is a forced server-side
/// apply, and it would overwrite whatever the other push made.
#[test]
fn test_check_plan_refuses_a_create_of_a_tracked_object() -> Result<()> {
    let file = tempfile::NamedTempFile::new()?;
    write_plan(
        vec![diff(DiffAction::Create(object("created")), "new")],
        &no_database(),
        file.path(),
    )?;
    let planned = read_plan(file.path())?;

    let error = check_plan_is_current(
        &planned,
        &database(vec![(key("new"), object("somebody else made this"))]),
    )
    .expect_err("a create of a tracked object must not apply");
    assert!(
        error.to_string().contains("ConfigMap echo/new")
            && error.to_string().contains("already exists"),
        "the error must say the object is there, and it said {}",
        error
    );
    Ok(())
}

/// An object that no change touches may change freely. A plan of one namespace must not fail
/// because a push in another namespace ran.
#[test]
fn test_check_plan_ignores_an_object_of_no_change() -> Result<()> {
    let file = tempfile::NamedTempFile::new()?;
    write_plan(
        vec![diff(DiffAction::Delete, "tracked")],
        &database(vec![(key("tracked"), object("as planned"))]),
        file.path(),
    )?;
    let planned = read_plan(file.path())?;

    check_plan_is_current(
        &planned,
        &database(vec![
            (key("tracked"), object("as planned")),
            (key("untouched"), object("somebody pushed this")),
        ]),
    )?;
    Ok(())
}

/// A plan can hold the values of a Secret, and no other user of the machine may read it.
#[cfg(unix)]
#[test]
fn test_write_plan_makes_a_private_file() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir()?;
    let path = directory.path().join("plan.json");
    // Make the file first, with permissions that show it to everyone. The write must fix them.
    std::fs::write(&path, "{}")?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))?;

    write_plan(
        vec![diff(DiffAction::Delete, "deleted")],
        &no_database(),
        &path,
    )?;

    let mode = std::fs::metadata(&path)?.permissions().mode() & 0o777;
    assert_eq!(mode, 0o600, "the plan file must be private");
    Ok(())
}
