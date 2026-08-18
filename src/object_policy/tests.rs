use super::*;
use sqlx::any::AnyPoolOptions;
use std::sync::Once;

// The `main` function installs the drivers, and a test does not call `main`.
static DRIVERS: Once = Once::new();

fn echo() -> ObjectRef {
    ObjectRef::new("Deployment", "echo", "echo")
}

#[test]
fn test_rendered_type_maps_only_the_kinds_with_a_config_image() {
    assert_eq!(rendered_type("Deployment"), Some(("apps/v1", "Deployment")));
    assert_eq!(rendered_type("CronJob"), Some(("batch/v1", "CronJob")));
    // Raw yaml gives its own images, and Sisyphus has no tag to resolve or to hold.
    assert_eq!(rendered_type("KubernetesYaml"), None);
}

#[tokio::test]
async fn test_pause_and_resume() -> Result<()> {
    let pool = memory_pool().await?;

    assert!(get_pause(&echo(), &pool).await?.is_none());

    pause_object(&echo(), "flapping", &pool).await?;
    assert_eq!(
        get_pause(&echo(), &pool).await?.expect("expected a pause").reason,
        "flapping"
    );

    // A second pause replaces the reason. It does not cause a primary key error.
    pause_object(&echo(), "still flapping", &pool).await?;
    assert_eq!(list_pauses(&pool).await?.len(), 1);
    assert_eq!(
        get_pause(&echo(), &pool).await?.unwrap().reason,
        "still flapping"
    );

    resume_object(&echo(), &pool).await?;
    assert!(get_pause(&echo(), &pool).await?.is_none());
    assert!(resume_object(&echo(), &pool).await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_a_pause_is_scoped_to_one_object() -> Result<()> {
    let pool = memory_pool().await?;
    pause_object(&echo(), "flapping", &pool).await?;

    // The same name with a different namespace or kind: no change to these objects.
    for other in [
        ObjectRef::new("Deployment", "other", "echo"),
        ObjectRef::new("Deployment", "echo", "other"),
        ObjectRef::new("CronJob", "echo", "echo"),
    ] {
        assert!(get_pause(&other, &pool).await?.is_none(), "{}", other);
    }

    Ok(())
}

#[tokio::test]
async fn test_raw_yaml_can_be_paused() -> Result<()> {
    // A pause holds each object that the resource rendered, and the type of object does not matter.
    let pool = memory_pool().await?;
    let raw = ObjectRef::new("KubernetesYaml", "echo", "ingress");

    pause_object(&raw, "migrating by hand", &pool).await?;
    assert!(get_pause(&raw, &pool).await?.is_some());

    Ok(())
}

#[tokio::test]
async fn test_an_unknown_kind_is_rejected() -> Result<()> {
    let pool = memory_pool().await?;
    let bogus = ObjectRef::new("Ingress", "echo", "echo");

    // `Ingress` is a Kubernetes kind, and not a Sisyphus kind. A caller that gives this kind
    // addresses a rendered object, and not the resource that renders it.
    assert!(pause_object(&bogus, "flapping", &pool).await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_history_and_status_are_empty_for_kinds_with_no_config_image() -> Result<()> {
    let pool = memory_pool().await?;
    let raw = ObjectRef::new("KubernetesYaml", "echo", "ingress");
    pause_object(&raw, "migrating by hand", &pool).await?;

    assert!(list_history(&raw, &pool).await?.is_empty());
    let status = get_status(&raw, &pool).await?;
    assert_eq!(status.state, "paused");
    assert!(status.deployed.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_record_push_reads_the_config_image_off_the_object() -> Result<()> {
    let pool = memory_pool().await?;
    record_push(
        &deployment_key("cluster1", "echo", "echo"),
        &rendered_deployment("echo", "echo", Some("reg/config@sha256:a")),
        &pool,
    )
    .await?;

    let history = list_history(&echo(), &pool).await?;
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].image, "reg/config@sha256:a");
    assert_eq!(history[0].clusters, vec!["cluster1".to_string()]);

    Ok(())
}

#[tokio::test]
async fn test_record_push_skips_objects_with_no_config_image() -> Result<()> {
    // A Service, a Namespace, and raw Kubernetes yaml have no config image. There is no version to
    // roll them back to.
    let pool = memory_pool().await?;
    record_push(
        &deployment_key("cluster1", "echo", "echo"),
        &rendered_deployment("echo", "echo", None),
        &pool,
    )
    .await?;

    assert!(list_history(&echo(), &pool).await?.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_history_folds_clusters_and_runs_newest_first() -> Result<()> {
    let pool = memory_pool().await?;
    // The same image in two clusters is one version, and not two versions.
    insert_history("cluster1", "echo", "echo", "reg/config@sha256:old", "2026-08-11 11:40:00+00", &pool).await?;
    insert_history("cluster2", "echo", "echo", "reg/config@sha256:old", "2026-08-11 11:40:01+00", &pool).await?;
    insert_history("cluster1", "echo", "echo", "reg/config@sha256:new", "2026-08-12 17:02:00+00", &pool).await?;

    let history = list_history(&echo(), &pool).await?;
    assert_eq!(
        history.iter().map(|e| e.image.as_str()).collect::<Vec<_>>(),
        vec!["reg/config@sha256:new", "reg/config@sha256:old"]
    );
    assert_eq!(history[0].clusters, vec!["cluster1".to_string()]);
    assert_eq!(
        history[1].clusters,
        vec!["cluster1".to_string(), "cluster2".to_string()]
    );
    // This is the most recent push of that image, and not the first push.
    assert_eq!(history[1].last_pushed_at, "2026-08-11 11:40:01+00");

    Ok(())
}

#[tokio::test]
async fn test_history_marks_what_is_deployed() -> Result<()> {
    // This flag makes the list a set of rollback targets, and not only a log. One entry is the
    // current version, and the other entries are the possible targets.
    let pool = memory_pool().await?;
    insert_history("cluster1", "echo", "echo", "reg/config@sha256:old", "2026-08-11 11:40:00+00", &pool).await?;
    insert_history("cluster1", "echo", "echo", "reg/config@sha256:new", "2026-08-12 17:02:00+00", &pool).await?;
    insert_deployment("cluster1", "echo", "echo", Some("reg/config@sha256:new"), &pool).await?;

    let history = list_history(&echo(), &pool).await?;
    assert!(history[0].deployed);
    assert!(!history[1].deployed);

    Ok(())
}

#[tokio::test]
async fn test_history_is_scoped_to_one_object() -> Result<()> {
    let pool = memory_pool().await?;
    insert_history("cluster1", "echo", "echo", "reg/config@sha256:a", "2026-08-11 11:40:00+00", &pool).await?;

    for other in [
        ObjectRef::new("Deployment", "other", "echo"),
        ObjectRef::new("Deployment", "echo", "other"),
        // A CronJob renders to batch/v1, and it cannot match the rows of the Deployment.
        ObjectRef::new("CronJob", "echo", "echo"),
    ] {
        assert!(list_history(&other, &pool).await?.is_empty(), "{}", other);
    }

    Ok(())
}

#[tokio::test]
async fn test_status_reports_a_plain_object() -> Result<()> {
    let pool = memory_pool().await?;
    insert_deployment("cluster1", "echo", "echo", Some("reg/config@sha256:a"), &pool).await?;

    let status = get_status(&echo(), &pool).await?;
    assert_eq!(status.state, "tracking");
    assert!(status.pause.is_none());
    assert_eq!(status.deployed.len(), 1);
    assert_eq!(status.deployed[0].cluster, "cluster1");
    assert_eq!(status.deployed[0].image, "reg/config@sha256:a");

    Ok(())
}

#[tokio::test]
async fn test_status_shows_clusters_that_disagree() -> Result<()> {
    // Two clusters with different images is the condition that `status` must show. The report
    // gives both images, and it does not select one of them.
    let pool = memory_pool().await?;
    insert_deployment("cluster1", "echo", "echo", Some("reg/config@sha256:a"), &pool).await?;
    insert_deployment("cluster2", "echo", "echo", Some("reg/config@sha256:b"), &pool).await?;

    let status = get_status(&echo(), &pool).await?;
    assert_eq!(
        status
            .deployed
            .iter()
            .map(|d| (d.cluster.as_str(), d.image.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("cluster1", "reg/config@sha256:a"),
            ("cluster2", "reg/config@sha256:b"),
        ]
    );

    Ok(())
}

#[tokio::test]
async fn test_status_is_not_an_error_for_something_never_pushed() -> Result<()> {
    // The database has no data for an object in the configuration files that had no push. It also
    // has no data for an object that does not exist. These two conditions are the same here, and
    // neither one is an error.
    let pool = memory_pool().await?;

    let status = get_status(&echo(), &pool).await?;
    assert_eq!(status.state, "tracking");
    assert!(status.deployed.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_status_carries_the_pause_reason() -> Result<()> {
    // The reason is the important data. It tells you why this object has a different version from
    // the other objects.
    let pool = memory_pool().await?;
    pause_object(&echo(), "flapping readiness probe", &pool).await?;

    let status = get_status(&echo(), &pool).await?;
    assert_eq!(status.state, "paused");
    assert_eq!(
        status.pause.expect("expected a pause").reason,
        "flapping readiness probe"
    );

    Ok(())
}

#[tokio::test]
async fn test_objects_without_the_annotation_report_nothing_deployed() -> Result<()> {
    // An object from a push before the config image annotation existed gives no data.
    let pool = memory_pool().await?;
    insert_deployment("cluster1", "echo", "echo", None, &pool).await?;

    assert!(get_status(&echo(), &pool).await?.deployed.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_a_global_resource_uses_the_default_namespace() -> Result<()> {
    // `global` is the name of a directory, and it is not a Kubernetes namespace. A resource in that
    // directory renders into the default namespace, and the database holds that namespace as an
    // empty text value. The queries must use the empty value, or `history` and `status` show
    // nothing for each resource in `global`.
    let pool = memory_pool().await?;
    let global = ObjectRef::new("Deployment", GLOBAL_NAMESPACE, "echo");

    record_push(
        &KubernetesKey {
            namespace: None,
            ..deployment_key("cluster1", "", "echo")
        },
        &rendered_deployment("", "echo", Some("reg/config@sha256:a")),
        &pool,
    )
    .await?;
    insert_deployment("cluster1", "", "echo", Some("reg/config@sha256:a"), &pool).await?;

    let history = list_history(&global, &pool).await?;
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].image, "reg/config@sha256:a");
    assert!(history[0].deployed);

    let status = get_status(&global, &pool).await?;
    assert_eq!(status.deployed.len(), 1);
    assert_eq!(status.deployed[0].image, "reg/config@sha256:a");

    // A resource in a namespace directory keeps the name of that directory.
    assert!(list_history(&echo(), &pool).await?.is_empty());

    Ok(())
}

const MIGRATIONS: [&str; 2] = [
    include_str!("../../20250326020918_initialize.sql"),
    include_str!("../../20260812000000_objects.sql"),
];

async fn memory_pool() -> Result<AnyPool> {
    DRIVERS.call_once(sqlx::any::install_default_drivers);
    let pool = AnyPoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    for migration in MIGRATIONS {
        for statement in create_table_statements(migration)? {
            sqlx::query(&statement).execute(&pool).await?;
        }
    }
    Ok(pool)
}

/// The `CREATE TABLE` statements from a migration file. The README tells a user of a different
/// database to run the same statements. This function reads the statements from the migration and
/// does not repeat them here, and then a new column in the schema is also in the test schema.
///
/// The other parts of each migration apply to Postgres only. These parts are the audit tables and
/// the `create_audit_table` function that makes them. These tests do not use them.
fn create_table_statements(migration: &str) -> Result<Vec<String>> {
    let upper = migration.to_uppercase();
    let mut statements = Vec::new();
    let mut offset = 0;
    // A statement that starts in the first column is a top-level statement. `create_audit_table`
    // makes its audit tables from indented SQL in a text value, and this function must not run that
    // SQL.
    while let Some(start) = starts_a_line(&upper[offset..], "CREATE TABLE") {
        let statement = &migration[offset + start..];
        let Some(end) = end_of_statement(statement) else {
            bail!("unterminated CREATE TABLE in a migration");
        };
        statements.push(sqlite_dialect(&statement[..end]));
        offset += start + end;
    }
    Ok(statements)
}

/// The position of the next `needle` at the start of a line, if a `needle` is there.
fn starts_a_line(haystack: &str, needle: &str) -> Option<usize> {
    haystack
        .match_indices(needle)
        .find(|(i, _)| *i == 0 || haystack.as_bytes()[i - 1] == b'\n')
        .map(|(i, _)| i)
}

/// The end of the statement at the start of `sql`. The function counts the parentheses, and it
/// does not use a `;` in the column list as the end.
fn end_of_statement(sql: &str) -> Option<usize> {
    let mut depth = 0;
    for (i, c) in sql.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ';' if depth == 0 => return Some(i + 1),
            _ => {}
        }
    }
    None
}

/// Sqlite accepts the schema as written, with the exception of `current_setting`. Postgres uses
/// `current_setting` to read the username that the CLI sets for each connection.
fn sqlite_dialect(statement: &str) -> String {
    statement
        .lines()
        .map(strip_current_setting_default)
        .collect::<Vec<_>>()
        .join("\n")
}

/// Removes the `DEFAULT current_setting(...)` from a column, but keeps the column. A test has no
/// user to record, but the queries in the test still select the column.
fn strip_current_setting_default(line: &str) -> String {
    let Some(start) = line.find("DEFAULT current_setting(") else {
        return line.to_string();
    };
    let Some(end) = line[start..].find(')') else {
        return line.to_string();
    };
    format!("{}{}", &line[..start], &line[start + end + 1..])
}

fn deployment_key(cluster: &str, namespace: &str, name: &str) -> KubernetesKey {
    KubernetesKey {
        api_version: "apps/v1".to_string(),
        cluster: cluster.to_string(),
        kind: "Deployment".to_string(),
        name: name.to_string(),
        namespace: Some(namespace.to_string()),
    }
}

/// A Deployment in the form that `kubernetes_rendering` makes. The apply code sends this object to
/// `record_push`.
fn rendered_deployment(namespace: &str, name: &str, config_image: Option<&str>) -> DynamicObject {
    serde_yaml::from_str(&deployment_yaml(namespace, name, config_image))
        .expect("the test yaml should parse")
}

fn deployment_yaml(namespace: &str, name: &str, config_image: Option<&str>) -> String {
    let annotations = match config_image {
        Some(image) => format!("  annotations:\n    {}: {}\n", CONFIG_IMAGE_ANNOTATION, image),
        None => "".to_string(),
    };
    format!(
        "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: {}\n  namespace: {}\n{}",
        name, namespace, annotations
    )
}

async fn insert_history(
    cluster: &str,
    namespace: &str,
    name: &str,
    image: &str,
    deployed_at: &str,
    pool: &AnyPool,
) -> Result<()> {
    // This function writes the row directly. `record_push` lets the database set deployed_at.
    // These tests examine the sequence, and they must set their own timestamps.
    sqlx::query(
        "INSERT INTO object_history \
         (api_version, cluster, deployed_at, image, kind, name, namespace) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind("apps/v1")
    .bind(cluster)
    .bind(deployed_at)
    .bind(image)
    .bind("Deployment")
    .bind(name)
    .bind(namespace)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_deployment(
    cluster: &str,
    namespace: &str,
    name: &str,
    config_image: Option<&str>,
    pool: &AnyPool,
) -> Result<()> {
    let yaml = deployment_yaml(namespace, name, config_image);
    sqlx::query(
        "INSERT INTO kubernetes_objects (api_version, cluster, kind, name, namespace, yaml) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind("apps/v1")
    .bind(cluster)
    .bind("Deployment")
    .bind(name)
    .bind(namespace)
    .bind(yaml)
    .execute(pool)
    .await?;
    Ok(())
}
