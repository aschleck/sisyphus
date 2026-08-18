use crate::{
    apply_diff::namespace_or_default, kubernetes_io::KubernetesKey,
    kubernetes_rendering::CONFIG_IMAGE_ANNOTATION,
};
use anyhow::{bail, Result};
use kube::api::DynamicObject;
use sqlx::{AnyPool, Row};
use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
mod tests;

/// A Sisyphus object as the yaml writes it. This is the key of a pause, and a rendered Kubernetes
/// object is never the key. The render shows which objects the resource makes.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct ObjectRef {
    pub kind: String,
    pub name: String,
    pub namespace: String,
}

impl ObjectRef {
    pub fn new(kind: &str, namespace: &str, name: &str) -> Self {
        ObjectRef {
            kind: kind.to_string(),
            name: name.to_string(),
            namespace: namespace.to_string(),
        }
    }

    /// The namespace of the Kubernetes objects that this resource renders.
    ///
    /// For a resource in a namespace directory, this is the name of that directory. A resource in
    /// the `global` directory has no namespace of its own, and its objects go to the default
    /// namespace. The database holds that namespace as an empty text value, which is also what
    /// `namespace_or_default` gives for it.
    fn rendered_namespace(&self) -> &str {
        match self.namespace.as_str() {
            GLOBAL_NAMESPACE => "",
            namespace => namespace,
        }
    }
}

impl std::fmt::Display for ObjectRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}/{}", self.kind, self.namespace, self.name)
    }
}

/// The address of a resource in the special `global` directory. That directory holds
/// cluster-level objects and objects of other namespaces. It is not a Kubernetes namespace, and
/// this name cannot be the same as the name of a namespace.
pub(crate) const GLOBAL_NAMESPACE: &str = "global";

/// Each kind that you can write in a Sisyphus yaml. You can pause each of these kinds.
const PAUSABLE_KINDS: [&str; 3] = ["CronJob", "Deployment", "KubernetesYaml"];

/// The Kubernetes type that has the config image annotation. This is the object itself, and not
/// the Service or another object from the same render. The result is `None` for a kind with no
/// config image. Raw Kubernetes yaml gives its own images, and there is no config image to record
/// or to roll back to.
pub(crate) fn rendered_type(sisyphus_kind: &str) -> Option<(&'static str, &'static str)> {
    match sisyphus_kind {
        "CronJob" => Some(("batch/v1", "CronJob")),
        "Deployment" => Some(("apps/v1", "Deployment")),
        _ => None,
    }
}

#[derive(Debug)]
pub(crate) struct ObjectPause {
    pub created_at: String,
    pub created_by: Option<String>,
    pub kind: String,
    pub name: String,
    pub namespace: String,
    pub reason: String,
}

/// All the data about one object. Read this data before you do a rollback.
#[derive(Debug)]
pub(crate) struct ObjectStatus {
    /// The last recorded image of each cluster, from the config image annotation on the stored
    /// object. A cluster that is not in this list has no deployment. Two clusters can have
    /// different images. This list shows both of them, and this is not an error.
    pub deployed: Vec<DeployedImage>,
    pub kind: String,
    pub name: String,
    pub namespace: String,
    pub pause: Option<ObjectPause>,
    /// A one-word form of `pause`, for a caller that needs no other data.
    pub state: &'static str,
}

#[derive(Debug)]
pub(crate) struct DeployedImage {
    pub cluster: String,
    pub image: String,
}

#[derive(Debug)]
pub(crate) struct ObjectHistoryEntry {
    /// The clusters that received this image. These can be fewer clusters than the full footprint,
    /// because a push can have a filter, and a footprint can change.
    pub clusters: Vec<String>,
    /// True if the object has this image now.
    pub deployed: bool,
    pub image: String,
    /// The time of the last push of this image. The database gives the format.
    pub last_pushed_at: String,
}

pub(crate) async fn pause_object(reference: &ObjectRef, reason: &str, pool: &AnyPool) -> Result<()> {
    if !PAUSABLE_KINDS.contains(&reference.kind.as_str()) {
        bail!(
            "{} isn't a Sisyphus kind; --kind takes one of {}",
            reference.kind,
            PAUSABLE_KINDS.join(", ")
        );
    }
    // Use one transaction. A failure between the delete and the insert would leave the object in
    // the unpaused state, and that is not safe.
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM object_pauses WHERE kind = $1 AND namespace = $2 AND name = $3")
        .bind(&reference.kind)
        .bind(&reference.namespace)
        .bind(&reference.name)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "INSERT INTO object_pauses (kind, name, namespace, reason) VALUES ($1, $2, $3, $4)",
    )
    .bind(&reference.kind)
    .bind(&reference.name)
    .bind(&reference.namespace)
    .bind(reason)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn resume_object(reference: &ObjectRef, pool: &AnyPool) -> Result<()> {
    let result =
        sqlx::query("DELETE FROM object_pauses WHERE kind = $1 AND namespace = $2 AND name = $3")
            .bind(&reference.kind)
            .bind(&reference.namespace)
            .bind(&reference.name)
            .execute(pool)
            .await?;
    if result.rows_affected() == 0 {
        bail!("{} is not paused", reference);
    }
    Ok(())
}

/// Each pause, with the key that the render code uses for each object.
pub(crate) async fn load_pauses(pool: &AnyPool) -> Result<BTreeMap<ObjectRef, ObjectPause>> {
    Ok(list_pauses(pool)
        .await?
        .into_iter()
        .map(|pause| {
            (
                ObjectRef::new(&pause.kind, &pause.namespace, &pause.name),
                pause,
            )
        })
        .collect())
}

async fn list_pauses(pool: &AnyPool) -> Result<Vec<ObjectPause>> {
    let rows = sqlx::query(
        "SELECT CAST(created_at AS TEXT) AS created_at, created_by, kind, name, namespace, reason \
         FROM object_pauses ORDER BY kind, namespace, name",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.iter().map(read_pause).collect())
}

/// All the data about one object. This function reads the database only. An object that is in the
/// configuration files, but that had no push, has the state `tracking` and no deployment.
pub(crate) async fn get_status(reference: &ObjectRef, pool: &AnyPool) -> Result<ObjectStatus> {
    let pause = get_pause(reference, pool).await?;
    Ok(ObjectStatus {
        deployed: deployed_images(reference, pool).await?,
        kind: reference.kind.clone(),
        name: reference.name.clone(),
        namespace: reference.namespace.clone(),
        state: match pause {
            Some(_) => "paused",
            None => "tracking",
        },
        pause,
    })
}

/// Records a push of an object with a config image. Sisyphus calls this function one time for each
/// applied object. An object in more than one cluster gets one row for each cluster, and
/// `list_history` groups the rows again.
///
/// This history contains pushes only. The `refresh` command makes the database agree with the
/// cluster and deploys nothing, and it adds no history.
pub(crate) async fn record_push(
    key: &KubernetesKey,
    object: &DynamicObject,
    pool: &AnyPool,
) -> Result<()> {
    let Some(image) = config_image(object) else {
        // These objects have no config image: a Service, a Namespace, or raw Kubernetes yaml.
        // There is no version to roll back to.
        return Ok(());
    };
    // Let the database set deployed_at. All the clients share the clock of the database.
    sqlx::query(
        "INSERT INTO object_history (api_version, cluster, image, kind, name, namespace) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(&key.api_version)
    .bind(&key.cluster)
    .bind(image)
    .bind(&key.kind)
    .bind(&key.name)
    .bind(namespace_or_default(key.namespace.clone()))
    .execute(pool)
    .await?;
    Ok(())
}

/// Each config image of the object, most recent first. Each image has one entry, and the number of
/// pushes does not change this. A rollback gets the digest from this list.
pub(crate) async fn list_history(
    reference: &ObjectRef,
    pool: &AnyPool,
) -> Result<Vec<ObjectHistoryEntry>> {
    // This kind has no config image, and Sisyphus recorded nothing for it.
    let Some((api_version, rendered_kind)) = rendered_type(&reference.kind) else {
        return Ok(Vec::new());
    };
    // The sqlx `Any` driver has no timestamp type, and a cast is necessary to read deployed_at.
    // This code applies to Postgres and Sqlite only, because MySQL uses a different cast.
    let rows = sqlx::query(
        "SELECT cluster, CAST(deployed_at AS TEXT) AS deployed_at, image FROM object_history \
         WHERE api_version = $1 AND kind = $2 AND name = $3 AND namespace = $4 \
         ORDER BY deployed_at DESC",
    )
    .bind(api_version)
    .bind(rendered_kind)
    .bind(&reference.name)
    .bind(reference.rendered_namespace())
    .fetch_all(pool)
    .await?;

    let deployed: BTreeSet<String> = deployed_images(reference, pool)
        .await?
        .into_iter()
        .map(|d| d.image)
        .collect();

    // This code groups the rows in Rust. Postgres makes a cluster list with `string_agg`, and the
    // other databases use `group_concat`. The number of rows for one object is small. The rows come
    // in with the most recent first, and the first row for an image sets its position.
    let mut order = Vec::new();
    let mut clusters_by_image: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for row in &rows {
        let image: String = row.get("image");
        if !clusters_by_image.contains_key(&image) {
            order.push((image.clone(), row.get::<String, _>("deployed_at")));
        }
        clusters_by_image
            .entry(image)
            .or_default()
            .insert(row.get("cluster"));
    }

    Ok(order
        .into_iter()
        .map(|(image, last_pushed_at)| ObjectHistoryEntry {
            clusters: clusters_by_image
                .remove(&image)
                .unwrap_or_default()
                .into_iter()
                .collect(),
            deployed: deployed.contains(&image),
            image,
            last_pushed_at,
        })
        .collect())
}

/// The last recorded image of each cluster, from the config image annotation on the stored object.
/// A cluster with no deployment is not in the list. An object from a push before the annotation
/// existed is also not in the list.
async fn deployed_images(reference: &ObjectRef, pool: &AnyPool) -> Result<Vec<DeployedImage>> {
    let Some((api_version, rendered_kind)) = rendered_type(&reference.kind) else {
        return Ok(Vec::new());
    };
    let rows = sqlx::query(
        "SELECT cluster, yaml FROM kubernetes_objects \
         WHERE api_version = $1 AND kind = $2 AND name = $3 AND namespace = $4 \
         ORDER BY cluster",
    )
    .bind(api_version)
    .bind(rendered_kind)
    .bind(&reference.name)
    .bind(reference.rendered_namespace())
    .fetch_all(pool)
    .await?;

    let mut deployed = Vec::new();
    for row in &rows {
        let object: DynamicObject = serde_yaml::from_str(row.get("yaml"))?;
        if let Some(image) = config_image(&object) {
            deployed.push(DeployedImage {
                cluster: row.get("cluster"),
                image: image.clone(),
            });
        }
    }
    Ok(deployed)
}

fn config_image(object: &DynamicObject) -> Option<&String> {
    object
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(CONFIG_IMAGE_ANNOTATION))
}

pub(crate) async fn get_pause(reference: &ObjectRef, pool: &AnyPool) -> Result<Option<ObjectPause>> {
    let row = sqlx::query(
        "SELECT CAST(created_at AS TEXT) AS created_at, created_by, kind, name, namespace, reason \
         FROM object_pauses WHERE kind = $1 AND namespace = $2 AND name = $3",
    )
    .bind(&reference.kind)
    .bind(&reference.namespace)
    .bind(&reference.name)
    .fetch_optional(pool)
    .await?;
    Ok(row.as_ref().map(read_pause))
}

fn read_pause(row: &sqlx::any::AnyRow) -> ObjectPause {
    ObjectPause {
        created_at: row.get("created_at"),
        created_by: row.get("created_by"),
        kind: row.get("kind"),
        name: row.get("name"),
        namespace: row.get("namespace"),
        reason: row.get("reason"),
    }
}
