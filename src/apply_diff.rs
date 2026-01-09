use anyhow::{bail, Context, Result};
use kube::{api::{DeleteParams, DynamicObject, Patch, PatchParams}, discovery::Scope};
use sqlx::AnyPool;
use std::time::Duration;
use tokio::time::sleep;

use crate::{
    generate_diff::DiffAction,
    kubernetes_io::{
        get_kubernetes_api, get_kubernetes_clients, KubernetesKey, MANAGER,
    },
};

pub(crate) async fn apply_diff(
    changed: Vec<(KubernetesKey, DiffAction)>,
    pool: &AnyPool,
) -> Result<()> {
    let (clients, types) = get_kubernetes_clients(changed.iter().map(|(k, _)| k)).await?;
    // Check that we don't have any namespace vs resource scope mismatches
    for (key, _) in &changed {
        let Some((_, caps)) = types.get(&(key.api_version.clone(), key.kind.clone())) else {
            bail!("Unable to find Kubernetes type for key {:?}", key);
        };
        match (&caps.scope, &key.namespace) {
            (Scope::Cluster, None) => {},
            (Scope::Cluster, Some(_)) =>
                bail!("Creating a cluster-scoped resource with a namespace will fail"),
            (Scope::Namespaced, Some(_)) => {},
            (Scope::Namespaced, None) =>
                bail!("Creating a namespaced-scoped resource without a namespace is disallowed"),
        }
    }
    let mut pending_deletions: Vec<(kube::Api<DynamicObject>, String)> = Vec::new();
    for (key, action) in changed {
        let api = get_kubernetes_api(&key, &clients, &types)?;
        let is_delete = matches!(action, DiffAction::Delete);
        apply_single_diff(action, &key, &api, pool).await?;
        if is_delete {
            pending_deletions.push((api, key.name.clone()));
        }
    }
    // Wait for all deletions to complete before returning
    for (api, name) in &pending_deletions {
        wait_for_deletion(api, name).await?;
    }
    Ok(())
}

async fn apply_single_diff(
    action: DiffAction,
    key: &KubernetesKey,
    api: &kube::Api<DynamicObject>,
    pool: &AnyPool,
) -> Result<()> {
    match action {
        DiffAction::Create(v) => {
            let result = api
                .patch(
                    &key.name,
                    &PatchParams::apply(MANAGER).force(),
                    &Patch::Apply(v),
                )
                .await
                .with_context(|| format!("while creating {}", key))?;
            sqlx::query(
                r#"
                INSERT INTO kubernetes_objects (api_version, cluster, kind, name, namespace, yaml)
                VALUES ($1, $2, $3, $4, $5, $6)
                "#,
            )
            .bind(key.api_version.clone())
            .bind(key.cluster.clone())
            .bind(key.kind.clone())
            .bind(key.name.clone())
            .bind(namespace_or_default(key.namespace.clone()))
            .bind(serde_yaml::to_string(&result)?)
            .execute(pool)
            .await?;
            println!("Created {}", key);
        }
        DiffAction::Delete => {
            api.delete(&key.name, &DeleteParams::default())
                .await
                .with_context(|| format!("while deleting {}", key))?;
            sqlx::query(
                r#"
                DELETE FROM kubernetes_objects
                WHERE
                    api_version = $1
                    AND cluster = $2
                    AND kind = $3
                    AND name = $4
                    AND namespace = $5
                "#,
            )
            .bind(key.api_version.clone())
            .bind(key.cluster.clone())
            .bind(key.kind.clone())
            .bind(key.name.clone())
            .bind(namespace_or_default(key.namespace.clone()))
            .execute(pool)
            .await?;
            println!("Deleted {}", key);
        }
        DiffAction::Patch { patch, .. } => {
            let result = api
                .patch(
                    &key.name,
                    &PatchParams::apply(MANAGER),
                    &Patch::<()>::Json(patch),
                )
                .await
                .with_context(|| format!("while updating {}", key))?;
            sqlx::query(
                r#"
                UPDATE kubernetes_objects
                SET last_updated = CURRENT_TIMESTAMP, yaml = $1
                WHERE
                    api_version = $2
                    AND cluster = $3
                    AND kind = $4
                    AND name = $5
                    AND namespace = $6
                "#,
            )
            .bind(serde_yaml::to_string(&result)?)
            .bind(key.api_version.clone())
            .bind(key.cluster.clone())
            .bind(key.kind.clone())
            .bind(key.name.clone())
            .bind(namespace_or_default(key.namespace.clone()))
            .execute(pool)
            .await?;
            println!("Updated {}", key);
        }
        DiffAction::Recreate(v) => {
            api.delete(&key.name, &DeleteParams::default())
                .await
                .with_context(|| format!("while replacing {}", key))?;
            println!("Deleting prior to recreate {}", key);
            wait_for_deletion(api, &key.name).await?;
            let result = api
                .patch(
                    &key.name,
                    &PatchParams::apply(MANAGER).force(),
                    &Patch::Apply(v),
                )
                .await
                .with_context(|| format!("while replacing {}", key))?;
            sqlx::query(
                r#"
                UPDATE kubernetes_objects
                SET last_updated = CURRENT_TIMESTAMP, yaml = $1
                WHERE
                    api_version = $2
                    AND cluster = $3
                    AND kind = $4
                    AND name = $5
                    AND namespace = $6
                "#,
            )
            .bind(serde_yaml::to_string(&result)?)
            .bind(key.api_version.clone())
            .bind(key.cluster.clone())
            .bind(key.kind.clone())
            .bind(key.name.clone())
            .bind(namespace_or_default(key.namespace.clone()))
            .execute(pool)
            .await?;
            println!("Recreated {}", key);
        }
    }
    Ok(())
}

pub(crate) fn namespace_or_default(namespace: Option<String>) -> String {
    namespace.unwrap_or_else(|| "".to_string())
}

async fn wait_for_deletion(api: &kube::Api<DynamicObject>, name: &str) -> Result<()> {
    let mut i = 0;
    loop {
        if i == 1 {
            println!("Waiting for {} to be deleted...", name);
        }

        match api.get_opt(name).await? {
            Some(_) => {
                sleep(Duration::from_millis(500)).await;
            }
            None => {
                return Ok(());
            }
        }

        i += 1;
    }
}
