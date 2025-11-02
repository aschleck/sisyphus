use anyhow::{Context, Result};
use kube::api::{DeleteParams, DynamicObject, Patch, PatchParams};
use sqlx::AnyPool;

use crate::{
    generate_diff::DiffAction,
    kubernetes::{
        get_kubernetes_api, get_kubernetes_clients, KubernetesKey, KubernetesResources, MANAGER,
    },
};

pub(crate) async fn apply_diff(
    changed: Vec<(KubernetesKey, DiffAction)>,
    have: &KubernetesResources,
    want: &KubernetesResources,
    pool: &AnyPool,
) -> Result<()> {
    let (clients, types) =
        get_kubernetes_clients(have.by_key.keys().chain(want.by_key.keys())).await?;
    for (key, action) in changed {
        let api = get_kubernetes_api(&key, &clients, &types)?;
        apply_single_diff(action, &key, &api, pool).await?;
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
