use crate::{
    generate_diff::ResourceDiff,
    kubernetes_io::{KubernetesKey, KubernetesResources},
};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
};

#[cfg(test)]
mod tests;

const LATEST_VERSION: u32 = 1;

/// The changes of one push, as `plan` writes them and as `push --plan` applies them.
#[derive(Deserialize, Serialize)]
struct Plan {
    changes: Vec<PlannedChange>,
    version: u32,
}

/// One change of a plan, with the database row that `plan` computed the change from.
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct PlannedChange {
    pub change: ResourceDiff,
    /// The yaml of this object in the database when `plan` ran (or None if there wasn't one)
    pub database: Option<String>,
}

#[derive(Deserialize)]
struct PlanVersion {
    version: u32,
}

/// Writes the changes of a push to a file, for a later `push --plan`.
pub(crate) fn write_plan(
    changes: Vec<ResourceDiff>,
    from_database: &KubernetesResources,
    path: &Path,
) -> Result<()> {
    let changes = changes
        .into_iter()
        .map(|change| {
            let database = database_yaml(&change.key, from_database)?;
            Ok(PlannedChange { change, database })
        })
        .collect::<Result<Vec<PlannedChange>>>()?;
    let plan = Plan {
        changes,
        version: LATEST_VERSION,
    };
    let mut file = create_private_file(path)
        .with_context(|| format!("while creating the plan file {}", path.display()))?;
    file.write_all(serde_json::to_string_pretty(&plan)?.as_bytes())
        .with_context(|| format!("while writing the plan file {}", path.display()))?;
    Ok(())
}

/// Reads the changes of a plan file but doesn't check any state
pub(crate) fn read_plan(path: &Path) -> Result<Vec<PlannedChange>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("while reading the plan file {}", path.display()))?;
    let version: PlanVersion = serde_json::from_str(&text).with_context(|| {
        format!(
            "while reading the version of the plan file {}",
            path.display()
        )
    })?;
    if version.version != LATEST_VERSION {
        bail!(
            "{} is a version {} plan, and this Sisyphus writes and applies version {}. Make the \
             plan again with this version.",
            path.display(),
            version.version,
            LATEST_VERSION
        );
    }
    let plan: Plan = serde_json::from_str(&text)
        .with_context(|| format!("while reading the plan file {}", path.display()))?;
    Ok(plan.changes)
}

/// Checks a plan against the database state and rejects it if there's a mismatch.
pub(crate) fn check_plan_is_current(
    planned: &[PlannedChange],
    from_database: &KubernetesResources,
) -> Result<()> {
    let mut stale = Vec::new();
    for p in planned {
        let now = database_yaml(&p.change.key, from_database)?;
        if now == p.database {
            continue;
        }
        stale.push(match (&p.database, &now) {
            (None, Some(_)) => format!("  Object {} already exists", p.change.key),
            (Some(_), None) => format!("  Object {} is not tracked by Sisyphus", p.change.key),
            _ => format!(
                "  Object {} has changed since the plan was written",
                p.change.key
            ),
        });
    }
    if !stale.is_empty() {
        bail!("Aborting push due to an out-of-date plan:\n\n{}", stale.join("\n"));
    }
    Ok(())
}

fn database_yaml(
    key: &KubernetesKey,
    from_database: &KubernetesResources,
) -> Result<Option<String>> {
    let object = from_database
        .by_key
        .get(key)
        .or_else(|| from_database.namespaces.get(key));
    match object {
        Some(o) => Ok(Some(serde_yaml::to_string(o).with_context(|| {
            format!("while reading the database state of {}", key)
        })?)),
        None => Ok(None),
    }
}

/// Creates a file that only its owner can read.
fn create_private_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    // `mode` applies to a new file only. An existing file keeps the permissions it has.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(file)
}
