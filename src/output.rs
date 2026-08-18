use crate::{
    generate_diff::{DiffAction, ResourceDiff},
    kubernetes_io::KubernetesKey,
    object_policy::{ObjectHistoryEntry, ObjectPause, ObjectRef, ObjectStatus},
};
use console::{style, Style};
use similar::{ChangeTag, TextDiff};

/// The name of an action in a report. The code copies this name before the apply step uses the
/// action. Then it is not necessary to copy the full object only to name it after the apply step.
#[derive(Clone, Copy)]
pub(crate) struct ActionLabels {
    past_tense: &'static str,
}

impl From<&DiffAction> for ActionLabels {
    fn from(action: &DiffAction) -> Self {
        let past_tense = match action {
            DiffAction::Create(_) => "Created",
            DiffAction::Delete => "Deleted",
            DiffAction::Patch { .. } => "Updated",
            DiffAction::Recreate(_) => "Recreated",
        };
        ActionLabels { past_tense }
    }
}

/// Reports each change before Sisyphus asks the user for permission.
pub(crate) fn report_diffs(diffs: &[ResourceDiff]) {
    for diff in diffs {
        println!("• {} {}\n", styled_verb(&diff.action), diff.key);
        print_diff(&diff.before, &diff.after);
        println!("");
    }
}

/// Reports an object that a pause keeps out of the push, and gives the names of the Kubernetes
/// objects in that pause. One Sisyphus object can render more than one Kubernetes object. For
/// example, a Deployment also renders a Service. The list shows the full range of the pause.
pub(crate) fn report_paused(pause: &ObjectPause, held: &[KubernetesKey]) {
    let reference = ObjectRef::new(&pause.kind, &pause.namespace, &pause.name);
    // This is a progress message and not a change, and it goes to stderr with the other messages.
    eprintln!("Skipping {}: paused ({})", reference, pause.reason);
    for key in held {
        eprintln!("  {}", style(key).dim());
    }
}

/// Reports a change that Sisyphus made to the cluster and to the database.
pub(crate) fn report_applied(key: &KubernetesKey, action: ActionLabels) {
    println!("{} {}", action.past_tense, key);
}

/// All the data that controls one object.
pub(crate) fn report_status(status: &ObjectStatus) {
    println!(
        "{}\n",
        style(format!(
            "{} {}/{}",
            status.kind, status.namespace, status.name
        ))
        .bold()
    );
    println!("  State    {}", styled_state(status.state));
    match &status.pause {
        Some(pause) => {
            println!("  Paused   {}", pause.reason);
            println!("           {}", style(attribution(&pause.created_by, &pause.created_at)).dim());
        }
        None => println!("  Paused   {}", style("no").dim()),
    }
    println!("\n  Deployed");
    if status.deployed.is_empty() {
        println!("    {}", style("nothing deployed").dim());
        return;
    }
    // Usually each cluster runs the same image, and one line shows this. If the clusters have
    // different images, each image gets its own line.
    let mut by_image: std::collections::BTreeMap<&str, Vec<&str>> = Default::default();
    for entry in &status.deployed {
        by_image
            .entry(entry.image.as_str())
            .or_default()
            .push(entry.cluster.as_str());
    }
    let width = by_image.values().map(|c| c.join(", ").len()).max().unwrap_or(0);
    for (image, clusters) in by_image {
        println!("    {:width$}  {}", clusters.join(", "), image, width = width);
    }
}

/// Lists each image of an object. A rollback uses this list to select a version that did run.
pub(crate) fn report_history(history: &[ObjectHistoryEntry]) {
    if history.is_empty() {
        println!("Nothing pushed yet");
        return;
    }
    for entry in history {
        let deployed = match entry.deployed {
            true => format!("  {}", style("deployed").green()),
            false => "".to_string(),
        };
        println!("{}{}", entry.image, deployed);
        println!(
            "  {} {}",
            style(&entry.last_pushed_at).dim(),
            style(entry.clusters.join(", ")).dim()
        );
    }
}

pub(crate) fn print_diff(before: &str, after: &str) {
    for change in TextDiff::from_lines(before, after).iter_all_changes() {
        let (sign, style) = match change.tag() {
            ChangeTag::Delete => ("-", Style::new().red()),
            ChangeTag::Insert => ("+", Style::new().green()),
            ChangeTag::Equal => (" ", Style::new()),
        };
        print!("{}{}", style.apply_to(sign).bold(), style.apply_to(change));
    }
}

/// The user that made the change, and the time of the change. The database has no user name if the
/// CLI did not set SISYPHUS_USERNAME.
fn attribution(created_by: &Option<String>, created_at: &str) -> String {
    match created_by {
        Some(who) => format!("{}, {}", who, created_at),
        None => created_at.to_string(),
    }
}

fn styled_state(state: &str) -> console::StyledObject<&str> {
    match state {
        "paused" => style(state).yellow(),
        _ => style(state).green(),
    }
}

fn styled_verb(action: &DiffAction) -> console::StyledObject<&'static str> {
    match action {
        DiffAction::Create(_) => style("create").green(),
        DiffAction::Delete => style("delete").red(),
        DiffAction::Patch { .. } => style("patch").yellow(),
        DiffAction::Recreate(_) => style("delete and recreate").red(),
    }
}
