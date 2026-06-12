use crate::kubernetes_io::KubernetesKey;
use clap::Args;
use std::collections::HashSet;

#[derive(Args, Debug)]
pub(crate) struct PartialKey {
    #[arg(long)]
    api_version: Option<String>,

    #[arg(long)]
    cluster: Option<String>,

    #[arg(long)]
    kind: Option<String>,

    #[arg(long)]
    name: Option<String>,

    #[arg(long)]
    namespace: Option<String>,
}

pub(crate) fn key_matches_filter(key: &KubernetesKey, filter: &PartialKey) -> bool {
    if let Some(v) = &filter.api_version {
        if &key.api_version != v {
            return false;
        }
    }
    if let Some(v) = &filter.cluster {
        if &key.cluster != v {
            return false;
        }
    }
    if let Some(v) = &filter.kind {
        if &key.kind != v {
            return false;
        }
    }
    if let Some(v) = &filter.name {
        if &key.name != v {
            return false;
        }
    }
    if filter.namespace.is_some() {
        if key.namespace != filter.namespace {
            return false;
        }
    }
    true
}

/// The `(namespace, cluster)` pairs that the given resource keys live in.
pub(crate) fn required_namespace_identities<'a>(
    keys: impl Iterator<Item = &'a KubernetesKey>,
) -> HashSet<(String, String)> {
    keys.filter_map(|k| k.namespace.clone().map(|ns| (ns, k.cluster.clone())))
        .collect()
}

/// Whether to keep a Namespace object when filtering a push. A Namespace is
/// cluster-scoped, so its name is the namespace and its own namespace field is
/// `None`; a filter like `--name foo` never matches it. Keep it when it holds a
/// resource that survived the filter, or that resource's create fails for want
/// of a namespace.
pub(crate) fn namespace_key_retained(
    key: &KubernetesKey,
    filter: &PartialKey,
    required: &HashSet<(String, String)>,
) -> bool {
    key_matches_filter(key, filter)
        || required.contains(&(key.name.clone(), key.cluster.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests for key_matches_filter
    #[test]
    fn test_key_matches_filter_empty_filter() {
        let key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "Pod".to_string(),
            name: "my-pod".to_string(),
            namespace: Some("default".to_string()),
        };
        let filter = PartialKey {
            api_version: None,
            cluster: None,
            kind: None,
            name: None,
            namespace: None,
        };

        assert!(key_matches_filter(&key, &filter));
    }

    #[test]
    fn test_key_matches_filter_api_version_mismatch() {
        let key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "Pod".to_string(),
            name: "my-pod".to_string(),
            namespace: Some("default".to_string()),
        };
        let filter = PartialKey {
            api_version: Some("apps/v1".to_string()),
            cluster: None,
            kind: None,
            name: None,
            namespace: None,
        };

        assert!(!key_matches_filter(&key, &filter));
    }

    #[test]
    fn test_key_matches_filter_cluster_mismatch() {
        let key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "Pod".to_string(),
            name: "my-pod".to_string(),
            namespace: Some("default".to_string()),
        };
        let filter = PartialKey {
            api_version: None,
            cluster: Some("dev".to_string()),
            kind: None,
            name: None,
            namespace: None,
        };

        assert!(!key_matches_filter(&key, &filter));
    }

    #[test]
    fn test_key_matches_filter_kind_mismatch() {
        let key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "Pod".to_string(),
            name: "my-pod".to_string(),
            namespace: Some("default".to_string()),
        };
        let filter = PartialKey {
            api_version: None,
            cluster: None,
            kind: Some("Deployment".to_string()),
            name: None,
            namespace: None,
        };

        assert!(!key_matches_filter(&key, &filter));
    }

    #[test]
    fn test_key_matches_filter_name_mismatch() {
        let key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "Pod".to_string(),
            name: "my-pod".to_string(),
            namespace: Some("default".to_string()),
        };
        let filter = PartialKey {
            api_version: None,
            cluster: None,
            kind: None,
            name: Some("other-pod".to_string()),
            namespace: None,
        };

        assert!(!key_matches_filter(&key, &filter));
    }

    #[test]
    fn test_key_matches_filter_namespace_mismatch() {
        let key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "Pod".to_string(),
            name: "my-pod".to_string(),
            namespace: Some("default".to_string()),
        };
        let filter = PartialKey {
            api_version: None,
            cluster: None,
            kind: None,
            name: None,
            namespace: Some("production".to_string()),
        };

        assert!(!key_matches_filter(&key, &filter));
    }

    #[test]
    fn test_key_matches_filter_partial_match() {
        let key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "Pod".to_string(),
            name: "my-pod".to_string(),
            namespace: Some("default".to_string()),
        };
        let filter = PartialKey {
            api_version: Some("v1".to_string()),
            cluster: Some("prod".to_string()),
            kind: None,
            name: None,
            namespace: None,
        };

        assert!(key_matches_filter(&key, &filter));
    }

    #[test]
    fn test_key_matches_filter_none_namespace_key() {
        let key = KubernetesKey {
            api_version: "v1".to_string(),
            cluster: "prod".to_string(),
            kind: "Namespace".to_string(),
            name: "default".to_string(),
            namespace: None,
        };
        let filter = PartialKey {
            api_version: None,
            cluster: None,
            kind: None,
            name: None,
            namespace: None,
        };

        assert!(key_matches_filter(&key, &filter));
    }

    fn resource_key(name: &str, cluster: &str, namespace: &str) -> KubernetesKey {
        KubernetesKey {
            api_version: "apps/v1".to_string(),
            cluster: cluster.to_string(),
            kind: "Deployment".to_string(),
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
        }
    }

    fn namespace_key(name: &str, cluster: &str) -> KubernetesKey {
        KubernetesKey {
            api_version: "v1".to_string(),
            cluster: cluster.to_string(),
            kind: "Namespace".to_string(),
            name: name.to_string(),
            namespace: None,
        }
    }

    fn empty_filter() -> PartialKey {
        PartialKey {
            api_version: None,
            cluster: None,
            kind: None,
            name: None,
            namespace: None,
        }
    }

    // Tests for required_namespace_identities

    #[test]
    fn test_required_namespace_identities_collects_namespace_and_cluster() {
        let keys = vec![
            resource_key("my-deployment", "cluster-a", "my-namespace"),
            resource_key("my-deployment", "cluster-b", "my-namespace"),
        ];
        let required = required_namespace_identities(keys.iter());

        assert_eq!(required.len(), 2);
        assert!(required.contains(&("my-namespace".to_string(), "cluster-a".to_string())));
        assert!(required.contains(&("my-namespace".to_string(), "cluster-b".to_string())));
    }

    #[test]
    fn test_required_namespace_identities_ignores_cluster_scoped_keys() {
        // A cluster-scoped key has no namespace of its own to contribute.
        let keys = vec![namespace_key("my-namespace", "cluster-a")];
        let required = required_namespace_identities(keys.iter());

        assert!(required.is_empty());
    }

    // Tests for namespace_key_retained

    #[test]
    fn test_namespace_retained_when_it_contains_a_surviving_resource() {
        // `--name` filters out the Namespace object, but it's kept because a
        // resource that survived the filter lives in it.
        let filter = PartialKey {
            name: Some("my-deployment".to_string()),
            ..empty_filter()
        };
        let required = required_namespace_identities(
            [resource_key("my-deployment", "cluster-b", "my-namespace")].iter(),
        );

        assert!(namespace_key_retained(
            &namespace_key("my-namespace", "cluster-b"),
            &filter,
            &required
        ));
    }

    #[test]
    fn test_namespace_dropped_when_unrelated_and_filtered_out() {
        let filter = PartialKey {
            name: Some("my-deployment".to_string()),
            ..empty_filter()
        };
        let required = required_namespace_identities(
            [resource_key("my-deployment", "cluster-b", "my-namespace")].iter(),
        );

        // A namespace that holds nothing we're pushing is dropped.
        assert!(!namespace_key_retained(
            &namespace_key("other", "cluster-b"),
            &filter,
            &required
        ));
    }

    #[test]
    fn test_namespace_retained_when_it_matches_filter_directly() {
        let required = required_namespace_identities(std::iter::empty());

        assert!(namespace_key_retained(
            &namespace_key("my-namespace", "cluster-b"),
            &empty_filter(),
            &required
        ));
    }

    #[test]
    fn test_namespace_retention_is_cluster_specific() {
        // The resource is pushed only to cluster-b, so the namespace is kept
        // there but not on cluster-a.
        let filter = PartialKey {
            name: Some("my-deployment".to_string()),
            ..empty_filter()
        };
        let required = required_namespace_identities(
            [resource_key("my-deployment", "cluster-b", "my-namespace")].iter(),
        );

        assert!(namespace_key_retained(
            &namespace_key("my-namespace", "cluster-b"),
            &filter,
            &required
        ));
        assert!(!namespace_key_retained(
            &namespace_key("my-namespace", "cluster-a"),
            &filter,
            &required
        ));
    }
}
