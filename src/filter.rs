use crate::kubernetes::KubernetesKey;
use clap::Args;

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
}
