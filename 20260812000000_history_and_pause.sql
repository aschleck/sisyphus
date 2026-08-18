-- Keeps a Sisyphus object out of all pushes. The object continues to run with the configuration
-- from its last push, until a user resumes it. To change the image but keep the pause, use
-- `object push`.
CREATE TABLE IF NOT EXISTS object_pauses
(
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT DEFAULT current_setting('app.username', true),
    -- Deployment, CronJob, or KubernetesYaml, as written in the Sisyphus yaml. This is not the
    -- kind of the rendered Kubernetes object.
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    namespace TEXT NOT NULL,
    reason TEXT NOT NULL,
    PRIMARY KEY (kind, namespace, name)
);

SELECT create_audit_table('object_pauses');

-- A log of each push. Sisyphus only adds rows to this table. A rollback uses this log to select a
-- version that did run. The key is the rendered Kubernetes type, which agrees with
-- kubernetes_objects.
CREATE TABLE IF NOT EXISTS object_history
(
    api_version TEXT NOT NULL,
    cluster TEXT NOT NULL,
    deployed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deployed_by TEXT DEFAULT current_setting('app.username', true),
    image TEXT NOT NULL,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    namespace TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS object_history_resource
    ON object_history (kind, namespace, name, api_version, deployed_at);
