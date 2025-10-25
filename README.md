# Sisyphus

Sisyphus is a deployment platform for Kubernetes with four key features:

* Binary containers are paired with a version-specific configuration container. This configuration
  defines flags (arguments and environment variables) that adapt to different execution
  environments (like prod, test, and dev), ensuring configuration always matches the binary version.
* Configuration container tags (e.g., `:latest`) are always resolved to their specific digests
  during a `push`. When a new container is tagged, Sisyphus ensures that exact version is applied on
  the next deployment, guaranteeing reproducibility.
* Resources declare which Kubernetes clusters they should be pushed to. Deployments use "footprints"
  to define replicas per cluster, and Kubernetes YAML resources specify target clusters for their objects.
* Namespaces are defined by the folder structure, which makes organizational oversight easy using
  `CODEOWNERS` mechanisms.

## Why use Sisyphus?

Sisyphus makes it easy to both develop servers and manage multiple Kubernetes clusters.

One challenge with developing a server is that you often define a new flag long before you actually
set the flag in production. This means that every time you push a new version, you have to remember
what flags and environment variables apply to that version. When rolling forward it's generally
tolerable, but rollbacks can become dangerous when you no longer remember what parameters were set
at a particular version. Sisyphus fixes this by defining a "config container" (it's just a Docker container
with some text file inside of it) that tightly couple 1:1 with each version of your server's
binary container. When you tell Sisyphus to push a particular config, Sisyphus pushes both the flags
defined by the config and the correct version of the binary.

Another challenge is that servers need to run in multiple environments. When developing, you want to
hardcode specific values like tokens. But, when you run in production or in a staging environment,
you want to use a secret. You can solve this by passing long lists of arguments to the binary when
developing, and by duplicating lists of arguments with small differences when creating yaml for
production and staging, but it's extremely error-prone. Sisyphus fixes this by defining "execution
environments" in the config container. You tell Sisyphus what environment a deployment should run in
and Sisyphus will resolve the flags accordingly.

Managing a single cluster is trivial but managing multiple clusters often requires use of tools
like Terraform/Terragrunt and Pulumi. Furthermore, depending on your load requirements, you may want
to only put certain resources in certain clusters or define different numbers of replicas per
cluster. Sisyphus footprints control both the clusters a resource runs in and the number of
replicas.

When defining a resource in Kubernetes yaml, you often want to run the latest version of something
by using the `:latest` tag. However IaC tools like Pulumi don't understand that label points at a
specific version and they will not notice if a new version is pushed. Sisyphus deployments fix this
by being container-aware and automatically detect new versions of containers during comparisons.

## What it looks like

### Defining a configuration

````starlark
# filepath: example/echo/frontend.star

def main(ctx):
    return Application(
        args=[
            "--service_spec",
            {
                "prod": "backend-prod.svc",
                "test": "backend-test.svc",
                "dev": "backend-test.svc",
            },
        ],
        env={
            "COLOR": "pink",
            "SECRET_TOKEN": StringVariable("secret-token"),
            "GOOGLE_APPLICATION_CREDENTIALS": (
                FileVariable(name="google-credentials", path="/etc/google/credentials.json")
            ),
            "HTTP_PORT": {
                "prod": Port(name="http", number=80),
                "test": Port(name="http", number=80),
                "dev": Port(name="http", number=8080),
            },
        },
    )
````

This file defines the configuration the binary should run with. Values can either be specified
directly or a dictionary can be passed with different values per environment. Values like secrets
are received using `StringVariable`s and are defined in the yaml in the next section. `FileVariable`
is like a `StringVariable` but ensures the value of the string is mounted in the container at the
specified path. `Port` is a special marker that ensures the ports are exposed in the Kubernetes
deployment object and are available for use by Kubernetes `Service`s.

### Defining a config image

Configuration images can be built using various tools. If you use Bazel, you can use the
`sisyphus_pushable` rule (from `defs.bzl`) in your `BUILD.bazel` file:

````bazel
# filepath: example/echo/BUILD.bazel

load("@dev_april_sisyphus//:defs.bzl", "sisyphus_pushable")

sisyphus_pushable(
    name = "sisyphus",
    binary_image = ":image", # refers to a rules_oci oci_image target
    binary_repository = "us-docker.pkg.dev/acme/containers/echo",  # use your repository here
    config_entrypoint = "frontend.star",
    remote_tags = ["latest"],
)

... your existing code ...
````

`config_entrypoint` refers to the Starlark configuration defined in the last section.

The config and binary images can be built and pushed simultaneously by running
`bazel run //echo:sisyphus_push`. If you like, you can inspect the binary and config images by
pulling `us-docker.pkg.dev/acme/containers/echo:latest` and
`us-docker.pkg.dev/acme/containers/echo_config:latest` respectively.

For users who don't use Bazel, a config image can be created with a trivial `Containerfile` and pushed to
a registry. A config image only requires two files: an `index.json` and the Starlark
file from the last section. An example `index.json` file is shown below.

```json
{
  "binary_digest": "sha256:a130de16c89c07a0a0061fce19a0cb78a30210dad3218a49379e6a8735eb19a1",
  "binary_image": "us-docker.pkg.dev/acme/containers/echo",
  "config_entrypoint": "echo/frontend.star"
}
```

### Deploying with `SisyphusDeployment`

Once your images are built and pushed, you define your Kubernetes deployment using a
`SisyphusDeployment` resource in a Sisyphus YAML file. This object's `image` property references
your config image and `variables` assigns values to the variables define in your Starlark file.

````yaml
# filepath: example/production/echo/index.yaml

apiVersion: sisyphus/v1
kind: Deployment
metadata:
  name: echo
config:
  env: prod # This chooses the "prod" branch of the dictionaries defined in the Starlark
  image: us-docker.pkg.dev/acme/containers/echo_config:latest
  variables:
    google-credentials:
      secretKeyRef: # Connects to `FileVariable(name="google-credentials", ...)` above
        name: google
        key: google-application-credentials.json
    secret-token:
      secretKeyRef: # Connects to `StringVariable("secret-token")` above
        name: tokens
        key: secret-token
footprint: # Per-cluster sizing. These cluster IDs are contexts defined in kubeconfig.
  gke_acme_us-central1_ap-us-central1:
    replicas: 1
  gke_acme_us-west4_ap-us-west4:
    replicas: 1
````

Note that this object does not define a namespace. Because the path is `echo/index.yaml`
Sisyphus automatically assigns the namespace `echo` to all objects in that folder.

### Deploying with `KubernetesYaml`

````yaml
# filepath: example/production/echo/index.yaml

# ... the Deployment above ...

---

apiVersion: sisyphus/v1
kind: KubernetesYaml
metadata:
  name: secrets
sources: # List of yaml files to load
  - secrets.yaml
clusters: # Clusters to apply the yaml files in
  - gke_acme_us-central1_ap-us-central1
  - gke_acme_us-west4_ap-us-west4
````

````yaml
# filepath: example/production/echo/secrets.yaml

apiVersion: v1
kind: Secret
metadata:
  name: google
stringData:
  google-application-credentials.json: replace-me

---

apiVersion: v1
kind: Secret
metadata:
  name: tokens
stringData:
  secret-token: replace-me
````

Sisyphus treats secrets specially: refreshing resources will never download the secret values and pushing will never override secret values. This allows you to commit values like `replace-me` in code and then use kubectl to set your secrets in the cluster without fear of them leaking via Sisyphus.

## Running Sisyphus

### Database setup

Sisyphus requires PostgreSQL, MySQL, or Sqlite. If you're using PostgreSQL, you can run
`20250326020918_initialize.sql` directly. For other databases, just run the `CREATE TABLE`
statement.

### Deploying your configuration

To apply your configurations, use the `push` command. You'll need to specify your database URL and
the directory containing your Sisyphus resource definitions.

````bash
sisyphus \
    --database-url 'postgres://user:password@some.server/sisyphus' \
    push \
    --monitor-directory './production'
````

This command will compare your local configuration with the last configuration applied by Sisyphus.
If you consent to pushing the changes, they will be applied to your clusters.

You can also use `refresh` to synchronize the database with the current state of your clusters.

````bash
sisyphus \
    --database-url 'postgres://user:password@some.server/sisyphus' \
    refresh
````

# What's missing

* [ ] Support for Kubernetes cronjobs
* [ ] `sisyphus run config` for dev: run binaries locally and allow specifying the variables
* [ ] `sisyphus run image`: run a config image in an environment and allow specifying the variables
* [ ] Starlark `load()` statements to allow code reuse
* [ ] Resource requests and limits on Sisyphus deployments
* [ ] Default values for StringVariables so server specs can be overridden when running locally
* [ ] `SisyphusYaml` objects to include more yaml from the index.yaml file
* [ ] Tests

