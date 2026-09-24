use crate::sisyphus_yaml::{HasKind, SisyphusResource, VariableSource, VariablesYaml};
use anyhow::{anyhow, bail, Context, Result};
use kube::{
    api::{DynamicObject, ObjectMeta},
    ResourceExt,
};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File},
    path::Path,
};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct SisyphusKey {
    pub api_version: String,
    pub kind: String,
    pub name: String,
}

#[derive(Debug)]
pub(crate) struct SisyphusResources {
    pub by_namespace_by_key: HashMap<String, HashMap<SisyphusKey, SisyphusResource>>,
    pub global_by_key: HashMap<SisyphusKey, SisyphusResource>,
}

/// Loads the resources of each directory in `directory`. Each directory is one namespace, except
/// `global`, and its `index.yaml` names the resources it holds. A `variables.yaml` beside the
/// directories sets variables that every directory inherits.
pub(crate) fn get_sisyphus_resources_from_files(directory: &Path) -> Result<SisyphusResources> {
    let root_variables = get_root_variables(&directory.join("variables.yaml"))?;
    let mut resources = SisyphusResources {
        by_namespace_by_key: HashMap::new(),
        global_by_key: HashMap::new(),
    };
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.is_dir() {
            let (resources, allow_any_namespace, namespace) =
                match path.file_name().map(|s| s.to_str()).flatten() {
                    Some("global") => (&mut resources.global_by_key, true, None),
                    Some(namespace) => (
                        resources
                            .by_namespace_by_key
                            .entry(namespace.to_string())
                            .or_insert_with(|| HashMap::new()),
                        false,
                        Some(namespace.to_string()),
                    ),
                    None => bail!("Path has no filename"),
                };
            get_objects_from_namespace(
                &path,
                resources,
                allow_any_namespace,
                &namespace,
                &root_variables,
            )?;
        }
    }
    Ok(resources)
}

/// Reads the variables that every directory inherits.
fn get_root_variables(path: &Path) -> Result<BTreeMap<String, VariableSource>> {
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let root: VariablesYaml = serde_yaml::from_reader(File::open(&path)?)
        .with_context(|| format!("in file {:?}", path))?;
    Ok(root.variables)
}

fn get_objects_from_namespace(
    directory: &Path,
    resources: &mut HashMap<SisyphusKey, SisyphusResource>,
    allow_any_namespace: bool,
    namespace: &Option<String>,
    inherited: &BTreeMap<String, VariableSource>,
) -> Result<()> {
    let index_path = directory.join("index.yaml");
    if !index_path.exists() {
        return Ok(());
    }
    get_objects_from_file(
        &index_path,
        resources,
        allow_any_namespace,
        &namespace,
        inherited,
    )
}

fn get_objects_from_file(
    path: &Path,
    resources: &mut HashMap<SisyphusKey, SisyphusResource>,
    allow_any_namespace: bool,
    namespace: &Option<String>,
    inherited: &BTreeMap<String, VariableSource>,
) -> Result<()> {
    let directory = path
        .parent()
        .ok_or_else(|| anyhow!("Expected to be in a child folder"))?;
    let reader = File::open(&path)?;
    for document in serde_yaml::Deserializer::from_reader(&reader) {
        let mut object: SisyphusResource = SisyphusResource::deserialize(document)
            .with_context(|| format!("in file {:?}", path))?;

        match &mut object {
            SisyphusResource::KubernetesYaml(v) => {
                let mut extra_objects = Vec::new();
                for source_path in &v.sources {
                    load_objects_from_kubernetes_yaml(
                        &directory.join(source_path),
                        &mut extra_objects,
                    )
                    .with_context(|| {
                        format!("reading file {:?} referenced by {:?}", source_path, path)
                    })?;
                }
                v.sources.clear();
                v.objects.append(&mut extra_objects);

                for object in &mut v.objects {
                    if let Some(namespace) = object.metadata.namespace.as_ref() {
                        if !allow_any_namespace {
                            let types = object
                                .types
                                .as_ref()
                                .map(|t| format!("{}/{}", t.api_version, t.kind))
                                .unwrap_or_else(|| "unknown".to_string());
                            bail!(
                                "{}/{} referenced by {} in {:?} should not specify namespace {:?}",
                                types,
                                object.name_any(),
                                v.metadata.name,
                                path,
                                namespace
                            );
                        }
                    } else {
                        object.metadata.namespace = namespace.clone();
                    }
                }
            }
            SisyphusResource::SisyphusCronJob(v) => {
                inherit_variables(&mut v.config.variables, inherited);
            }
            SisyphusResource::SisyphusDeployment(v) => {
                inherit_variables(&mut v.config.variables, inherited);
            }
            SisyphusResource::SisyphusYaml(v) => {
                // The variables of this index reach only the files it lists, and there they
                // override the variables that it inherits.
                let mut layered = inherited.clone();
                layered.extend(v.variables.clone());
                for source_path in &v.sources {
                    get_objects_from_file(
                        &directory.join(source_path),
                        resources,
                        allow_any_namespace,
                        namespace,
                        &layered,
                    )?;
                }
                continue;
            }
        }
        insert_sisyphus_resource(object, resources)?;
    }

    Ok(())
}

fn load_objects_from_kubernetes_yaml(path: &Path, into: &mut Vec<DynamicObject>) -> Result<()> {
    let reader = File::open(&path)?;
    for document in serde_yaml::Deserializer::from_reader(&reader) {
        let object: DynamicObject = DynamicObject::deserialize(document)?;
        if object.types.is_none() && object.metadata == ObjectMeta::default() {
            // kubectl tolerates these, so we do too
            continue;
        }
        into.push(object);
    }

    Ok(())
}

/// Gives a Deployment or a CronJob each inherited variable that it doesn't set itself.
fn inherit_variables(
    variables: &mut BTreeMap<String, VariableSource>,
    inherited: &BTreeMap<String, VariableSource>,
) {
    for (name, source) in inherited {
        variables
            .entry(name.clone())
            .or_insert_with(|| source.clone());
    }
}

fn insert_sisyphus_resource(
    object: SisyphusResource,
    resources: &mut HashMap<SisyphusKey, SisyphusResource>,
) -> Result<()> {
    let (api_version, kind, name) = match &object {
        SisyphusResource::KubernetesYaml(v) => (&v.api_version, v.kind(), &v.metadata.name),
        SisyphusResource::SisyphusCronJob(v) => (&v.api_version, v.kind(), &v.metadata.name),
        SisyphusResource::SisyphusDeployment(v) => (&v.api_version, v.kind(), &v.metadata.name),
        SisyphusResource::SisyphusYaml(_) => unreachable!("These should already have been loaded"),
    };
    let key = SisyphusKey {
        api_version: api_version.clone(),
        kind: kind.to_string(),
        name: name.clone(),
    };
    if resources.contains_key(&key) {
        bail!("Key {:?} already exists", key);
    }
    resources.insert(key, object);
    Ok(())
}
