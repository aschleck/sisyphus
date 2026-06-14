use allocative::Allocative;
use anyhow::{anyhow, bail, Result};
use serde::Deserialize;
use starlark::{
    any::ProvidesStaticType,
    environment::{Globals, GlobalsBuilder, LibraryExtension},
    eval::Evaluator,
    starlark_module,
    values::{
        dict::UnpackDictEntries, float::StarlarkFloat, list_or_tuple::UnpackListOrTuple,
        starlark_value, NoSerialize, StarlarkValue, UnpackValue, Value, ValueLike,
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    convert::TryInto,
    fmt,
    path::Path,
};

#[cfg(test)]
mod tests;

#[derive(Deserialize, Debug)]
pub(crate) struct ConfigImageIndex {
    pub binary_digest: String,
    pub binary_repository: String,
    pub config_entrypoint: String,
}

#[derive(Allocative, Clone, Debug, NoSerialize, ProvidesStaticType)]
pub(crate) struct Application {
    pub args: Vec<ArgumentValues>,
    pub env: BTreeMap<String, ArgumentValues>,
    pub labels: BTreeMap<String, String>,
    pub liveness: Option<Probe>,
    pub readiness: Option<Probe>,
    pub resources: Resources,
    pub startup: Option<Probe>,
}

impl fmt::Display for Application {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "Application()")
    }
}

#[starlark_value(type = "Application", UnpackValue, StarlarkTypeRepr)]
impl<'v> StarlarkValue<'v> for Application {}

#[derive(Allocative, Clone, Debug)]
pub(crate) enum Argument {
    FileVariable(FileVariable),
    Port(Port),
    String(String),
    StringVariable(StringVariable),
}

impl Argument {
    fn unpack_value(value: Value) -> starlark::Result<Self> {
        if let Some(v) = value.downcast_ref::<Port>() {
            Ok(Self::Port(v.clone()))
        } else if let Some(v) = value.downcast_ref::<FileVariable>() {
            Ok(Self::FileVariable(v.clone()))
        } else if let Some(v) = value.downcast_ref::<StringVariable>() {
            Ok(Self::StringVariable(v.clone()))
        } else if let Some(v) = value.unpack_bool() {
            Ok(Self::String(v.to_string()))
        } else if let Some(v) = StarlarkFloat::unpack_value(value)? {
            Ok(Self::String(v.to_string()))
        } else if let Some(v) = value.unpack_i32() {
            Ok(Self::String(v.to_string()))
        } else if let Some(v) = value.unpack_str() {
            Ok(Self::String(v.to_string()))
        } else {
            Err(starlark::Error::new_kind(starlark::ErrorKind::Function(
                anyhow!("invalid argument: {:?}", value),
            )))
        }
    }
}

#[derive(Allocative, Clone, Debug)]
pub(crate) enum ArgumentValues {
    Uniform(Argument),
    Varying(BTreeMap<String, Argument>),
}

impl ArgumentValues {
    fn unpack_value(value: Value) -> starlark::Result<Self> {
        if let Some(v) = UnpackDictEntries::<String, Value>::unpack_value(value)? {
            Ok(Self::Varying(
                v.entries
                    .into_iter()
                    .filter_map(|(k, v)| {
                        if v.is_none() {
                            None
                        } else {
                            Some(Argument::unpack_value(v).map(|v| (k, v)))
                        }
                    })
                    .collect::<starlark::Result<BTreeMap<_, _>>>()?,
            ))
        } else {
            Ok(Self::Uniform(Argument::unpack_value(value)?))
        }
    }
}

#[derive(Allocative, Clone, Debug, NoSerialize, ProvidesStaticType)]
pub(crate) struct FileVariable {
    pub name: String,
    pub path: String,
}

impl fmt::Display for FileVariable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "FileVariable(name={}, path={})", self.name, self.path)
    }
}

#[starlark_value(type = "FileVariable", UnpackValue, StarlarkTypeRepr)]
impl<'v> StarlarkValue<'v> for FileVariable {}

#[derive(Allocative, Clone, Debug, NoSerialize, ProvidesStaticType)]
pub(crate) struct Port {
    pub name: String,
    pub number: Option<u16>,
    pub protocol: Protocol,
}

impl fmt::Display for Port {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let mut args = vec![format!("name={}", self.name)];
        if let Some(number) = self.number {
            args.push(format!("number={}", number));
        }
        args.push(format!("protocol={}", self.protocol));
        write!(f, "Port({})", args.join(", "))
    }
}

#[derive(Allocative, Clone, Debug)]
pub(crate) enum Protocol {
    TCP,
    UDP,
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "{}",
            match self {
                Protocol::TCP => "TCP",
                Protocol::UDP => "UDP",
            }
        )
    }
}

#[starlark_value(type = "Port", UnpackValue, StarlarkTypeRepr)]
impl<'v> StarlarkValue<'v> for Port {}

#[derive(Allocative, Clone, Debug)]
pub(crate) enum ProbeAction {
    HttpGet { path: String, port: String },
}

#[derive(Allocative, Clone, Debug, NoSerialize, ProvidesStaticType)]
pub(crate) struct Probe {
    pub action: ProbeAction,
    pub initial_delay_seconds: Option<i32>,
    pub period_seconds: Option<i32>,
    pub timeout_seconds: Option<i32>,
    pub success_threshold: Option<i32>,
    pub failure_threshold: Option<i32>,
}

impl fmt::Display for Probe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let (name, mut args) = match &self.action {
            ProbeAction::HttpGet { path, port } => (
                "HttpGetProbe",
                vec![format!("path={}", path), format!("port={}", port)],
            ),
        };
        for (field, value) in [
            ("initial_delay", self.initial_delay_seconds),
            ("period", self.period_seconds),
            ("timeout", self.timeout_seconds),
            ("success_threshold", self.success_threshold),
            ("failure_threshold", self.failure_threshold),
        ] {
            if let Some(value) = value {
                args.push(format!("{}={}", field, value));
            }
        }
        write!(f, "{}({})", name, args.join(", "))
    }
}

#[starlark_value(type = "Probe", UnpackValue, StarlarkTypeRepr)]
impl<'v> StarlarkValue<'v> for Probe {}

#[derive(Allocative, Clone, Debug, Default, NoSerialize, ProvidesStaticType)]
pub(crate) struct Resources {
    pub requests: BTreeMap<String, ArgumentValues>,
    pub limits: BTreeMap<String, ArgumentValues>,
}

impl fmt::Display for Resources {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "Resources(requests={:?}, limits={:?})",
            self.requests, self.limits
        )
    }
}

#[starlark_value(type = "Resources", UnpackValue, StarlarkTypeRepr)]
impl<'v> StarlarkValue<'v> for Resources {}

#[derive(Allocative, Clone, Debug, NoSerialize, ProvidesStaticType)]
pub(crate) struct StringVariable {
    pub name: String,
}

impl fmt::Display for StringVariable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "StringVariable(name={})", self.name)
    }
}

#[starlark_value(type = "StringVariable", UnpackValue, StarlarkTypeRepr)]
impl<'v> StarlarkValue<'v> for StringVariable {}

#[starlark_module]
fn starlark_types(builder: &mut GlobalsBuilder) {
    fn Application<'v>(
        #[starlark(require = named)] args: Option<Value>,
        #[starlark(require = named)] env: Option<Value>,
        #[starlark(require = named)] labels: Option<Value>,
        #[starlark(require = named)] liveness: Option<Value>,
        #[starlark(require = named)] readiness: Option<Value>,
        #[starlark(require = named)] resources: Option<Value>,
        #[starlark(require = named)] startup: Option<Value>,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<Value<'v>> {
        let args_value = match args {
            Some(a) => unpack_vec("args", a)?,
            None => Vec::new(),
        };
        let env_value = match env {
            Some(e) => unpack_map("env", e)?,
            None => BTreeMap::new(),
        };
        let labels_value = match labels {
            Some(l) => unpack_string_map("labels", l)?,
            None => BTreeMap::new(),
        };
        let liveness_value = unpack_probe("liveness", liveness)?;
        let readiness_value = unpack_probe("readiness", readiness)?;
        let resources_value = match resources {
            Some(r) => r
                .downcast_ref::<Resources>()
                .ok_or_else(|| function_error("resources must be a Resources object"))?
                .clone(),
            None => Resources::default(),
        };
        let startup_value = unpack_probe("startup", startup)?;
        Ok(eval.heap().alloc_simple(Application {
            args: args_value,
            env: env_value,
            labels: labels_value,
            liveness: liveness_value,
            readiness: readiness_value,
            resources: resources_value,
            startup: startup_value,
        }))
    }

    fn FileVariable<'v>(
        #[starlark(require = named)] name: Value,
        #[starlark(require = named)] path: Value,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<Value<'v>> {
        let name_str = name
            .unpack_str()
            .ok_or_else(|| function_error("name must be a str"))?;
        let path_str = path
            .unpack_str()
            .ok_or_else(|| function_error("path must be a str"))?;
        Ok(eval.heap().alloc_simple(FileVariable {
            name: name_str.to_string(),
            path: path_str.to_string(),
        }))
    }

    fn Port<'v>(
        #[starlark(require = named)] name: Value,
        #[starlark(require = named)] number: Option<Value>,
        #[starlark(require = named)] protocol: Option<Value>,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<Value<'v>> {
        let name_str = name
            .unpack_str()
            .ok_or_else(|| function_error("name must be a str"))?
            .to_string();
        let number_value = match number {
            Some(n) => {
                let as_i32 = n
                    .unpack_i32()
                    .ok_or_else(|| function_error("number must be an integer"))?;
                Some(
                    as_i32
                        .try_into()
                        .map_err(|_| function_error("number must be a u16"))?,
                )
            }
            None => None,
        };
        let protocol = match protocol {
            Some(n) => match n
                .unpack_str()
                .ok_or_else(|| function_error("protocol must be a str"))?
            {
                "TCP" => Protocol::TCP,
                "UDP" => Protocol::UDP,
                _ => return Err(function_error("protocol must be either TCP or UDP or None")),
            },
            None => Protocol::TCP,
        };

        Ok(eval.heap().alloc_simple(Port {
            name: name_str,
            number: number_value,
            protocol,
        }))
    }

    fn HttpGetProbe<'v>(
        #[starlark(require = named)] path: Value,
        #[starlark(require = named)] port: Value,
        #[starlark(require = named)] initial_delay: Option<Value>,
        #[starlark(require = named)] period: Option<Value>,
        #[starlark(require = named)] timeout: Option<Value>,
        #[starlark(require = named)] success_threshold: Option<Value>,
        #[starlark(require = named)] failure_threshold: Option<Value>,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<Value<'v>> {
        let path = path
            .unpack_str()
            .ok_or_else(|| function_error("path must be a str"))?
            .to_string();
        let port = port
            .unpack_str()
            .ok_or_else(|| function_error("port must be a str"))?
            .to_string();
        Ok(eval.heap().alloc_simple(Probe {
            action: ProbeAction::HttpGet { path, port },
            initial_delay_seconds: unpack_optional_i32("initial_delay", initial_delay)?,
            period_seconds: unpack_optional_i32("period", period)?,
            timeout_seconds: unpack_optional_i32("timeout", timeout)?,
            success_threshold: unpack_optional_i32("success_threshold", success_threshold)?,
            failure_threshold: unpack_optional_i32("failure_threshold", failure_threshold)?,
        }))
    }

    fn Resources<'v>(
        #[starlark(require = named)] requests: Option<Value>,
        #[starlark(require = named)] limits: Option<Value>,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<Value<'v>> {
        let requests_value = match requests {
            Some(r) => unpack_map("requests", r)?,
            None => BTreeMap::new(),
        };
        let limits_value = match limits {
            Some(l) => unpack_map("limits", l)?,
            None => BTreeMap::new(),
        };
        Ok(eval.heap().alloc_simple(Resources {
            requests: requests_value,
            limits: limits_value,
        }))
    }

    fn StringVariable<'v>(
        name: Value,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<Value<'v>> {
        let name_str = name
            .unpack_str()
            .ok_or_else(|| function_error("name must be a str"))?;
        Ok(eval.heap().alloc_simple(StringVariable {
            name: name_str.to_string(),
        }))
    }
}

pub(crate) async fn get_config(
    root: &Path,
    name: &str,
    namespace: Option<&str>,
) -> Result<(ConfigImageIndex, Application)> {
    let index_path = root.join("index.json");
    let index: ConfigImageIndex =
        serde_json::from_str(&tokio::fs::read_to_string(index_path).await?)?;
    let config_path = root.join(&index.config_entrypoint);
    let application =
        crate::starlark::load_starlark_config(root, &config_path, name, namespace).await?;
    Ok((index, application))
}

// The first port number handed out by auto-assignment.
const BASE_PORT: u16 = 8080;

// Resolves a concrete number for every port the application exposes in `environment`. Ports with an
// explicit number keep it; the rest are auto-assigned deterministically (sorted by name, counting up
// from BASE_PORT and skipping numbers already taken). Returns an error if a name is declared with
// conflicting numbers or if two ports want the same number.
pub(crate) fn assign_ports(
    application: &Application,
    environment: &str,
) -> Result<BTreeMap<String, u16>> {
    let mut names: BTreeSet<String> = BTreeSet::new();
    let mut assigned: BTreeMap<String, u16> = BTreeMap::new();
    let values = application
        .args
        .iter()
        .chain(application.env.values())
        .chain(application.resources.requests.values())
        .chain(application.resources.limits.values());
    for value in values {
        collect_port(value, environment, &mut names, &mut assigned)?;
    }

    let mut owners: BTreeMap<u16, &str> = BTreeMap::new();
    for (name, number) in &assigned {
        if let Some(other) = owners.insert(*number, name) {
            bail!(
                "Ports {} and {} both request number {}",
                other,
                name,
                number
            );
        }
    }

    let mut used: BTreeSet<u16> = assigned.values().copied().collect();
    let mut next = BASE_PORT;
    for name in &names {
        if assigned.contains_key(name) {
            continue;
        }
        while used.contains(&next) {
            next += 1;
        }
        used.insert(next);
        assigned.insert(name.clone(), next);
        next += 1;
    }

    Ok(assigned)
}

fn collect_port(
    values: &ArgumentValues,
    environment: &str,
    names: &mut BTreeSet<String>,
    assigned: &mut BTreeMap<String, u16>,
) -> Result<()> {
    let resolved = match values {
        ArgumentValues::Uniform(a) => Some(a),
        ArgumentValues::Varying(m) => m.get(environment),
    };
    if let Some(Argument::Port(port)) = resolved {
        names.insert(port.name.clone());
        if let Some(number) = port.number {
            match assigned.get(&port.name) {
                Some(existing) if *existing != number => bail!(
                    "Port {} is declared with conflicting numbers {} and {}",
                    port.name,
                    existing,
                    number
                ),
                _ => {
                    assigned.insert(port.name.clone(), number);
                }
            }
        }
    }
    Ok(())
}

fn function_error(message: impl AsRef<str>) -> starlark::Error {
    return starlark::Error::new_kind(starlark::ErrorKind::Function(anyhow::Error::msg(
        message.as_ref().to_string(),
    )));
}

pub(crate) fn make_starlark_globals() -> Globals {
    GlobalsBuilder::extended_by(&[
        LibraryExtension::Debug,
        LibraryExtension::EnumType,
        LibraryExtension::Filter,
        LibraryExtension::Json,
        LibraryExtension::Map,
        LibraryExtension::Partial,
        LibraryExtension::Print,
        LibraryExtension::RecordType,
        LibraryExtension::StructType,
    ])
    .with(starlark_types)
    .build()
}

fn unpack_map(name: &str, source: Value) -> starlark::Result<BTreeMap<String, ArgumentValues>> {
    UnpackDictEntries::<String, Value>::unpack_value(source)?
        .ok_or_else(|| function_error(format!("{} must be a list or tuple", name)))?
        .entries
        .into_iter()
        .map(|(k, v)| ArgumentValues::unpack_value(v).map(|v| (k, v)))
        .collect::<starlark::Result<BTreeMap<_, _>>>()
}

fn unpack_string_map(name: &str, source: Value) -> starlark::Result<BTreeMap<String, String>> {
    UnpackDictEntries::<String, String>::unpack_value(source)?
        .ok_or_else(|| function_error(format!("{} must be a dict of str to str", name)))
        .map(|d| d.entries.into_iter().collect())
}

fn unpack_optional_i32(name: &str, value: Option<Value>) -> starlark::Result<Option<i32>> {
    match value {
        Some(v) => Ok(Some(
            v.unpack_i32()
                .ok_or_else(|| function_error(format!("{} must be an integer", name)))?,
        )),
        None => Ok(None),
    }
}

fn unpack_probe(name: &str, value: Option<Value>) -> starlark::Result<Option<Probe>> {
    match value {
        Some(v) if !v.is_none() => Ok(Some(
            v.downcast_ref::<Probe>()
                .ok_or_else(|| function_error(format!("{} must be a Probe object", name)))?
                .clone(),
        )),
        _ => Ok(None),
    }
}

fn unpack_vec(name: &str, source: Value) -> starlark::Result<Vec<ArgumentValues>> {
    UnpackListOrTuple::unpack_value(source)?
        .ok_or_else(|| function_error(format!("{} must be a list or tuple", name)))?
        .into_iter()
        .map(|v| ArgumentValues::unpack_value(v))
        .collect::<starlark::Result<Vec<_>>>()
}
