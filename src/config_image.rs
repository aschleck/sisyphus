use allocative::Allocative;
use anyhow::{Result, anyhow};
use serde::Deserialize;
use starlark::{
    any::ProvidesStaticType,
    environment::{GlobalsBuilder, LibraryExtension, Module},
    eval::Evaluator,
    starlark_module,
    syntax::{AstModule, Dialect},
    values::{
        NoSerialize, StarlarkValue, UnpackValue, Value, ValueLike, dict::UnpackDictEntries,
        float::StarlarkFloat, list_or_tuple::UnpackListOrTuple, starlark_value,
    },
};
use std::{collections::BTreeMap, convert::TryInto, fmt, path::Path};

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
    pub resources: Resources,
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
    pub number: u16,
    pub protocol: Protocol,
}

impl fmt::Display for Port {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "Port(name={}, number={}, protocol={})",
            self.name, self.number, self.protocol
        )
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
        #[starlark(require = named)] resources: Option<Value>,
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
        let resources_value = match resources {
            Some(r) => r
                .downcast_ref::<Resources>()
                .ok_or_else(|| function_error("resources must be a Resources object"))?
                .clone(),
            None => Resources::default(),
        };
        Ok(eval.heap().alloc_simple(Application {
            args: args_value,
            env: env_value,
            resources: resources_value,
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
        #[starlark(require = named)] number: Value,
        #[starlark(require = named)] protocol: Option<Value>,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<Value<'v>> {
        let name_str = name
            .unpack_str()
            .ok_or_else(|| function_error("name must be a str"))?
            .to_string();
        let as_i32 = number
            .unpack_i32()
            .ok_or_else(|| function_error("number must be an integer"))?;
        let as_u16: u16 = as_i32
            .try_into()
            .map_err(|_| function_error("number must be a u16"))?;
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
            number: as_u16,
            protocol,
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

pub(crate) async fn get_config(root: &Path) -> Result<(ConfigImageIndex, Application)> {
    let index_path = root.join("index.json");
    let index: ConfigImageIndex =
        serde_json::from_str(&tokio::fs::read_to_string(index_path).await?)?;
    let ast = AstModule::parse(
        &index.config_entrypoint,
        tokio::fs::read_to_string(root.join(&index.config_entrypoint)).await?,
        &Dialect::Standard,
    )
    .map_err(|e| anyhow!("Unable to parse config: {:?}", e))?;
    let globals = GlobalsBuilder::extended_by(&[
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
    .build();
    let module = Module::new();
    let mut eval: Evaluator = Evaluator::new(&module);
    // Expected to define a main method
    eval.eval_module(ast, &globals)
        .map_err(|e| anyhow!("Cannot load config: {:?}", e))?;
    // Get the main method
    let main = AstModule::parse("", "main".to_string(), &Dialect::Standard)
        .map(|a| eval.eval_module(a, &globals))
        .flatten()
        .map_err(|e| anyhow!("No main function: {:?}", e))?;
    let result = eval
        .eval_function(main, &[Value::new_none()], &[])
        .map_err(|e| anyhow!("Cannot evaluate config: {:?}", e))?;
    let application = result
        .downcast_ref::<Application>()
        .ok_or_else(|| anyhow!("Config didn't return an Application"))?
        .clone();
    Ok((index, application))
}

fn function_error(message: impl AsRef<str>) -> starlark::Error {
    return starlark::Error::new_kind(starlark::ErrorKind::Function(anyhow::Error::msg(
        message.as_ref().to_string(),
    )));
}

fn unpack_map(name: &str, source: Value) -> starlark::Result<BTreeMap<String, ArgumentValues>> {
    UnpackDictEntries::<String, Value>::unpack_value(source)?
        .ok_or_else(|| function_error(format!("{} must be a list or tuple", name)))?
        .entries
        .into_iter()
        .map(|(k, v)| ArgumentValues::unpack_value(v).map(|v| (k, v)))
        .collect::<starlark::Result<BTreeMap<_, _>>>()
}

fn unpack_vec(name: &str, source: Value) -> starlark::Result<Vec<ArgumentValues>> {
    UnpackListOrTuple::unpack_value(source)?
        .ok_or_else(|| function_error(format!("{} must be a list or tuple", name)))?
        .into_iter()
        .map(|v| ArgumentValues::unpack_value(v))
        .collect::<starlark::Result<Vec<_>>>()
}
