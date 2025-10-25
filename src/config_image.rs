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
    pub binary_image: String,
    pub config_entrypoint: String,
}

#[derive(Allocative, Clone, Debug, NoSerialize, ProvidesStaticType)]
pub(crate) struct Application {
    pub args: Vec<ArgumentValues>,
    pub env: BTreeMap<String, ArgumentValues>,
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
                    .map(|(k, v)| Argument::unpack_value(v).map(|v| (k, v)))
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
}

impl fmt::Display for Port {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "Port(name={}, number={})", self.name, self.number)
    }
}

#[starlark_value(type = "Port", UnpackValue, StarlarkTypeRepr)]
impl<'v> StarlarkValue<'v> for Port {}

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
        #[starlark(require = named)] args: Value,
        #[starlark(require = named)] env: Value,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<Value<'v>> {
        let args_value = UnpackListOrTuple::unpack_value(args)?
            .ok_or_else(|| function_error("args must be a list or tuple"))?
            .into_iter()
            .map(|v| ArgumentValues::unpack_value(v))
            .collect::<starlark::Result<Vec<_>>>()?;
        let env_value = UnpackDictEntries::<String, Value>::unpack_value(env)?
            .ok_or_else(|| function_error("env must be a list or tuple"))?
            .entries
            .into_iter()
            .map(|(k, v)| ArgumentValues::unpack_value(v).map(|v| (k, v)))
            .collect::<starlark::Result<BTreeMap<_, _>>>()?;
        Ok(eval.heap().alloc_simple(Application {
            args: args_value,
            env: env_value,
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
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<Value<'v>> {
        let as_string = name
            .unpack_str()
            .ok_or_else(|| function_error("name must be a str"))?
            .to_string();
        let as_i32 = number
            .unpack_i32()
            .ok_or_else(|| function_error("number must be an integer"))?;
        let as_u16: u16 = as_i32
            .try_into()
            .map_err(|_| function_error("number must be a u16"))?;
        Ok(eval.heap().alloc_simple(Port {
            name: as_string,
            number: as_u16,
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

fn function_error(message: &str) -> starlark::Error {
    return starlark::Error::new_kind(starlark::ErrorKind::Function(anyhow::Error::msg(
        message.to_string(),
    )));
}
