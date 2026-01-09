use crate::config_image::{make_starlark_globals, Application};
use allocative::Allocative;
use anyhow::{anyhow, Result};
use starlark::{
    any::ProvidesStaticType,
    environment::Module,
    eval::Evaluator,
    starlark_simple_value,
    syntax::{AstModule, Dialect},
    values::{starlark_value, NoSerialize, StarlarkValue, ValueLike},
};
use std::{fmt, path::Path};

#[derive(Allocative, Clone, Debug, NoSerialize, ProvidesStaticType)]
pub(crate) struct Context {
    pub namespace: Option<String>,
}

impl fmt::Display for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Context(namespace={:?})", self.namespace)
    }
}

starlark_simple_value!(Context);

#[starlark_value(type = "Context")]
impl<'v> StarlarkValue<'v> for Context {
    fn get_methods() -> Option<&'static starlark::environment::Methods> {
        static RES: starlark::environment::MethodsStatic = starlark::environment::MethodsStatic::new();
        RES.methods(context_methods)
    }
}

#[starlark::starlark_module]
fn context_methods(builder: &mut starlark::environment::MethodsBuilder) {
    fn namespace(this: &Context) -> starlark::Result<String> {
        this.namespace.clone().ok_or_else(|| {
            starlark::Error::new_other(anyhow!(
                "ctx.namespace() is not available (hint: do you need to pass --namespace?)"
            ))
        })
    }
}

pub(crate) async fn load_starlark_config(
    path: &Path,
    namespace: Option<&str>,
) -> Result<Application> {
    let content = tokio::fs::read_to_string(path).await?;
    let path_str = path.to_str().unwrap_or("config.star");

    let ast = AstModule::parse(path_str, content, &Dialect::Standard)
        .map_err(|e| anyhow!("Unable to parse config: {:?}", e))?;

    let globals = make_starlark_globals();
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

    let ctx = module.heap().alloc_simple(Context {
        namespace: namespace.map(|s| s.to_string()),
    });

    let result = eval
        .eval_function(main, &[ctx], &[])
        .map_err(|e| anyhow!("Cannot evaluate config: {:?}", e))?;

    let application = result
        .downcast_ref::<Application>()
        .ok_or_else(|| anyhow!("Config didn't return an Application"))?
        .clone();

    Ok(application)
}
