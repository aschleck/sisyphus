use crate::config_image::{make_starlark_globals, Application};
use anyhow::{anyhow, Result};
use starlark::{
    environment::Module,
    eval::Evaluator,
    syntax::{AstModule, Dialect},
    values::{Value, ValueLike},
};
use std::path::Path;

pub(crate) async fn load_starlark_config(path: &Path) -> Result<Application> {
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

    let result = eval
        .eval_function(main, &[Value::new_none()], &[])
        .map_err(|e| anyhow!("Cannot evaluate config: {:?}", e))?;

    let application = result
        .downcast_ref::<Application>()
        .ok_or_else(|| anyhow!("Config didn't return an Application"))?
        .clone();

    Ok(application)
}
