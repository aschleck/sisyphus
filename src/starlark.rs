use crate::config_image::{make_starlark_globals, Application};
use allocative::Allocative;
use anyhow::{anyhow, Result};
use starlark::{
    any::ProvidesStaticType,
    environment::{FrozenModule, Globals, Module},
    eval::{Evaluator, FileLoader},
    starlark_simple_value,
    syntax::{AstModule, Dialect},
    values::{starlark_value, NoSerialize, StarlarkValue, ValueLike},
};
use std::{
    fmt,
    path::{Path, PathBuf},
};

#[derive(Allocative, Clone, Debug, NoSerialize, ProvidesStaticType)]
pub(crate) struct Context {
    pub name: String,
    pub namespace: Option<String>,
}

impl fmt::Display for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Context(name={:?}, namespace={:?})",
            self.name, self.namespace
        )
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
    fn name(this: &Context) -> starlark::Result<String> {
        Ok(this.name.clone())
    }

    fn namespace(this: &Context) -> starlark::Result<String> {
        this.namespace.clone().ok_or_else(|| {
            starlark::Error::new_other(anyhow!(
                "ctx.namespace() is not available (hint: do you need to pass --namespace?)"
            ))
        })
    }
}

pub(crate) async fn load_starlark_config(
    root: &Path,
    path: &Path,
    name: &str,
    namespace: Option<&str>,
) -> Result<Application> {
    let content = tokio::fs::read_to_string(path).await?;
    let path_str = path.to_str().unwrap_or("config.star");

    let ast = AstModule::parse(path_str, content, &Dialect::Standard)
        .map_err(|e| anyhow!("Unable to parse config: {:?}", e))?;

    let globals = make_starlark_globals();
    let loader = ConfigFileLoader {
        globals: globals.clone(),
        root: root.to_path_buf(),
        current_dir: path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| root.to_path_buf()),
    };
    let module = Module::new();
    let mut eval: Evaluator = Evaluator::new(&module);
    eval.set_loader(&loader);

    // Expected to define a main method
    eval.eval_module(ast, &globals)
        .map_err(|e| anyhow!("Cannot load config: {:?}", e))?;

    // Get the main method
    let main = AstModule::parse("", "main".to_string(), &Dialect::Standard)
        .map(|a| eval.eval_module(a, &globals))
        .flatten()
        .map_err(|e| anyhow!("No main function: {:?}", e))?;

    let ctx = module.heap().alloc_simple(Context {
        name: name.to_string(),
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

struct ConfigFileLoader {
    globals: Globals,
    root: PathBuf,
    current_dir: PathBuf,
}

impl FileLoader for ConfigFileLoader {
    fn load(&self, path: &str) -> starlark::Result<FrozenModule> {
        let resolved = match path.strip_prefix("//") {
            Some(from_root) => self.root.join(from_root),
            None => self.current_dir.join(path),
        };
        let content = std::fs::read_to_string(&resolved).map_err(|e| {
            starlark::Error::new_other(anyhow!("Unable to read load() target {:?}: {}", resolved, e))
        })?;
        let ast = AstModule::parse(
            resolved.to_str().unwrap_or(path),
            content,
            &Dialect::Standard,
        )?;
        let module = Module::new();
        {
            // Nested load()s resolve relative to the file we are about to evaluate.
            let nested = ConfigFileLoader {
                globals: self.globals.clone(),
                root: self.root.clone(),
                current_dir: resolved
                    .parent()
                    .map(Path::to_path_buf)
                    .unwrap_or_else(|| self.root.clone()),
            };
            let mut eval = Evaluator::new(&module);
            eval.set_loader(&nested);
            eval.eval_module(ast, &self.globals)?;
        }
        module.freeze().map_err(starlark::Error::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeMap, fs};
    use tempfile::TempDir;

    fn write(root: &Path, rel: &str, content: &str) {
        let path = root.join(rel);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, content).unwrap();
    }

    #[tokio::test]
    async fn load_resolves_root_and_file_relative_paths() -> Result<()> {
        let tmp = TempDir::new()?;
        let root = tmp.path();

        // Root-relative target, reached via `//` from the entrypoint in a different directory.
        write(
            root,
            "lib/shared.star",
            r#"
def root_value():
    return "root"
"#,
        );

        // Sibling of the entrypoint that itself loads a sibling relative to its own directory.
        write(
            root,
            "app/local.star",
            r#"
load("nested.star", "nested_value")

def local_value():
    return nested_value()
"#,
        );
        write(
            root,
            "app/nested.star",
            r#"
def nested_value():
    return "nested"
"#,
        );

        // Entrypoint: one `//` load and one file-relative load.
        write(
            root,
            "app/main.star",
            r#"
load("//lib/shared.star", "root_value")
load("local.star", "local_value")

def main(ctx):
    return Application(labels = {
        "root": root_value(),
        "local": local_value(),
    })
"#,
        );

        let application =
            load_starlark_config(root, &root.join("app/main.star"), "test-app", None).await?;

        assert_eq!(
            application.labels,
            BTreeMap::from([
                ("root".to_string(), "root".to_string()),
                ("local".to_string(), "nested".to_string()),
            ])
        );
        Ok(())
    }
}
