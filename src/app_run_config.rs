use crate::{
    config_image::{Application, Argument, ArgumentValues},
    starlark::load_starlark_config,
};
use anyhow::{Context, Result};
use clap::Args;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};
use tokio::process::Command;

#[derive(Args, Debug)]
pub(crate) struct RunConfigArgs {
    #[arg(long)]
    pub binary: PathBuf,

    #[arg(long)]
    pub config: PathBuf,

    #[arg(long)]
    pub environment: String,
}

pub(crate) async fn run_config(args: RunConfigArgs) -> Result<()> {
    let application = load_starlark_config(&args.config)
        .await
        .with_context(|| format!("Failed to load config from {}", args.config.display()))?;
    let (cmd_args, env_vars) = build_config_local(&application, &args.environment)?;
    run_binary_local(&args.binary, cmd_args, env_vars).await
}

fn build_config_local(
    app: &Application,
    environment: &str,
) -> Result<(Vec<String>, HashMap<String, String>)> {
    let mut args = Vec::new();
    for arg_val in &app.args {
        if let Some((_, resolved)) = resolve_argument_local(arg_val, environment)? {
            args.push(resolved);
        }
    }

    let mut env = HashMap::new();
    for (key, arg_val) in &app.env {
        if let Some((_, resolved)) = resolve_argument_local(arg_val, environment)? {
            env.insert(key.clone(), resolved);
        }
    }

    Ok((args, env))
}

pub(crate) fn resolve_argument_local<'a>(
    arg: &'a ArgumentValues,
    environment: &str,
) -> Result<Option<(&'a Argument, String)>> {
    let maybe = match arg {
        ArgumentValues::Varying(map) => map.get(environment),
        ArgumentValues::Uniform(a) => Some(a),
    };
    let Some(single) = maybe else {
        return Ok(None);
    };

    Ok(Some((
        single,
        match single {
            Argument::String(s) => s.clone(),
            Argument::FileVariable(v) => {
                let key = as_env_key(&v.name);
                std::env::var(&key)
                    .with_context(|| format!("Environment file variable {} not set", key))?
            }
            Argument::Port(p) => {
                let env_var_name = format!("PORT_{}", as_env_key(&p.name));
                match std::env::var(&env_var_name) {
                    Ok(val) => val,
                    Err(_) => p.number.to_string(),
                }
            }
            Argument::StringVariable(v) => {
                let key = as_env_key(&v.name);
                std::env::var(&key)
                    .with_context(|| format!("Environment string variable {} not set", key))?
            }
        },
    )))
}

async fn run_binary_local(
    binary: &Path,
    args: Vec<String>,
    env: HashMap<String, String>,
) -> Result<()> {
    let mut cmd = Command::new(binary);
    cmd.args(&args);
    cmd.envs(&env);

    let status = cmd
        .status()
        .await
        .with_context(|| format!("Failed to execute binary: {}", binary.display()))?;
    if !status.success() {
        let code = status.code().unwrap_or(1);
        std::process::exit(code);
    }

    Ok(())
}

fn as_env_key(v: &str) -> String {
    v.to_uppercase().replace("-", "_")
}
