use crate::{
    app_run_config::resolve_argument_local,
    config_image::{Application, Argument, ArgumentValues},
    kubernetes_rendering::prepare_image_config,
    registry_clients::{resolve_image_tag, RegistryClients},
};
use anyhow::{Context, Result};
use clap::Args;
use std::collections::HashMap;
use tokio::process::Command;

#[derive(Args, Debug)]
pub(crate) struct RunImageArgs {
    #[arg(long)]
    pub environment: String,

    #[arg(long)]
    pub image: String,
}

#[derive(Debug)]
struct ContainerConfig {
    args: Vec<String>,
    env: HashMap<String, String>,
    mounts: Vec<(String, String)>, // (host_path, container_path)
    ports: Vec<String>,
}

#[derive(Debug)]
enum ResolvedArgument {
    Port(String),
    String(String),
    VolumeMount {
        host_path: String,
        container_path: String,
    },
}

pub async fn run_image(args: RunImageArgs) -> Result<()> {
    let mut registries = RegistryClients::new();
    let (binary_image, application) = load_config_from_image(&args.image, &mut registries)
        .await
        .with_context(|| format!("Failed to load config from image: {}", args.image))?;
    let config = build_config_container(&application, &args.environment)?;
    run_container_podman(&binary_image, config).await
}

fn build_config_container(app: &Application, environment: &str) -> Result<ContainerConfig> {
    let mut mounts = Vec::new();
    let mut ports = Vec::new();
    let mut cmd_args = Vec::new();
    for arg_val in &app.args {
        if let Some(resolved) = resolve_argument_container(arg_val, environment)? {
            match resolved {
                ResolvedArgument::Port(s) => {
                    cmd_args.push(s.clone());
                    ports.push(s);
                }
                ResolvedArgument::String(s) => cmd_args.push(s),
                ResolvedArgument::VolumeMount {
                    container_path,
                    host_path,
                } => {
                    cmd_args.push(container_path.clone());
                    mounts.push((host_path, container_path));
                }
            }
        }
    }

    let mut env_vars = HashMap::new();
    for (key, arg_val) in &app.env {
        if let Some(resolved) = resolve_argument_container(arg_val, environment)? {
            match resolved {
                ResolvedArgument::Port(s) => {
                    env_vars.insert(key.clone(), s.clone());
                    ports.push(s);
                }
                ResolvedArgument::String(s) => {
                    env_vars.insert(key.clone(), s);
                }
                ResolvedArgument::VolumeMount {
                    host_path,
                    container_path,
                } => {
                    env_vars.insert(key.clone(), container_path.clone());
                    mounts.push((host_path, container_path));
                }
            }
        }
    }

    Ok(ContainerConfig {
        args: cmd_args,
        env: env_vars,
        mounts,
        ports,
    })
}

async fn load_config_from_image(
    image: &String,
    registries: &mut RegistryClients,
) -> Result<(String, Application)> {
    let reference = resolve_image_tag(image, registries).await?;
    let (index, application) = prepare_image_config(&reference.to_string(), registries).await?;
    let binary_image = format!("{}@{}", index.binary_repository, index.binary_digest);
    Ok((binary_image, application))
}

fn resolve_argument_container(
    arg: &ArgumentValues,
    environment: &str,
) -> Result<Option<ResolvedArgument>> {
    let Some((arg, value)) = resolve_argument_local(arg, environment)? else {
        return Ok(None);
    };
    Ok(Some(match arg {
        Argument::FileVariable(v) => ResolvedArgument::VolumeMount {
            host_path: value,
            container_path: v.path.clone(),
        },
        Argument::Port(_) => ResolvedArgument::Port(value),
        _ => ResolvedArgument::String(value),
    }))
}

async fn run_container_podman(binary_image: &str, config: ContainerConfig) -> Result<()> {
    let mut cmd = Command::new("podman");
    cmd.arg("run").arg("--rm");

    if binary_image.starts_with("http://") {
        cmd.arg("--tls-verify=false");
    }

    for (key, value) in &config.env {
        cmd.arg("--env").arg(format!("{}={}", key, value));
    }

    for (host_path, container_path) in &config.mounts {
        cmd.arg("--mount").arg(format!(
            "type=bind,src={},dst={},readonly",
            host_path, container_path
        ));
    }

    for port in &config.ports {
        cmd.arg("--publish").arg(format!("{}:{}", port, port));
    }

    cmd.arg(binary_image);
    cmd.args(&config.args);

    let status = cmd
        .status()
        .await
        .with_context(|| format!("Failed to execute container: {}", binary_image))?;
    if !status.success() {
        let code = status.code().unwrap_or(1);
        std::process::exit(code);
    }

    Ok(())
}
