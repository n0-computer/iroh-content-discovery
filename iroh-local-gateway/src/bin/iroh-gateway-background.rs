//! Per-user launcher for the standalone gateway.
#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]
#[path = "../background.rs"]
#[allow(dead_code)]
mod background;
#[cfg(target_os = "macos")]
#[path = "../background_macos.rs"]
mod macos;

use anyhow::{Context, Result, ensure};
use background::{default_state_dir, running};
use clap::{Parser, Subcommand};
use std::{
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    state_dir: Option<PathBuf>,
    #[command(subcommand)]
    action: Option<Action>,
}
#[derive(Subcommand)]
enum Action {
    Stop,
    Status,
    #[cfg(target_os = "macos")]
    InstallAgent,
    #[cfg(target_os = "macos")]
    RemoveAgent,
}
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let state = args.state_dir.unwrap_or(default_state_dir()?);
    let result = run(&state, args.action).await;
    if let Err(error) = &result {
        let _ = std::fs::create_dir_all(&state);
        if let Ok(mut log) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(state.join("launcher.log"))
        {
            let _ = writeln!(log, "{:?}: {error:#}", std::time::SystemTime::now());
        }
    }
    result
}
async fn run(state: &Path, action: Option<Action>) -> Result<()> {
    std::fs::create_dir_all(state)?;
    let _lock = background::lock(&state.join("launcher.lock"))?;
    match action {
        Some(Action::Stop) => stop(state).await,
        Some(Action::Status) => {
            ensure!(running(state)?, "gateway is stopped");
            Ok(())
        }
        #[cfg(target_os = "macos")]
        Some(Action::InstallAgent) => macos::install(state).await,
        #[cfg(target_os = "macos")]
        Some(Action::RemoveAgent) => macos::remove(state).await,
        None => start(state).await,
    }
}
fn gateway() -> Result<PathBuf> {
    Ok(std::env::current_exe()?.with_file_name(if cfg!(windows) {
        "iroh-local-gateway.exe"
    } else {
        "iroh-local-gateway"
    }))
}
fn arguments(state: &Path) -> Result<Vec<String>> {
    let path = state.join("arguments.json");
    if !path.exists() {
        std::fs::write(&path, "[]\n")?;
    }
    let args = serde_json::from_slice(&std::fs::read(path)?)
        .context("arguments.json must be a JSON array of gateway command-line arguments")?;
    Ok(args)
}
async fn start(state: &Path) -> Result<()> {
    if running(state)? {
        return Ok(());
    }
    let args = arguments(state)?;
    if state.join("ready").exists() {
        std::fs::remove_file(state.join("ready"))?;
    }
    let log_path = state.join("gateway.log");
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;
    let mut command = Command::new(gateway()?);
    command
        .args(args)
        .arg("--state-dir")
        .arg(state)
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000); // CREATE_NO_WINDOW
    }
    let mut child = command.spawn().context("cannot start gateway")?;
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(status) = child.try_wait()? {
                anyhow::bail!(
                    "gateway exited ({status}); see gateway.log (port 45475 may already be in use)"
                );
            }
            if running(state)? && state.join("ready").exists() {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    match result {
        Ok(result) => result,
        Err(error) => {
            // This handle belongs to the process just spawned, never a saved PID.
            let _ = child.kill();
            let _ = child.wait();
            Err(error).context("gateway startup timed out; see gateway.log")
        }
    }
}
async fn stop(state: &Path) -> Result<()> {
    if !running(state)? {
        return Ok(());
    }
    std::fs::write(state.join("stop"), [])?;
    tokio::time::timeout(Duration::from_secs(30), async {
        while running(state)? {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("gateway did not stop; see gateway.log")??;
    Ok(())
}
