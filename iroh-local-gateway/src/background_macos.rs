//! Per-user LaunchAgent registration used by the macOS installer.
use super::*;

const LABEL: &str = "computer.n0.iroh-local-gateway";

fn domain() -> Result<String> {
    let output = Command::new("/usr/bin/id").arg("-u").output()?;
    let uid: u32 = String::from_utf8(output.stdout)?.trim().parse()?;
    anyhow::ensure!(
        uid != 0,
        "install Iroh Gateway for the logged-in user, without sudo"
    );
    let gui = format!("gui/{uid}");
    if Command::new("/bin/launchctl")
        .args(["print", &gui])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?
        .success()
    {
        Ok(gui)
    } else {
        Ok(format!("user/{uid}"))
    }
}
fn plist_path() -> Result<PathBuf> {
    Ok(dirs::home_dir()
        .context("cannot determine home directory")?
        .join("Library/LaunchAgents")
        .join(format!("{LABEL}.plist")))
}
fn launchctl(arguments: &[&std::ffi::OsStr]) -> Result<()> {
    let output = Command::new("/bin/launchctl").args(arguments).output()?;
    anyhow::ensure!(
        output.status.success(),
        "launchctl failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
fn definition(executable: &Path, home: &Path, state: &Path, args: Vec<String>) -> plist::Value {
    let mut values = plist::Dictionary::new();
    values.insert("Label".into(), LABEL.into());
    values.insert(
        "ProgramArguments".into(),
        plist::Value::Array(
            [
                executable.to_string_lossy().into_owned(),
                "--state-dir".into(),
                state.to_string_lossy().into_owned(),
            ]
            .into_iter()
            .chain(args)
            .map(plist::Value::String)
            .collect(),
        ),
    );
    values.insert(
        "WorkingDirectory".into(),
        home.to_string_lossy().into_owned().into(),
    );
    values.insert("RunAtLoad".into(), true.into());
    let mut keep_alive = plist::Dictionary::new();
    keep_alive.insert("SuccessfulExit".into(), false.into());
    values.insert("KeepAlive".into(), plist::Value::Dictionary(keep_alive));
    values.insert("ThrottleInterval".into(), 10_u64.into());
    values.insert("ExitTimeOut".into(), 30_u64.into());
    values.insert(
        "AssociatedBundleIdentifiers".into(),
        plist::Value::Array(vec!["computer.n0.iroh-local-gateway".into()]),
    );
    for key in ["StandardOutPath", "StandardErrorPath"] {
        values.insert(
            key.into(),
            state
                .join("gateway.log")
                .to_string_lossy()
                .into_owned()
                .into(),
        );
    }
    plist::Value::Dictionary(values)
}
pub async fn install(state: &Path) -> Result<()> {
    let home = dirs::home_dir().context("cannot determine home directory")?;
    let executable = std::env::current_exe()?.with_file_name("iroh-local-gateway");
    anyhow::ensure!(
        executable.is_file(),
        "the daemon must be installed beside its helper"
    );
    let domain = domain()?;
    remove(state).await?;
    std::fs::create_dir_all(state)?;
    let path = plist_path()?;
    std::fs::create_dir_all(path.parent().context("invalid LaunchAgent path")?)?;
    definition(&executable, &home, state, super::arguments(state)?).to_file_xml(&path)?;
    launchctl(&["enable".as_ref(), format!("{domain}/{LABEL}").as_ref()])?;
    launchctl(&["bootstrap".as_ref(), domain.as_ref(), path.as_os_str()])?;
    tokio::time::timeout(Duration::from_secs(30), async {
        while !running(state)? || !state.join("ready").exists() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("gateway startup timed out; see gateway.log (port 8080 may already be in use)")??;
    Ok(())
}
pub async fn remove(state: &Path) -> Result<()> {
    let domain = domain()?;
    super::stop(state).await?;
    let service = format!("{domain}/{LABEL}");
    if Command::new("/bin/launchctl")
        .args(["print", &service])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?
        .success()
    {
        launchctl(&["bootout".as_ref(), service.as_ref()])?;
    }
    match std::fs::remove_file(plist_path()?) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn launch_agent_preserves_paths_and_only_restarts_failed_exits() -> Result<()> {
        let home = Path::new("/Users/A & B");
        let state = home.join("Library/Application Support/iroh-local-gateway");
        let executable =
            home.join("Applications/Iroh Gateway.app/Contents/MacOS/iroh-local-gateway");
        let expected = definition(&executable, home, &state, vec![]);
        let mut bytes = Vec::new();
        expected.to_writer_xml(&mut bytes)?;
        let decoded = plist::Value::from_reader(std::io::Cursor::new(bytes))?;
        assert_eq!(expected, decoded);
        let values = decoded.as_dictionary().unwrap();
        assert_eq!(
            values["KeepAlive"].as_dictionary().unwrap()["SuccessfulExit"].as_boolean(),
            Some(false)
        );
        assert_eq!(
            values["ProgramArguments"].as_array().unwrap()[0].as_string(),
            executable.to_str()
        );
        Ok(())
    }
}
