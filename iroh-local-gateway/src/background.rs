//! Same-user lifecycle files for the installed gateway.
use anyhow::{Context, Result};
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    time::Duration,
};

pub fn default_state_dir() -> Result<PathBuf> {
    Ok(dirs::data_local_dir()
        .context("cannot determine user data directory")?
        .join("iroh-local-gateway"))
}

pub fn lock(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    file.try_lock()
        .context("gateway or launcher already running")?;
    Ok(file)
}

pub fn running(state: &Path) -> Result<bool> {
    let file = match OpenOptions::new()
        .read(true)
        .write(true)
        .open(state.join("gateway.lock"))
    {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e.into()),
    };
    match file.try_lock() {
        Ok(()) => Ok(false),
        Err(std::fs::TryLockError::WouldBlock) => Ok(true),
        Err(e) => Err(e.into()),
    }
}

pub struct Runtime {
    state: PathBuf,
    _lock: File,
}
impl Runtime {
    pub fn acquire(state: &Path) -> Result<Self> {
        std::fs::create_dir_all(state)?;
        let lock = lock(&state.join("gateway.lock"))?;
        remove(&state.join("stop"))?;
        remove(&state.join("ready"))?;
        let log = state.join("gateway.log");
        if log.metadata().is_ok_and(|m| m.len() > 5 * 1024 * 1024) {
            // launchd (or the launcher) already opened stdout/stderr. Keep that
            // file in place so subsequent appends still reach gateway.log.
            std::fs::copy(&log, state.join("gateway.previous.log"))?;
            OpenOptions::new().write(true).open(&log)?.set_len(0)?;
        }
        Ok(Self {
            state: state.into(),
            _lock: lock,
        })
    }
    pub fn ready(&self) -> Result<()> {
        std::fs::write(self.state.join("ready"), b"HTTP listener bound\n")?;
        Ok(())
    }
    pub async fn stopped(&self) {
        while !self.state.join("stop").exists() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
impl Drop for Runtime {
    fn drop(&mut self) {
        let _ = remove(&self.state.join("ready"));
        let _ = remove(&self.state.join("stop"));
    }
}
fn remove(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn log_rotation_preserves_open_output_handles() -> Result<()> {
        let state = tempfile::tempdir()?;
        let log = state.path().join("gateway.log");
        let previous = state.path().join("gateway.previous.log");
        let mut output = OpenOptions::new().create(true).append(true).open(&log)?;
        let contents = vec![b'x'; 5 * 1024 * 1024 + 1];
        output.write_all(&contents)?;
        std::fs::write(&previous, b"older log")?;

        let runtime = Runtime::acquire(state.path())?;
        assert_eq!(std::fs::read(&previous)?, contents);
        output.write_all(b"new log\n")?;
        assert_eq!(std::fs::read(&log)?, b"new log\n");
        drop(runtime);

        let _runtime = Runtime::acquire(state.path())?;
        assert_eq!(std::fs::read(&log)?, b"new log\n");
        assert_eq!(std::fs::read(&previous)?, contents);
        Ok(())
    }
}
