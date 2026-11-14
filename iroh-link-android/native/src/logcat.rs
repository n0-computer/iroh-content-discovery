//! Sends tracing output to logcat on Android, and to stderr elsewhere.

use std::io::{self, Write};

use tracing_subscriber::{EnvFilter, fmt::MakeWriter};

/// Default filter: lifecycle from our crates, warnings from everything else.
const FILTER: &str =
    "warn,iroh_link_android=info,iroh_link_gateway=info,iroh_mainline_endpoint_discovery=info";

/// Installs the global subscriber, unless one is already set.
pub(crate) fn init() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(FILTER))
        .with_ansi(false)
        .without_time()
        .with_writer(Logcat)
        .try_init();
}

/// Creates one [`Line`] per event.
struct Logcat;

impl<'a> MakeWriter<'a> for Logcat {
    type Writer = Line;

    fn make_writer(&'a self) -> Self::Writer {
        Line(Vec::new())
    }
}

/// Buffers one formatted event and writes it as one log entry when dropped.
struct Line(Vec<u8>);

impl Write for Line {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for Line {
    fn drop(&mut self) {
        while self.0.last() == Some(&b'\n') {
            self.0.pop();
        }
        if self.0.is_empty() {
            return;
        }
        emit(std::mem::take(&mut self.0));
    }
}

#[cfg(target_os = "android")]
fn emit(mut text: Vec<u8>) {
    use std::ffi::{CStr, c_char, c_int};

    #[link(name = "log")]
    unsafe extern "C" {
        fn __android_log_write(priority: c_int, tag: *const c_char, text: *const c_char) -> c_int;
    }
    /// `ANDROID_LOG_INFO`; the level is part of the formatted text.
    const INFO: c_int = 4;
    const TAG: &CStr = c"iroh-link";
    text.retain(|byte| *byte != 0);
    text.push(0);
    // SAFETY: Both strings are NUL terminated, and interior NULs were removed
    // from `text`.
    unsafe {
        __android_log_write(INFO, TAG.as_ptr(), text.as_ptr().cast());
    }
}

#[cfg(not(target_os = "android"))]
fn emit(text: Vec<u8>) {
    let mut stderr = io::stderr().lock();
    let _ = stderr.write_all(&text);
    let _ = stderr.write_all(b"\n");
}
