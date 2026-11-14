//! JNI entry points for `computer.iroh.link.NativeGateway`.

use std::{panic::catch_unwind, path::PathBuf, sync::Once};

use jni::{
    JNIEnv,
    objects::{JClass, JObject, JString},
    sys::{jint, jstring},
};

/// Starts the gateway and returns `null`, or an error message.
///
/// `context` should be the application context; iroh reads the system DNS
/// configuration through it.
#[unsafe(no_mangle)]
pub extern "system" fn Java_computer_iroh_link_NativeGateway_start<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    context: JObject<'local>,
    port: jint,
    state_dir: JString<'local>,
) -> jstring {
    let result = (|| -> Result<(), String> {
        install_context(&mut env, &context)?;
        let state_dir: String = env
            .get_string(&state_dir)
            .map_err(|error| format!("invalid state directory: {error}"))?
            .into();
        let port = u16::try_from(port).map_err(|_| format!("invalid port {port}"))?;
        catch_unwind(|| super::start(port, PathBuf::from(state_dir)))
            .map_err(|_| "gateway start panicked".to_owned())?
    })();
    match result {
        Ok(()) => std::ptr::null_mut(),
        Err(message) => env
            .new_string(message)
            .map(JString::into_raw)
            .unwrap_or(std::ptr::null_mut()),
    }
}

/// Returns the gateway status as a string.
#[unsafe(no_mangle)]
pub extern "system" fn Java_computer_iroh_link_NativeGateway_status<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jstring {
    let status = catch_unwind(super::status).unwrap_or_else(|_| "status panicked".to_owned());
    env.new_string(status)
        .map(JString::into_raw)
        .unwrap_or(std::ptr::null_mut())
}

/// Hands the Java VM and application context to iroh, once per process.
fn install_context(env: &mut JNIEnv<'_>, context: &JObject<'_>) -> Result<(), String> {
    static INSTALLED: Once = Once::new();
    let mut result = Ok(());
    INSTALLED.call_once(|| {
        result = (|| {
            let vm = env.get_java_vm().map_err(|error| error.to_string())?;
            let context = env
                .new_global_ref(context)
                .map_err(|error| error.to_string())?;
            let context_pointer = context.as_obj().as_raw();
            // The context must stay valid until the process exits.
            std::mem::forget(context);
            #[cfg(target_os = "android")]
            // SAFETY: The VM pointer comes from the running JVM, which outlives
            // the process's native code, and the context is a leaked global
            // reference, so both stay valid until the process exits.
            unsafe {
                iroh::dns::install_android_jni_context(
                    vm.get_java_vm_pointer().cast(),
                    context_pointer.cast(),
                );
            }
            #[cfg(not(target_os = "android"))]
            let _ = (vm, context_pointer);
            Ok(())
        })();
    });
    result
}
