package computer.iroh.link

import android.content.Context

/** The iroh link gateway, implemented in Rust in `iroh-link-android/native`. */
object NativeGateway {
    init {
        System.loadLibrary("iroh_link_android")
    }

    /**
     * Starts the gateway on `127.0.0.1:[port]`, unless it is already running.
     *
     * Returns `null` on success, or an error message. Index server discovery
     * continues in the background; see [status].
     */
    @JvmStatic
    external fun start(context: Context, port: Int, stateDir: String): String?

    /** Returns `starting`, `discovering index servers`, `ready`, or a failure description. */
    @JvmStatic
    external fun status(): String
}
