package computer.iroh.link

import android.app.Application
import android.content.pm.ApplicationInfo
import android.util.Log
import android.webkit.WebView

/** Starts the gateway once per process, before any page loads. */
class IrohLinkApp : Application() {
    /** The error from starting the gateway, or `null` if it started. */
    var startError: String? = null
        private set

    /** Loads gateway URLs for all tabs, through one disk cache. */
    val gatewayClient by lazy { GatewayClient(java.io.File(cacheDir, "gateway")) }

    override fun onCreate() {
        super.onCreate()
        if (applicationInfo.flags and ApplicationInfo.FLAG_DEBUGGABLE != 0) {
            WebView.setWebContentsDebuggingEnabled(true)
        }
        startError = try {
            NativeGateway.start(this, Links.PORT, filesDir.path)
        } catch (error: Throwable) {
            error.toString()
        }
        startError?.let { Log.e("iroh-link", "gateway did not start: $it") }
    }
}
