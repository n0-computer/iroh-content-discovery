package computer.iroh.link

import android.app.Application
import android.content.pm.ApplicationInfo
import android.net.ConnectivityManager
import android.net.NetworkCapabilities
import android.util.Log
import android.webkit.WebView
import java.io.File

/** Starts the gateway once per process, before any page loads. */
class IrohLinkApp : Application() {
    /** The error from starting the gateway, or `null` if it started. */
    var startError: String? = null
        private set

    /** Loads gateway URLs for all tabs and web apps, through one disk cache. */
    val gatewayClient by lazy { GatewayClient(File(cacheDir, "gateway"), ::canReachContent) }

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

    /**
     * Returns whether the gateway can fetch content now.
     *
     * When it cannot, cached pages are served without asking it, so
     * offline-capable sites open at once instead of after a lookup timeout.
     */
    private fun canReachContent(): Boolean {
        if (startError != null || NativeGateway.status() != "ready") return false
        val connectivity = getSystemService(ConnectivityManager::class.java) ?: return true
        val capabilities = connectivity.getNetworkCapabilities(connectivity.activeNetwork) ?: return false
        return capabilities.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
    }
}
