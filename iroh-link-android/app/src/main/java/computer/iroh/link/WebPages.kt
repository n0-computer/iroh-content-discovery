package computer.iroh.link

import android.annotation.SuppressLint
import android.app.Activity
import android.app.DownloadManager
import android.content.ActivityNotFoundException
import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.Environment
import android.os.Handler
import android.os.Looper
import android.util.Log
import android.webkit.CookieManager
import android.webkit.ServiceWorkerClient
import android.webkit.ServiceWorkerController
import android.webkit.URLUtil
import android.webkit.WebResourceRequest
import android.webkit.WebResourceResponse
import android.webkit.WebSettings
import android.webkit.WebView
import android.widget.Toast
import com.google.android.material.dialog.MaterialAlertDialogBuilder

/** WebView setup and behaviour shared by the browser and pinned web apps. */
object WebPages {
    private const val TAG = "iroh-link"
    private var serviceWorkersRouted = false

    /** Applies the settings every page gets: scripts, storage, zoom, popups. */
    @SuppressLint("SetJavaScriptEnabled")
    fun configure(webView: WebView) {
        webView.settings.apply {
            javaScriptEnabled = true
            domStorageEnabled = true
            @Suppress("DEPRECATION")
            databaseEnabled = true
            setSupportMultipleWindows(true)
            javaScriptCanOpenWindowsAutomatically = false
            mediaPlaybackRequiresUserGesture = true
            loadWithOverviewMode = true
            useWideViewPort = true
            builtInZoomControls = true
            displayZoomControls = false
            allowFileAccess = false
            allowContentAccess = false
            mixedContentMode = WebSettings.MIXED_CONTENT_COMPATIBILITY_MODE
            setGeolocationEnabled(false)
        }
        CookieManager.getInstance().setAcceptThirdPartyCookies(webView, false)
        routeServiceWorkers(webView.context)
    }

    /**
     * Serves a request from the gateway through the app's cache, or returns `null`.
     *
     * Covers gateway URLs and absolute public `blake3.net` and `pkarr.net`
     * URLs in pages, which would otherwise go to the real domains.
     */
    fun intercept(context: Context, request: WebResourceRequest): WebResourceResponse? {
        val url = request.url.toString()
        val client = (context.applicationContext as IrohLinkApp).gatewayClient
        Links.toInternal(url)?.let {
            return client.load(it, request.method, request.requestHeaders, followRedirects = true)
        }
        if (!Links.isInternal(url)) return null
        return client.load(url, request.method, request.requestHeaders, followRedirects = false)
    }

    /** Sends service worker requests through the cache too, so offline-capable sites work offline. */
    private fun routeServiceWorkers(context: Context) {
        if (serviceWorkersRouted) return
        serviceWorkersRouted = true
        val app = context.applicationContext
        ServiceWorkerController.getInstance().setServiceWorkerClient(object : ServiceWorkerClient() {
            override fun shouldInterceptRequest(request: WebResourceRequest): WebResourceResponse? =
                intercept(app, request)
        })
    }

    /** Returns whether the WebView itself should load a URL with this scheme. */
    fun isWebScheme(scheme: String?): Boolean =
        scheme?.lowercase() in setOf("http", "https", "about", "data", "blob", "javascript")

    /** Opens a non-web link, like `mailto:` or `intent:`, in another app. */
    fun openExternal(activity: Activity, url: String) {
        try {
            val intent = if (url.startsWith("intent:")) {
                Intent.parseUri(url, Intent.URI_INTENT_SCHEME).apply {
                    addCategory(Intent.CATEGORY_BROWSABLE)
                    component = null
                    selector = null
                }
            } else {
                Intent(Intent.ACTION_VIEW, Uri.parse(url)).addCategory(Intent.CATEGORY_BROWSABLE)
            }
            activity.startActivity(intent)
        } catch (error: ActivityNotFoundException) {
            Toast.makeText(activity, R.string.no_app, Toast.LENGTH_SHORT).show()
        } catch (error: Exception) {
            Log.w(TAG, "cannot open $url", error)
        }
    }

    /** Starts a download with the system download manager; see [DownloadsActivity]. */
    fun download(activity: Activity, url: String, userAgent: String?, disposition: String?, mime: String?) {
        if (!url.startsWith("http://") && !url.startsWith("https://")) {
            Toast.makeText(activity, R.string.download_unsupported, Toast.LENGTH_SHORT).show()
            return
        }
        val name = URLUtil.guessFileName(url, disposition, mime)
        // The download manager runs in another process, which cannot resolve
        // `*.localhost`; the gateway serves the same content by path.
        val request = DownloadManager.Request(Uri.parse(Links.toPathUrl(Links.toInternal(url) ?: url)))
            .setTitle(name)
            .setNotificationVisibility(DownloadManager.Request.VISIBILITY_VISIBLE_NOTIFY_COMPLETED)
            .setDestinationInExternalPublicDir(Environment.DIRECTORY_DOWNLOADS, name)
        mime?.takeIf { it.isNotBlank() }?.let(request::setMimeType)
        userAgent?.let { request.addRequestHeader("User-Agent", it) }
        CookieManager.getInstance().getCookie(url)?.let { request.addRequestHeader("Cookie", it) }
        try {
            activity.getSystemService(DownloadManager::class.java).enqueue(request)
            Toast.makeText(activity, activity.getString(R.string.downloading, name), Toast.LENGTH_SHORT).show()
        } catch (error: Exception) {
            Log.w(TAG, "download of $url failed", error)
            Toast.makeText(activity, R.string.download_unsupported, Toast.LENGTH_SHORT).show()
        }
    }

    fun copy(context: Context, url: String) {
        context.getSystemService(ClipboardManager::class.java).setPrimaryClip(ClipData.newPlainText("link", url))
        // Android 13 and later confirm copies themselves.
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) {
            Toast.makeText(context, R.string.link_copied, Toast.LENGTH_SHORT).show()
        }
    }

    fun share(activity: Activity, url: String) {
        val send = Intent(Intent.ACTION_SEND).setType("text/plain").putExtra(Intent.EXTRA_TEXT, url)
        activity.startActivity(Intent.createChooser(send, null))
    }

    /**
     * Shows the long-press menu for a link or image, if the press was on one.
     *
     * Returns whether a menu is shown; otherwise the WebView keeps its own
     * long-press behaviour, like text selection.
     */
    fun onLongPress(activity: Activity, webView: WebView, openInNewTab: (String) -> Unit): Boolean {
        val result = webView.hitTestResult
        val extra = result.extra ?: return false
        return when (result.type) {
            WebView.HitTestResult.SRC_ANCHOR_TYPE -> {
                showLinkMenu(activity, webView, extra, null, openInNewTab)
                true
            }
            WebView.HitTestResult.SRC_IMAGE_ANCHOR_TYPE -> {
                // The extra is the image; the link comes from the focused node.
                val message = Handler(Looper.getMainLooper()) { message ->
                    val link = message.data.getString("url")?.takeIf { it.isNotBlank() } ?: extra
                    showLinkMenu(activity, webView, link, extra, openInNewTab)
                    true
                }.obtainMessage()
                webView.requestFocusNodeHref(message)
                true
            }
            WebView.HitTestResult.IMAGE_TYPE -> {
                showLinkMenu(activity, webView, null, extra, openInNewTab)
                true
            }
            else -> false
        }
    }

    private fun showLinkMenu(activity: Activity, webView: WebView, link: String?, image: String?, openInNewTab: (String) -> Unit) {
        val actions = mutableListOf<Pair<Int, () -> Unit>>()
        if (link != null && !link.startsWith("javascript:")) {
            val shown = Links.toDisplay(link)
            actions += R.string.open_new_tab to { openInNewTab(shown) }
            actions += R.string.copy_link to { copy(activity, shown) }
            actions += R.string.share to { share(activity, shown) }
            actions += R.string.download_link to { download(activity, shown, webView.settings.userAgentString, null, null) }
        }
        if (image != null && !image.startsWith("data:")) {
            val shown = Links.toDisplay(image)
            actions += R.string.open_image_new_tab to { openInNewTab(shown) }
            actions += R.string.download_image to { download(activity, shown, webView.settings.userAgentString, null, null) }
        }
        if (actions.isEmpty()) return
        MaterialAlertDialogBuilder(activity)
            .setTitle(AddressFormat.format(activity, link ?: image ?: return))
            .setItems(actions.map { activity.getString(it.first) }.toTypedArray()) { _, which -> actions[which].second() }
            .show()
    }
}
