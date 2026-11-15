package computer.iroh.link

import android.app.ActivityManager
import android.content.Intent
import android.graphics.BitmapFactory
import android.net.Uri
import android.os.Bundle
import android.os.Message
import android.view.View
import android.view.ViewGroup
import android.webkit.ConsoleMessage
import android.webkit.ValueCallback
import android.webkit.WebChromeClient
import android.webkit.WebResourceRequest
import android.webkit.WebResourceResponse
import android.webkit.WebView
import android.webkit.WebViewClient
import android.widget.FrameLayout
import androidx.activity.OnBackPressedCallback
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts.StartActivityForResult
import androidx.appcompat.app.AppCompatActivity
import androidx.core.view.ViewCompat
import androidx.core.view.WindowInsetsCompat
import com.google.android.material.progressindicator.LinearProgressIndicator

/**
 * A site pinned to the home screen, shown without browser controls.
 *
 * Each pinned site runs in its own task, so it appears in recents like an app,
 * with its name and icon. It shares storage with the browser. Links to other
 * sites open in the browser.
 */
class WebAppActivity : AppCompatActivity() {
    private lateinit var webView: WebView
    private lateinit var progress: LinearProgressIndicator
    private lateinit var origin: String
    private var fileCallback: ValueCallback<Array<Uri>>? = null

    private val fileChooser = registerForActivityResult(StartActivityForResult()) { result ->
        fileCallback?.onReceiveValue(WebChromeClient.FileChooserParams.parseResult(result.resultCode, result.data))
        fileCallback = null
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        enableEdgeToEdge()
        super.onCreate(savedInstanceState)
        val start = intent.dataString ?: return finish()
        val startInternal = Links.toInternal(start) ?: start
        origin = Links.origin(startInternal) ?: return finish()
        describeTask()

        val root = FrameLayout(this)
        webView = WebView(this)
        progress = LinearProgressIndicator(this).apply {
            trackThickness = (2 * resources.displayMetrics.density).toInt()
            visibility = View.INVISIBLE
        }
        root.addView(webView, FrameLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.MATCH_PARENT))
        root.addView(progress, FrameLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT))
        setContentView(root)
        ViewCompat.setOnApplyWindowInsetsListener(root) { view, insets ->
            val bars = insets.getInsets(
                WindowInsetsCompat.Type.systemBars() or WindowInsetsCompat.Type.ime() or
                    WindowInsetsCompat.Type.displayCutout(),
            )
            view.setPadding(bars.left, bars.top, bars.right, bars.bottom)
            WindowInsetsCompat.CONSUMED
        }

        WebPages.configure(webView)
        webView.webViewClient = Client()
        webView.webChromeClient = Chrome()
        webView.setDownloadListener { url, userAgent, disposition, mime, _ ->
            WebPages.download(this, url, userAgent, disposition, mime)
        }
        webView.setOnLongClickListener { WebPages.onLongPress(this, webView, ::openInBrowser) }
        onBackPressedDispatcher.addCallback(this, object : OnBackPressedCallback(true) {
            override fun handleOnBackPressed() {
                if (webView.canGoBack()) webView.goBack() else moveTaskToBack(true)
            }
        })
        if (savedInstanceState == null || webView.restoreState(savedInstanceState) == null) {
            webView.loadUrl(startInternal)
        }
    }

    override fun onSaveInstanceState(outState: Bundle) {
        super.onSaveInstanceState(outState)
        webView.saveState(outState)
    }

    override fun onResume() {
        super.onResume()
        webView.onResume()
    }

    override fun onPause() {
        webView.onPause()
        super.onPause()
    }

    override fun onDestroy() {
        webView.destroy()
        super.onDestroy()
    }

    /** Names the task in recents after the web app, with its icon. */
    private fun describeTask() {
        val name = intent.getStringExtra(EXTRA_NAME) ?: getString(R.string.app_name)
        title = name
        val icon = intent.getStringExtra(EXTRA_ID)?.let { WebApps.iconFile(this, it) }
            ?.takeIf { it.exists() }?.let { BitmapFactory.decodeFile(it.path) }
        // The builder only takes icon resources; this constructor still works
        // and is the only way to show a bitmap icon.
        @Suppress("DEPRECATION")
        setTaskDescription(ActivityManager.TaskDescription(name, icon))
    }

    private fun openInBrowser(url: String) {
        startActivity(Intent(Intent.ACTION_VIEW, Uri.parse(Links.toDisplay(url)), this, MainActivity::class.java))
    }

    private inner class Client : WebViewClient() {
        override fun shouldOverrideUrlLoading(view: WebView, request: WebResourceRequest): Boolean {
            val url = request.url.toString()
            if (!WebPages.isWebScheme(request.url.scheme)) {
                WebPages.openExternal(this@WebAppActivity, url)
                return true
            }
            if (!request.isForMainFrame) return false
            val internal = Links.toInternal(url) ?: url
            if (Links.origin(internal) != origin) {
                // Other sites are not part of this app.
                openInBrowser(url)
                return true
            }
            if (internal != url) {
                view.loadUrl(internal)
                return true
            }
            return false
        }

        override fun shouldInterceptRequest(view: WebView, request: WebResourceRequest): WebResourceResponse? =
            WebPages.intercept(this@WebAppActivity, request)
    }

    private inner class Chrome : WebChromeClient() {
        override fun onProgressChanged(view: WebView, newProgress: Int) {
            progress.visibility = if (newProgress < 100) View.VISIBLE else View.INVISIBLE
            progress.setProgressCompat(newProgress, true)
        }

        override fun onCreateWindow(view: WebView, isDialog: Boolean, isUserGesture: Boolean, resultMsg: Message): Boolean {
            // Popups open in the browser: catch the first URL the new window loads.
            val catcher = WebView(this@WebAppActivity)
            catcher.webViewClient = object : WebViewClient() {
                override fun shouldOverrideUrlLoading(view: WebView, request: WebResourceRequest): Boolean {
                    openInBrowser(request.url.toString())
                    view.destroy()
                    return true
                }
            }
            (resultMsg.obj as WebView.WebViewTransport).webView = catcher
            resultMsg.sendToTarget()
            return true
        }

        override fun onShowFileChooser(
            webView: WebView,
            filePathCallback: ValueCallback<Array<Uri>>,
            fileChooserParams: FileChooserParams,
        ): Boolean {
            fileCallback?.onReceiveValue(null)
            fileCallback = filePathCallback
            return try {
                fileChooser.launch(fileChooserParams.createIntent())
                true
            } catch (error: Exception) {
                fileCallback = null
                false
            }
        }

        override fun onConsoleMessage(message: ConsoleMessage): Boolean {
            android.util.Log.d("iroh-link-web", "${message.messageLevel()} ${message.message()} (${message.sourceId()}:${message.lineNumber()})")
            return true
        }
    }

    companion object {
        const val EXTRA_NAME = "name"
        const val EXTRA_ID = "id"
    }
}
