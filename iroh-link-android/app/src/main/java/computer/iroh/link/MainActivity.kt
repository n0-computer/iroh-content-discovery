package computer.iroh.link

import android.annotation.SuppressLint
import android.app.DownloadManager
import android.content.ActivityNotFoundException
import android.content.ClipData
import android.content.ClipboardManager
import android.content.Intent
import android.graphics.Bitmap
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.os.Environment
import android.os.Handler
import android.os.Looper
import android.os.Message
import android.provider.Settings
import android.text.Html
import android.util.Log
import android.view.KeyEvent
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.view.inputmethod.EditorInfo
import android.view.inputmethod.InputMethodManager
import android.webkit.ConsoleMessage
import android.webkit.CookieManager
import android.webkit.URLUtil
import android.webkit.ValueCallback
import android.webkit.WebChromeClient
import android.webkit.WebResourceRequest
import android.webkit.WebResourceResponse
import android.webkit.WebSettings
import android.webkit.WebView
import android.webkit.WebViewClient
import android.widget.EditText
import android.widget.FrameLayout
import android.widget.ImageButton
import android.widget.PopupMenu
import android.widget.TextView
import android.widget.Toast
import androidx.activity.OnBackPressedCallback
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts.StartActivityForResult
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import androidx.core.view.ViewCompat
import androidx.core.view.WindowInsetsCompat
import androidx.core.view.WindowInsetsControllerCompat
import androidx.core.view.isVisible
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import com.google.android.material.bottomsheet.BottomSheetBehavior
import com.google.android.material.bottomsheet.BottomSheetDialog
import com.google.android.material.progressindicator.LinearProgressIndicator
import org.json.JSONArray

/** The browser: tabs of WebViews, an address bar, and a menu. */
class MainActivity : AppCompatActivity() {
    private lateinit var store: Store
    private lateinit var urlBar: EditText
    private lateinit var progress: LinearProgressIndicator
    private lateinit var banner: TextView
    private lateinit var container: FrameLayout
    private lateinit var fullscreen: FrameLayout
    private lateinit var tabCount: TextView

    private val tabs = mutableListOf<Tab>()
    private lateinit var current: Tab
    private var customViewCallback: WebChromeClient.CustomViewCallback? = null
    private var fileCallback: ValueCallback<Array<Uri>>? = null
    private val handler = Handler(Looper.getMainLooper())

    private val fileChooser = registerForActivityResult(StartActivityForResult()) { result ->
        fileCallback?.onReceiveValue(WebChromeClient.FileChooserParams.parseResult(result.resultCode, result.data))
        fileCallback = null
    }

    private val pickEntry = registerForActivityResult(StartActivityForResult()) { result ->
        val data = result.data ?: return@registerForActivityResult
        val url = data.getStringExtra(ListActivity.EXTRA_URL) ?: return@registerForActivityResult
        if (data.getBooleanExtra(ListActivity.EXTRA_NEW_TAB, false)) openTab(url) else load(url)
    }

    /** A tab and its WebView. */
    private inner class Tab(val webView: WebView, val parent: Tab?, val external: Boolean) {
        var title = ""
        var progress = 100
        /** A URL to load when the tab is first shown, for tabs restored at startup. */
        var pendingUrl: String? = null

        /** The page URL in public form, or an empty string for the start page. */
        val url: String
            get() {
                val raw = webView.url ?: pendingUrl ?: return ""
                return if (raw == START_URL || raw == "about:blank") "" else Links.toDisplay(raw)
            }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        enableEdgeToEdge()
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)
        store = Store.get(this)
        urlBar = findViewById(R.id.url_bar)
        progress = findViewById(R.id.progress)
        banner = findViewById(R.id.banner)
        container = findViewById(R.id.web_container)
        fullscreen = findViewById(R.id.fullscreen)
        tabCount = findViewById(R.id.tab_count)

        ViewCompat.setOnApplyWindowInsetsListener(findViewById(R.id.root)) { view, insets ->
            val bars = insets.getInsets(
                WindowInsetsCompat.Type.systemBars() or WindowInsetsCompat.Type.ime() or
                    WindowInsetsCompat.Type.displayCutout(),
            )
            view.setPadding(bars.left, bars.top, bars.right, bars.bottom)
            WindowInsetsCompat.CONSUMED
        }

        urlBar.setOnEditorActionListener { _, actionId, event ->
            val enter = event?.keyCode == KeyEvent.KEYCODE_ENTER && event.action == KeyEvent.ACTION_DOWN
            if (actionId == EditorInfo.IME_ACTION_GO || enter) {
                navigate(urlBar.text.toString())
                true
            } else {
                false
            }
        }
        urlBar.setOnFocusChangeListener { _, focused -> if (!focused) showUrl() }
        findViewById<View>(R.id.tabs_button).setOnClickListener { showTabs() }
        findViewById<ImageButton>(R.id.menu_button).setOnClickListener { showMenu(it) }
        onBackPressedDispatcher.addCallback(this, object : OnBackPressedCallback(true) {
            override fun handleOnBackPressed() = goBack()
        })

        val link = if (savedInstanceState == null) linkFromIntent(intent) else null
        restoreTabs(select = link == null)
        if (link != null) openTab(link, external = true)
        if (tabs.isEmpty()) openTab(null)
        watchGateway()
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        linkFromIntent(intent)?.let { openTab(it, external = true) }
    }

    override fun onResume() {
        super.onResume()
        current.webView.onResume()
    }

    override fun onPause() {
        current.webView.onPause()
        super.onPause()
    }

    override fun onStop() {
        super.onStop()
        saveTabs()
    }

    override fun onDestroy() {
        handler.removeCallbacksAndMessages(null)
        tabs.forEach { it.webView.destroy() }
        super.onDestroy()
    }

    private fun linkFromIntent(intent: Intent?): String? = when (intent?.action) {
        Intent.ACTION_VIEW -> intent.dataString
        Intent.ACTION_SEND -> intent.getStringExtra(Intent.EXTRA_TEXT)?.let(Links::findInText)
        else -> null
    }

    // Navigation

    private fun navigate(input: String) {
        val url = Links.fromInput(input)
        if (url == null) {
            Toast.makeText(this, getString(R.string.not_a_link, input), Toast.LENGTH_SHORT).show()
            return
        }
        hideKeyboard()
        load(url)
        current.webView.requestFocus()
    }

    /** Loads [url], in public or gateway form, in the current tab. */
    private fun load(url: String) = load(current, url)

    private fun load(tab: Tab, url: String) {
        tab.pendingUrl = null
        tab.webView.loadUrl(Links.toInternal(url) ?: url)
    }

    private fun goBack() {
        val tab = current
        when {
            fullscreen.isVisible -> hideCustomView()
            urlBar.hasFocus() -> {
                urlBar.clearFocus()
                hideKeyboard()
            }
            tab.webView.canGoBack() -> tab.webView.goBack()
            tab.parent != null && tabs.size > 1 -> closeTab(tab)
            tab.external && tabs.size > 1 -> {
                // Return to the app that opened the link, as browsers do.
                closeTab(tab)
                moveTaskToBack(true)
            }
            else -> moveTaskToBack(true)
        }
    }

    // Tabs

    @SuppressLint("SetJavaScriptEnabled")
    private fun createWebView(): WebView = WebView(this).apply {
        layoutParams = ViewGroup.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.MATCH_PARENT)
        settings.apply {
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
        CookieManager.getInstance().setAcceptThirdPartyCookies(this, false)
    }

    private fun openTab(url: String?, parent: Tab? = null, external: Boolean = false, blank: Boolean = false): Tab {
        val webView = createWebView()
        val tab = Tab(webView, parent, external)
        webView.webViewClient = Client(tab)
        webView.webChromeClient = Chrome(tab)
        webView.setDownloadListener { downloadUrl, userAgent, disposition, mime, _ ->
            download(downloadUrl, userAgent, disposition, mime)
        }
        val index = if (parent != null) tabs.indexOf(parent) + 1 else tabs.size
        tabs.add(index, tab)
        when {
            url != null -> load(tab, url)
            !blank -> showStartPage(tab)
        }
        if (url != null && urlBar.hasFocus()) hideKeyboard()
        selectTab(tab)
        if (url == null && !blank) {
            urlBar.requestFocus()
            showKeyboard()
        }
        return tab
    }

    private fun selectTab(tab: Tab) {
        if (::current.isInitialized && current !== tab) current.webView.onPause()
        current = tab
        container.removeAllViews()
        container.addView(tab.webView)
        tab.webView.onResume()
        tab.pendingUrl?.let { load(tab, it) }
        updateUi()
    }

    private fun closeTab(tab: Tab) {
        val index = tabs.indexOf(tab)
        if (index < 0) return
        tabs.removeAt(index)
        if (tab === current) {
            when {
                tabs.isEmpty() -> openTab(null)
                tab.parent != null && tab.parent in tabs -> selectTab(tab.parent)
                else -> selectTab(tabs[minOf(index, tabs.size - 1)])
            }
        }
        container.removeView(tab.webView)
        tab.webView.destroy()
        updateUi()
    }

    private fun showStartPage(tab: Tab) {
        tab.pendingUrl = null
        tab.webView.loadDataWithBaseURL(START_URL, startPage(), "text/html", "utf-8", START_URL)
    }

    private fun startPage(): String {
        fun escape(text: String) = Html.escapeHtml(text)
        fun links(entries: List<Entry>) = entries.joinToString("") {
            "<li><a href=\"${escape(it.url)}\">${escape(it.title.ifBlank { it.url })}</a><small>${escape(it.url)}</small></li>"
        }
        val bookmarks = store.bookmarks()
        val recent = store.history(limit = 8)
        return buildString {
            append(
                """<!doctype html><html><head><meta charset="utf-8">
                <meta name="viewport" content="width=device-width, initial-scale=1">
                <title>New tab</title><style>
                :root{color-scheme:light dark;font-family:sans-serif}
                body{margin:24px 20px;line-height:1.4}
                h1{font-size:22px;font-weight:500} h2{font-size:15px;opacity:.7;margin-top:28px}
                ul{list-style:none;padding:0} li{margin:0 0 14px}
                a{display:block;text-decoration:none;color:inherit;font-size:16px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
                small{display:block;opacity:.6;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
                p{opacity:.75}
                </style></head><body><h1>iroh link</h1>
                <p>Open https://&lt;hash&gt;.blake3.net and https://&lt;key&gt;.pkarr.net links. Content is found on the Mainline DHT and fetched over iroh, verified against its BLAKE3 hash.</p>""",
            )
            if (bookmarks.isNotEmpty()) append("<h2>Bookmarks</h2><ul>${links(bookmarks)}</ul>")
            if (recent.isNotEmpty()) append("<h2>Recent</h2><ul>${links(recent)}</ul>")
            append("</body></html>")
        }
    }

    private fun showTabs() {
        val dialog = BottomSheetDialog(this)
        val view = layoutInflater.inflate(R.layout.sheet_tabs, null)
        val list = view.findViewById<RecyclerView>(R.id.tab_list)
        list.layoutManager = LinearLayoutManager(this)
        list.adapter = TabAdapter(dialog)
        list.scrollToPosition(tabs.indexOf(current))
        view.findViewById<View>(R.id.new_tab).setOnClickListener {
            dialog.dismiss()
            openTab(null)
        }
        dialog.setContentView(view)
        dialog.behavior.state = BottomSheetBehavior.STATE_EXPANDED
        dialog.show()
    }

    private inner class TabAdapter(val dialog: BottomSheetDialog) : RecyclerView.Adapter<TabAdapter.Holder>() {
        inner class Holder(view: View) : RecyclerView.ViewHolder(view) {
            val title: TextView = view.findViewById(R.id.title)
            val subtitle: TextView = view.findViewById(R.id.subtitle)
            val close: View = view.findViewById(R.id.close)
        }

        override fun onCreateViewHolder(parent: ViewGroup, viewType: Int) =
            Holder(LayoutInflater.from(parent.context).inflate(R.layout.item_tab, parent, false))

        override fun getItemCount() = tabs.size

        override fun onBindViewHolder(holder: Holder, position: Int) {
            val tab = tabs[position]
            holder.title.text = tab.title.ifBlank { tab.url.ifBlank { getString(R.string.new_tab) } }
            holder.subtitle.text = tab.url
            holder.subtitle.isVisible = tab.url.isNotEmpty()
            holder.title.setTypeface(null, if (tab === current) android.graphics.Typeface.BOLD else android.graphics.Typeface.NORMAL)
            holder.itemView.setOnClickListener {
                selectTab(tab)
                dialog.dismiss()
            }
            holder.close.setOnClickListener {
                closeTab(tab)
                notifyDataSetChanged()
            }
        }
    }

    private fun saveTabs() {
        val urls = JSONArray()
        var selected = 0
        for (tab in tabs) {
            if (tab === current) selected = urls.length()
            if (tab.url.isNotEmpty()) urls.put(tab.url)
        }
        getPreferences(MODE_PRIVATE).edit()
            .putString(PREF_TABS, urls.toString())
            .putInt(PREF_SELECTED, selected)
            .apply()
    }

    /**
     * Restores the tabs of the last session.
     *
     * Tabs load when first shown, so only the selected one loads right away,
     * and none if [select] is false because a link opens in a new tab.
     */
    private fun restoreTabs(select: Boolean) {
        val prefs = getPreferences(MODE_PRIVATE)
        val urls = try {
            JSONArray(prefs.getString(PREF_TABS, "[]"))
        } catch (error: Exception) {
            JSONArray()
        }
        if (urls.length() == 0) return
        for (i in 0 until urls.length()) {
            openTab(null, blank = true).pendingUrl = urls.getString(i)
        }
        if (select) selectTab(tabs[prefs.getInt(PREF_SELECTED, 0).coerceIn(0, tabs.size - 1)])
    }

    // Toolbar and menu

    private fun updateUi() {
        tabCount.text = if (tabs.size > 99) ":D" else tabs.size.toString()
        showUrl()
        showProgress()
    }

    private fun showUrl() {
        if (!urlBar.hasFocus()) urlBar.setText(current.url)
    }

    private fun showProgress() {
        val loading = current.progress < 100
        progress.visibility = if (loading) View.VISIBLE else View.INVISIBLE
        if (loading) progress.setProgressCompat(current.progress, true)
    }

    private fun showMenu(anchor: View) {
        val url = current.url
        val bookmarked = url.isNotEmpty() && store.isBookmarked(url)
        val popup = PopupMenu(this, anchor)
        popup.inflate(R.menu.main)
        popup.menu.apply {
            findItem(R.id.forward).isEnabled = current.webView.canGoForward()
            findItem(R.id.bookmark).setTitle(if (bookmarked) R.string.remove_bookmark else R.string.add_bookmark)
            findItem(R.id.bookmark).isEnabled = url.isNotEmpty()
            findItem(R.id.share).isEnabled = url.isNotEmpty()
            findItem(R.id.copy_link).isEnabled = url.isNotEmpty()
        }
        popup.setOnMenuItemClickListener { item ->
            when (item.itemId) {
                R.id.forward -> current.webView.goForward()
                R.id.reload -> if (current.url.isEmpty()) showStartPage(current) else current.webView.reload()
                R.id.new_tab -> openTab(null)
                R.id.bookmark -> toggleBookmark(url, bookmarked)
                R.id.bookmarks -> pickEntry.launch(ListActivity.intent(this, ListActivity.MODE_BOOKMARKS))
                R.id.history -> pickEntry.launch(ListActivity.intent(this, ListActivity.MODE_HISTORY))
                R.id.share -> share(url)
                R.id.copy_link -> copy(url)
                R.id.open_by_default -> openByDefaultSettings()
                R.id.status -> showStatus()
            }
            true
        }
        popup.show()
    }

    private fun toggleBookmark(url: String, bookmarked: Boolean) {
        if (bookmarked) {
            store.removeBookmark(url)
            Toast.makeText(this, R.string.bookmark_removed, Toast.LENGTH_SHORT).show()
        } else {
            store.addBookmark(url, current.title.ifBlank { url })
            Toast.makeText(this, R.string.bookmark_added, Toast.LENGTH_SHORT).show()
        }
    }

    private fun share(url: String) {
        val send = Intent(Intent.ACTION_SEND).setType("text/plain").putExtra(Intent.EXTRA_TEXT, url)
        startActivity(Intent.createChooser(send, null))
    }

    private fun copy(url: String) {
        getSystemService(ClipboardManager::class.java).setPrimaryClip(ClipData.newPlainText("link", url))
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) {
            Toast.makeText(this, R.string.link_copied, Toast.LENGTH_SHORT).show()
        }
    }

    private fun openByDefaultSettings() {
        val packageUri = Uri.parse("package:$packageName")
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
            AlertDialog.Builder(this)
                .setTitle(R.string.open_by_default)
                .setMessage(R.string.open_by_default_help)
                .setPositiveButton(R.string.ok) { _, _ ->
                    startActivity(Intent(Settings.ACTION_APP_OPEN_BY_DEFAULT_SETTINGS, packageUri))
                }
                .setNegativeButton(R.string.cancel, null)
                .show()
        } else {
            startActivity(Intent(Settings.ACTION_APPLICATION_DETAILS_SETTINGS, packageUri))
        }
    }

    private fun showStatus() {
        val status = (application as IrohLinkApp).startError?.let { "not started: $it" } ?: NativeGateway.status()
        AlertDialog.Builder(this)
            .setTitle(R.string.gateway_status)
            .setMessage(getString(R.string.status_message, status, Links.PORT))
            .setPositiveButton(R.string.ok, null)
            .show()
    }

    /** Shows a banner until the gateway has found its index servers. */
    private fun watchGateway() {
        val error = (application as IrohLinkApp).startError
        if (error != null) {
            banner.text = getString(R.string.start_failed, error)
            banner.isVisible = true
            return
        }
        val status = NativeGateway.status()
        if (status == "ready") {
            banner.isVisible = false
            return
        }
        banner.text = getString(R.string.connecting, status)
        banner.isVisible = true
        handler.postDelayed(::watchGateway, 1000)
    }

    // Keyboard, downloads, external apps

    private fun showKeyboard() {
        urlBar.post {
            getSystemService(InputMethodManager::class.java).showSoftInput(urlBar, InputMethodManager.SHOW_IMPLICIT)
        }
    }

    private fun hideKeyboard() {
        getSystemService(InputMethodManager::class.java).hideSoftInputFromWindow(urlBar.windowToken, 0)
        urlBar.clearFocus()
    }

    private fun download(url: String, userAgent: String, disposition: String?, mime: String?) {
        if (!url.startsWith("http://") && !url.startsWith("https://")) {
            Toast.makeText(this, R.string.download_unsupported, Toast.LENGTH_SHORT).show()
            return
        }
        val name = URLUtil.guessFileName(url, disposition, mime)
        val request = DownloadManager.Request(Uri.parse(Links.toPathUrl(url)))
            .setMimeType(mime)
            .addRequestHeader("User-Agent", userAgent)
            .setTitle(name)
            .setNotificationVisibility(DownloadManager.Request.VISIBILITY_VISIBLE_NOTIFY_COMPLETED)
            .setDestinationInExternalPublicDir(Environment.DIRECTORY_DOWNLOADS, name)
        CookieManager.getInstance().getCookie(url)?.let { request.addRequestHeader("Cookie", it) }
        try {
            getSystemService(DownloadManager::class.java).enqueue(request)
            Toast.makeText(this, getString(R.string.downloading, name), Toast.LENGTH_SHORT).show()
        } catch (error: Exception) {
            Log.w(TAG, "download failed", error)
            Toast.makeText(this, R.string.download_unsupported, Toast.LENGTH_SHORT).show()
        }
    }

    private fun openExternal(url: String) {
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
            startActivity(intent)
        } catch (error: ActivityNotFoundException) {
            Toast.makeText(this, R.string.no_app, Toast.LENGTH_SHORT).show()
        } catch (error: Exception) {
            Log.w(TAG, "cannot open $url", error)
        }
    }

    private fun hideCustomView() {
        fullscreen.removeAllViews()
        fullscreen.isVisible = false
        WindowInsetsControllerCompat(window, window.decorView).show(WindowInsetsCompat.Type.systemBars())
        customViewCallback?.onCustomViewHidden()
        customViewCallback = null
    }

    private inner class Client(val tab: Tab) : WebViewClient() {
        override fun shouldOverrideUrlLoading(view: WebView, request: WebResourceRequest): Boolean {
            val url = request.url.toString()
            val internal = Links.toInternal(url)
            if (internal != null) {
                // Frames are served by shouldInterceptRequest under the public URL.
                if (!request.isForMainFrame) return false
                view.loadUrl(internal)
                return true
            }
            return when (request.url.scheme?.lowercase()) {
                "http", "https", "about", "data", "blob", "javascript" -> false
                else -> {
                    if (request.hasGesture() || request.isForMainFrame) openExternal(url)
                    true
                }
            }
        }

        override fun shouldInterceptRequest(view: WebView, request: WebResourceRequest): WebResourceResponse? {
            val url = request.url.toString()
            val client = (application as IrohLinkApp).gatewayClient
            // Absolute public URLs in a page: load them from the gateway under the public URL.
            Links.toInternal(url)?.let { return client.load(it, request.method, request.requestHeaders, followRedirects = true) }
            if (!Links.isInternal(url)) return null
            return client.load(url, request.method, request.requestHeaders, followRedirects = false)
        }

        override fun onPageStarted(view: WebView, url: String, favicon: Bitmap?) {
            tab.title = ""
            if (tab === current) updateUi()
        }

        override fun onPageFinished(view: WebView, url: String) {
            if (tab === current) updateUi()
        }

        override fun doUpdateVisitedHistory(view: WebView, url: String, isReload: Boolean) {
            val display = Links.toDisplay(url)
            if (!isReload && (display.startsWith("https://") || display.startsWith("http://"))) {
                store.addVisit(display, view.title?.takeUnless { it == url } ?: "")
            }
            if (tab === current) showUrl()
        }
    }

    private inner class Chrome(val tab: Tab) : WebChromeClient() {
        override fun onProgressChanged(view: WebView, newProgress: Int) {
            tab.progress = newProgress
            if (tab === current) showProgress()
        }

        override fun onReceivedTitle(view: WebView, title: String?) {
            tab.title = title.orEmpty()
            val url = tab.url
            if (url.isNotEmpty() && title != null && title != view.url) store.setVisitTitle(url, title)
        }

        override fun onCreateWindow(view: WebView, isDialog: Boolean, isUserGesture: Boolean, resultMsg: Message): Boolean {
            val child = openTab(null, parent = tab, blank = true)
            (resultMsg.obj as WebView.WebViewTransport).webView = child.webView
            resultMsg.sendToTarget()
            return true
        }

        override fun onCloseWindow(window: WebView) {
            tabs.firstOrNull { it.webView === window }?.let(::closeTab)
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
            } catch (error: ActivityNotFoundException) {
                fileCallback = null
                false
            }
        }

        override fun onShowCustomView(view: View, callback: CustomViewCallback) {
            if (fullscreen.isVisible) {
                callback.onCustomViewHidden()
                return
            }
            customViewCallback = callback
            fullscreen.addView(view, FrameLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.MATCH_PARENT))
            fullscreen.isVisible = true
            WindowInsetsControllerCompat(window, window.decorView).apply {
                hide(WindowInsetsCompat.Type.systemBars())
                systemBarsBehavior = WindowInsetsControllerCompat.BEHAVIOR_SHOW_TRANSIENT_BARS_BY_SWIPE
            }
        }

        override fun onHideCustomView() = hideCustomView()

        override fun onConsoleMessage(message: ConsoleMessage): Boolean {
            Log.d(TAG_WEB, "${message.messageLevel()} ${message.message()} (${message.sourceId()}:${message.lineNumber()})")
            return true
        }
    }

    private companion object {
        const val TAG = "iroh-link"
        const val TAG_WEB = "iroh-link-web"
        const val PREF_TABS = "tabs"
        const val PREF_SELECTED = "selected"
        /** Base and history URL of the start page. */
        const val START_URL = "about:blank"
    }
}
