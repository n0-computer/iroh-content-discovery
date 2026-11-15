package computer.iroh.link

import android.content.Intent
import android.content.res.Configuration
import android.graphics.Bitmap
import android.graphics.Canvas
import android.graphics.Rect
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.os.Message
import android.provider.Settings
import android.text.Html
import android.util.Log
import android.view.KeyEvent
import android.view.LayoutInflater
import android.view.PixelCopy
import android.view.View
import android.view.ViewGroup
import android.view.inputmethod.EditorInfo
import android.view.inputmethod.InputMethodManager
import android.webkit.ConsoleMessage
import android.webkit.ValueCallback
import android.webkit.WebChromeClient
import android.webkit.WebResourceRequest
import android.webkit.WebResourceResponse
import android.webkit.WebView
import android.webkit.WebViewClient
import android.widget.EditText
import android.widget.FrameLayout
import android.widget.ImageButton
import android.widget.ImageView
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
import androidx.recyclerview.widget.GridLayoutManager
import androidx.recyclerview.widget.ItemTouchHelper
import androidx.recyclerview.widget.RecyclerView
import androidx.swiperefreshlayout.widget.SwipeRefreshLayout
import com.google.android.material.card.MaterialCardView
import com.google.android.material.color.MaterialColors
import com.google.android.material.progressindicator.LinearProgressIndicator
import com.google.android.material.snackbar.Snackbar
import org.json.JSONArray

/** The browser: tabs of WebViews, an address bar, and a menu. */
class MainActivity : AppCompatActivity() {
    private lateinit var store: Store
    private lateinit var urlBar: EditText
    private lateinit var urlDisplay: TextView
    private lateinit var progress: LinearProgressIndicator
    private lateinit var banner: TextView
    private lateinit var container: FrameLayout
    private lateinit var fullscreen: FrameLayout
    private lateinit var tabCount: TextView
    private lateinit var switcher: View
    private lateinit var switcherGrid: RecyclerView
    private lateinit var switcherTitle: TextView
    private lateinit var snackbarHost: View
    private lateinit var swipeRefresh: SwipeRefreshLayout
    private val switcherAdapter = TabAdapter()

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
    private inner class Tab(val webView: BrowserWebView, val parent: Tab?, val external: Boolean) {
        var title = ""
        var progress = 100
        var favicon: Bitmap? = null
        /** The top of the page as last seen, for the tab switcher. */
        var thumbnail: Bitmap? = null
        /** A URL to load when the tab is first shown, for tabs restored at startup or opened in the background. */
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
        urlDisplay = findViewById(R.id.url_display)
        progress = findViewById(R.id.progress)
        banner = findViewById(R.id.banner)
        container = findViewById(R.id.web_container)
        fullscreen = findViewById(R.id.fullscreen)
        tabCount = findViewById(R.id.tab_count)
        switcher = findViewById(R.id.switcher)
        switcherGrid = findViewById(R.id.switcher_grid)
        switcherTitle = findViewById(R.id.switcher_title)
        snackbarHost = findViewById(R.id.snackbar_host)
        swipeRefresh = findViewById(R.id.swipe_refresh)
        // Pull to refresh only when the page itself is pulled past its top.
        swipeRefresh.setOnChildScrollUpCallback { _, _ -> current.webView.keepsGesture() }
        swipeRefresh.setOnRefreshListener {
            if (current.url.isEmpty()) {
                showStartPage(current)
                swipeRefresh.isRefreshing = false
            } else {
                current.webView.reload()
            }
        }

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
        urlDisplay.setOnClickListener { editAddress() }
        findViewById<View>(R.id.tabs_button).setOnClickListener { showSwitcher() }
        findViewById<ImageButton>(R.id.menu_button).setOnClickListener { showMenu(it) }
        setUpSwitcher()
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
        linkFromIntent(intent)?.let {
            hideSwitcher()
            openTab(it, external = true)
        }
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

    override fun onConfigurationChanged(newConfig: Configuration) {
        super.onConfigurationChanged(newConfig)
        // The pill colours follow the theme, and the grid the screen width.
        showUrl()
        (switcherGrid.layoutManager as GridLayoutManager).spanCount = spanCount()
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
            switcher.isVisible -> hideSwitcher()
            urlBar.hasFocus() -> hideKeyboard()
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

    // Address bar

    /** Shows the formatted address, unless the user is editing it. */
    private fun showUrl() {
        if (urlBar.hasFocus()) return
        urlDisplay.isVisible = true
        val url = current.url
        urlDisplay.text = if (url.isEmpty()) "" else AddressFormat.format(this, url)
    }

    /** Switches to the editable full address, all selected. */
    private fun editAddress() {
        urlDisplay.isVisible = false
        urlBar.setText(current.url)
        urlBar.requestFocus()
        urlBar.selectAll()
        showKeyboard()
    }

    // Tabs

    private fun createWebView(tab: () -> Tab): BrowserWebView = BrowserWebView(this).apply {
        layoutParams = ViewGroup.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.MATCH_PARENT)
        WebPages.configure(this)
        setDownloadListener { url, userAgent, disposition, mime, _ ->
            WebPages.download(this@MainActivity, url, userAgent, disposition, mime)
        }
        setOnLongClickListener { WebPages.onLongPress(this@MainActivity, this) { url -> openInBackground(url, tab()) } }
    }

    /**
     * Opens a tab after [parent], or at the end.
     *
     * Without a [url], the tab shows the start page, or nothing if [blank].
     * Unless [select] is false, the new tab becomes the current one.
     */
    private fun openTab(
        url: String?,
        parent: Tab? = null,
        external: Boolean = false,
        blank: Boolean = false,
        select: Boolean = true,
    ): Tab {
        lateinit var tab: Tab
        val webView = createWebView { tab }
        tab = Tab(webView, parent, external)
        webView.webViewClient = Client(tab)
        webView.webChromeClient = Chrome(tab)
        val index = if (parent != null && parent in tabs) tabs.indexOf(parent) + 1 else tabs.size
        tabs.add(index, tab)
        when {
            !select && url != null -> tab.pendingUrl = url
            url != null -> load(tab, url)
            !blank -> showStartPage(tab)
        }
        if (!select) {
            updateUi()
            return tab
        }
        if (url != null && urlBar.hasFocus()) hideKeyboard()
        selectTab(tab)
        if (url == null && !blank) editAddress()
        return tab
    }

    /** Opens a link from a page's long-press menu in a new tab behind the current one. */
    private fun openInBackground(url: String, parent: Tab) {
        val tab = openTab(url, parent = parent, select = false)
        Snackbar.make(snackbarHost, R.string.opened_in_new_tab, Snackbar.LENGTH_LONG)
            .setAction(R.string.switch_tab) { if (tab in tabs) selectTab(tab) }
            .show()
    }

    private fun selectTab(tab: Tab) {
        if (::swipeRefresh.isInitialized) swipeRefresh.isRefreshing = false
        if (::current.isInitialized && current !== tab) {
            captureThumbnail(current)
            current.webView.onPause()
        }
        current = tab
        container.removeAllViews()
        container.addView(tab.webView)
        tab.webView.onResume()
        tab.pendingUrl?.let { load(tab, it) }
        updateUi()
    }

    /** Removes a tab; a snackbar offers to bring it back for a few seconds. */
    private fun closeTab(tab: Tab, offerUndo: Boolean = false) {
        val index = tabs.indexOf(tab)
        if (index < 0) return
        tabs.removeAt(index)
        container.removeView(tab.webView)
        tab.webView.onPause()
        if (tab === current) {
            when {
                // A fresh start page, without opening the keyboard.
                tabs.isEmpty() -> showStartPage(openTab(null, blank = true))
                tab.parent != null && tab.parent in tabs -> selectTab(tab.parent)
                else -> selectTab(tabs[minOf(index, tabs.size - 1)])
            }
        }
        updateUi()
        if (!offerUndo) {
            tab.webView.destroy()
            return
        }
        val host = if (switcher.isVisible) switcher else snackbarHost
        Snackbar.make(host, R.string.tab_closed, Snackbar.LENGTH_LONG)
            .setAction(R.string.undo) {
                tabs.add(minOf(index, tabs.size), tab)
                updateUi()
            }
            .addCallback(object : Snackbar.Callback() {
                override fun onDismissed(snackbar: Snackbar, event: Int) {
                    if (event != DISMISS_EVENT_ACTION) tab.webView.destroy()
                }
            })
            .show()
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

    // Tab switcher

    private fun spanCount(): Int = maxOf(2, resources.configuration.screenWidthDp / 200)

    private fun setUpSwitcher() {
        switcherGrid.layoutManager = GridLayoutManager(this, spanCount())
        switcherGrid.adapter = switcherAdapter
        findViewById<View>(R.id.switcher_close).setOnClickListener { hideSwitcher() }
        findViewById<View>(R.id.switcher_new_tab).setOnClickListener {
            hideSwitcher()
            openTab(null)
        }
        // Swipe a card sideways to close its tab, as in Chrome.
        ItemTouchHelper(object : ItemTouchHelper.SimpleCallback(0, ItemTouchHelper.LEFT or ItemTouchHelper.RIGHT) {
            override fun onMove(recycler: RecyclerView, from: RecyclerView.ViewHolder, to: RecyclerView.ViewHolder) = false

            override fun onSwiped(holder: RecyclerView.ViewHolder, direction: Int) {
                tabs.getOrNull(holder.bindingAdapterPosition)?.let { closeTab(it, offerUndo = true) }
            }
        }).attachToRecyclerView(switcherGrid)
    }

    private fun showSwitcher() {
        hideKeyboard()
        captureThumbnail(current) {
            switcher.isVisible = true
            updateUi()
            switcherGrid.scrollToPosition(tabs.indexOf(current))
        }
    }

    private fun hideSwitcher() {
        switcher.isVisible = false
    }

    /**
     * Captures the top of a tab's page for the switcher, then runs [done].
     *
     * Copies the pixels on screen, which also works for GPU-drawn content;
     * the tab must be the one shown.
     */
    private fun captureThumbnail(tab: Tab, done: () -> Unit = {}) {
        val view = tab.webView
        if (!view.isAttachedToWindow || view.width == 0 || view.height == 0 || tab.url.isEmpty() && tab.title.isEmpty()) {
            if (tab.url.isEmpty()) tab.thumbnail = null
            done()
            return
        }
        // The card shows a 4:5 portrait of the top of the page, at a third of its size.
        val height = minOf(view.height, view.width * 5 / 4)
        val bitmap = Bitmap.createBitmap(view.width / 3, height / 3, Bitmap.Config.ARGB_8888)
        val location = IntArray(2)
        view.getLocationInWindow(location)
        val source = Rect(location[0], location[1], location[0] + view.width, location[1] + height)
        try {
            PixelCopy.request(window, source, bitmap, { result ->
                if (result == PixelCopy.SUCCESS) {
                    tab.thumbnail = bitmap
                } else {
                    drawThumbnail(view, bitmap)?.let { tab.thumbnail = it }
                }
                done()
            }, handler)
        } catch (error: IllegalArgumentException) {
            drawThumbnail(view, bitmap)?.let { tab.thumbnail = it }
            done()
        }
    }

    /** Draws the view in software, for when the pixels cannot be copied. */
    private fun drawThumbnail(view: View, bitmap: Bitmap): Bitmap? = try {
        val canvas = Canvas(bitmap)
        canvas.scale(1 / 3f, 1 / 3f)
        view.draw(canvas)
        bitmap
    } catch (error: Exception) {
        null
    }

    private inner class TabAdapter : RecyclerView.Adapter<TabAdapter.Holder>() {
        inner class Holder(view: View) : RecyclerView.ViewHolder(view) {
            val card: MaterialCardView = view.findViewById(R.id.card)
            val favicon: ImageView = view.findViewById(R.id.favicon)
            val title: TextView = view.findViewById(R.id.title)
            val close: View = view.findViewById(R.id.close)
            val thumbnail: ImageView = view.findViewById(R.id.thumbnail)
        }

        override fun onCreateViewHolder(parent: ViewGroup, viewType: Int) =
            Holder(LayoutInflater.from(parent.context).inflate(R.layout.item_tab_card, parent, false))

        override fun getItemCount() = tabs.size

        override fun onBindViewHolder(holder: Holder, position: Int) {
            val tab = tabs[position]
            holder.title.text = tab.title.ifBlank {
                Links.parse(tab.url)?.let { parsed ->
                    if (parsed.kind != null) AddressFormat.shorten(parsed.label) else parsed.host
                } ?: getString(R.string.new_tab)
            }
            holder.favicon.setImageBitmap(tab.favicon)
            holder.favicon.isVisible = tab.favicon != null
            holder.thumbnail.setImageBitmap(tab.thumbnail)
            // Portrait cards: the image is 5/4 as tall as the card is wide.
            // The grid may not be laid out yet on first show, so use the screen width.
            val width = (resources.displayMetrics.widthPixels - switcherGrid.paddingLeft - switcherGrid.paddingRight) / spanCount()
            holder.thumbnail.layoutParams.height = (width * 5 / 4).coerceAtLeast(1)
            val selected = tab === current
            holder.card.strokeWidth = if (selected) (3 * resources.displayMetrics.density).toInt() else 0
            holder.card.strokeColor = MaterialColors.getColor(holder.card, com.google.android.material.R.attr.colorPrimary)
            holder.itemView.setOnClickListener {
                selectTab(tab)
                hideSwitcher()
            }
            holder.close.setOnClickListener { closeTab(tab, offerUndo = true) }
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
            openTab(urls.getString(i), select = false)
        }
        if (select) selectTab(tabs[prefs.getInt(PREF_SELECTED, 0).coerceIn(0, tabs.size - 1)])
    }

    // Toolbar and menu

    private fun updateUi() {
        tabCount.text = if (tabs.size > 99) ":D" else tabs.size.toString()
        switcherTitle.text = if (tabs.size == 1) getString(R.string.one_tab) else getString(R.string.tab_count, tabs.size)
        if (switcher.isVisible) switcherAdapter.notifyDataSetChanged()
        if (::current.isInitialized) {
            showUrl()
            showProgress()
        }
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
            for (id in listOf(R.id.bookmark, R.id.share, R.id.copy_link, R.id.pin)) findItem(id).isEnabled = url.isNotEmpty()
        }
        popup.setOnMenuItemClickListener { item ->
            when (item.itemId) {
                R.id.forward -> current.webView.goForward()
                R.id.reload -> if (current.url.isEmpty()) showStartPage(current) else current.webView.reload()
                R.id.new_tab -> openTab(null)
                R.id.bookmark -> toggleBookmark(url, bookmarked)
                R.id.bookmarks -> pickEntry.launch(ListActivity.intent(this, ListActivity.MODE_BOOKMARKS))
                R.id.history -> pickEntry.launch(ListActivity.intent(this, ListActivity.MODE_HISTORY))
                R.id.downloads -> startActivity(Intent(this, DownloadsActivity::class.java))
                R.id.pin -> WebApps.pin(this, current.webView, current.favicon)
                R.id.share -> WebPages.share(this, url)
                R.id.copy_link -> WebPages.copy(this, url)
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

    private fun showKeyboard() {
        urlBar.post {
            getSystemService(InputMethodManager::class.java).showSoftInput(urlBar, InputMethodManager.SHOW_IMPLICIT)
        }
    }

    private fun hideKeyboard() {
        getSystemService(InputMethodManager::class.java).hideSoftInputFromWindow(urlBar.windowToken, 0)
        urlBar.clearFocus()
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
            if (WebPages.isWebScheme(request.url.scheme)) return false
            if (request.hasGesture() || request.isForMainFrame) WebPages.openExternal(this@MainActivity, url)
            return true
        }

        override fun shouldInterceptRequest(view: WebView, request: WebResourceRequest): WebResourceResponse? =
            WebPages.intercept(this@MainActivity, request)

        override fun onPageStarted(view: WebView, url: String, favicon: Bitmap?) {
            tab.title = ""
            tab.favicon = favicon
            if (tab === current) updateUi()
        }

        override fun onPageFinished(view: WebView, url: String) {
            if (tab === current) {
                swipeRefresh.isRefreshing = false
                updateUi()
            }
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

        override fun onReceivedIcon(view: WebView, icon: Bitmap?) {
            tab.favicon = icon
        }

        override fun onCreateWindow(view: WebView, isDialog: Boolean, isUserGesture: Boolean, resultMsg: Message): Boolean {
            val child = openTab(null, parent = tab, blank = true)
            (resultMsg.obj as WebView.WebViewTransport).webView = child.webView
            resultMsg.sendToTarget()
            return true
        }

        override fun onCloseWindow(window: WebView) {
            tabs.firstOrNull { it.webView === window }?.let { closeTab(it) }
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
        const val TAG_WEB = "iroh-link-web"
        const val PREF_TABS = "tabs"
        const val PREF_SELECTED = "selected"
        /** Base and history URL of the start page. */
        const val START_URL = "about:blank"
    }
}
