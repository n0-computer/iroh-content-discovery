package computer.iroh.link

import android.app.DownloadManager
import android.content.ActivityNotFoundException
import android.content.Intent
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.text.format.DateUtils
import android.text.format.Formatter
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.PopupMenu
import android.widget.TextView
import android.widget.Toast
import androidx.activity.enableEdgeToEdge
import androidx.appcompat.app.AppCompatActivity
import androidx.core.view.ViewCompat
import androidx.core.view.WindowInsetsCompat
import androidx.core.view.isVisible
import androidx.recyclerview.widget.DividerItemDecoration
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import com.google.android.material.appbar.MaterialToolbar
import com.google.android.material.progressindicator.LinearProgressIndicator

/** Lists the app's downloads from the system download manager, with progress. */
class DownloadsActivity : AppCompatActivity() {
    private lateinit var manager: DownloadManager
    private lateinit var empty: TextView
    private val adapter = Adapter()
    private val handler = Handler(Looper.getMainLooper())

    /** One download, read from the download manager. */
    private data class Download(
        val id: Long, val title: String, val status: Int, val done: Long, val total: Long,
        val mime: String?, val time: Long,
    )

    override fun onCreate(savedInstanceState: Bundle?) {
        enableEdgeToEdge()
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_list)
        manager = getSystemService(DownloadManager::class.java)
        ViewCompat.setOnApplyWindowInsetsListener(findViewById(R.id.root)) { view, insets ->
            val bars = insets.getInsets(WindowInsetsCompat.Type.systemBars() or WindowInsetsCompat.Type.displayCutout())
            view.setPadding(bars.left, bars.top, bars.right, bars.bottom)
            WindowInsetsCompat.CONSUMED
        }
        findViewById<MaterialToolbar>(R.id.toolbar).apply {
            setTitle(R.string.downloads)
            setNavigationIcon(R.drawable.ic_arrow_back)
            setNavigationOnClickListener { finish() }
        }
        empty = findViewById(R.id.empty)
        empty.setText(R.string.no_downloads)
        findViewById<RecyclerView>(R.id.list).apply {
            layoutManager = LinearLayoutManager(this@DownloadsActivity)
            addItemDecoration(DividerItemDecoration(this@DownloadsActivity, DividerItemDecoration.VERTICAL))
            adapter = this@DownloadsActivity.adapter
        }
    }

    override fun onResume() {
        super.onResume()
        refresh()
    }

    override fun onPause() {
        handler.removeCallbacksAndMessages(null)
        super.onPause()
    }

    /** Reloads the list, and again every second while a download runs. */
    private fun refresh() {
        handler.removeCallbacksAndMessages(null)
        val downloads = mutableListOf<Download>()
        manager.query(DownloadManager.Query()).use { cursor ->
            fun column(name: String) = cursor.getColumnIndexOrThrow(name)
            while (cursor.moveToNext()) {
                downloads += Download(
                    id = cursor.getLong(column(DownloadManager.COLUMN_ID)),
                    title = cursor.getString(column(DownloadManager.COLUMN_TITLE)).orEmpty(),
                    status = cursor.getInt(column(DownloadManager.COLUMN_STATUS)),
                    done = cursor.getLong(column(DownloadManager.COLUMN_BYTES_DOWNLOADED_SO_FAR)),
                    total = cursor.getLong(column(DownloadManager.COLUMN_TOTAL_SIZE_BYTES)),
                    mime = cursor.getString(column(DownloadManager.COLUMN_MEDIA_TYPE)),
                    time = cursor.getLong(column(DownloadManager.COLUMN_LAST_MODIFIED_TIMESTAMP)),
                )
            }
        }
        adapter.downloads = downloads.sortedByDescending { it.id }
        adapter.notifyDataSetChanged()
        empty.isVisible = downloads.isEmpty()
        val running = downloads.any { it.status == DownloadManager.STATUS_RUNNING || it.status == DownloadManager.STATUS_PENDING }
        if (running) handler.postDelayed(::refresh, 1000)
    }

    private fun open(download: Download) {
        if (download.status != DownloadManager.STATUS_SUCCESSFUL) return
        val uri = manager.getUriForDownloadedFile(download.id) ?: return
        val mime = download.mime ?: manager.getMimeTypeForDownloadedFile(download.id)
        try {
            startActivity(Intent(Intent.ACTION_VIEW).setDataAndType(uri, mime).addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION))
        } catch (error: ActivityNotFoundException) {
            Toast.makeText(this, R.string.no_app_for_file, Toast.LENGTH_SHORT).show()
        }
    }

    private fun showActions(anchor: View, download: Download) {
        val popup = PopupMenu(this, anchor)
        if (download.status == DownloadManager.STATUS_SUCCESSFUL) {
            popup.menu.add(0, 1, 0, R.string.open)
            popup.menu.add(0, 2, 1, R.string.share_file)
        }
        popup.menu.add(0, 3, 2, if (download.status == DownloadManager.STATUS_SUCCESSFUL) R.string.delete else R.string.cancel_download)
        popup.setOnMenuItemClickListener { item ->
            when (item.itemId) {
                1 -> open(download)
                2 -> manager.getUriForDownloadedFile(download.id)?.let { uri ->
                    val send = Intent(Intent.ACTION_SEND).setType(download.mime ?: "*/*")
                        .putExtra(Intent.EXTRA_STREAM, uri).addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
                    startActivity(Intent.createChooser(send, null))
                }
                3 -> {
                    // Removing a download also deletes its file.
                    manager.remove(download.id)
                    refresh()
                }
            }
            true
        }
        popup.show()
    }

    private fun describe(download: Download): String {
        val total = if (download.total > 0) Formatter.formatShortFileSize(this, download.total) else "?"
        return when (download.status) {
            DownloadManager.STATUS_SUCCESSFUL -> {
                val now = System.currentTimeMillis()
                val time = if (now - download.time < DateUtils.MINUTE_IN_MILLIS) {
                    getString(R.string.just_now)
                } else {
                    DateUtils.getRelativeTimeSpanString(download.time, now, DateUtils.MINUTE_IN_MILLIS)
                }
                "$total · $time"
            }
            DownloadManager.STATUS_RUNNING -> {
                val done = Formatter.formatShortFileSize(this, download.done)
                getString(R.string.download_progress, done, total)
            }
            DownloadManager.STATUS_PENDING -> getString(R.string.download_pending)
            DownloadManager.STATUS_PAUSED -> getString(R.string.download_paused)
            else -> getString(R.string.download_failed)
        }
    }

    private inner class Adapter : RecyclerView.Adapter<Adapter.Holder>() {
        var downloads: List<Download> = emptyList()

        inner class Holder(view: View) : RecyclerView.ViewHolder(view) {
            val title: TextView = view.findViewById(R.id.title)
            val subtitle: TextView = view.findViewById(R.id.subtitle)
            val progress: LinearProgressIndicator = view.findViewById(R.id.progress)
        }

        override fun onCreateViewHolder(parent: ViewGroup, viewType: Int) =
            Holder(LayoutInflater.from(parent.context).inflate(R.layout.item_download, parent, false))

        override fun getItemCount() = downloads.size

        override fun onBindViewHolder(holder: Holder, position: Int) {
            val download = downloads[position]
            holder.title.text = download.title
            holder.subtitle.text = describe(download)
            val running = download.status == DownloadManager.STATUS_RUNNING || download.status == DownloadManager.STATUS_PENDING
            holder.progress.isVisible = running
            val percent = if (download.total > 0) (download.done * 100 / download.total).toInt() else 0
            holder.progress.setProgressCompat(percent, false)
            holder.itemView.setOnClickListener { open(download) }
            holder.itemView.setOnLongClickListener {
                showActions(it, download)
                true
            }
        }
    }
}
