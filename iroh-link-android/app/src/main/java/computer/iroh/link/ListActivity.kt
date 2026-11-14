package computer.iroh.link

import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.content.Intent
import android.os.Bundle
import android.text.format.DateUtils
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.EditText
import android.widget.PopupMenu
import android.widget.TextView
import androidx.activity.enableEdgeToEdge
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import androidx.core.view.ViewCompat
import androidx.core.view.WindowInsetsCompat
import androidx.core.view.isVisible
import androidx.recyclerview.widget.DividerItemDecoration
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import com.google.android.material.appbar.MaterialToolbar

/** Lists history or bookmarks, and returns the chosen URL to [MainActivity]. */
class ListActivity : AppCompatActivity() {
    private lateinit var store: Store
    private lateinit var adapter: Adapter
    private lateinit var empty: TextView
    private val history get() = intent.getStringExtra(EXTRA_MODE) == MODE_HISTORY

    override fun onCreate(savedInstanceState: Bundle?) {
        enableEdgeToEdge()
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_list)
        store = Store.get(this)
        ViewCompat.setOnApplyWindowInsetsListener(findViewById(R.id.root)) { view, insets ->
            val bars = insets.getInsets(WindowInsetsCompat.Type.systemBars() or WindowInsetsCompat.Type.displayCutout())
            view.setPadding(bars.left, bars.top, bars.right, bars.bottom)
            WindowInsetsCompat.CONSUMED
        }
        val toolbar = findViewById<MaterialToolbar>(R.id.toolbar)
        toolbar.setTitle(if (history) R.string.history else R.string.bookmarks)
        toolbar.setNavigationIcon(R.drawable.ic_arrow_back)
        toolbar.setNavigationOnClickListener { finish() }
        if (history) {
            toolbar.inflateMenu(R.menu.history)
            toolbar.setOnMenuItemClickListener {
                AlertDialog.Builder(this)
                    .setMessage(R.string.clear_history_confirm)
                    .setPositiveButton(R.string.clear) { _, _ ->
                        store.clearHistory()
                        refresh()
                    }
                    .setNegativeButton(R.string.cancel, null)
                    .show()
                true
            }
        }
        empty = findViewById(R.id.empty)
        empty.setText(if (history) R.string.no_history else R.string.no_bookmarks)
        adapter = Adapter()
        findViewById<RecyclerView>(R.id.list).apply {
            layoutManager = LinearLayoutManager(this@ListActivity)
            addItemDecoration(DividerItemDecoration(this@ListActivity, DividerItemDecoration.VERTICAL))
            adapter = this@ListActivity.adapter
        }
        refresh()
    }

    private fun refresh() {
        adapter.entries = if (history) store.history() else store.bookmarks()
        adapter.notifyDataSetChanged()
        empty.isVisible = adapter.entries.isEmpty()
    }

    private fun open(entry: Entry, newTab: Boolean) {
        setResult(RESULT_OK, Intent().putExtra(EXTRA_URL, entry.url).putExtra(EXTRA_NEW_TAB, newTab))
        finish()
    }

    private fun showActions(anchor: View, entry: Entry) {
        val popup = PopupMenu(this, anchor)
        popup.menu.add(0, 1, 0, R.string.open_new_tab)
        popup.menu.add(0, 2, 1, R.string.copy_link)
        if (!history) popup.menu.add(0, 3, 2, R.string.rename)
        popup.menu.add(0, 4, 3, R.string.delete)
        popup.setOnMenuItemClickListener { item ->
            when (item.itemId) {
                1 -> open(entry, newTab = true)
                2 -> getSystemService(ClipboardManager::class.java)
                    .setPrimaryClip(ClipData.newPlainText("link", entry.url))
                3 -> rename(entry)
                4 -> {
                    if (history) store.deleteVisit(entry.id) else store.deleteBookmark(entry.id)
                    refresh()
                }
            }
            true
        }
        popup.show()
    }

    private fun rename(entry: Entry) {
        val input = EditText(this).apply {
            setText(entry.title)
            setSingleLine()
            setSelection(text.length)
        }
        val padding = (20 * resources.displayMetrics.density).toInt()
        val frame = android.widget.FrameLayout(this).apply {
            setPadding(padding, padding / 2, padding, 0)
            addView(input)
        }
        AlertDialog.Builder(this)
            .setTitle(R.string.rename)
            .setView(frame)
            .setPositiveButton(R.string.ok) { _, _ ->
                store.renameBookmark(entry.id, input.text.toString().ifBlank { entry.url })
                refresh()
            }
            .setNegativeButton(R.string.cancel, null)
            .show()
    }

    private inner class Adapter : RecyclerView.Adapter<Adapter.Holder>() {
        var entries: List<Entry> = emptyList()

        inner class Holder(view: View) : RecyclerView.ViewHolder(view) {
            val title: TextView = view.findViewById(R.id.title)
            val subtitle: TextView = view.findViewById(R.id.subtitle)
        }

        override fun onCreateViewHolder(parent: ViewGroup, viewType: Int) =
            Holder(LayoutInflater.from(parent.context).inflate(R.layout.item_entry, parent, false))

        override fun getItemCount() = entries.size

        override fun onBindViewHolder(holder: Holder, position: Int) {
            val entry = entries[position]
            holder.title.text = entry.title.ifBlank { entry.url }
            holder.subtitle.text = if (history) {
                val now = System.currentTimeMillis()
                val time = if (now - entry.time < DateUtils.MINUTE_IN_MILLIS) {
                    getString(R.string.just_now)
                } else {
                    DateUtils.getRelativeTimeSpanString(entry.time, now, DateUtils.MINUTE_IN_MILLIS)
                }
                "$time  ${entry.url}"
            } else {
                entry.url
            }
            holder.itemView.setOnClickListener { open(entry, newTab = false) }
            holder.itemView.setOnLongClickListener {
                showActions(it, entry)
                true
            }
        }
    }

    companion object {
        const val EXTRA_MODE = "mode"
        const val EXTRA_URL = "url"
        const val EXTRA_NEW_TAB = "new_tab"
        const val MODE_HISTORY = "history"
        const val MODE_BOOKMARKS = "bookmarks"

        fun intent(context: Context, mode: String): Intent =
            Intent(context, ListActivity::class.java).putExtra(EXTRA_MODE, mode)
    }
}
