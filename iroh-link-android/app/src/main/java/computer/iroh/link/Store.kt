package computer.iroh.link

import android.content.ContentValues
import android.content.Context
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteOpenHelper

/** A history entry or bookmark. URLs are always in their public form. */
data class Entry(val id: Long, val url: String, val title: String, val time: Long)

/** History and bookmarks, in a small SQLite database. */
class Store private constructor(context: Context) :
    SQLiteOpenHelper(context, "browser.db", null, 1) {

    override fun onCreate(db: SQLiteDatabase) {
        db.execSQL("CREATE TABLE history (id INTEGER PRIMARY KEY, url TEXT NOT NULL, title TEXT NOT NULL, time INTEGER NOT NULL)")
        db.execSQL("CREATE INDEX history_time ON history(time)")
        db.execSQL("CREATE TABLE bookmarks (id INTEGER PRIMARY KEY, url TEXT NOT NULL UNIQUE, title TEXT NOT NULL, time INTEGER NOT NULL)")
    }

    override fun onUpgrade(db: SQLiteDatabase, oldVersion: Int, newVersion: Int) {}

    /** Records a visit, unless it repeats the most recent one. */
    fun addVisit(url: String, title: String) {
        val db = writableDatabase
        db.rawQuery("SELECT url FROM history ORDER BY time DESC, id DESC LIMIT 1", null).use {
            if (it.moveToFirst() && it.getString(0) == url) {
                touch(db, url, title)
                return
            }
        }
        db.insert("history", null, values(url, title))
    }

    /** Sets the title of the latest visit to [url], once the page reports it. */
    fun setVisitTitle(url: String, title: String) {
        if (title.isBlank()) return
        writableDatabase.execSQL(
            "UPDATE history SET title = ? WHERE id = (SELECT id FROM history WHERE url = ? ORDER BY time DESC, id DESC LIMIT 1)",
            arrayOf(title, url),
        )
    }

    private fun touch(db: SQLiteDatabase, url: String, title: String) {
        db.execSQL(
            "UPDATE history SET time = ?, title = CASE WHEN ? = '' THEN title ELSE ? END WHERE id = (SELECT id FROM history WHERE url = ? ORDER BY time DESC, id DESC LIMIT 1)",
            arrayOf(System.currentTimeMillis(), title, title, url),
        )
    }

    fun history(limit: Int = 1000): List<Entry> = query("history", limit)

    fun deleteVisit(id: Long) {
        writableDatabase.delete("history", "id = ?", arrayOf(id.toString()))
    }

    fun clearHistory() {
        writableDatabase.delete("history", null, null)
    }

    fun bookmarks(): List<Entry> = query("bookmarks", 10_000)

    fun isBookmarked(url: String): Boolean =
        readableDatabase.rawQuery("SELECT 1 FROM bookmarks WHERE url = ?", arrayOf(url)).use { it.moveToFirst() }

    fun addBookmark(url: String, title: String) {
        writableDatabase.insertWithOnConflict("bookmarks", null, values(url, title), SQLiteDatabase.CONFLICT_REPLACE)
    }

    fun removeBookmark(url: String) {
        writableDatabase.delete("bookmarks", "url = ?", arrayOf(url))
    }

    fun renameBookmark(id: Long, title: String) {
        writableDatabase.update("bookmarks", ContentValues().apply { put("title", title) }, "id = ?", arrayOf(id.toString()))
    }

    fun deleteBookmark(id: Long) {
        writableDatabase.delete("bookmarks", "id = ?", arrayOf(id.toString()))
    }

    private fun values(url: String, title: String) = ContentValues().apply {
        put("url", url)
        put("title", title)
        put("time", System.currentTimeMillis())
    }

    private fun query(table: String, limit: Int): List<Entry> =
        readableDatabase.rawQuery("SELECT id, url, title, time FROM $table ORDER BY time DESC, id DESC LIMIT $limit", null).use {
            buildList {
                while (it.moveToNext()) add(Entry(it.getLong(0), it.getString(1), it.getString(2), it.getLong(3)))
            }
        }

    companion object {
        @Volatile
        private var instance: Store? = null

        fun get(context: Context): Store =
            instance ?: synchronized(this) {
                instance ?: Store(context.applicationContext).also { instance = it }
            }
    }
}
