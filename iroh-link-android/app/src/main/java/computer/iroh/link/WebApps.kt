package computer.iroh.link

import android.app.Activity
import android.content.Context
import android.content.Intent
import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.graphics.Canvas
import android.graphics.Color
import android.graphics.Paint
import android.graphics.Rect
import android.graphics.RectF
import android.graphics.Typeface
import android.net.Uri
import android.util.Log
import android.view.LayoutInflater
import android.webkit.WebView
import android.widget.EditText
import android.widget.ImageView
import android.widget.Toast
import androidx.core.content.ContextCompat
import androidx.core.content.pm.ShortcutInfoCompat
import androidx.core.content.pm.ShortcutManagerCompat
import androidx.core.graphics.drawable.IconCompat
import com.google.android.material.dialog.MaterialAlertDialogBuilder
import org.json.JSONArray
import org.json.JSONObject
import org.json.JSONTokener
import java.io.File
import java.net.URL
import java.security.MessageDigest

/**
 * Pins sites to the home screen as standalone web apps.
 *
 * Reads the page's web app manifest and icons, like browsers do for "Add to
 * Home screen", and opens the pinned site in [WebAppActivity].
 */
object WebApps {
    private const val TAG = "iroh-link"
    /** Edge length of the adaptive icon bitmap: 108 dp at 4x. */
    private const val ICON_PX = 432

    /** What a page says about itself. */
    private class Info(val name: String, val startUrl: String, val icon: Bitmap)

    /** Collects the title, manifest and icon links of the page; runs in the page. */
    private const val COLLECT = """(function() {
        function abs(h) { try { return new URL(h, document.baseURI).href } catch (e) { return null } }
        var icons = Array.prototype.map.call(
            document.querySelectorAll('link[rel~="icon"], link[rel="apple-touch-icon"], link[rel="apple-touch-icon-precomposed"]'),
            function(l) { return { src: abs(l.getAttribute('href')), sizes: l.getAttribute('sizes') || '', purpose: '' } });
        var m = document.querySelector('link[rel="manifest"]');
        return JSON.stringify({ title: document.title, manifest: m ? abs(m.getAttribute('href')) : null, icons: icons });
    })()"""

    /** Offers to pin the page shown in [webView], with [favicon] as the last-resort icon. */
    fun pin(activity: Activity, webView: WebView, favicon: Bitmap?) {
        val pageUrl = webView.url ?: return
        if (!ShortcutManagerCompat.isRequestPinShortcutSupported(activity)) {
            Toast.makeText(activity, R.string.pin_unsupported, Toast.LENGTH_SHORT).show()
            return
        }
        webView.evaluateJavascript(COLLECT) { result ->
            val page = try {
                JSONObject(JSONTokener(result).nextValue() as String)
            } catch (error: Exception) {
                JSONObject()
            }
            Thread {
                val info = describe(activity, pageUrl, page, favicon)
                activity.runOnUiThread { if (!activity.isFinishing) confirm(activity, info) }
            }.start()
        }
    }

    /** Reads the manifest and picks a name, start URL and icon; does network I/O. */
    private fun describe(context: Context, pageUrl: String, page: JSONObject, favicon: Bitmap?): Info {
        val client = (context.applicationContext as IrohLinkApp).gatewayClient
        var name = page.optString("title").ifBlank { AddressFormat.shorten(Links.parse(pageUrl)?.label.orEmpty()) }
        var startUrl = pageUrl
        var background: Int? = null
        val candidates = mutableListOf<Candidate>()
        val manifestUrl = page.optString("manifest").takeIf { it.isNotBlank() && it != "null" }
        if (manifestUrl != null) {
            try {
                val manifest = JSONObject(String(client.fetchBytes(manifestUrl) ?: ByteArray(0)))
                name = manifest.optString("short_name").ifBlank { manifest.optString("name") }.ifBlank { name }
                manifest.optString("start_url").takeIf { it.isNotBlank() }?.let { start ->
                    val resolved = URL(URL(manifestUrl), start).toString()
                    if (Links.origin(resolved) == Links.origin(pageUrl)) startUrl = resolved
                }
                background = parseColor(manifest.optString("background_color"))
                    ?: parseColor(manifest.optString("theme_color"))
                candidates += candidates(manifest.optJSONArray("icons"), manifestUrl)
            } catch (error: Exception) {
                Log.w(TAG, "cannot read web app manifest $manifestUrl: $error")
            }
        }
        candidates += candidates(page.optJSONArray("icons"), pageUrl)
        val icon = candidates
            .sortedWith(compareByDescending<Candidate> { it.maskable && it.size >= 144 }.thenByDescending { it.size })
            .firstNotNullOfOrNull { candidate ->
                val bytes = client.fetchBytes(candidate.src) ?: return@firstNotNullOfOrNull null
                BitmapFactory.decodeByteArray(bytes, 0, bytes.size)?.takeIf { it.width >= 32 }?.let { bitmap ->
                    if (candidate.maskable) fill(bitmap) else framed(bitmap, background ?: Color.WHITE)
                }
            }
            ?: favicon?.takeIf { it.width >= 32 }?.let { framed(it, background ?: Color.WHITE) }
            ?: generated(context, name, pageUrl)
        return Info(name, Links.toDisplay(startUrl), icon)
    }

    private class Candidate(val src: String, val size: Int, val maskable: Boolean)

    private fun candidates(icons: JSONArray?, base: String): List<Candidate> {
        icons ?: return emptyList()
        return (0 until icons.length()).mapNotNull { index ->
            val icon = icons.optJSONObject(index) ?: return@mapNotNull null
            val src = icon.optString("src").takeIf { it.isNotBlank() } ?: return@mapNotNull null
            val resolved = try {
                URL(URL(base), src).toString()
            } catch (error: Exception) {
                return@mapNotNull null
            }
            // SVG icons cannot be decoded to a bitmap here.
            if (resolved.substringBefore('?').endsWith(".svg", ignoreCase = true)) return@mapNotNull null
            val sizes = icon.optString("sizes")
            val size = if (sizes.contains("any")) 512 else {
                sizes.split(' ').mapNotNull { it.substringBefore('x').toIntOrNull() }.maxOrNull() ?: 0
            }
            Candidate(resolved, size, icon.optString("purpose").contains("maskable"))
        }
    }

    private fun parseColor(text: String): Int? = try {
        if (text.isBlank()) null else Color.parseColor(text.trim())
    } catch (error: IllegalArgumentException) {
        null
    }

    /** A maskable icon already has its safe zone; it fills the adaptive icon. */
    private fun fill(icon: Bitmap): Bitmap = Bitmap.createScaledBitmap(icon, ICON_PX, ICON_PX, true)

    /** Places a plain icon inside the adaptive icon's safe zone, on [background]. */
    private fun framed(icon: Bitmap, background: Int): Bitmap {
        val bitmap = Bitmap.createBitmap(ICON_PX, ICON_PX, Bitmap.Config.ARGB_8888)
        val canvas = Canvas(bitmap)
        canvas.drawColor(background)
        val inner = ICON_PX * 0.5f
        val offset = (ICON_PX - inner) / 2
        canvas.drawBitmap(icon, Rect(0, 0, icon.width, icon.height), RectF(offset, offset, offset + inner, offset + inner),
            Paint(Paint.FILTER_BITMAP_FLAG or Paint.ANTI_ALIAS_FLAG))
        return bitmap
    }

    /** Draws an icon with the site's initial, in the colour of its kind of link. */
    private fun generated(context: Context, name: String, url: String): Bitmap {
        val kind = Links.parse(url)?.kind
        val color = ContextCompat.getColor(context, if (kind == Links.Kind.PKARR) R.color.pkarr_badge else R.color.blake3_badge)
        val bitmap = Bitmap.createBitmap(ICON_PX, ICON_PX, Bitmap.Config.ARGB_8888)
        val canvas = Canvas(bitmap)
        canvas.drawColor(color)
        val letter = name.trim().firstOrNull { it.isLetterOrDigit() }?.uppercase() ?: "?"
        val paint = Paint(Paint.ANTI_ALIAS_FLAG).apply {
            this.color = Color.WHITE
            textSize = ICON_PX * 0.42f
            typeface = Typeface.create(Typeface.DEFAULT, Typeface.BOLD)
            textAlign = Paint.Align.CENTER
        }
        val baseline = ICON_PX / 2f - (paint.descent() + paint.ascent()) / 2
        canvas.drawText(letter, ICON_PX / 2f, baseline, paint)
        return bitmap
    }

    /** Lets the user edit the name, then asks the launcher to pin the shortcut. */
    private fun confirm(activity: Activity, info: Info) {
        val view = LayoutInflater.from(activity).inflate(R.layout.dialog_pin, null)
        view.findViewById<ImageView>(R.id.icon).setImageBitmap(info.icon)
        val nameField = view.findViewById<EditText>(R.id.name).apply {
            setText(info.name)
            setSelection(text.length)
        }
        MaterialAlertDialogBuilder(activity)
            .setTitle(R.string.add_to_home_screen)
            .setView(view)
            .setPositiveButton(R.string.add) { _, _ ->
                val name = nameField.text.toString().trim().ifBlank { info.name }
                requestPin(activity, info, name)
            }
            .setNegativeButton(R.string.cancel, null)
            .show()
    }

    private fun requestPin(activity: Activity, info: Info, name: String) {
        val digest = MessageDigest.getInstance("SHA-256").digest(info.startUrl.toByteArray())
        val id = "webapp-" + digest.take(12).joinToString("") { "%02x".format(it) }
        try {
            iconFile(activity, id).apply { parentFile?.mkdirs() }.outputStream().use {
                info.icon.compress(Bitmap.CompressFormat.PNG, 100, it)
            }
        } catch (error: Exception) {
            Log.w(TAG, "cannot store icon for $id: $error")
        }
        val intent = Intent(Intent.ACTION_VIEW, Uri.parse(info.startUrl), activity, WebAppActivity::class.java)
            .putExtra(WebAppActivity.EXTRA_NAME, name)
            .putExtra(WebAppActivity.EXTRA_ID, id)
        val shortcut = ShortcutInfoCompat.Builder(activity, id)
            .setShortLabel(name)
            .setLongLabel(name)
            .setIcon(IconCompat.createWithAdaptiveBitmap(info.icon))
            .setIntent(intent)
            .build()
        ShortcutManagerCompat.requestPinShortcut(activity, shortcut, null)
    }

    /** Returns where the icon of web app [id] is stored, for its task in recents. */
    fun iconFile(context: Context, id: String) = File(context.filesDir, "webapps/$id.png")
}
