package computer.iroh.link

import android.content.Context
import android.graphics.Canvas
import android.graphics.Paint
import android.graphics.RectF
import android.graphics.Typeface
import android.text.SpannableStringBuilder
import android.text.Spanned
import android.text.style.ForegroundColorSpan
import android.text.style.ReplacementSpan
import androidx.core.content.ContextCompat
import com.google.android.material.color.MaterialColors

/**
 * Formats URLs for the address bar and menus.
 *
 * A content link shows as a pill, `b3` for a BLAKE3 hash or `pk` for a Pkarr
 * key, with the first and last three characters of the hash or key, followed
 * by the path. Other URLs show their host and path.
 */
object AddressFormat {
    /** Characters kept from each end of a hash or key. */
    private const val ENDS = 3

    /** Returns the formatted [url], a public or gateway URL. */
    fun format(context: Context, url: String): CharSequence {
        val parsed = Links.parse(url) ?: return url
        val text = SpannableStringBuilder()
        val strong = MaterialColors.getColor(context, com.google.android.material.R.attr.colorOnSurface, 0)
        val weak = MaterialColors.getColor(context, com.google.android.material.R.attr.colorOnSurfaceVariant, 0)
        if (parsed.kind != null) {
            text.append(" ", pill(context, parsed.kind, parsed.label), Spanned.SPAN_EXCLUSIVE_EXCLUSIVE)
            val path = parsed.path.takeUnless { it == "/" || it.isEmpty() }.orEmpty()
            // The pill ends in a small gap, so the path follows without a space.
            if (path.isNotEmpty()) text.append(decode(path), ForegroundColorSpan(strong), 0)
        } else {
            text.append(parsed.host, ForegroundColorSpan(strong), 0)
            val path = parsed.path.takeUnless { it == "/" }.orEmpty()
            text.append(decode(path), ForegroundColorSpan(weak), 0)
        }
        if (parsed.query.isNotEmpty()) text.append(decode(parsed.query), ForegroundColorSpan(weak), 0)
        return text
    }

    /** Shortens a hash or key to its first and last characters. */
    fun shorten(label: String): String =
        if (label.length <= ENDS * 2 + 1) label else label.take(ENDS) + "…" + label.takeLast(ENDS)

    private fun pill(context: Context, kind: Links.Kind, label: String): PillSpan {
        val (badge, badgeText, body, bodyText) = when (kind) {
            Links.Kind.BLAKE3 -> listOf(R.color.blake3_badge, R.color.blake3_badge_text, R.color.blake3_body, R.color.blake3_body_text)
            Links.Kind.PKARR -> listOf(R.color.pkarr_badge, R.color.pkarr_badge_text, R.color.pkarr_body, R.color.pkarr_body_text)
        }.map { ContextCompat.getColor(context, it) }
        val tag = if (kind == Links.Kind.BLAKE3) "b3" else "pk"
        return PillSpan(tag, shorten(label), badge, badgeText, body, bodyText, context.resources.displayMetrics.density)
    }

    /** Shows percent-encoded paths readably, keeping them unchanged if they do not decode. */
    private fun decode(text: String): String = try {
        java.net.URLDecoder.decode(text.replace("+", "%2B"), "UTF-8")
    } catch (error: IllegalArgumentException) {
        text
    }

    /** Draws a two-part pill: a coloured badge, then the shortened hash or key on a lighter tint. */
    private class PillSpan(
        private val tag: String,
        private val label: String,
        private val badge: Int,
        private val badgeText: Int,
        private val body: Int,
        private val bodyText: Int,
        density: Float,
    ) : ReplacementSpan() {
        private val padding = 7 * density
        private val gap = 6 * density
        private val tagPaint = Paint(Paint.ANTI_ALIAS_FLAG)
        private val labelPaint = Paint(Paint.ANTI_ALIAS_FLAG)
        private val fill = Paint(Paint.ANTI_ALIAS_FLAG)

        private fun prepare(paint: Paint) {
            tagPaint.set(paint)
            tagPaint.typeface = Typeface.create(Typeface.DEFAULT, Typeface.BOLD)
            tagPaint.textSize = paint.textSize * 0.82f
            labelPaint.set(paint)
            labelPaint.typeface = Typeface.MONOSPACE
            labelPaint.textSize = paint.textSize * 0.9f
        }

        override fun getSize(paint: Paint, text: CharSequence?, start: Int, end: Int, fm: Paint.FontMetricsInt?): Int {
            prepare(paint)
            fm?.let { paint.getFontMetricsInt(it) }
            return (padding * 4 + tagPaint.measureText(tag) + labelPaint.measureText(label) + gap).toInt()
        }

        override fun draw(
            canvas: Canvas, text: CharSequence?, start: Int, end: Int,
            x: Float, top: Int, y: Int, bottom: Int, paint: Paint,
        ) {
            prepare(paint)
            val metrics = paint.fontMetrics
            val height = (metrics.descent - metrics.ascent) * 1.05f
            val center = y + (metrics.ascent + metrics.descent) / 2
            val rectTop = center - height / 2
            val rectBottom = center + height / 2
            val radius = height / 2
            val tagWidth = padding * 2 + tagPaint.measureText(tag)
            val total = tagWidth + padding * 2 + labelPaint.measureText(label)
            val pill = RectF(x, rectTop, x + total, rectBottom)
            // The tinted body under the whole pill, then the badge over its left end.
            fill.color = body
            canvas.drawRoundRect(pill, radius, radius, fill)
            fill.color = badge
            canvas.save()
            canvas.clipRect(x, rectTop, x + tagWidth, rectBottom)
            canvas.drawRoundRect(pill, radius, radius, fill)
            canvas.restore()
            val baseline = center - (tagPaint.fontMetrics.ascent + tagPaint.fontMetrics.descent) / 2
            tagPaint.color = badgeText
            canvas.drawText(tag, x + padding, baseline, tagPaint)
            val labelBaseline = center - (labelPaint.fontMetrics.ascent + labelPaint.fontMetrics.descent) / 2
            labelPaint.color = bodyText
            canvas.drawText(label, x + tagWidth + padding, labelBaseline, labelPaint)
        }
    }
}
