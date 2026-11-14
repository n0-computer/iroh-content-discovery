package computer.iroh.link

import android.util.Log
import android.webkit.WebResourceResponse
import org.json.JSONObject
import java.io.ByteArrayInputStream
import java.io.EOFException
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.io.InputStream
import java.net.HttpURLConnection
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Proxy
import java.net.URL
import java.security.MessageDigest

/**
 * Loads gateway URLs for the WebView, with a disk cache and resumable transfers.
 *
 * The WebView's own HTTP cache is about 20 MB and cannot be changed, so large
 * files like WebAssembly modules are fetched again on every visit. This cache
 * follows the gateway's headers instead:
 *
 * - `immutable` responses (`blake3.net`) are served from disk without asking.
 * - Other responses (`pkarr.net`) are revalidated with `If-None-Match`, so an
 *   unchanged file costs a `304`. If the gateway cannot reach the content, the
 *   cached copy is served.
 *
 * The gateway's ETag is the BLAKE3 hash of the body, so bodies are stored once
 * per hash. If a transfer breaks, it resumes with `Range` and `If-Range` from
 * the byte where it stopped, so the page never sees a truncated file.
 *
 * Range requests, as media elements send them, and other methods than `GET`
 * are left to the WebView.
 */
class GatewayClient(dir: File) {
    private val metaDir = File(dir, "meta").apply { mkdirs() }
    private val blobDir = File(dir, "blobs").apply { mkdirs() }
    private val tempDir = File(dir, "tmp").apply {
        mkdirs()
        listFiles()?.forEach { it.delete() }
    }

    /** A cached response for one URL. */
    private data class Entry(val etag: String, val headers: Map<String, String>, val blob: File) {
        val immutable get() = headers.entries.any { (name, value) ->
            name.equals("Cache-Control", ignoreCase = true) && value.contains("immutable")
        }
    }

    /**
     * Returns the response for gateway URL [url], or `null` to let the WebView load it itself.
     *
     * With [followRedirects], redirects are followed here. Otherwise the
     * WebView loads redirected URLs itself, since a [WebResourceResponse]
     * cannot be a redirect.
     */
    fun load(url: String, method: String, headers: Map<String, String>, followRedirects: Boolean): WebResourceResponse? {
        val ranged = headers.keys.any { it.equals("Range", ignoreCase = true) }
        if (method != "GET" || ranged) return if (followRedirects) passThrough(url, method, headers) else null
        val requestHeaders = headers.filterKeys { name ->
            !name.equals("Host", true) && !name.startsWith("If-", true) && !name.equals("Accept-Encoding", true)
        }
        val cached = lookup(url)
        if (cached != null && cached.immutable) return fromCache(cached)
        var target = url
        var retried = false
        var redirects = 0
        while (true) {
            val connection: HttpURLConnection
            val status: Int
            try {
                val conditional = if (target == url) cached?.etag else null
                connection = open(target, "GET", requestHeaders, conditional?.let { mapOf("If-None-Match" to it) })
                status = connection.responseCode
            } catch (error: IOException) {
                if (!retried) {
                    Log.w(TAG, "request for $target failed, retrying: $error")
                    retried = true
                    Thread.sleep(RETRY_DELAY_MS)
                    continue
                }
                Log.w(TAG, "request for $target failed: $error")
                return cached?.let(::fromCache)
            }
            when {
                status == 304 && cached != null -> {
                    connection.disconnect()
                    return fromCache(cached)
                }
                status in 300..399 -> {
                    val location = connection.getHeaderField("Location")
                    connection.disconnect()
                    if (!followRedirects || location == null || ++redirects > MAX_REDIRECTS) return null
                    val next = URL(URL(target), location).toString()
                    target = Links.toInternal(next) ?: next
                    if (!Links.isInternal(target)) return passThrough(target, "GET", requestHeaders)
                }
                (status == 502 || status == 504) && !retried -> {
                    Log.w(TAG, "gateway answered $status for $target, retrying")
                    connection.disconnect()
                    retried = true
                    Thread.sleep(RETRY_DELAY_MS)
                }
                status >= 400 -> {
                    val body = connection.errorStream?.use { it.readBytes() } ?: ByteArray(0)
                    val unreachable = status >= 500 || String(body).contains("no verified provider")
                    if (unreachable && cached != null) {
                        Log.w(TAG, "gateway answered $status for $target; serving the cached copy")
                        connection.disconnect()
                        return fromCache(cached)
                    }
                    return response(connection, status, ByteArrayInputStream(body))
                }
                else -> {
                    val etag = connection.getHeaderField("ETag")?.takeIf { blobName(it) != null }
                    val length = connection.getHeaderField("Content-Length")?.toLongOrNull()
                    if (status != 200 || etag == null || target != url) {
                        return response(connection, status, connection.inputStream)
                    }
                    val body = Body(connection, url, requestHeaders, etag, length)
                    return response(connection, status, body)
                }
            }
        }
    }

    /** Loads a URL without caching, following redirects. */
    private fun passThrough(url: String, method: String, headers: Map<String, String>): WebResourceResponse? {
        var target = url
        repeat(MAX_REDIRECTS) {
            val connection = try {
                open(target, method, headers, null)
            } catch (error: IOException) {
                Log.w(TAG, "request for $target failed: $error")
                return null
            }
            try {
                val status = connection.responseCode
                if (status in 300..399) {
                    val location = connection.getHeaderField("Location") ?: return null
                    connection.disconnect()
                    val next = URL(URL(target), location).toString()
                    target = Links.toInternal(next) ?: next
                    return@repeat
                }
                val body = (if (status >= 400) connection.errorStream else connection.inputStream)
                    ?: ByteArrayInputStream(ByteArray(0))
                return response(connection, status, body)
            } catch (error: IOException) {
                Log.w(TAG, "request for $target failed: $error")
                connection.disconnect()
                return null
            }
        }
        return null
    }

    private fun open(url: String, method: String, headers: Map<String, String>, extra: Map<String, String>?): HttpURLConnection {
        // The gateway is an HTTP proxy for `*.localhost`, which the system
        // resolver does not know.
        val proxy = if (Links.isInternal(url)) GATEWAY else Proxy.NO_PROXY
        return (URL(url).openConnection(proxy) as HttpURLConnection).apply {
            requestMethod = method
            instanceFollowRedirects = false
            connectTimeout = 10_000
            readTimeout = 60_000
            for ((name, value) in headers) setRequestProperty(name, value)
            extra?.forEach { (name, value) -> setRequestProperty(name, value) }
            // Keep the body and its Content-Length as the gateway sent them.
            setRequestProperty("Accept-Encoding", "identity")
            connect()
        }
    }

    private fun headersOf(connection: HttpURLConnection): Map<String, String> =
        connection.headerFields
            .filterKeys { it != null }
            .mapValues { (_, values) -> values.joinToString(", ") }

    private fun response(connection: HttpURLConnection, status: Int, body: InputStream): WebResourceResponse {
        val headers = headersOf(connection)
        val reason = connection.responseMessage?.takeIf { it.isNotBlank() } ?: "OK"
        return webResponse(headers, status, reason, body)
    }

    private fun webResponse(headers: Map<String, String>, status: Int, reason: String, body: InputStream): WebResourceResponse {
        val contentType = headers.entries.firstOrNull { it.key.equals("Content-Type", true) }?.value
            ?: "application/octet-stream"
        val mime = contentType.substringBefore(';').trim()
        val charset = Regex("charset=\"?([^;\"]+)", RegexOption.IGNORE_CASE).find(contentType)?.groupValues?.get(1)
        // The WebView sets Content-Type from the mime type and charset. Passing
        // the header too repeats it, as "application/wasm, application/wasm",
        // which WebAssembly.instantiateStreaming rejects.
        val rest = headers.filterKeys { !it.equals("Content-Type", ignoreCase = true) }
        return WebResourceResponse(mime, charset, status, reason, rest, body)
    }

    private fun fromCache(entry: Entry): WebResourceResponse {
        Log.d(TAG, "serving ${entry.blob.name} (${entry.blob.length()} bytes) from the cache")
        entry.blob.setLastModified(System.currentTimeMillis())
        val headers = entry.headers.filterKeys { !it.equals("Content-Length", true) } +
            ("Content-Length" to entry.blob.length().toString())
        return webResponse(headers, 200, "OK", FileInputStream(entry.blob))
    }

    // Storage

    private fun metaFile(url: String): File {
        val digest = MessageDigest.getInstance("SHA-256").digest(url.toByteArray())
        return File(metaDir, digest.joinToString("") { "%02x".format(it) })
    }

    private fun lookup(url: String): Entry? {
        val file = metaFile(url)
        if (!file.exists()) return null
        return try {
            val json = JSONObject(file.readText())
            val etag = json.getString("etag")
            val blob = File(blobDir, blobName(etag) ?: return null)
            if (!blob.exists()) {
                file.delete()
                return null
            }
            val headers = json.getJSONObject("headers").let { object_ ->
                object_.keys().asSequence().associateWith { object_.getString(it) }
            }
            Entry(etag, headers, blob)
        } catch (error: Exception) {
            file.delete()
            null
        }
    }

    private fun store(url: String, etag: String, headers: Map<String, String>, temp: File) {
        val name = blobName(etag) ?: return
        val blob = File(blobDir, name)
        if (blob.exists()) {
            temp.delete()
        } else if (!temp.renameTo(blob)) {
            temp.delete()
            return
        }
        blob.setLastModified(System.currentTimeMillis())
        val kept = headers.filterKeys { name ->
            DROPPED_HEADERS.none { it.equals(name, ignoreCase = true) }
        }
        val json = JSONObject().put("etag", etag).put("headers", JSONObject(kept))
        val meta = metaFile(url)
        val metaTemp = File(tempDir, meta.name + ".meta")
        metaTemp.writeText(json.toString())
        metaTemp.renameTo(meta)
        Log.d(TAG, "cached $url as $name (${blob.length()} bytes)")
        trim()
    }

    /** Deletes the least recently used bodies once the cache is over [MAX_BYTES]. */
    @Synchronized
    private fun trim() {
        val blobs = blobDir.listFiles()?.toMutableList() ?: return
        var total = blobs.sumOf { it.length() }
        if (total <= MAX_BYTES) return
        blobs.sortBy { it.lastModified() }
        for (blob in blobs) {
            if (total <= MAX_BYTES * 8 / 10) break
            total -= blob.length()
            blob.delete()
        }
        Log.i(TAG, "trimmed the content cache to $total bytes")
    }

    /**
     * The body of a cacheable response.
     *
     * Copies what the WebView reads into a temporary file and stores it once
     * complete. Resumes the transfer if it breaks.
     */
    private inner class Body(
        private var connection: HttpURLConnection,
        private val url: String,
        private val headers: Map<String, String>,
        private val etag: String,
        private val length: Long?,
    ) : InputStream() {
        private val responseHeaders = headersOf(connection)
        private var input: InputStream = connection.inputStream
        private var offset = 0L
        private var resumes = 0
        private var finished = false
        private val temp = File.createTempFile("body", null, tempDir)
        private var copy: FileOutputStream? = try {
            FileOutputStream(temp)
        } catch (error: IOException) {
            null
        }

        override fun read(): Int {
            val byte = ByteArray(1)
            return if (read(byte, 0, 1) < 0) -1 else byte[0].toInt() and 0xff
        }

        override fun read(buffer: ByteArray, off: Int, len: Int): Int {
            while (true) {
                val count = try {
                    input.read(buffer, off, len)
                } catch (error: IOException) {
                    if (resume(error)) continue
                    throw error
                }
                if (count < 0) {
                    if (length != null && offset < length) {
                        if (resume(EOFException("ended at $offset of $length bytes"))) continue
                        throw EOFException("transfer of $url ended at $offset of $length bytes")
                    }
                    finish()
                    return -1
                }
                copy?.let { file ->
                    try {
                        file.write(buffer, off, count)
                    } catch (error: IOException) {
                        // Caching is best effort; keep serving the page.
                        file.close()
                        copy = null
                    }
                }
                offset += count
                return count
            }
        }

        /**
         * Continues the transfer at [offset] on a new request.
         *
         * Keeps trying for [RESUME_WINDOW_MS], since the gateway may need to find
         * the provider again, for example after the network changed.
         */
        private fun resume(error: Exception): Boolean {
            if (length == null || resumes >= MAX_RESUMES) return false
            resumes++
            Log.w(TAG, "transfer of $url broke at $offset of $length bytes, resuming: $error")
            connection.disconnect()
            val deadline = System.currentTimeMillis() + RESUME_WINDOW_MS
            var delay = RETRY_DELAY_MS
            while (System.currentTimeMillis() < deadline) {
                Thread.sleep(delay)
                delay = minOf(delay * 2, MAX_RETRY_DELAY_MS)
                val next = try {
                    open(url, "GET", headers, mapOf("Range" to "bytes=$offset-", "If-Range" to etag))
                } catch (resumeError: IOException) {
                    Log.w(TAG, "cannot resume $url yet: $resumeError")
                    continue
                }
                val status = try {
                    next.responseCode
                } catch (resumeError: IOException) {
                    Log.w(TAG, "cannot resume $url yet: $resumeError")
                    next.disconnect()
                    continue
                }
                val range = next.getHeaderField("Content-Range")
                if (status == 206 && range?.startsWith("bytes $offset-") == true) {
                    Log.i(TAG, "resumed $url at $offset bytes")
                    connection = next
                    input = next.inputStream
                    return true
                }
                next.disconnect()
                // A full response means the content changed; the rest would not fit.
                if (status == 200 || status == 416) {
                    Log.w(TAG, "cannot resume $url: $status")
                    return false
                }
                Log.w(TAG, "cannot resume $url yet: $status")
            }
            return false
        }

        private fun finish() {
            if (finished) return
            finished = true
            val file = copy
            copy = null
            if (file == null) {
                temp.delete()
                return
            }
            try {
                file.close()
                if (length == null || temp.length() == length) {
                    store(url, etag, responseHeaders, temp)
                } else {
                    temp.delete()
                }
            } catch (error: IOException) {
                Log.w(TAG, "cannot cache $url: $error")
                temp.delete()
            }
        }

        override fun close() {
            if (!finished) {
                finished = true
                try {
                    copy?.close()
                } catch (_: IOException) {
                }
                copy = null
                temp.delete()
            }
            try {
                input.close()
            } catch (_: IOException) {
            }
            connection.disconnect()
        }
    }

    private companion object {
        const val TAG = "iroh-link"
        /** Upper bound for cached bodies; the least recently used go first. */
        const val MAX_BYTES = 512L * 1024 * 1024
        const val MAX_REDIRECTS = 5
        const val MAX_RESUMES = 10
        /** How long one resume keeps trying. */
        const val RESUME_WINDOW_MS = 90_000L
        const val RETRY_DELAY_MS = 1000L
        const val MAX_RETRY_DELAY_MS = 5000L
        val GATEWAY = Proxy(Proxy.Type.HTTP, InetSocketAddress(InetAddress.getByAddress(byteArrayOf(127, 0, 0, 1)), Links.PORT))
        /** Per-response headers that a cached copy must not repeat. */
        val DROPPED_HEADERS = listOf("Date", "Content-Length", "Connection", "Keep-Alive", "Transfer-Encoding")
        /** A strong ETag; the gateway's are the z-base-32 BLAKE3 hash. */
        private val ETAG = Regex("^\"([A-Za-z0-9]{1,128})\"$")

        /** Returns the file name for a strong ETag of the gateway, or `null` for other ETags. */
        fun blobName(etag: String): String? = ETAG.matchEntire(etag.trim())?.groupValues?.get(1)
    }
}
