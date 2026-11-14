package computer.iroh.link

/**
 * Maps between public content links and the in-app gateway.
 *
 * `https://<label>.blake3.net/path` is loaded from
 * `http://<label>.blake3.localhost:<port>/path`, and the same for `pkarr.net`,
 * like the desktop browser extension does. Every hash and key keeps its own
 * origin, so local storage and root-relative links work per site. The app
 * shows, stores and shares the public form.
 */
object Links {
    /** Loopback port of the in-app gateway; part of every page origin, so it must not change. */
    const val PORT = 45475

    private val URL = Regex("^([a-zA-Z][a-zA-Z0-9+.-]*)://([^/?#:]*)(:\\d+)?(.*)$", RegexOption.DOT_MATCHES_ALL)
    private val PUBLIC_HOST = Regex("^([a-z0-9]+)\\.(blake3|pkarr)\\.net$")
    private val LOCAL_HOST = Regex("^([a-z0-9]+)\\.(blake3|pkarr)\\.localhost$")

    private data class Parts(val scheme: String, val host: String, val port: String?, val rest: String)

    private fun split(url: String): Parts? {
        val match = URL.matchEntire(url.trim()) ?: return null
        val (scheme, host, port, rest) = match.destructured
        return Parts(scheme.lowercase(), host.lowercase(), port.ifEmpty { null }, rest)
    }

    /** Returns whether [url] is an http(s) link under a `blake3.net` or `pkarr.net` subdomain. */
    fun isPublic(url: String): Boolean = toInternal(url) != null

    /** Returns the gateway URL for a public content link, or `null` for any other URL. */
    fun toInternal(url: String): String? {
        val parts = split(url) ?: return null
        if (parts.scheme != "https" && parts.scheme != "http") return null
        val match = PUBLIC_HOST.matchEntire(parts.host) ?: return null
        val (label, kind) = match.destructured
        return "http://$label.$kind.localhost:$PORT${parts.rest.ifEmpty { "/" }}"
    }

    /** Returns whether [url] is a gateway URL on a `*.blake3.localhost` or `*.pkarr.localhost` origin. */
    fun isInternal(url: String): Boolean = toDisplay(url) != url

    /** Returns the public form of a gateway URL, or [url] unchanged for any other URL. */
    fun toDisplay(url: String): String {
        val parts = split(url) ?: return url
        if (parts.scheme != "http" || parts.port != ":$PORT") return url
        val match = LOCAL_HOST.matchEntire(parts.host) ?: return url
        val (label, kind) = match.destructured
        return "https://$label.$kind.net${parts.rest.ifEmpty { "/" }}"
    }

    /**
     * Returns a gateway URL on `127.0.0.1` that does not need a `*.localhost` subdomain.
     *
     * Other apps, such as the system download manager, resolve names through
     * the system resolver, which does not know `*.localhost`. The gateway serves
     * the same content under `/blake3/<hash>` and `/pkarr/<key>`. Other URLs are
     * returned unchanged.
     */
    fun toPathUrl(url: String): String {
        val parts = split(url) ?: return url
        if (parts.scheme != "http" || parts.port != ":$PORT") return url
        val match = LOCAL_HOST.matchEntire(parts.host) ?: return url
        val (label, kind) = match.destructured
        // The same mapping as the gateway's own subdomain rewrite.
        val rest = parts.rest.substringBefore('#').removePrefix("/")
        val separator = if (rest.isEmpty() || rest.startsWith("?")) "" else "/"
        return "http://127.0.0.1:$PORT/$kind/$label$separator$rest"
    }

    /**
     * Returns the URL to load for text typed into the address bar, or `null` if it is not a link.
     *
     * Public content links become gateway URLs. Text without a scheme gets `https://`.
     */
    fun fromInput(input: String): String? {
        val text = input.trim()
        if (text.isEmpty() || text.any { it.isWhitespace() }) return null
        val url = when {
            text.contains("://") -> text
            text.startsWith("about:") || text.startsWith("data:") -> return text
            text.contains('.') || text.startsWith("localhost") -> "https://$text"
            else -> return null
        }
        return toInternal(url) ?: url
    }

    /** Finds the first public content link in shared text. */
    fun findInText(text: String): String? =
        Regex("https?://[^\\s<>\"']+").findAll(text).map { it.value }.firstOrNull { isPublic(it) }
}
