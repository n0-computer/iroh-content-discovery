package computer.iroh.link

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class LinksTest {
    private val hash = "y7rmokt6h5mryuauw83em4u1br6tqrukaw3ngtde7zp8p3bg6hto"
    private val key = "5ti57aszf7kaicsncb4wgigkf9bju39kofiz8dthwdujkmz85u8y"

    @Test
    fun publicLinksMapToGatewaySubdomains() {
        assertEquals("http://$hash.blake3.localhost:45475/", Links.toInternal("https://$hash.blake3.net/"))
        assertEquals("http://$hash.blake3.localhost:45475/", Links.toInternal("https://$hash.blake3.net"))
        assertEquals(
            "http://$key.pkarr.localhost:45475/a/b.html?x=1#top",
            Links.toInternal("https://$key.pkarr.net/a/b.html?x=1#top"),
        )
        assertEquals("http://$key.pkarr.localhost:45475/", Links.toInternal("HTTP://${key.uppercase()}.PKARR.NET:443/"))
    }

    @Test
    fun otherUrlsAreNotMapped() {
        assertNull(Links.toInternal("https://blake3.net/"))
        assertNull(Links.toInternal("https://a.b.blake3.net/"))
        assertNull(Links.toInternal("https://example.com/"))
        assertNull(Links.toInternal("https://$hash.blake3.net.evil.com/"))
        assertNull(Links.toInternal("ftp://$hash.blake3.net/"))
        assertNull(Links.toInternal("about:blank"))
    }

    @Test
    fun gatewayUrlsDisplayInPublicForm() {
        assertEquals("https://$hash.blake3.net/x?y#z", Links.toDisplay("http://$hash.blake3.localhost:45475/x?y#z"))
        assertEquals("https://$key.pkarr.net/", Links.toDisplay("http://$key.pkarr.localhost:45475"))
        // Other ports are not the in-app gateway.
        assertEquals("http://$key.pkarr.localhost:8080/", Links.toDisplay("http://$key.pkarr.localhost:8080/"))
        assertEquals("https://example.com/", Links.toDisplay("https://example.com/"))
        assertEquals("about:blank", Links.toDisplay("about:blank"))
    }

    @Test
    fun roundTrip() {
        val url = "https://$hash.blake3.net/dir/file.wasm?v=2"
        assertEquals(url, Links.toDisplay(Links.toInternal(url)!!))
    }

    @Test
    fun pathUrlsMatchTheGatewayRewrite() {
        val base = "http://$hash.blake3.localhost:45475"
        assertEquals("http://127.0.0.1:45475/blake3/$hash", Links.toPathUrl("$base/"))
        assertEquals("http://127.0.0.1:45475/blake3/$hash?download", Links.toPathUrl("$base/?download"))
        assertEquals("http://127.0.0.1:45475/blake3/$hash/a/b.zip", Links.toPathUrl("$base/a/b.zip#frag"))
        assertEquals("http://127.0.0.1:45475/pkarr/$key/f", Links.toPathUrl("http://$key.pkarr.localhost:45475/f"))
        assertEquals("https://example.com/f", Links.toPathUrl("https://example.com/f"))
    }

    @Test
    fun addressBarInput() {
        assertEquals("http://$hash.blake3.localhost:45475/", Links.fromInput("  $hash.blake3.net  "))
        assertEquals("http://$key.pkarr.localhost:45475/p", Links.fromInput("https://$key.pkarr.net/p"))
        assertEquals("https://example.com", Links.fromInput("example.com"))
        assertNull(Links.fromInput("two words"))
        assertNull(Links.fromInput("word"))
        assertNull(Links.fromInput(""))
    }

    @Test
    fun findsLinksInSharedText() {
        assertEquals(
            "https://$key.pkarr.net/",
            Links.findInText("look at https://example.com and https://$key.pkarr.net/ now"),
        )
        assertNull(Links.findInText("nothing here https://example.com"))
    }
}
