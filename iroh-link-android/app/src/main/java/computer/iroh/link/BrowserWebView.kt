package computer.iroh.link

import android.annotation.SuppressLint
import android.content.Context
import android.view.MotionEvent
import android.webkit.WebView

/**
 * A WebView that reports when the page is pulled down past its top.
 *
 * Pull to refresh starts only then, as in Chrome: the page's root scroller
 * has to overscroll at the top. A page that scrolls an inner element, or
 * handles drags itself, does not trigger a reload by being swiped down.
 */
@SuppressLint("ViewConstructor")
class BrowserWebView(context: Context) : WebView(context) {
    /** Whether the current gesture has pulled the page down past its top. */
    private var pulledPastTop = false
    private var dragging = false

    /**
     * Returns whether a pull-to-refresh layout must leave the gesture to the page.
     *
     * The layout asks on touch down, before this view sees the event, and
     * again on each move. On touch down, a page at its top may start a pull;
     * during the drag, the pull starts once the page overscrolls.
     */
    fun keepsGesture(): Boolean = if (dragging) !pulledPastTop else scrollY > 0

    override fun dispatchTouchEvent(event: MotionEvent): Boolean {
        when (event.actionMasked) {
            MotionEvent.ACTION_DOWN -> {
                // Each gesture has to overscroll again.
                pulledPastTop = false
                dragging = true
            }
            MotionEvent.ACTION_UP, MotionEvent.ACTION_CANCEL -> dragging = false
        }
        return super.dispatchTouchEvent(event)
    }

    override fun onOverScrolled(scrollX: Int, scrollY: Int, clampedX: Boolean, clampedY: Boolean) {
        if (clampedY && scrollY <= 0) pulledPastTop = true
        super.onOverScrolled(scrollX, scrollY, clampedX, clampedY)
    }

    override fun onScrollChanged(left: Int, top: Int, oldLeft: Int, oldTop: Int) {
        if (top > 0) pulledPastTop = false
        super.onScrollChanged(left, top, oldLeft, oldTop)
    }
}
