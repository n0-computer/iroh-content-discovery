//! Failures as pages for browsers, which try again by themselves when the
//! failure is likely to pass.

use std::{borrow::Cow, time::Duration};

use axum::{
    body::Body,
    extract::Request,
    http::{HeaderValue, StatusCode, header},
    middleware::Next,
    response::Response,
};

use crate::html_escape;

/// When to try again after a failure that should pass within seconds.
pub(crate) const RETRY_SOON: Duration = Duration::from_secs(10);

/// Lets scripts and styles inline in the page run, and nothing else.
///
/// Error pages come from the gateway, not from content, so they do not need
/// the sandbox of path routes, which would stop the refresh.
const CSP: HeaderValue = HeaderValue::from_static(
    "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'",
);

/// A failure as `HttpError` describes it, kept for [`html`].
#[derive(Debug, Clone)]
pub(crate) struct Failure {
    pub(crate) message: Cow<'static, str>,
    pub(crate) retry: Option<Duration>,
}

/// Turns a failure into a page for requests that accept HTML.
///
/// Other clients, such as media elements and `curl`, keep the plain text
/// reason.
pub(crate) async fn html(request: Request, next: Next) -> Response {
    let wants_html = request
        .headers()
        .get(header::ACCEPT)
        .and_then(|accept| accept.to_str().ok())
        .is_some_and(|accept| accept.contains("text/html"));
    let mut response = next.run(request).await;
    if !wants_html {
        return response;
    }
    let Some(failure) = response.extensions_mut().remove::<Failure>() else {
        return response;
    };
    let page = render(response.status(), &failure);
    let headers = response.headers_mut();
    headers.remove(header::CONTENT_LENGTH);
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    headers.insert(header::CONTENT_SECURITY_POLICY, CSP);
    *response.body_mut() = Body::from(page);
    response
}

fn render(status: StatusCode, failure: &Failure) -> String {
    let title = format!(
        "{} {}",
        status.as_str(),
        status.canonical_reason().unwrap_or("")
    );
    let mut message = html_escape(&failure.message);
    if let Some(first) = message.get_mut(..1) {
        first.make_ascii_uppercase();
    }
    // The meta refresh works without the script, which only counts down.
    let (refresh, countdown) = match failure.retry.map(|retry| retry.as_secs()) {
        Some(seconds) => (
            format!(r#"<meta http-equiv="refresh" content="{seconds}">"#),
            format!(
                r#"<p>Trying again in <span id="left">{seconds}</span>&nbsp;s. <a href="">Try now</a></p>
<script>
let left = {seconds};
setInterval(() => {{ if (left > 0) document.getElementById("left").textContent = --left; }}, 1000);
</script>"#
            ),
        ),
        None => (String::new(), String::new()),
    };
    format!(
        r#"<!doctype html>
<html lang="en">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
{refresh}
<title>{title}</title>
<style>
body {{ font: 16px/1.5 system-ui, sans-serif; max-width: 40rem; margin: 4rem auto; padding: 0 1rem; color-scheme: light dark; }}
h1 {{ font-size: 1.25rem; font-weight: 600; }}
.status {{ color: GrayText; }}
</style>
<h1>{message}</h1>
{countdown}
<p class="status">iroh-link gateway: {title}</p>
</html>
"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_failures_refresh() {
        let failure = Failure {
            message: "no <packet>".into(),
            retry: Some(Duration::from_secs(5)),
        };
        let page = render(StatusCode::SERVICE_UNAVAILABLE, &failure);
        assert!(page.contains(r#"<meta http-equiv="refresh" content="5">"#));
        assert!(page.contains("<h1>No &lt;packet&gt;</h1>"));
        let failure = Failure {
            retry: None,
            ..failure
        };
        assert!(!render(StatusCode::UNPROCESSABLE_ENTITY, &failure).contains("refresh"));
    }
}
