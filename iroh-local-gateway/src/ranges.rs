//! Single HTTP byte ranges, normalized to an exclusive end.

use std::ops::Range;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Selection {
    Full,
    Partial(Vec<Range<u64>>),
    Unsatisfiable,
}

/// Ignore unknown units or malformed ranges. Bound multipart work to 16 parts;
/// oversized range sets may be ignored under HTTP range semantics.
pub(crate) fn select(value: Option<&str>, size: u64) -> Selection {
    let Some(value) = value.and_then(|s| s.strip_prefix("bytes=")) else {
        return Selection::Full;
    };
    let mut ranges = Vec::new();
    for (index, part) in value.split(',').enumerate() {
        if index >= 16 {
            return Selection::Full;
        }
        match single(part.trim(), size) {
            Selection::Full => return Selection::Full,
            Selection::Partial(parts) => ranges.extend(parts),
            Selection::Unsatisfiable => {}
        }
    }
    if ranges.is_empty() {
        Selection::Unsatisfiable
    } else {
        Selection::Partial(ranges)
    }
}

// This is deliberately a vector of ranges, not a vector of byte offsets.
#[allow(clippy::single_range_in_vec_init)]
fn single(value: &str, size: u64) -> Selection {
    let Some((start, end)) = value.split_once('-') else {
        return Selection::Full;
    };
    let number = |s: &str| {
        if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
            None
        } else {
            s.parse::<u64>().ok()
        }
    };
    if start.is_empty() {
        let Some(suffix) = number(end) else {
            return Selection::Full;
        };
        if suffix == 0 || size == 0 {
            return Selection::Unsatisfiable;
        }
        return Selection::Partial(vec![size.saturating_sub(suffix)..size]);
    }
    let Some(start) = number(start) else {
        return Selection::Full;
    };
    let end = if end.is_empty() {
        size
    } else {
        let Some(last) = number(end) else {
            return Selection::Full;
        };
        if last < start {
            return Selection::Full;
        }
        last.saturating_add(1).min(size)
    };
    if start >= size {
        return Selection::Unsatisfiable;
    }
    Selection::Partial(vec![start..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn video_ranges_and_boundaries() {
        assert_eq!(select(None, 100), Selection::Full);
        for (header, expected) in [
            ("bytes=0-0", 0..1),
            ("bytes=1-9", 1..10),
            ("bytes=90-", 90..100),
            ("bytes=-10", 90..100),
            ("bytes=-200", 0..100),
            ("bytes=90-200", 90..100),
            ("bytes=0-18446744073709551615", 0..100),
        ] {
            assert_eq!(
                select(Some(header), 100),
                Selection::Partial(vec![expected]),
                "{header}"
            );
        }
        assert_eq!(
            select(Some("bytes=0-1, 5-6,999-"), 100),
            Selection::Partial(vec![0..2, 5..7])
        );
        for header in ["bytes=100-", "bytes=100-101", "bytes=-0"] {
            assert_eq!(select(Some(header), 100), Selection::Unsatisfiable);
        }
        for header in ["bytes=0-0", "bytes=-1"] {
            assert_eq!(select(Some(header), 0), Selection::Unsatisfiable);
        }
        for header in [
            "bytes=9-1",
            "items=0-1",
            "bytes=a-b",
            "bytes=-",
            "bytes=+1-2",
            "bytes=18446744073709551616-",
        ] {
            assert_eq!(select(Some(header), 100), Selection::Full, "{header}");
        }
    }
}
