use super::*;

#[test]
fn a_conditional_request_selects_by_weak_comparison_over_the_whole_list() {
    let etag = Tagged::new(b"a body".to_vec()).etag;
    let other = Tagged::new(b"another body".to_vec()).etag;
    let header = |s: &str| HeaderValue::from_str(s).unwrap();

    assert!(if_none_match_selects(&header(etag.to_str().unwrap()), &etag), "the tag itself");
    assert!(if_none_match_selects(&header(&format!("W/{}", etag.to_str().unwrap())), &etag), "weakened by a cache");
    assert!(if_none_match_selects(&header("*"), &etag), "* is any current representation");
    assert!(
        if_none_match_selects(&header(&format!("{}, {}", other.to_str().unwrap(), etag.to_str().unwrap())), &etag),
        "one of several stored representations"
    );

    assert!(!if_none_match_selects(&header(other.to_str().unwrap()), &etag), "a different body");
    assert!(!if_none_match_selects(&header(""), &etag), "no tag offered is not a match");
    // Quotes are part of the tag, so a prefix does not match.
    assert!(!if_none_match_selects(&header(etag.to_str().unwrap().trim_end_matches('"')), &etag));
}

/// `as_secs()` truncates, so the header never outlasts the memo.
#[test]
fn a_memo_hit_never_advertises_more_life_than_it_has() {
    let ttl = Duration::from_secs(60);
    for age_ms in [0u64, 1, 999, 1_000, 30_000, 59_001, 59_999] {
        let CachePolicy::Ttl(remaining) = live_cache_policy(ttl, age_ms) else {
            panic!("a live body always declares a TTL");
        };
        assert!(
            remaining.as_secs() * 1000 + age_ms <= crate::convert::millis(ttl),
            "age {age_ms} ms advertised {} s against a {} s window",
            remaining.as_secs(),
            ttl.as_secs()
        );
    }
}
