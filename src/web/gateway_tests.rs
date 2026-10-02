use super::*;

fn host(s: &str) -> HeaderValue {
    HeaderValue::from_str(s).unwrap()
}

#[test]
fn label_comes_off_the_host() {
    assert_eq!(gateway_label("kaspa.name", Some(&host("abc.kaspa.name"))).as_deref(), Some("abc"));
    assert_eq!(gateway_label("kaspa.name", Some(&host("ABC.Kaspa.Name:443"))).as_deref(), Some("abc"), "case and port");
    assert_eq!(gateway_label("kaspa.name", Some(&host("abc.kaspa.name."))).as_deref(), Some("abc"), "the root dot");
    assert_eq!(
        gateway_label("kaspa.name", Some(&host("blog.abc.kaspa.name"))).as_deref(),
        Some("blog.abc"),
        "deeper is refused later, as not a name"
    );
    assert_eq!(gateway_label("kaspa.name", Some(&host("kaspa.name"))), None, "the bare domain is the router's");
    assert_eq!(gateway_label("kaspa.name", Some(&host("abckaspa.name"))), None, "a suffix match is not a label");
    assert_eq!(gateway_label("kaspa.name", Some(&host("api.dotk.name"))), None);
    assert_eq!(gateway_label("kaspa.name", None), None);
    assert_eq!(gateway_label("", Some(&host("abc.kaspa.name"))), None, "off by default");
}

#[test]
fn redirect_targets_follow_the_url_rule() {
    assert_eq!(redirect_target("https://alice.example").as_deref(), Some("https://alice.example/"));
    assert_eq!(redirect_target(" HTTP://alice.example/p?q=1 ").as_deref(), Some("http://alice.example/p?q=1"));
    assert_eq!(redirect_target("alice.example").as_deref(), Some("https://alice.example/"), "a bare host gets https");
    assert_eq!(redirect_target("alice.example:8443/page").as_deref(), Some("https://alice.example:8443/page"));
    assert_eq!(redirect_target("bücher.example").as_deref(), None, "the bare-host rule is ASCII");
    assert_eq!(redirect_target("aéééé"), None, "a character across the seventh byte is no panic");
    assert_eq!(
        redirect_target("https://bücher.example").as_deref(),
        Some("https://xn--bcher-kva.example/"),
        "an explicit URL is parsed and re-serialized as ASCII"
    );
    assert_eq!(redirect_target("localhost"), None, "one label is not a site");
    assert_eq!(redirect_target("alice.example/a b"), None, "whitespace in the path");
    assert_eq!(redirect_target("javascript:alert(1)"), None);
    assert_eq!(redirect_target("data:text/html,hi"), None);
    assert_eq!(redirect_target("ftp://alice.example"), None);
    assert_eq!(redirect_target("https://"), None, "no host");
    assert_eq!(redirect_target(""), None);
}

#[test]
fn labels_are_escaped_on_the_page() {
    assert_eq!(escape_html("<b>&\"'"), "&lt;b&gt;&amp;&quot;&#39;");
}
