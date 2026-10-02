use super::*;

#[test]
fn gateway_domain_is_validated() {
    assert_eq!(parse_gateway_domain("").unwrap(), "", "blank is off");
    assert_eq!(parse_gateway_domain("kaspa.name").unwrap(), "kaspa.name");
    assert_eq!(parse_gateway_domain(" Kaspa.Name. ").unwrap(), "kaspa.name", "case and the root dot never reach a Host comparison");
    assert!(parse_gateway_domain("*.kaspa.name").is_err(), "the wildcard is the DNS record's, not the flag's");
    assert!(parse_gateway_domain("https://kaspa.name").is_err(), "a scheme is not part of a Host");
    assert!(parse_gateway_domain("kaspa.name:443").is_err(), "the port is dropped from Host before the comparison");
    assert!(parse_gateway_domain("kaspa_name").is_err(), "not a DNS label");
    assert_eq!(CliArgs::defaults().gateway_domain, "", "off by default");
}

#[test]
fn snapshot_url_keywords_and_urls() {
    assert_eq!(parse_snapshot_url("builtin").unwrap(), SnapshotUrl::Builtin);
    assert_eq!(parse_snapshot_url("none").unwrap(), SnapshotUrl::None);
    assert_eq!(parse_snapshot_url("").unwrap(), SnapshotUrl::None);
    assert_eq!(
        parse_snapshot_url("https://api.dotk.name/v1/snapshot?proven=false").unwrap(),
        SnapshotUrl::Url("https://api.dotk.name/v1/snapshot?proven=false".into())
    );
    assert_eq!(
        parse_snapshot_url("http://127.0.0.1:7799/v1/snapshot").unwrap(),
        SnapshotUrl::Url("http://127.0.0.1:7799/v1/snapshot".into())
    );
    assert!(parse_snapshot_url("buildin").is_err(), "a bare word is neither keyword nor URL");
    assert!(parse_snapshot_url("file:///tmp/snapshot.json").is_err(), "that is what --snapshot-file is");
    assert!(parse_snapshot_url("https://").is_err(), "no host");
}

/// A value that passes here reaches a browser or the journal, where the mistake is silent.
#[test]
fn every_parser_refuses_what_would_fail_silently() {
    assert_eq!(parse_allowed_origins("*").unwrap(), "*");
    assert_eq!(parse_allowed_origins("https://dotk.name, http://localhost:5173").unwrap(), "https://dotk.name, http://localhost:5173");
    assert!(parse_allowed_origins("https://dotk.name/").is_err(), "a trailing slash matches no Origin");
    assert!(parse_allowed_origins("dotk.name").is_err(), "no scheme");
    assert!(parse_allowed_origins("*, https://dotk.name").is_err(), "a wildcard beside an origin");
    assert_eq!(parse_base_path("").unwrap(), "");
    assert_eq!(parse_base_path("/api/").unwrap(), "/api");
    assert!(parse_base_path("api").is_err(), "a base path starts with a slash");
    assert_eq!(parse_journal_retention("0").unwrap(), 0, "zero keeps the journal forever");
    assert_eq!(parse_journal_retention(&JOURNAL_RETENTION_FLOOR.to_string()).unwrap(), JOURNAL_RETENTION_FLOOR);
    assert!(parse_journal_retention(&(JOURNAL_RETENTION_FLOOR - 1).to_string()).is_err(), "below finality depth");
}

#[test]
fn rpc_url_is_required_and_keeps_its_order() {
    use clap::Parser;
    let kind = CliArgs::try_parse_from(["dotk-indexer"]).err().map(|e| e.kind());
    assert_eq!(kind, Some(clap::error::ErrorKind::MissingRequiredArgument), "a node must be named");
    let args = CliArgs::try_parse_from(["dotk-indexer", "--rpc-url", "ws://a:17110,resolver", "-s", "ws://b:17110"]).unwrap();
    assert_eq!(args.rpc_urls, ["ws://a:17110", dotk_core::net::RESOLVER, "ws://b:17110"]);
}
