use super::*;

#[test]
fn a_present_file_wins_over_every_url_setting() {
    for url in [SnapshotUrl::Builtin, SnapshotUrl::None, SnapshotUrl::Url("https://mirror.example/s.json".into())] {
        assert_eq!(decide(true, &url, "mainnet"), Bootstrap::File, "{url:?}");
    }
}

#[test]
fn without_a_file_the_url_setting_decides() {
    assert_eq!(decide(false, &SnapshotUrl::None, "mainnet"), Bootstrap::None);
    assert_eq!(
        decide(false, &SnapshotUrl::Builtin, "mainnet"),
        Bootstrap::Fetch { url: "https://api.dotk.name/v1/snapshot".into(), explicit: false }
    );
    assert_eq!(
        decide(false, &SnapshotUrl::Builtin, "testnet-10"),
        Bootstrap::Fetch { url: "https://api-tn10.dotk.name/v1/snapshot".into(), explicit: false }
    );
    // No registry is published for a private network, so the default fetches nothing.
    assert_eq!(decide(false, &SnapshotUrl::Builtin, "testnet-11"), Bootstrap::None);
    assert_eq!(decide(false, &SnapshotUrl::Builtin, "simnet"), Bootstrap::None);
    assert_eq!(
        decide(false, &SnapshotUrl::Url("https://mirror.example/s.json".into()), "mainnet"),
        Bootstrap::Fetch { url: "https://mirror.example/s.json".into(), explicit: true }
    );
}
