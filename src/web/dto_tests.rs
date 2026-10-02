use crate::web::openapi_doc;

#[test]
fn the_manifest_schema_names_every_field_a_real_manifest_carries() {
    let (_, raw) = crate::genesis::builtin("mainnet").unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(&raw).expect("the manifest must parse");
    let served: std::collections::BTreeSet<&str> = manifest.as_object().expect("an object").keys().map(String::as_str).collect();

    let doc = serde_json::to_value(openapi_doc("")).expect("the description serializes");
    let schema = &doc["components"]["schemas"]["Manifest"];
    // The flattened catch-all splits the schema into `allOf`.
    let named = schema["allOf"].as_array().expect("allOf").iter().find(|part| part.get("properties").is_some()).expect("a named half");
    let documented: std::collections::BTreeSet<&str> =
        named["properties"].as_object().expect("properties").keys().map(String::as_str).collect();

    assert_eq!(documented, served, "the /genesis schema and a real manifest must name the same fields");
    assert!(served.contains("genesisBinding") && served.len() > 5, "a real manifest, not an empty object: {served:?}");

    // A field the manifest can omit must not be documented as required, or a generated
    // client refuses a valid manifest.
    let required: std::collections::BTreeSet<&str> =
        named["required"].as_array().expect("required").iter().map(|v| v.as_str().unwrap()).collect();
    let mut minimal = manifest.clone();
    let optional = minimal.as_object_mut().unwrap();
    for field in ["version", "genesisBinding"] {
        optional.remove(field).unwrap_or_else(|| panic!("the example manifest carries {field}"));
        assert!(!required.contains(field), "{field} is documented as required, and a manifest may omit it");
    }
    serde_json::from_value::<dotk_core::watch::GenesisFile>(minimal)
        .expect("a manifest without its defaulted fields must still parse, which is what makes them optional");
    assert!(
        schema["allOf"].as_array().unwrap().iter().any(|part| part.get("properties").is_none()),
        "unknown fields stay documented as open"
    );
}
