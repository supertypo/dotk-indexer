//! Prints the indexer's OpenAPI description, without a database, node or configuration.

fn main() {
    let base = std::env::args().nth(1).unwrap_or_default();
    let doc = dotk_indexer::web::openapi_doc(&base);
    println!("{}", serde_json::to_string_pretty(&doc).expect("the OpenAPI document serializes"));
}
