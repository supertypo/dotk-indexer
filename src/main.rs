use clap::Parser;
use dotk_indexer::config::CliArgs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = CliArgs::parse();
    let (genesis, genesis_raw) = dotk_indexer::genesis::builtin(&args.network)?;
    dotk_indexer::service::run(args, genesis, genesis_raw).await
}
