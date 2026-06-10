use clap::Parser;
use nervemq::cli::Cli;
use nervemq::kms::sqlite::SqliteKeyManager;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        // Admin subcommand: run it against the configured database and exit.
        Some(command) => nervemq::cli::execute(command).await,

        // No subcommand: start the server.
        None => {
            nervemq::run()
                .kms_factory(|db| SqliteKeyManager::new(db))
                .start()
                .await
        }
    }
}
