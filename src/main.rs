use clap::Parser;
use eyre::WrapErr;
use nervemq::cli::Cli;
use nervemq::kms::sqlite::SqliteKeyManager;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let cli = Cli::parse();

    // The SQLite connect options create the database files but not their
    // parent directory, so make sure an explicitly requested one exists.
    if let Some(dir) = &cli.data_dir {
        std::fs::create_dir_all(dir)
            .wrap_err_with(|| format!("failed to create data directory '{}'", dir.display()))?;
    }

    match cli.command {
        // Admin subcommand: run it against the configured database and exit.
        Some(command) => nervemq::cli::execute(command, cli.data_dir).await,

        // No subcommand: start the server.
        None => {
            nervemq::run()
                .kms_factory(|db| SqliteKeyManager::new(db))
                .maybe_data_dir(cli.data_dir)
                .start()
                .await
        }
    }
}
