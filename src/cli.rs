//! Command-line administration interface.
//!
//! Running `nervemq` with no subcommand starts the server; the subcommands
//! here perform one-off admin operations (user and API key management)
//! directly against the database and exit. Configuration (in particular
//! `NERVEMQ_DB_PATH`) is read from the environment exactly as for the server,
//! so the commands operate on the same database. SQLite's WAL mode makes it
//! safe to run them while the server is up.

use actix_identity::Identity;
use clap::{Parser, Subcommand};
use eyre::{bail, WrapErr};
use serde_email::Email;

use crate::api::auth::Role;
use crate::config::{Config, ConfigBuilder, DefaultsLayer, EnvironmentLayer};
use crate::kms::sqlite::SqliteKeyManager;
use crate::service::Service;

#[derive(Parser)]
#[command(
    name = "nervemq",
    version,
    about = "Portable, SQS-compatible message queue backed by SQLite.",
    long_about = "Runs the NerveMQ server when invoked without a subcommand. \
                  Subcommands perform admin operations against the configured \
                  database and exit."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand)]
pub enum Command {
    /// Manage user accounts.
    User {
        #[command(subcommand)]
        command: UserCommand,
    },
    /// Manage API keys (credentials for the SQS-compatible API).
    #[command(name = "apikey")]
    ApiKey {
        #[command(subcommand)]
        command: ApiKeyCommand,
    },
    /// Manage namespaces.
    Namespace {
        #[command(subcommand)]
        command: NamespaceCommand,
    },
}

#[derive(Subcommand)]
pub enum UserCommand {
    /// Create a user.
    Add {
        /// Email address identifying the user.
        email: String,

        /// Password for the new user; prompted for interactively if omitted.
        #[arg(long)]
        password: Option<String>,

        /// Role for the new user.
        #[arg(long, default_value = "user", value_parser = parse_role)]
        role: Role,

        /// Namespace the user may access (repeatable).
        #[arg(long = "namespace", value_name = "NAMESPACE")]
        namespaces: Vec<String>,
    },
    /// List all users.
    List,
    /// Delete a user.
    Remove {
        /// Email address of the user to delete.
        email: String,
    },
}

#[derive(Subcommand)]
pub enum ApiKeyCommand {
    /// Create an API key scoped to a namespace. The secret is printed once.
    Add {
        /// Name identifying the key (unique per user).
        #[arg(long)]
        name: String,

        /// Namespace the key grants access to.
        #[arg(long)]
        namespace: String,

        /// Email of the owning user; defaults to the root administrator.
        #[arg(long)]
        user: Option<String>,
    },
    /// List all API keys.
    List,
    /// Delete an API key.
    Remove {
        /// Name of the key to delete.
        #[arg(long)]
        name: String,

        /// Email of the owning user; defaults to the root administrator.
        #[arg(long)]
        user: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum NamespaceCommand {
    /// Create a namespace.
    Add {
        /// Name of the namespace.
        name: String,
    },
    /// List all namespaces.
    List,
    /// Delete a namespace and everything in it.
    Remove {
        /// Name of the namespace to delete.
        name: String,
    },
}

fn parse_role(s: &str) -> Result<Role, String> {
    match s.to_ascii_lowercase().as_str() {
        "user" => Ok(Role::User),
        "admin" => Ok(Role::Admin),
        other => Err(format!("invalid role '{other}': must be 'user' or 'admin'")),
    }
}

fn role_name(role: &Role) -> &'static str {
    match role {
        Role::User => "user",
        Role::Admin => "admin",
    }
}

/// Loads configuration from the environment (same layers as the server) and
/// connects to the service.
async fn connect() -> eyre::Result<(Service, Config)> {
    let config = ConfigBuilder::new()
        .with_layer(DefaultsLayer)
        .with_layer(EnvironmentLayer)
        .load()
        .await?;

    let service = Service::connect_with()
        .config(config.clone())
        .kms_factory(SqliteKeyManager::new)
        .call()
        .await?;

    Ok((service, config))
}

fn prompt_password() -> eyre::Result<String> {
    let password =
        rpassword::prompt_password("Password: ").wrap_err("failed to read password")?;
    if password.is_empty() {
        bail!("password must not be empty");
    }
    let confirm =
        rpassword::prompt_password("Confirm password: ").wrap_err("failed to read password")?;
    if password != confirm {
        bail!("passwords do not match");
    }
    Ok(password)
}

fn parse_email(email: &str) -> eyre::Result<Email> {
    Email::from_str(email).map_err(|e| eyre::eyre!("invalid email address '{email}': {e}"))
}

pub async fn execute(command: Command) -> eyre::Result<()> {
    let (service, config) = connect().await?;

    match command {
        Command::User { command } => execute_user(command, &service).await,
        Command::ApiKey { command } => execute_apikey(command, &service, &config).await,
        Command::Namespace { command } => execute_namespace(command, &service, &config).await,
    }
}

async fn execute_namespace(
    command: NamespaceCommand,
    service: &Service,
    config: &Config,
) -> eyre::Result<()> {
    // Namespace operations act as the root administrator.
    let root = || Identity::mock(config.root_email().to_owned());

    match command {
        NamespaceCommand::Add { name } => {
            service.create_namespace(&name, root()).await?;
            println!("Created namespace '{name}'");
        }

        NamespaceCommand::List => {
            let namespaces = service.list_namespaces(root()).await?;

            println!("{:<32} CREATED BY", "NAME");
            for ns in namespaces {
                println!("{:<32} {}", ns.name, ns.created_by);
            }
        }

        NamespaceCommand::Remove { name } => {
            if service
                .get_namespace_id(&name, service.db())
                .await?
                .is_none()
            {
                bail!("no such namespace: {name}");
            }

            service.delete_namespace(&name, root()).await?;
            println!("Deleted namespace '{name}'");
        }
    }

    Ok(())
}

async fn execute_user(command: UserCommand, service: &Service) -> eyre::Result<()> {
    match command {
        UserCommand::Add {
            email,
            password,
            role,
            namespaces,
        } => {
            let email = parse_email(&email)?;

            // Validate up front for a friendly error instead of a NOT NULL
            // constraint failure from the permissions insert.
            for namespace in &namespaces {
                if service
                    .get_namespace_id(namespace, service.db())
                    .await?
                    .is_none()
                {
                    bail!("no such namespace: {namespace}");
                }
            }

            let password = match password {
                Some(password) => password,
                None => prompt_password()?,
            };

            service
                .create_user(email.clone(), password, Some(role.clone()), namespaces)
                .await?;

            println!("Created {} '{}'", role_name(&role), email);
        }

        UserCommand::List => {
            let users: Vec<(String, Role)> =
                sqlx::query_as("SELECT email, role FROM users ORDER BY email")
                    .fetch_all(service.db())
                    .await?;

            println!("{:<40} ROLE", "EMAIL");
            for (email, role) in users {
                println!("{email:<40} {}", role_name(&role));
            }
        }

        UserCommand::Remove { email } => {
            let email = parse_email(&email)?;

            // Pre-check for a friendly message instead of a bare RowNotFound.
            let exists: Option<i64> = sqlx::query_scalar("SELECT id FROM users WHERE email = $1")
                .bind(email.as_str())
                .fetch_optional(service.db())
                .await?;
            if exists.is_none() {
                bail!("no such user: {email}");
            }

            service.delete_user(email.clone()).await?;

            println!("Deleted user '{email}'");
        }
    }

    Ok(())
}

async fn execute_apikey(
    command: ApiKeyCommand,
    service: &Service,
    config: &Config,
) -> eyre::Result<()> {
    match command {
        ApiKeyCommand::Add {
            name,
            namespace,
            user,
        } => {
            let user = user.unwrap_or_else(|| config.root_email().to_owned());

            let creds = service
                .create_token(name, namespace, Identity::mock(user.clone()))
                .await?;

            println!(
                "Created API key '{}' for namespace '{}' (user '{}'):",
                creds.name, creds.namespace, user
            );
            println!("  Access key: {}", creds.access_key);
            println!("  Secret key: {}", creds.secret_key);
            println!("Store the secret key now: it cannot be retrieved later.");
        }

        ApiKeyCommand::List => {
            let keys: Vec<(String, String, String)> = sqlx::query_as(
                "
                SELECT k.name, ns.name, u.email FROM api_keys k
                JOIN users u ON u.id = k.user
                JOIN namespaces ns ON ns.id = k.ns
                ORDER BY u.email, k.name
                ",
            )
            .fetch_all(service.db())
            .await?;

            println!("{:<24} {:<24} USER", "NAME", "NAMESPACE");
            for (name, namespace, email) in keys {
                println!("{name:<24} {namespace:<24} {email}");
            }
        }

        ApiKeyCommand::Remove { name, user } => {
            let user = user.unwrap_or_else(|| config.root_email().to_owned());

            let result = sqlx::query(
                "
                DELETE FROM api_keys
                WHERE name = $1
                AND user IN (SELECT id FROM users WHERE email = $2)
                ",
            )
            .bind(&name)
            .bind(&user)
            .execute(service.db())
            .await?;

            if result.rows_affected() == 0 {
                bail!("no API key named '{name}' for user '{user}'");
            }

            println!("Deleted API key '{name}' (user '{user}')");
        }
    }

    Ok(())
}
