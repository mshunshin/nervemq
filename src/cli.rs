//! Command-line administration interface.
//!
//! Running `nervemq` with no subcommand starts the server; the subcommands
//! here perform one-off admin operations (user and API key management)
//! directly against the database and exit. Configuration (in particular
//! `NERVEMQ_DB_PATH`) is read from the environment exactly as for the server,
//! so the commands operate on the same database; the global `--data-dir`
//! flag relocates that database the same way it does for the server. SQLite's
//! WAL mode makes it safe to run them while the server is up.

use std::path::PathBuf;

use actix_identity::Identity;
use clap::{Parser, Subcommand};
use eyre::{bail, WrapErr};
use serde_email::Email;

use crate::api::auth::Role;
use crate::auth::credential::KeyAccess;
use crate::config::{Config, ConfigBuilder, DataDirLayer, DefaultsLayer, EnvironmentLayer};
use crate::kms::sqlite::SqliteKeyManager;
use crate::service::{Service, SuppliedCredentials};

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
    /// Directory to store the SQLite database files (`nervemq.db` and
    /// `sessions.db`) in. Created if it does not exist. Overrides
    /// `NERVEMQ_DB_PATH`; without it the files are written to the current
    /// directory.
    #[arg(long = "data-dir", value_name = "DIR", global = true)]
    pub data_dir: Option<PathBuf>,

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
    /// Change a user's password.
    Passwd {
        /// Email address of the user.
        email: String,

        /// New password; prompted for interactively if omitted.
        #[arg(long)]
        password: Option<String>,
    },
    /// Delete a user.
    Remove {
        /// Email address of the user to delete.
        email: String,
    },
    /// Change a user's role. The last active admin cannot be demoted.
    Role {
        /// Email address of the user.
        email: String,

        /// The new role.
        #[arg(value_parser = parse_role)]
        role: Role,
    },
    /// Disable a user: they can no longer log in or use their API keys.
    /// Their account, keys and permissions are kept.
    Disable {
        /// Email address of the user.
        email: String,
    },
    /// Re-enable a disabled user.
    Enable {
        /// Email address of the user.
        email: String,
    },
}

#[derive(Subcommand)]
pub enum ApiKeyCommand {
    /// Create an API key scoped to a namespace. The secret is printed once,
    /// unless you supply it.
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

        /// Use this access key instead of generating one. Requires
        /// --secret-key. Supply both when the credentials have to be known
        /// before the key exists — a generated secret is printed once and
        /// cannot be recovered, so a config rendered ahead of time cannot
        /// contain it.
        #[arg(long, requires = "secret_key")]
        access_key: Option<String>,

        /// Use this secret key instead of generating one. Requires
        /// --access-key. Note that it is visible in the process list and the
        /// shell history of whoever runs the command.
        #[arg(long, requires = "access_key")]
        secret_key: Option<String>,

        /// The most the key may do: `member` (send and receive), `owner`
        /// (also manage the namespace's queues) or `admin` (everything its
        /// user can do, including the admin API). At most the user's own
        /// level in the namespace; defaults to it.
        #[arg(long, value_parser = parse_access)]
        access: Option<KeyAccess>,
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
    /// Manage a namespace's owners, who may delete it and manage its queues.
    Owner {
        #[command(subcommand)]
        command: OwnerCommand,
    },
}

#[derive(Subcommand)]
pub enum OwnerCommand {
    /// Make a user an owner, granting them access to the namespace if they
    /// have none.
    Add {
        /// Name of the namespace.
        namespace: String,

        /// Email address of the user.
        email: String,
    },
    /// Stop a user being an owner. They keep access as a member.
    Remove {
        /// Name of the namespace.
        namespace: String,

        /// Email address of the user.
        email: String,
    },
}

fn parse_role(s: &str) -> Result<Role, String> {
    match s.to_ascii_lowercase().as_str() {
        "user" => Ok(Role::User),
        "admin" => Ok(Role::Admin),
        other => Err(format!("invalid role '{other}': must be 'user' or 'admin'")),
    }
}

fn parse_access(s: &str) -> Result<KeyAccess, String> {
    s.parse()
}

fn role_name(role: &Role) -> &'static str {
    match role {
        Role::User => "user",
        Role::Admin => "admin",
    }
}

/// Loads configuration from the environment (same layers as the server) and
/// connects to the service. An explicit `data_dir` (from `--data-dir`)
/// relocates the database files and overrides `NERVEMQ_DB_PATH`.
async fn connect(data_dir: Option<PathBuf>) -> eyre::Result<(Service, Config)> {
    let mut builder = ConfigBuilder::new()
        .with_layer(DefaultsLayer)
        .with_layer(EnvironmentLayer);
    if let Some(dir) = data_dir {
        builder = builder.with_layer(DataDirLayer::new(dir));
    }
    let config = builder.load().await?;

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

pub async fn execute(command: Command, data_dir: Option<PathBuf>) -> eyre::Result<()> {
    let (service, config) = connect(data_dir).await?;

    match command {
        Command::User { command } => execute_user(command, &service, &config).await,
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
            let namespaces = service.list_namespace_statistics(root()).await?;

            println!("{:<32} {:<32} OWNERS", "NAME", "CREATED BY");
            for ns in namespaces {
                println!(
                    "{:<32} {:<32} {}",
                    ns.namespace.name,
                    ns.namespace.created_by.as_deref().unwrap_or("-"),
                    if ns.owners.is_empty() {
                        "-".to_string()
                    } else {
                        ns.owners.join(", ")
                    }
                );
            }
        }

        NamespaceCommand::Owner { command } => match command {
            OwnerCommand::Add { namespace, email } => {
                let email = parse_email(&email)?;
                service.set_namespace_owner(&namespace, &email, true).await?;
                println!("'{email}' now owns namespace '{namespace}'");
            }
            OwnerCommand::Remove { namespace, email } => {
                let email = parse_email(&email)?;
                service
                    .set_namespace_owner(&namespace, &email, false)
                    .await?;
                println!("'{email}' no longer owns namespace '{namespace}'");
            }
        },

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

async fn execute_user(
    command: UserCommand,
    service: &Service,
    config: &Config,
) -> eyre::Result<()> {
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
            let users = service.list_users().await?;

            println!("{:<40} {:<6} STATUS", "EMAIL", "ROLE");
            for user in users {
                println!(
                    "{:<40} {:<6} {}",
                    user.email,
                    role_name(&user.role),
                    if user.disabled { "disabled" } else { "active" }
                );
            }
        }

        UserCommand::Role { email, role } => {
            let email = parse_email(&email)?;
            service.set_user_role(&email, role.clone()).await?;
            println!("'{email}' is now {}", role_name(&role));
        }

        UserCommand::Disable { email } => {
            let email = parse_email(&email)?;
            service.set_user_disabled(&email, true).await?;
            println!("Disabled user '{email}'");
        }

        UserCommand::Enable { email } => {
            let email = parse_email(&email)?;
            service.set_user_disabled(&email, false).await?;
            println!("Enabled user '{email}'");
        }

        UserCommand::Passwd { email, password } => {
            let email = parse_email(&email)?;

            // Pre-check so we fail with a clear message before prompting for a
            // password rather than after.
            let exists: Option<i64> = sqlx::query_scalar("SELECT id FROM users WHERE email = $1")
                .bind(email.as_str())
                .fetch_optional(service.db())
                .await?;
            if exists.is_none() {
                bail!("no such user: {email}");
            }

            let password = match password {
                Some(password) => password,
                None => prompt_password()?,
            };

            service.set_user_password(email.clone(), password).await?;

            println!("Updated password for '{email}'");
        }

        UserCommand::Remove { email } => {
            let email = parse_email(&email)?;

            // The root administrator comes from the environment
            // (NERVEMQ_ROOT_EMAIL / NERVEMQ_ROOT_PASSWORD) and is recreated
            // in the database on every server start, so deleting it is
            // futile. Explain instead.
            if email.as_str() == config.root_email() {
                bail!(
                    "'{email}' is the root administrator, which is configured \
                     via the NERVEMQ_ROOT_EMAIL / NERVEMQ_ROOT_PASSWORD \
                     environment variables and recreated at startup; it \
                     cannot be deleted"
                );
            }

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
            access_key,
            secret_key,
            access,
        } => {
            let user = user.unwrap_or_else(|| config.root_email().to_owned());

            let supplied = match (access_key, secret_key) {
                (Some(access_key), Some(secret_key)) => Some(SuppliedCredentials {
                    access_key,
                    secret_key,
                }),
                // clap's `requires` pairs the two flags, so this is "neither".
                _ => None,
            };
            let was_supplied = supplied.is_some();

            let creds = service
                .create_token_with(
                    name,
                    namespace,
                    Identity::mock(user.clone()),
                    supplied,
                    access,
                )
                .await?;

            println!(
                "Created API key '{}' for namespace '{}' (user '{}', {} access):",
                creds.name,
                creds.namespace,
                user,
                creds.access.as_str()
            );
            println!("  Access key: {}", creds.access_key);
            if was_supplied {
                println!("  Secret key: (supplied)");
            } else {
                println!("  Secret key: {}", creds.secret_key);
                println!("Store the secret key now: it cannot be retrieved later.");
            }
        }

        ApiKeyCommand::List => {
            let keys: Vec<(String, String, KeyAccess, String)> = sqlx::query_as(
                "
                SELECT k.name, ns.name, k.access, u.email FROM api_keys k
                JOIN users u ON u.id = k.user
                JOIN namespaces ns ON ns.id = k.ns
                ORDER BY u.email, k.name
                ",
            )
            .fetch_all(service.db())
            .await?;

            println!("{:<24} {:<24} {:<7} USER", "NAME", "NAMESPACE", "ACCESS");
            for (name, namespace, access, email) in keys {
                println!("{name:<24} {namespace:<24} {:<7} {email}", access.as_str());
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kms::memory::InMemoryKeyManager;

    #[test]
    fn parse_role_accepts_known_roles_case_insensitively() {
        assert!(matches!(parse_role("user"), Ok(Role::User)));
        assert!(matches!(parse_role("Admin"), Ok(Role::Admin)));
        assert!(parse_role("superuser").is_err());
    }

    #[test]
    fn role_name_round_trips_parse_role() {
        for name in ["user", "admin"] {
            assert_eq!(role_name(&parse_role(name).unwrap()), name);
        }
    }

    #[test]
    fn parse_email_validates() {
        assert_eq!(
            parse_email("bob@example.com").unwrap().as_str(),
            "bob@example.com"
        );
        assert!(parse_email("not-an-email").is_err());
    }

    /// `--access-key` and `--secret-key` are all-or-nothing: an access key with
    /// a generated secret would still be unrecoverable.
    #[test]
    fn apikey_add_credentials_come_in_pairs() {
        let cli = Cli::try_parse_from([
            "nervemq",
            "apikey",
            "add",
            "--name",
            "ci",
            "--namespace",
            "ns",
            "--access-key",
            "AK",
            "--secret-key",
            "SK",
        ])
        .unwrap();
        let Some(Command::ApiKey {
            command:
                ApiKeyCommand::Add {
                    access_key,
                    secret_key,
                    ..
                },
        }) = cli.command
        else {
            panic!("expected apikey add");
        };
        assert_eq!(access_key.as_deref(), Some("AK"));
        assert_eq!(secret_key.as_deref(), Some("SK"));

        for half in [
            ["--access-key", "AK"].as_slice(),
            ["--secret-key", "SK"].as_slice(),
        ] {
            let mut argv = vec!["nervemq", "apikey", "add", "--name", "ci", "--namespace", "ns"];
            argv.extend_from_slice(half);
            assert!(
                Cli::try_parse_from(argv).is_err(),
                "{half:?} alone should be rejected"
            );
        }
    }

    /// The clap derive wiring: subcommands, flags, defaults and the
    /// `value_parser` hook all resolve as documented in `--help`.
    #[test]
    fn cli_parses_admin_subcommands() {
        let cli = Cli::try_parse_from([
            "nervemq", "user", "add", "bob@example.com", "--password", "pw",
            "--role", "admin", "--namespace", "ns1", "--namespace", "ns2",
        ])
        .unwrap();
        let Some(Command::User { command: UserCommand::Add { email, password, role, namespaces } }) =
            cli.command
        else {
            panic!("expected user add");
        };
        assert_eq!(email, "bob@example.com");
        assert_eq!(password.as_deref(), Some("pw"));
        assert!(matches!(role, Role::Admin));
        assert_eq!(namespaces, vec!["ns1", "ns2"]);

        let cli = Cli::try_parse_from(["nervemq", "apikey", "add", "--name", "k", "--namespace", "ns"])
            .unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::ApiKey { command: ApiKeyCommand::Add { user: None, .. } })
        ));

        let cli = Cli::try_parse_from(["nervemq", "user", "passwd", "bob@example.com", "--password", "pw"])
            .unwrap();
        let Some(Command::User { command: UserCommand::Passwd { email, password } }) = cli.command
        else {
            panic!("expected user passwd");
        };
        assert_eq!(email, "bob@example.com");
        assert_eq!(password.as_deref(), Some("pw"));

        // No subcommand runs the server.
        assert!(Cli::try_parse_from(["nervemq"]).unwrap().command.is_none());

        assert!(Cli::try_parse_from(["nervemq", "user", "add", "bob@example.com", "--role", "root"]).is_err());
    }

    /// `--data-dir` is a global option: it parses before a subcommand, after a
    /// subcommand, and on its own (server mode).
    #[test]
    fn cli_accepts_global_data_dir() {
        let dir = std::path::Path::new("/var/lib/nervemq");

        // Before the subcommand.
        let cli =
            Cli::try_parse_from(["nervemq", "--data-dir", "/var/lib/nervemq", "user", "list"])
                .unwrap();
        assert_eq!(cli.data_dir.as_deref(), Some(dir));
        assert!(matches!(
            cli.command,
            Some(Command::User { command: UserCommand::List })
        ));

        // After the subcommand (only possible because the arg is global).
        let cli =
            Cli::try_parse_from(["nervemq", "user", "list", "--data-dir", "/var/lib/nervemq"])
                .unwrap();
        assert_eq!(cli.data_dir.as_deref(), Some(dir));

        // No subcommand: the server, with a data directory.
        let cli = Cli::try_parse_from(["nervemq", "--data-dir", "/var/lib/nervemq"]).unwrap();
        assert!(cli.command.is_none());
        assert_eq!(cli.data_dir.as_deref(), Some(dir));

        // Omitted entirely: defaults to None.
        assert!(Cli::try_parse_from(["nervemq", "user", "list"]).unwrap().data_dir.is_none());
    }

    /// A throwaway service + config equivalent to what `connect()` builds,
    /// minus the environment dependence (and with the in-memory KMS).
    async fn test_service() -> (Service, Config, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db").to_string_lossy().to_string();

        let config: Config =
            serde_json::from_value(serde_json::json!({ "db_path": db_path })).unwrap();

        let service = Service::connect_with()
            .config(config.clone())
            .kms_factory(|_| async move { Ok(InMemoryKeyManager::new()) })
            .call()
            .await
            .unwrap();

        (service, config, dir)
    }

    #[actix_web::test]
    async fn namespace_commands_roundtrip() {
        let (service, config, _dir) = test_service().await;

        execute_namespace(NamespaceCommand::Add { name: "ns".into() }, &service, &config)
            .await
            .unwrap();
        assert!(service.get_namespace_id("ns", service.db()).await.unwrap().is_some());

        execute_namespace(NamespaceCommand::List, &service, &config)
            .await
            .unwrap();

        execute_namespace(NamespaceCommand::Remove { name: "ns".into() }, &service, &config)
            .await
            .unwrap();
        assert!(service.get_namespace_id("ns", service.db()).await.unwrap().is_none());

        let err = execute_namespace(NamespaceCommand::Remove { name: "ns".into() }, &service, &config)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no such namespace"), "{err}");
    }

    #[actix_web::test]
    async fn user_commands_roundtrip() {
        let (service, config, _dir) = test_service().await;

        execute_namespace(NamespaceCommand::Add { name: "ns".into() }, &service, &config)
            .await
            .unwrap();

        let add = |email: &str, namespaces: Vec<String>| UserCommand::Add {
            email: email.to_string(),
            password: Some("hunter2hunter2".into()),
            role: Role::User,
            namespaces,
        };

        execute_user(add("bob@example.com", vec!["ns".into()]), &service, &config)
            .await
            .unwrap();
        let exists: Option<i64> = sqlx::query_scalar("SELECT id FROM users WHERE email = $1")
            .bind("bob@example.com")
            .fetch_optional(service.db())
            .await
            .unwrap();
        assert!(exists.is_some());

        let err = execute_user(add("not-an-email", vec![]), &service, &config)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("invalid email address"), "{err}");

        let err = execute_user(add("eve@example.com", vec!["ghost".into()]), &service, &config)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no such namespace"), "{err}");

        execute_user(UserCommand::List, &service, &config).await.unwrap();

        execute_user(UserCommand::Remove { email: "bob@example.com".into() }, &service, &config)
            .await
            .unwrap();

        let err = execute_user(UserCommand::Remove { email: "bob@example.com".into() }, &service, &config)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no such user"), "{err}");

        // The root administrator is environment-managed and protected.
        let err = execute_user(
            UserCommand::Remove { email: config.root_email().to_string() },
            &service,
            &config,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("root administrator"), "{err}");
    }

    #[actix_web::test]
    async fn passwd_command_sets_a_new_working_password() {
        use crate::auth::crypto::verify_secret;
        use argon2::password_hash::PasswordHashString;
        use secrecy::SecretString;

        let (service, config, _dir) = test_service().await;

        execute_user(
            UserCommand::Add {
                email: "bob@example.com".into(),
                password: Some("originalpassword".into()),
                role: Role::User,
                namespaces: vec![],
            },
            &service,
            &config,
        )
        .await
        .unwrap();

        execute_user(
            UserCommand::Passwd {
                email: "bob@example.com".into(),
                password: Some("newpassword".into()),
            },
            &service,
            &config,
        )
        .await
        .unwrap();

        // The stored hash now verifies the new password and rejects the old one.
        let hash: String = sqlx::query_scalar("SELECT hashed_pass FROM users WHERE email = $1")
            .bind("bob@example.com")
            .fetch_one(service.db())
            .await
            .unwrap();
        assert!(verify_secret(
            SecretString::new("newpassword".into()),
            PasswordHashString::new(&hash).unwrap()
        )
        .is_ok());
        assert!(verify_secret(
            SecretString::new("originalpassword".into()),
            PasswordHashString::new(&hash).unwrap()
        )
        .is_err());

        // Changing an unknown user's password is a friendly error, not a no-op.
        let err = execute_user(
            UserCommand::Passwd {
                email: "ghost@example.com".into(),
                password: Some("whatever".into()),
            },
            &service,
            &config,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("no such user"), "{err}");
    }

    #[actix_web::test]
    async fn apikey_commands_roundtrip() {
        let (service, config, _dir) = test_service().await;

        execute_namespace(NamespaceCommand::Add { name: "ns".into() }, &service, &config)
            .await
            .unwrap();

        // Owner defaults to the root administrator.
        execute_apikey(
            ApiKeyCommand::Add {
                name: "ci".into(),
                namespace: "ns".into(),
                user: None,
                access_key: None,
                secret_key: None,
                access: None,
            },
            &service,
            &config,
        )
        .await
        .unwrap();

        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM api_keys WHERE name = 'ci'")
            .fetch_one(service.db())
            .await
            .unwrap();
        assert_eq!(count, 1);

        execute_apikey(ApiKeyCommand::List, &service, &config).await.unwrap();

        execute_apikey(ApiKeyCommand::Remove { name: "ci".into(), user: None }, &service, &config)
            .await
            .unwrap();

        let err = execute_apikey(ApiKeyCommand::Remove { name: "ci".into(), user: None }, &service, &config)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no API key named"), "{err}");
    }

    #[actix_web::test]
    async fn owner_role_and_disable_commands() {
        let (service, config, _dir) = test_service().await;

        execute_namespace(NamespaceCommand::Add { name: "ns".into() }, &service, &config)
            .await
            .unwrap();
        execute_user(
            UserCommand::Add {
                email: "bob@example.com".into(),
                password: Some("hunter2hunter2".into()),
                role: Role::User,
                namespaces: vec![],
            },
            &service,
            &config,
        )
        .await
        .unwrap();

        let owner = |add: bool| NamespaceCommand::Owner {
            command: if add {
                OwnerCommand::Add {
                    namespace: "ns".into(),
                    email: "bob@example.com".into(),
                }
            } else {
                OwnerCommand::Remove {
                    namespace: "ns".into(),
                    email: "bob@example.com".into(),
                }
            },
        };
        let bob_owns = || async {
            sqlx::query_scalar::<_, bool>(
                "SELECT p.is_owner FROM user_permissions p
                 JOIN users u ON u.id = p.user WHERE u.email = 'bob@example.com'",
            )
            .fetch_one(service.db())
            .await
            .unwrap()
        };

        execute_namespace(owner(true), &service, &config).await.unwrap();
        assert!(bob_owns().await);
        execute_namespace(NamespaceCommand::List, &service, &config)
            .await
            .unwrap();
        execute_namespace(owner(false), &service, &config).await.unwrap();
        assert!(!bob_owns().await, "removing ownership should keep membership");

        execute_user(
            UserCommand::Disable {
                email: "bob@example.com".into(),
            },
            &service,
            &config,
        )
        .await
        .unwrap();
        assert!(service.list_users().await.unwrap().iter().any(|u| u.email == "bob@example.com" && u.disabled));
        execute_user(
            UserCommand::Enable {
                email: "bob@example.com".into(),
            },
            &service,
            &config,
        )
        .await
        .unwrap();

        // The root admin is the only admin, so it cannot be demoted.
        let err = execute_user(
            UserCommand::Role {
                email: config.root_email().to_owned(),
                role: Role::User,
            },
            &service,
            &config,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("last active admin"), "{err}");
    }
}
