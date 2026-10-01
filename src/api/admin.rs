//! User administration: `/api/admin/users`, admin only (see `lib.rs`).

use actix_web::{
    delete, get, post, put,
    web::{self, Json},
    HttpResponse, Responder, Scope,
};
use serde::{Deserialize, Serialize};
use serde_email::Email;

use crate::{
    error::Error,
    service::{ApiKeyInfo, Service, UserInfo},
};

use super::auth::Role;

#[derive(Debug, Deserialize)]
pub struct CreateUserRequest {
    email: String,
    password: String,
    role: Role,
    namespaces: Vec<String>,
}

fn parse_email(email: &str) -> Result<Email, Error> {
    Email::from_str(email).map_err(|e| Error::invalid_parameter(format!("email: {e}")))
}

#[post("")]
pub async fn create_user(
    data: web::Json<CreateUserRequest>,
    service: web::Data<Service>,
) -> Result<impl Responder, Error> {
    let data = data.into_inner();

    let email = parse_email(&data.email)?;
    if data.password.is_empty() {
        return Err(Error::invalid_parameter("password must not be empty"));
    }

    service
        .create_user(email, data.password, Some(data.role), data.namespaces)
        .await?;

    Ok(HttpResponse::Ok())
}

#[get("")]
pub async fn list_users(service: web::Data<Service>) -> Result<Json<Vec<UserInfo>>, Error> {
    Ok(Json(service.list_users().await?))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteUserRequest {
    email: String,
}

/// Deletes a user, their API keys and their permissions. The last active
/// admin cannot be deleted (409).
#[delete("")]
pub async fn delete_user(
    data: web::Json<DeleteUserRequest>,
    service: web::Data<Service>,
) -> Result<impl Responder, Error> {
    service.delete_user(parse_email(&data.email)?).await?;

    Ok(HttpResponse::Ok())
}

/// The namespaces a user can access.
#[get("/{email}/permissions")]
pub async fn list_user_permissions(
    service: web::Data<Service>,
    email: web::Path<String>,
) -> Result<Json<Vec<String>>, Error> {
    let email = email.into_inner();

    let permissions: Vec<String> = sqlx::query_scalar(
        "
            SELECT ns.name FROM user_permissions p
            JOIN namespaces ns ON p.namespace = ns.id
            JOIN users u ON u.id = p.user
            WHERE u.email = $1
            ORDER BY ns.name
        ",
    )
    .bind(&email)
    .fetch_all(service.db())
    .await?;

    Ok(Json(permissions))
}

/// Adds namespaces to the ones a user can access, as a member.
#[put("/{email}/permissions")]
pub async fn grant_user_permissions(
    service: web::Data<Service>,
    email: web::Path<String>,
    data: Json<Vec<String>>,
) -> Result<impl Responder, Error> {
    service
        .grant_user_namespaces(&parse_email(&email)?, &data)
        .await?;

    Ok(HttpResponse::Ok())
}

/// Removes namespaces from the ones a user can access, ownership included.
#[delete("/{email}/permissions")]
pub async fn revoke_user_permissions(
    service: web::Data<Service>,
    email: web::Path<String>,
    data: Json<Vec<String>>,
) -> Result<impl Responder, Error> {
    service
        .revoke_user_namespaces(&parse_email(&email)?, &data)
        .await?;

    Ok(HttpResponse::Ok())
}

/// Makes the list exactly the namespaces a user can access. Namespaces that
/// stay keep their ownership.
#[post("/{email}/permissions")]
pub async fn update_user_permissions(
    service: web::Data<Service>,
    email: web::Path<String>,
    data: Json<Vec<String>>,
) -> Result<impl Responder, Error> {
    service
        .set_user_namespaces(&parse_email(&email)?, &data)
        .await?;

    Ok(HttpResponse::Ok())
}

#[get("/{email}/role")]
async fn get_user_role(
    service: web::Data<Service>,
    email: web::Path<String>,
) -> Result<Json<Role>, Error> {
    let email = email.into_inner();
    let role: Role = sqlx::query_scalar(
        "
            SELECT role FROM users
            WHERE email = $1
        ",
    )
    .bind(&email)
    .fetch_optional(service.db())
    .await?
    .ok_or_else(|| Error::not_found(format!("user {email}")))?;
    Ok(Json(role))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateUserRoleRequest {
    role: Role,
}

/// Changes a user's role. The last active admin cannot be demoted (409).
#[post("/{email}/role")]
async fn set_user_role(
    service: web::Data<Service>,
    email: web::Path<String>,
    data: web::Json<UpdateUserRoleRequest>,
) -> Result<impl Responder, Error> {
    service
        .set_user_role(&parse_email(&email)?, data.into_inner().role)
        .await?;
    Ok(HttpResponse::Ok())
}

/// Disables a user: they can no longer log in or use their API keys. The
/// last active admin cannot be disabled (409).
#[post("/{email}/disable")]
async fn disable_user(
    service: web::Data<Service>,
    email: web::Path<String>,
) -> Result<impl Responder, Error> {
    service
        .set_user_disabled(&parse_email(&email)?, true)
        .await?;
    Ok(HttpResponse::Ok())
}

/// Re-enables a disabled user.
#[post("/{email}/enable")]
async fn enable_user(
    service: web::Data<Service>,
    email: web::Path<String>,
) -> Result<impl Responder, Error> {
    service
        .set_user_disabled(&parse_email(&email)?, false)
        .await?;
    Ok(HttpResponse::Ok())
}

#[derive(Debug, Deserialize)]
pub struct ResetPasswordRequest {
    password: String,
}

/// Sets a user's password without knowing the old one.
#[post("/{email}/password")]
async fn reset_user_password(
    service: web::Data<Service>,
    email: web::Path<String>,
    data: web::Json<ResetPasswordRequest>,
) -> Result<impl Responder, Error> {
    let email = parse_email(&email)?;
    let password = data.into_inner().password;
    if password.is_empty() {
        return Err(Error::invalid_parameter("password must not be empty"));
    }

    if !service.set_user_password(email.clone(), password).await? {
        return Err(Error::not_found(format!("user {email}")));
    }
    Ok(HttpResponse::Ok())
}

/// A user's API keys (names and namespaces, never secrets).
#[get("/{email}/tokens")]
async fn list_user_tokens(
    service: web::Data<Service>,
    email: web::Path<String>,
) -> Result<Json<Vec<ApiKeyInfo>>, Error> {
    Ok(Json(service.list_user_tokens(&email).await?))
}

/// Revokes one of a user's API keys; it stops authenticating at once.
#[delete("/{email}/tokens/{name}")]
async fn delete_user_token(
    service: web::Data<Service>,
    path: web::Path<(String, String)>,
) -> Result<impl Responder, Error> {
    let (email, name) = path.into_inner();
    service.delete_user_token(&email, &name).await?;
    Ok(HttpResponse::Ok())
}

pub fn service() -> Scope {
    web::scope("/users")
        .service(create_user)
        .service(delete_user)
        .service(list_users)
        .service(list_user_permissions)
        .service(grant_user_permissions)
        .service(revoke_user_permissions)
        .service(update_user_permissions)
        .service(get_user_role)
        .service(set_user_role)
        .service(disable_user)
        .service(enable_user)
        .service(reset_user_password)
        .service(list_user_tokens)
        .service(delete_user_token)
}
