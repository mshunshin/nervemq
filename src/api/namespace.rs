//! Namespaces: `/api/admin/ns`, open to any logged-in user. Each route
//! checks its own rule: only admins create namespaces or change owners;
//! admins and owners delete them; members only list them.

use actix_identity::Identity;
use actix_web::{delete, get, post, put, web, HttpResponse, Responder, Scope};
use serde::{Deserialize, Serialize};
use serde_email::Email;

use crate::{
    error::Error,
    namespace::Namespace,
    service::{NamespaceMember, Service},
};

/// The namespaces the caller can access (all of them for an admin).
#[get("")]
async fn list_namespaces(
    service: web::Data<Service>,
    identity: Identity,
) -> Result<web::Json<Vec<Namespace>>, Error> {
    Ok(web::Json(service.list_namespaces(identity).await?))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateNamespaceResponse {
    id: u64,
}

/// Creates a namespace, owned by the creating admin. Admins only.
#[post("/{ns_name}")]
async fn create_namespace(
    service: web::Data<Service>,
    path: web::Path<String>,
    identity: Identity,
) -> Result<impl Responder, Error> {
    let id = service.create_namespace(&path, identity).await?;

    Ok(web::Json(CreateNamespaceResponse { id }))
}

/// Deletes a namespace and everything in it. Admins and owners only.
#[delete("/{ns_name}")]
async fn delete_namespace(
    service: web::Data<Service>,
    path: web::Path<String>,
    identity: Identity,
) -> Result<impl Responder, Error> {
    service.delete_namespace(&path, identity).await?;

    Ok("OK")
}

/// The namespace's members and which of them own it. Admins and owners only.
#[get("/{ns_name}/members")]
async fn list_members(
    service: web::Data<Service>,
    path: web::Path<String>,
    identity: Identity,
) -> Result<web::Json<Vec<NamespaceMember>>, Error> {
    let ns_id = service
        .get_namespace_id(&path, service.db())
        .await?
        .ok_or_else(|| Error::namespace_not_found(&*path))?;
    if !service
        .check_user_access(&identity, ns_id, service.db())
        .await?
        .can_manage()
    {
        return Err(Error::forbidden(format!(
            "only admins and owners of namespace {path} can list its members"
        )));
    }

    Ok(web::Json(service.list_namespace_members(&path).await?))
}

fn parse_email(email: &str) -> Result<Email, Error> {
    Email::from_str(email).map_err(|e| Error::invalid_parameter(format!("email: {e}")))
}

/// Makes a user an owner, granting them access if they had none. Admins only.
#[put("/{ns_name}/owners/{email}")]
async fn add_owner(
    service: web::Data<Service>,
    path: web::Path<(String, String)>,
    identity: Identity,
) -> Result<impl Responder, Error> {
    service.require_admin(&identity).await?;
    let (namespace, email) = path.into_inner();

    service
        .set_namespace_owner(&namespace, &parse_email(&email)?, true)
        .await?;

    Ok(HttpResponse::Ok())
}

/// Stops a user being an owner; they keep access as a member. Admins only.
#[delete("/{ns_name}/owners/{email}")]
async fn remove_owner(
    service: web::Data<Service>,
    path: web::Path<(String, String)>,
    identity: Identity,
) -> Result<impl Responder, Error> {
    service.require_admin(&identity).await?;
    let (namespace, email) = path.into_inner();

    service
        .set_namespace_owner(&namespace, &parse_email(&email)?, false)
        .await?;

    Ok(HttpResponse::Ok())
}

pub fn service() -> Scope {
    web::scope("/ns")
        .service(list_namespaces)
        .service(create_namespace)
        .service(delete_namespace)
        .service(list_members)
        .service(add_owner)
        .service(remove_owner)
}
