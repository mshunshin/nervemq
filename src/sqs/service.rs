use std::rc::Rc;

use actix_web::{
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform},
    http::header::{HeaderName, HeaderValue},
    HttpMessage,
};

use crate::error::Error;

use super::{error::SqsError, method::Method};

pub struct SqsApi;

impl<S, B> Transform<S, ServiceRequest> for SqsApi
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = actix_web::Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;

    type Error = actix_web::Error;

    type Transform = SqsApiMiddleware<S>;

    type InitError = ();

    type Future = std::future::Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        std::future::ready(Ok(SqsApiMiddleware {
            service: Rc::new(service),
        }))
    }
}

pub struct SqsApiMiddleware<S> {
    service: Rc<S>,
}

impl<S, B> Service<ServiceRequest> for SqsApiMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = actix_web::Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = actix_web::Error;
    type Future =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>>>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let service = Rc::clone(&self.service);
        Box::pin(async move {
            let method = req
                .headers()
                .get(HeaderName::from_static("x-amz-target"))
                .ok_or_else(|| Error::InvalidHeader {
                    header: "X-Amz-Target".to_owned(),
                })
                // Not visible ASCII: the caller's mistake, not the server's.
                .and_then(|header| {
                    header.to_str().map_err(|_| Error::InvalidHeader {
                        header: "X-Amz-Target".to_owned(),
                    })
                })
                .and_then(Method::parse)
                .map_err(SqsError)?;

            req.extensions_mut().insert(method);

            service.call(req).await
        })
    }
}

/// Adds AWS's `x-amzn-RequestId` to every SQS response, errors and refused
/// authentication included. It is the id the request's span records as
/// `request_id`, so a client's report can be matched to the server's logs.
/// It must run inside `TracingLogger`, which mints the id.
pub async fn request_id_header<B: actix_web::body::MessageBody + 'static>(
    req: ServiceRequest,
    next: actix_web::middleware::Next<B>,
) -> Result<ServiceResponse<B>, actix_web::Error> {
    let id = super::error::is_sqs_path(req.path())
        .then(|| req.extensions().get::<tracing_actix_web::RequestId>().copied())
        .flatten()
        .and_then(|id| HeaderValue::from_str(&id.to_string()).ok());
    let Some(id) = id else {
        return next.call(req).await;
    };
    match next.call(req).await {
        Ok(mut res) => {
            res.headers_mut().insert(X_AMZN_REQUEST_ID, id);
            Ok(res)
        }
        Err(err) => {
            let mut response = err.error_response();
            response.headers_mut().insert(X_AMZN_REQUEST_ID, id);
            Err(actix_web::error::InternalError::from_response(err, response).into())
        }
    }
}

const X_AMZN_REQUEST_ID: HeaderName = HeaderName::from_static("x-amzn-requestid");
