use super::Event;
use super::core::SharedConfig;
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use std::{sync::Arc, time::Duration};
use tokio::sync::{Semaphore, mpsc, oneshot, watch};

#[derive(Clone)]
struct Api {
    /// Replaced by membership reload, so revoked tokens stop authenticating.
    config: SharedConfig,
    tx: mpsc::Sender<Event>,
    uploads: Arc<Semaphore>,
}

pub(super) async fn serve<L>(
    listener: L,
    config: SharedConfig,
    tx: mpsc::Sender<Event>,
    mut stop: watch::Receiver<bool>,
) -> std::io::Result<()>
where
    L: axum::serve::Listener,
    L::Addr: std::fmt::Debug,
{
    let app = Router::new()
        .route("/v1/channels/{channel}/artifacts", post(upload))
        // A separate route: a server without staging rejects it before any
        // mutation, so a stage can never become an announced upload.
        .route("/v1/channels/{channel}/staged", post(stage))
        .route("/v1/artifacts/{id}", get(download))
        .with_state(Api {
            config,
            tx,
            uploads: Arc::new(Semaphore::new(8)),
        });
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = stop.changed().await;
        })
        .await
}

impl Api {
    fn config(&self) -> std::sync::Arc<crate::config::Config> {
        self.config
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

fn authenticate(config: &crate::config::Config, headers: &HeaderMap) -> Option<String> {
    let raw = headers
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    let decoded = if let Some(basic) = raw.strip_prefix("Basic ") {
        STANDARD
            .decode(basic)
            .ok()
            .and_then(|b| String::from_utf8(b).ok())
    } else {
        raw.strip_prefix("Bearer ").map(str::to_owned)
    };
    decoded
        .as_deref()
        .and_then(|s| s.split_once(':'))
        .and_then(|(name, token)| config.authenticate(name, token))
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [
            (
                header::WWW_AUTHENTICATE.as_str(),
                "Basic realm=\"AIRC artifacts\", charset=\"UTF-8\"",
            ),
            (REFUSAL, "unauthorized"),
        ],
        "Authentication required",
    )
        .into_response()
}

/// Marks an upload answer given before the request reached the store, so
/// nothing was stored. A status without it (a lost actor reply, a proxy
/// failure) proves nothing about the outcome.
const REFUSAL: &str = "x-airc-refusal";

fn refused(status: StatusCode, code: &'static str, reason: &'static str) -> Response {
    (status, [(REFUSAL, code)], reason).into_response()
}

async fn upload(
    State(api): State<Api>,
    Path(channel): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    accept(api, channel, headers, body, false).await
}

/// Store bytes unannounced for one later message to publish.
async fn stage(
    State(api): State<Api>,
    Path(channel): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    accept(api, channel, headers, body, true).await
}

async fn accept(
    api: Api,
    channel: String,
    headers: HeaderMap,
    body: Body,
    stage: bool,
) -> Response {
    // One snapshot authenticates and admits the request; its generation
    // travels with the body, so a deletion while the body arrives cannot
    // redirect it to a same-name successor.
    let config = api.config();
    let account = match authenticate(&config, &headers) {
        Some(account) => account,
        None => return unauthorized(),
    };
    if !config.may_read(&account, &channel) {
        return refused(StatusCode::NOT_FOUND, "not-found", "No such channel");
    }
    let admitted = config.membership_generation.unwrap_or(0);
    let filename = headers
        .get("x-airc-filename")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .to_owned();
    if filename.is_empty()
        || filename.len() > 100
        || !filename
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    {
        return refused(StatusCode::BAD_REQUEST, "filename", "Invalid filename");
    }
    let Ok(_permit) = api.uploads.clone().try_acquire_owned() else {
        return refused(
            StatusCode::SERVICE_UNAVAILABLE,
            "busy",
            "Uploads are busy; try later",
        );
    };
    let cap = config.artifacts.as_ref().unwrap().max_bytes;
    let bytes = match tokio::time::timeout(Duration::from_secs(30), to_bytes(body, cap)).await {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(_)) => {
            return refused(
                StatusCode::PAYLOAD_TOO_LARGE,
                "too-large",
                "Artifact too large",
            );
        }
        Err(_) => {
            return refused(
                StatusCode::REQUEST_TIMEOUT,
                "timeout",
                "Upload body timed out",
            );
        }
    };
    let (reply, rx) = oneshot::channel();
    if api
        .tx
        .send(Event::Artifact {
            account,
            channel,
            filename,
            data: bytes.to_vec(),
            stage,
            admitted,
            reply,
        })
        .await
        .is_err()
    {
        return refused(
            StatusCode::SERVICE_UNAVAILABLE,
            "stopping",
            "Server is stopping",
        );
    }
    // The actor stores nothing when it answers with an error.
    match rx.await {
        Ok(Ok(value)) => (StatusCode::CREATED, axum::Json(value)).into_response(),
        Ok(Err(e)) => match e.downcast_ref::<super::core::UploadRefusal>() {
            Some(refusal) => refused(
                StatusCode::from_u16(refusal.status).unwrap_or(StatusCode::FORBIDDEN),
                refusal.code,
                refusal.reason,
            ),
            None => refused(
                StatusCode::FORBIDDEN,
                "unavailable",
                "Upload unavailable; join with speaking permission",
            ),
        },
        // A lost reply: the outcome is unknown, so no refusal marker.
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

async fn download(State(api): State<Api>, Path(id): Path<String>, headers: HeaderMap) -> Response {
    let account = match authenticate(&api.config(), &headers) {
        Some(account) => account,
        None => return unauthorized(),
    };
    if uuid::Uuid::parse_str(&id).is_err() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let Ok(_permit) = api.uploads.clone().try_acquire_owned() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let (reply, rx) = oneshot::channel();
    if api
        .tx
        .send(Event::Download { account, id, reply })
        .await
        .is_err()
    {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    match rx.await {
        Ok(Ok(Some(artifact))) => (
            [
                (header::CONTENT_TYPE, "application/octet-stream".to_string()),
                (
                    header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{}\"", artifact.filename),
                ),
                (header::CACHE_CONTROL, "private, no-store".to_string()),
                (header::X_CONTENT_TYPE_OPTIONS, "nosniff".to_string()),
            ],
            artifact.data,
        )
            .into_response(),
        Ok(Ok(None)) => StatusCode::NOT_FOUND.into_response(),
        _ => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}
