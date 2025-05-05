use axum::http::StatusCode;

pub async fn health_api() -> StatusCode {
    // info!("Handling test request");
    StatusCode::OK
}
