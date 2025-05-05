use axum::{extract::Extension, http::StatusCode};
use std::sync::Arc;

use crate::app_state::models::AppState;

pub async fn health_db(
    Extension(app_state): Extension<Arc<AppState>>,
) -> Result<StatusCode, StatusCode> {
    // Check ClickHouse connection
    let client = app_state.clickhouse_service.connection.get_client();
    let clickhouse_ok = client.query("SELECT 1").execute().await.is_ok();

    // Check PostgreSQL connection
    let pg_health_check = app_state
        .postgres_service
        .repository_health_check
        .check()
        .await
        .is_ok();

    // Return OK only if the database is healthy
    if clickhouse_ok && pg_health_check {
        Ok(StatusCode::OK)
    } else {
        Err(StatusCode::INTERNAL_SERVER_ERROR)
    }
}
