// src/db/postgres/models/indicator_status.rs
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct PgIndicatorStatus {
    pub instrument_uid: String,
    pub last_processed_time: i64,
    pub update_time: DateTime<Utc>,
}