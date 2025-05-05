use sqlx::types::chrono::{DateTime, Utc};
use sqlx::FromRow;

/// Represents a record in the tinkoff_candles_1min_status table
#[derive(Debug, FromRow)]
pub struct TinkoffCandlesStatus {
    /// Unique identifier for the financial instrument
    pub instrument_uid: String,
    
    /// Timestamp in seconds indicating the latest candle data
    pub to_second: i64,
    
    /// When this status was last updated
    pub update_time: DateTime<Utc>,
}