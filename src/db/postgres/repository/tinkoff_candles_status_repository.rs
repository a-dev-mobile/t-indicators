use crate::db::postgres::connection::PostgresConnection;
use crate::db::postgres::models::tinkoff_candles_status::TinkoffCandlesStatus;
use async_trait::async_trait;
use sqlx::{Error as SqlxError, QueryBuilder};
use sqlx::postgres::PgQueryResult;
use std::sync::Arc;
use tracing::{debug, error, info};
use sqlx::types::chrono::{DateTime, Utc};

#[async_trait]
pub trait TraitTinkoffCandlesStatusRepository {
    /// Gets status record for a specific instrument
    async fn get_by_instrument_uid(&self, instrument_uid: &str) -> Result<Option<TinkoffCandlesStatus>, SqlxError>;
    
    /// Gets all status records
    async fn get_all(&self) -> Result<Vec<TinkoffCandlesStatus>, SqlxError>;
    
    /// Inserts a new record or updates an existing one
    async fn upsert(&self, instrument_uid: &str, to_second: i64) -> Result<PgQueryResult, SqlxError>;
    
    /// Updates the to_second value for an existing record
    async fn update_to_second(&self, instrument_uid: &str, to_second: i64) -> Result<PgQueryResult, SqlxError>;
    
    /// Deletes a status record
    async fn delete(&self, instrument_uid: &str) -> Result<PgQueryResult, SqlxError>;
}

pub struct StructTinkoffCandlesStatusRepository {
    connection: Arc<PostgresConnection>,
}

impl StructTinkoffCandlesStatusRepository {
    pub fn new(connection: Arc<PostgresConnection>) -> Self {
        Self { connection }
    }
}

#[async_trait]
impl TraitTinkoffCandlesStatusRepository for StructTinkoffCandlesStatusRepository {
    async fn get_by_instrument_uid(&self, instrument_uid: &str) -> Result<Option<TinkoffCandlesStatus>, SqlxError> {
        let pool = self.connection.get_pool();
        
        debug!("Fetching status for instrument_uid: {}", instrument_uid);
        
        let result = sqlx::query_as::<_, TinkoffCandlesStatus>(
            "SELECT instrument_uid, to_second, update_time 
             FROM market_data.tinkoff_candles_1min_status 
             WHERE instrument_uid = $1"
        )
        .bind(instrument_uid)
        .fetch_optional(pool)
        .await;
        
        match &result {
            Ok(Some(_)) => debug!("Found status for instrument_uid: {}", instrument_uid),
            Ok(None) => debug!("No status found for instrument_uid: {}", instrument_uid),
            Err(e) => error!("Error fetching status for instrument_uid {}: {}", instrument_uid, e),
        }
        
        result
    }
    
    async fn get_all(&self) -> Result<Vec<TinkoffCandlesStatus>, SqlxError> {
        let pool = self.connection.get_pool();
        
        debug!("Fetching all status records");
        
        let result = sqlx::query_as::<_, TinkoffCandlesStatus>(
            "SELECT instrument_uid, to_second, update_time 
             FROM market_data.tinkoff_candles_1min_status"
        )
        .fetch_all(pool)
        .await;
        
        match &result {
            Ok(records) => debug!("Fetched {} status records", records.len()),
            Err(e) => error!("Error fetching status records: {}", e),
        }
        
        result
    }
    
    async fn upsert(&self, instrument_uid: &str, to_second: i64) -> Result<PgQueryResult, SqlxError> {
        let pool = self.connection.get_pool();
        
        debug!("Upserting status for instrument_uid: {}, to_second: {}", instrument_uid, to_second);
        
        let result = sqlx::query(
            "INSERT INTO market_data.tinkoff_candles_1min_status (instrument_uid, to_second, update_time) 
             VALUES ($1, $2, NOW() AT TIME ZONE 'UTC') 
             ON CONFLICT (instrument_uid) 
             DO UPDATE SET to_second = $2, update_time = NOW() AT TIME ZONE 'UTC'"
        )
        .bind(instrument_uid)
        .bind(to_second)
        .execute(pool)
        .await;
        
        match &result {
            Ok(pg_result) => debug!("Upserted status for instrument_uid: {}, rows affected: {}", instrument_uid, pg_result.rows_affected()),
            Err(e) => error!("Error upserting status for instrument_uid {}: {}", instrument_uid, e),
        }
        
        result
    }
    
    async fn update_to_second(&self, instrument_uid: &str, to_second: i64) -> Result<PgQueryResult, SqlxError> {
        let pool = self.connection.get_pool();
        
        debug!("Updating to_second for instrument_uid: {}, new value: {}", instrument_uid, to_second);
        
        let result = sqlx::query(
            "UPDATE market_data.tinkoff_candles_1min_status 
             SET to_second = $2, update_time = NOW() AT TIME ZONE 'UTC' 
             WHERE instrument_uid = $1"
        )
        .bind(instrument_uid)
        .bind(to_second)
        .execute(pool)
        .await;
        
        match &result {
            Ok(pg_result) => {
                if pg_result.rows_affected() > 0 {
                    debug!("Updated to_second for instrument_uid: {}", instrument_uid);
                } else {
                    debug!("No record found to update for instrument_uid: {}", instrument_uid);
                }
            },
            Err(e) => error!("Error updating to_second for instrument_uid {}: {}", instrument_uid, e),
        }
        
        result
    }
    
    async fn delete(&self, instrument_uid: &str) -> Result<PgQueryResult, SqlxError> {
        let pool = self.connection.get_pool();
        
        debug!("Deleting status for instrument_uid: {}", instrument_uid);
        
        let result = sqlx::query(
            "DELETE FROM market_data.tinkoff_candles_1min_status 
             WHERE instrument_uid = $1"
        )
        .bind(instrument_uid)
        .execute(pool)
        .await;
        
        match &result {
            Ok(pg_result) => {
                if pg_result.rows_affected() > 0 {
                    debug!("Deleted status for instrument_uid: {}", instrument_uid);
                } else {
                    debug!("No record found to delete for instrument_uid: {}", instrument_uid);
                }
            },
            Err(e) => error!("Error deleting status for instrument_uid {}: {}", instrument_uid, e),
        }
        
        result
    }
}