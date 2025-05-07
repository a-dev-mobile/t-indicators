// src/db/postgres/repository/indicator_status_repository.rs
use crate::db::postgres::connection::PostgresConnection;
use crate::db::postgres::models::indicator_status::PgIndicatorStatus;
use async_trait::async_trait;
use sqlx::Error as SqlxError;
use std::sync::Arc;
use tracing::{debug, info};

#[async_trait]
pub trait TraitIndicatorStatusRepository {
    async fn get_last_processed_time(&self, instrument_uid: &str) -> Result<Option<i64>, SqlxError>;
    async fn update_last_processed_time(&self, instrument_uid: &str, time: i64) -> Result<(), SqlxError>;
}

pub struct StructIndicatorStatusRepository {
    connection: Arc<PostgresConnection>,
}

impl StructIndicatorStatusRepository {
    pub fn new(connection: Arc<PostgresConnection>) -> Self {
        Self { connection }
    }
}

#[async_trait]
impl TraitIndicatorStatusRepository for StructIndicatorStatusRepository {
    async fn get_last_processed_time(&self, instrument_uid: &str) -> Result<Option<i64>, SqlxError> {
        let pool = self.connection.get_pool();
        
        let result = sqlx::query_scalar::<_, i64>(
            "SELECT last_processed_time FROM market_data.tinkoff_indicators_status WHERE instrument_uid = $1"
        )
        .bind(instrument_uid)
        .fetch_optional(pool)
        .await?;
        
        debug!("Retrieved last processed time for {}: {:?}", instrument_uid, result);
        
        Ok(result)
    }
    
    async fn update_last_processed_time(&self, instrument_uid: &str, time: i64) -> Result<(), SqlxError> {
        let pool = self.connection.get_pool();
        
        sqlx::query(
            "INSERT INTO market_data.tinkoff_indicators_status (instrument_uid, last_processed_time, update_time) 
             VALUES ($1, $2, NOW()) 
             ON CONFLICT (instrument_uid) 
             DO UPDATE SET last_processed_time = $2, update_time = NOW()"
        )
        .bind(instrument_uid)
        .bind(time)
        .execute(pool)
        .await?;
        
        info!("Updated last processed time for {}: {}", instrument_uid, time);
        
        Ok(())
    }
}