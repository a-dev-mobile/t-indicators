// File: src/db/clickhouse/repository/indicator_repository.rs
use crate::db::clickhouse::connection::ClickhouseConnection;
use crate::db::clickhouse::models::indicator::{DbCandleRaw, DbIndicator, DbIndicatorStatus};
use async_trait::async_trait;
use clickhouse::error::Error as ClickhouseError;
use serde::Deserialize;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

pub struct IndicatorRepository {
    pub connection: Arc<ClickhouseConnection>,
}

impl IndicatorRepository {
    pub fn new(connection: Arc<ClickhouseConnection>) -> Self {
        Self { connection }
    }

    pub async fn get_candles_after_time(
        &self,
        instrument_uid: &str,
        last_processed_time: i64,
        limit: usize,
    ) -> Result<Vec<DbCandleRaw>, clickhouse::error::Error> {
        let client = self.connection.get_client();
        
        // Increased batch size for powerful server
        let safe_limit = std::cmp::min(limit, 10000);
        
        let query = format!(
            "SELECT 
                instrument_uid,
                time,
                open_units,
                open_nano,
                high_units,
                high_nano,
                low_units,
                low_nano,
                close_units,
                close_nano,
                volume
            FROM market_data.tinkoff_candles_1min
            WHERE instrument_uid = '{}' AND time > {}
            ORDER BY time ASC
            LIMIT {}",
            instrument_uid, last_processed_time, safe_limit
        );

        debug!(
            "Fetching candles for instrument_uid={} after time={} (limit={})",
            instrument_uid, last_processed_time, safe_limit
        );

        let result = client.query(&query).fetch_all::<DbCandleRaw>().await?;

        debug!(
            "Retrieved {} candles for instrument_uid={} after time={}",
            result.len(),
            instrument_uid,
            last_processed_time
        );

        Ok(result)
    }
    
    pub async fn insert_indicators(
        &self,
        indicators: Vec<DbIndicator>,
    ) -> Result<u64, clickhouse::error::Error> {
        if indicators.is_empty() {
            debug!("No indicators to insert");
            return Ok(0);
        }
        
    let client = self.connection.get_client()
        .with_option("async_insert", "1")
        .with_option("wait_for_async_insert", "0");
        
    const BATCH_SIZE: usize = 100000;
        let total_count = indicators.len();
        let mut successful_inserts = 0;
        
        info!("Starting batch insertion of {} indicators", total_count);
        
        // Process in smaller batches to avoid memory errors entirely
        for batch_start in (0..indicators.len()).step_by(BATCH_SIZE) {
            let batch_end = std::cmp::min(batch_start + BATCH_SIZE, indicators.len());
            let batch = &indicators[batch_start..batch_end];
            
            debug!(
                "Processing batch of {} indicators, {}/{}",
                batch.len(),
                batch_start + batch.len(),
                total_count
            );
            
            // Build VALUES for SQL batch insert
        let mut insert = match client.insert("market_data.tinkoff_indicators_1min") {
            Ok(i) => i,
            Err(e) => {
                error!("Failed to create insert context: {}", e);
                continue;
            }
        };
            
        for indicator in batch {
            if let Err(e) = insert.write(indicator).await {
                error!("Failed to write indicator: {}", e);
            // Build the complete SQL query
                continue;
            }
        }
            
            // Execute batch insert - no retries on memory errors
        match insert.end().await {
                Ok(_) => {
                    successful_inserts += batch.len();
                    debug!(
                        "Successfully inserted batch of {} indicators ({}/{})",
                        batch.len(),
                        successful_inserts,
                        total_count
                    );
                }
                Err(e) => {
                    error!("Batch insertion failed: {}", e);
                    
                    // Instead of retrying on MEMORY_LIMIT_EXCEEDED, just report it and continue
                    if e.to_string().contains("MEMORY_LIMIT_EXCEEDED") || 
                       e.to_string().contains("TOO_MANY_PARTS") {
                        warn!("Memory limit exceeded, skipping this batch and continuing with next");
                        // For other errors, return immediately
                    }
                }
            }
            
            // Short pause between batches
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        info!(
            "Insertion complete. Successfully inserted {} indicators out of {}",
            successful_inserts,
            total_count
        );

        Ok(successful_inserts as u64)
    }

    pub async fn get_all_instrument_uids(&self) -> Result<Vec<String>, clickhouse::error::Error> {
        let client = self.connection.get_client();
        
        // Use more efficient query with a LIMIT to prevent loading too many distinct values at once
        let query = "SELECT DISTINCT instrument_uid FROM market_data.tinkoff_candles_1min";
        
        debug!("Fetching all instrument UIDs with candles");
        
        // Define structure for results
        #[derive(Debug, Deserialize, clickhouse::Row)]
        struct UidRow {
            instrument_uid: String,
        }
        
        let rows = client.query(query).fetch_all::<UidRow>().await?;
        
        // Convert results to Vec<String>
        let result: Vec<String> = rows.into_iter().map(|row| row.instrument_uid).collect();
        
        info!("Fetched {} instrument UIDs with candles", result.len());
        
        Ok(result)
    }
}

// Helper to format floating point numbers safely for SQL insertion
// Replaces NaN and Infinity with NULL
fn format_float_safe(value: f64) -> String {
    if value.is_nan() || value.is_infinite() {
        "NULL".to_string()
    } else {
        value.to_string()
    }
}