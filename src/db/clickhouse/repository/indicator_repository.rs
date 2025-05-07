// File: src/db/clickhouse/repository/indicator_repository.rs
use crate::db::clickhouse::connection::ClickhouseConnection;
use crate::db::clickhouse::models::indicator::{DbCandleRaw, DbIndicator, DbIndicatorStatus};
use async_trait::async_trait;
use clickhouse::error::Error as ClickhouseError;
use serde::Deserialize;
use std::sync::Arc;
use tracing::{debug, error, info};

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
        
        // Apply a hard limit to avoid memory issues
        let safe_limit = std::cmp::min(limit, 2000);
        
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
        
        let client = self.connection.get_client();
        // Reduced batch size to avoid memory limits
        const BATCH_SIZE: usize = 50;
        
        let total_count = indicators.len();
        let mut successful_inserts = 0;
        
        info!("Starting batch insertion of {} indicators", total_count);
        
        // Process in small batches to avoid hitting the memory limit
        for batch_start in (0..indicators.len()).step_by(BATCH_SIZE) {
            let batch_end = std::cmp::min(batch_start + BATCH_SIZE, indicators.len());
            let batch = &indicators[batch_start..batch_end];
            
            debug!(
                "Processing sub-batch of {} indicators, {}/{}",
                batch.len(),
                successful_inserts + batch.len(),
                total_count
            );
            
            // Формируем части VALUES для SQL запроса пакетной вставки
            let mut values_parts = Vec::with_capacity(batch.len());
            for indicator in batch {
                // Безопасное форматирование значений для SQL
                // Заменяем NaN и Infinity на NULL
                let rsi_14 = format_float_safe(indicator.rsi_14);
                let ma_10 = format_float_safe(indicator.ma_10);
                let ma_30 = format_float_safe(indicator.ma_30);
                let volume_norm = format_float_safe(indicator.volume_norm);
                let ma_diff = format_float_safe(indicator.ma_diff);
                let price_change_15m = format_float_safe(indicator.price_change_15m);
                
                values_parts.push(format!(
                    "('{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                    indicator.instrument_uid,
                    indicator.time,
                    indicator.open_price,
                    indicator.high_price,
                    indicator.low_price,
                    indicator.close_price,
                    indicator.volume,
                    rsi_14,
                    ma_10,
                    ma_30,
                    volume_norm,
                    ma_diff,
                    indicator.ma_cross,
                    indicator.rsi_zone,
                    indicator.volume_anomaly,
                    indicator.hour_of_day,
                    indicator.day_of_week,
                    price_change_15m,
                    indicator.signal_15m
                ));
            }
            
            // Формируем полный SQL-запрос для пакетной вставки
            let sql = format!(
                "INSERT INTO market_data.tinkoff_indicators_1min 
                (instrument_uid, time, open_price, high_price, low_price, close_price, volume, 
                 rsi_14, ma_10, ma_30, volume_norm, ma_diff, ma_cross, rsi_zone, volume_anomaly, 
                 hour_of_day, day_of_week, price_change_15m, signal_15m) 
                VALUES {}",
                values_parts.join(",")
            );
            
            // Выполняем пакетную вставку
            match client.query(&sql).execute().await {
                Ok(_) => {
                    // Успешная вставка пакета
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
                    if e.to_string().contains("TOO_MANY_PARTS") || e.to_string().contains("MEMORY_LIMIT_EXCEEDED") {
                    info!("Trying row-by-row insertion as fallback");
                    for indicator in batch {
                        let row_sql = format!(
                            "INSERT INTO market_data.tinkoff_indicators_1min 
                            (instrument_uid, time, open_price, high_price, low_price, close_price, volume, 
                            rsi_14, ma_10, ma_30, volume_norm, ma_diff, ma_cross, rsi_zone, volume_anomaly, 
                            hour_of_day, day_of_week, price_change_15m, signal_15m) 
                            VALUES ('{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                            indicator.instrument_uid,
                            indicator.time,
                            indicator.open_price,
                            indicator.high_price,
                            indicator.low_price,
                            indicator.close_price,
                            indicator.volume,
                            format_float_safe(indicator.rsi_14),
                            format_float_safe(indicator.ma_10),
                            format_float_safe(indicator.ma_30),
                            format_float_safe(indicator.volume_norm),
                            format_float_safe(indicator.ma_diff),
                            indicator.ma_cross,
                            indicator.rsi_zone,
                            indicator.volume_anomaly,
                            indicator.hour_of_day,
                            indicator.day_of_week,
                            format_float_safe(indicator.price_change_15m),
                            indicator.signal_15m
                        );
                        // Add sleep to avoid overwhelming the database
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                        match client.query(&row_sql).execute().await {
                            Ok(_) => {
                                successful_inserts += 1;
                }
                Err(row_err) => {
                    error!("Single row insertion failed: {}", row_err);
                    // If even single row insertion fails, we should consider taking a break
                    if row_err.to_string().contains("MEMORY_LIMIT_EXCEEDED") {
                        info!("Memory limit still exceeded, taking a break for 5 seconds");
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    }
                }
        }
                    }
                    debug!(
                        "Completed row-by-row insertion for batch, total successful: {}/{}",
                        successful_inserts,
                        total_count
        );
                } else {
                    // For other errors, return immediately
                    return Err(e);
                }
            }
        }
                    // Add a pause between batches to allow memory to be freed
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    info!(
        "Insertion complete. Successfully inserted {} indicators",
        successful_inserts
    );

    Ok(successful_inserts as u64)
}

    pub async fn get_all_instrument_uids(&self) -> Result<Vec<String>, clickhouse::error::Error> {
        let client = self.connection.get_client();
        
        // Упрощенный подход - используем простой SQL для получения уникальных инструментов
        let query = "SELECT DISTINCT instrument_uid FROM market_data.tinkoff_candles_1min";
        
        debug!("Fetching all instrument UIDs with candles");
        
        // Определяем структуру для результата
        #[derive(Debug, Deserialize, clickhouse::Row)]
        struct UidRow {
            instrument_uid: String,
        }
        
        let rows = client.query(query).fetch_all::<UidRow>().await?;
        
        // Преобразуем результат в Vec<String>
        let result: Vec<String> = rows.into_iter().map(|row| row.instrument_uid).collect();
        
        info!("Fetched {} instrument UIDs with candles", result.len());
        
        Ok(result)
    }
}

// Форматирует число с плавающей точкой для безопасной вставки в SQL
// Заменяет NaN и Infinity на NULL
fn format_float_safe(value: f64) -> String {
    if value.is_nan() || value.is_infinite() {
        "NULL".to_string()
    } else {
        value.to_string()
    }
}
