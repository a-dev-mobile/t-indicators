// File: src/db/clickhouse/repository/indicator_repository.rs
use crate::db::clickhouse::connection::ClickhouseConnection;
use crate::db::clickhouse::models::indicator::{DbCandleRaw, DbIndicator, DbIndicatorStatus};
use async_trait::async_trait;
use clickhouse::error::Error as ClickhouseError;
use serde::Deserialize;
use std::sync::Arc;
use tracing::{debug, error, info};

#[async_trait]
pub trait IndicatorRepository {
    /// Получает необработанные свечи для указанного инструмента и временного диапазона
    async fn get_candles(
        &self,
        instrument_uid: &str,
        from_time: i64,
        to_time: i64,
    ) -> Result<Vec<DbCandleRaw>, ClickhouseError>;
    
    /// Вставляет рассчитанные индикаторы в БД
    async fn insert_indicators(
        &self,
        indicators: Vec<DbIndicator>,
    ) -> Result<u64, ClickhouseError>;
    
    /// Получает статус обработки для указанного инструмента
    async fn get_status(
        &self,
        instrument_uid: &str,
    ) -> Result<Option<DbIndicatorStatus>, ClickhouseError>;
    
    /// Обновляет статус обработки для указанного инструмента
    async fn update_status(
        &self,
        instrument_uid: &str,
        last_processed_time: i64,
    ) -> Result<(), ClickhouseError>;
    
    /// Получает список всех инструментов с свечами
    async fn get_all_instrument_uids(&self) -> Result<Vec<String>, ClickhouseError>;
}

pub struct ClickhouseIndicatorRepository {
    connection: Arc<ClickhouseConnection>,
}

impl ClickhouseIndicatorRepository {
    pub fn new(connection: Arc<ClickhouseConnection>) -> Self {
        Self { connection }
    }
}

#[async_trait]
impl IndicatorRepository for ClickhouseIndicatorRepository {
    async fn get_candles(
        &self,
        instrument_uid: &str,
        from_time: i64,
        to_time: i64,
    ) -> Result<Vec<DbCandleRaw>, ClickhouseError> {
        let client = self.connection.get_client();
        
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
            WHERE instrument_uid = '{}' AND time >= {} AND time <= {}
            ORDER BY time ASC",
            instrument_uid, from_time, to_time
        );
        
        debug!(
            "Fetching candles for instrument_uid={} from {} to {}",
            instrument_uid, from_time, to_time
        );
        
        let result = client.query(&query).fetch_all::<DbCandleRaw>().await?;
        
        info!(
            "Fetched {} candles for instrument_uid={}",
            result.len(),
            instrument_uid
        );
        
        Ok(result)
    }
    
    async fn insert_indicators(
        &self,
        indicators: Vec<DbIndicator>,
    ) -> Result<u64, ClickhouseError> {
        if indicators.is_empty() {
            debug!("No indicators to insert");
            return Ok(0);
        }
        
        let client = self.connection.get_client();
        const BATCH_SIZE: usize = 1000; // Оптимальный размер пакета для вставки
        
        let total_count = indicators.len();
        let mut successful_inserts = 0;
        
        info!("Starting batch insertion of {} indicators", total_count);
        
        // Обработка по пакетам
        for batch_start in (0..indicators.len()).step_by(BATCH_SIZE) {
            let batch_end = std::cmp::min(batch_start + BATCH_SIZE, indicators.len());
            let batch = &indicators[batch_start..batch_end];
            
            debug!(
                "Processing batch of {} indicators, {}/{}",
                batch.len(),
                batch_start + batch.len(),
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
                    successful_inserts += batch.len() as u64;
                    debug!(
                        "Successfully inserted batch of {} indicators ({}/{})",
                        batch.len(),
                        successful_inserts,
                        total_count
                    );
                }
                Err(e) => {
                    error!("Batch insertion failed: {}", e);
                    return Err(e);
                }
            }
        }
        
        info!(
            "Insertion complete. Successfully inserted {} indicators",
            successful_inserts
        );
        
        Ok(successful_inserts)
    }
    
    async fn get_status(
        &self,
        instrument_uid: &str,
    ) -> Result<Option<DbIndicatorStatus>, ClickhouseError> {
        let client = self.connection.get_client();
        
        let query = format!(
            "SELECT instrument_uid, last_processed_time, update_time
             FROM market_data.indicators_processing_status
             WHERE instrument_uid = '{}'",
            instrument_uid
        );
        
        debug!("Fetching status for instrument_uid={}", instrument_uid);
        
        let result = client.query(&query).fetch_optional::<DbIndicatorStatus>().await?;
        
        match &result {
            Some(status) => debug!(
                "Found status for instrument_uid={}, last_processed_time={}",
                instrument_uid, status.last_processed_time
            ),
            None => debug!("No status found for instrument_uid={}", instrument_uid),
        }
        
        Ok(result)
    }
    
    async fn update_status(
        &self,
        instrument_uid: &str,
        last_processed_time: i64,
    ) -> Result<(), ClickhouseError> {
        let client = self.connection.get_client();
        
        let query = format!(
            "INSERT INTO market_data.indicators_processing_status
             (instrument_uid, last_processed_time, update_time)
             VALUES ('{}', {}, now())
             SETTINGS optimize_on_insert=0",
            instrument_uid, last_processed_time
        );
        
        debug!(
            "Updating status for instrument_uid={}, last_processed_time={}",
            instrument_uid, last_processed_time
        );
        
        client.query(&query).execute().await?;
        
        debug!(
            "Status updated for instrument_uid={}, last_processed_time={}",
            instrument_uid, last_processed_time
        );
        
        Ok(())
    }
    
    async fn get_all_instrument_uids(&self) -> Result<Vec<String>, ClickhouseError> {
        let client = self.connection.get_client();
        
        // Упрощенный подход - используем простой SQL для получения уникальных инструментов
        let query = "SELECT DISTINCT instrument_uid FROM market_data.tinkoff_candles_1min";
        
        debug!("Fetching all instrument UIDs with candles");
        
        // Определяем структуру для результата с обоими необходимыми трейтами
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