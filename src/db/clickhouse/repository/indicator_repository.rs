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

    pub async fn get_candles_batch(
        &self,
        instrument_uid: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<DbCandleRaw>, clickhouse::error::Error> {
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
        WHERE instrument_uid = '{}'
        ORDER BY time ASC
        LIMIT {} OFFSET {}",
            instrument_uid, limit, offset
        );

        debug!(
            "Получение партии свечей для instrument_uid={} (limit={}, offset={})",
            instrument_uid, limit, offset
        );

        let result = client.query(&query).fetch_all::<DbCandleRaw>().await?;

        debug!(
            "Получено {} свечей для instrument_uid={}",
            result.len(),
            instrument_uid
        );

        Ok(result)
    }

    pub async fn get_candles_after_time(
        &self,
        instrument_uid: &str,
        last_processed_time: i64,
        limit: usize,
    ) -> Result<Vec<DbCandleRaw>, clickhouse::error::Error> {
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
            WHERE instrument_uid = '{}' AND time > {}
            ORDER BY time ASC
            LIMIT {}",
            instrument_uid, last_processed_time, limit
        );

        debug!(
            "Fetching candles for instrument_uid={} after time={} (limit={})",
            instrument_uid, last_processed_time, limit
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
        const BATCH_SIZE: usize = 1000;
    const MAX_PARTITIONS_PER_BATCH: usize = 50; // Keep well under the 100 limit

        let total_count = indicators.len();
        let mut successful_inserts = 0;

        info!("Starting batch insertion of {} indicators", total_count);

        // Обработка по пакетам
    let mut grouped_indicators: std::collections::HashMap<String, Vec<DbIndicator>> = 
        std::collections::HashMap::new();
    for indicator in indicators {
        grouped_indicators
            .entry(indicator.instrument_uid.clone())
            .or_insert_with(Vec::new)
            .push(indicator);
    }
    let mut partition_batch = Vec::new();
    let mut current_batch_partitions = 0;
    for (instrument_uid, indicators_for_instrument) in grouped_indicators {
        if current_batch_partitions >= MAX_PARTITIONS_PER_BATCH && !partition_batch.is_empty() {
            let inserted = self.process_indicator_batch(&client, partition_batch, total_count, successful_inserts).await?;
            successful_inserts += inserted;
            partition_batch = Vec::new();
            current_batch_partitions = 0;
        }
        partition_batch.push((instrument_uid, indicators_for_instrument));
        current_batch_partitions += 1;
    }
    if !partition_batch.is_empty() {
        let inserted = self.process_indicator_batch(&client, partition_batch, total_count, successful_inserts).await?;
        successful_inserts += inserted;
    }
    info!(
        "Insertion complete. Successfully inserted {} indicators",
        successful_inserts
    );
    Ok(successful_inserts)
}
async fn process_indicator_batch(
    &self,
    client: &clickhouse::Client,
    partition_batch: Vec<(String, Vec<DbIndicator>)>,
    total_count: usize,
    current_total: u64,
) -> Result<u64, clickhouse::error::Error> {
    const BATCH_SIZE: usize = 1000;
    let mut successful_inserts = 0;
    let mut all_indicators = Vec::new();
    for (_, indicators) in partition_batch.iter() {
        all_indicators.extend(indicators.iter().cloned());
    }
    debug!(
        "Processing batch with {} partitions, {} total indicators", 
        partition_batch.len(),
        all_indicators.len()
    );
    for batch_start in (0..all_indicators.len()).step_by(BATCH_SIZE) {
        let batch_end = std::cmp::min(batch_start + BATCH_SIZE, all_indicators.len());
        let batch = &all_indicators[batch_start..batch_end];

            debug!(
            "Processing sub-batch of {} indicators, {}/{}",
                batch.len(),
            current_total + successful_inserts + batch.len() as u64,
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
                    current_total + successful_inserts,
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
