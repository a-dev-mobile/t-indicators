// File: src/db/clickhouse/models/indicator.rs
use clickhouse::Row;
use serde::{Deserialize, Serialize};

/// Структура для хранения рассчитанных технических индикаторов
#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct DbIndicator {
    // Базовые поля для идентификации
    pub instrument_uid: String,
    pub time: i64,
    
    // Базовые цены
    pub open_price: f64,
    pub high_price: f64,
    pub low_price: f64,
    pub close_price: f64,
    pub volume: i64,
    
    // Технические индикаторы
    pub rsi_14: f64,
    pub ma_10: f64,
    pub ma_30: f64,
    pub volume_norm: f64,
    
    // Производные признаки
    pub ma_diff: f64,
    pub ma_cross: i8,
    pub rsi_zone: i8,
    pub volume_anomaly: i8,
    
    // Дополнительные признаки времени
    pub hour_of_day: i8,
    pub day_of_week: i8,
    
    // Целевая переменная
    pub price_change_15m: f64,
    pub signal_15m: i8,
}

/// Структура для хранения исходных данных минутной свечи
#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct DbCandleRaw {
    pub instrument_uid: String,
    pub time: i64,
    pub open_units: i64,
    pub open_nano: i32,
    pub high_units: i64,
    pub high_nano: i32,
    pub low_units: i64,
    pub low_nano: i32,
    pub close_units: i64,
    pub close_nano: i32,
    pub volume: i64,
}

/// Структура для хранения конвертированных данных минутной свечи
#[derive(Debug, Clone)]
pub struct DbCandleConverted {
    pub instrument_uid: String,
    pub time: i64,
    pub open_price: f64,
    pub high_price: f64,
    pub low_price: f64,
    pub close_price: f64,
    pub volume: i64,
}

impl From<DbCandleRaw> for DbCandleConverted {
    fn from(raw: DbCandleRaw) -> Self {
        Self {
            instrument_uid: raw.instrument_uid,
            time: raw.time,
            open_price: convert_price(raw.open_units, raw.open_nano),
            high_price: convert_price(raw.high_units, raw.high_nano),
            low_price: convert_price(raw.low_units, raw.low_nano),
            close_price: convert_price(raw.close_units, raw.close_nano),
            volume: raw.volume,
        }
    }
}

/// Преобразует units/nano в значение с плавающей точкой
fn convert_price(units: i64, nano: i32) -> f64 {
    units as f64 + (nano as f64 / 1_000_000_000.0)
}

/// Структура для статуса обработки индикаторов
#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct DbIndicatorStatus {
    pub instrument_uid: String,
    pub last_processed_time: i64,
    pub update_time: chrono::DateTime<chrono::Utc>,
}