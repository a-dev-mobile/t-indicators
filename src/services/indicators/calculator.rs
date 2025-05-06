
// File: src/services/indicators/calculator.rs
use crate::app_state::models::AppState;
use crate::db::clickhouse::models::indicator::{DbCandleConverted, DbCandleRaw, DbIndicator};
use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc, Weekday};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

pub struct IndicatorCalculator {
    app_state: Arc<AppState>,
    batch_size: usize,
    window_size: usize,
}

impl IndicatorCalculator {
    pub fn new(app_state: Arc<AppState>) -> Self {
        // Параметры для расчетов
        let batch_size = 10000;   // Обрабатываем свечей за раз 
        let window_size = 50;     // Размер окна для скользящих средних и RSI
        
        Self {
            app_state,
            batch_size,
            window_size,
        }
    }
    
    /// Очищает таблицу индикаторов перед новым расчетом
    pub async fn truncate_indicators_table(&self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Очистка таблицы индикаторов перед обновлением");
        
        // Получаем клиент ClickHouse
        let client = self.app_state.clickhouse_service.connection.get_client();
        
        // SQL запрос для очистки таблицы
        let query = "TRUNCATE TABLE market_data.tinkoff_indicators_1min";
        
        // Выполняем запрос
        match client.query(query).execute().await {
            Ok(_) => {
                info!("Таблица индикаторов успешно очищена");
                Ok(())
            },
            Err(e) => {
                error!("Ошибка при очистке таблицы индикаторов: {}", e);
                Err(Box::new(e))
            }
        }
    }
    
    /// Обрабатывает все инструменты и рассчитывает технические индикаторы
    pub async fn process_all_instruments(&self) -> Result<usize, Box<dyn std::error::Error>> {
        info!("Начало обработки всех инструментов");
        
        // Очищаем таблицу индикаторов перед обновлением
        self.truncate_indicators_table().await?;
        
        // Получаем репозиторий индикаторов
        let indicator_repo = &self.app_state.clickhouse_service.repository_indicator;
        
        // Получаем все инструменты с свечами
        let instrument_uids = indicator_repo.get_all_instrument_uids().await?;
        
        if instrument_uids.is_empty() {
            info!("Инструменты для обработки не найдены");
            return Ok(0);
        }
        
        info!("Найдено {} инструментов для обработки", instrument_uids.len());
        
        let mut total_processed = 0;
        
        // Обработка каждого инструмента
        for (index, instrument_uid) in instrument_uids.iter().enumerate() {
            info!(
                "Обработка инструмента {}/{}: {}",
                index + 1,
                instrument_uids.len(),
                instrument_uid
            );
            
            // Получаем все свечи для инструмента
            let mut offset = 0;
            let mut processed_count = 0;
            
            loop {
                // Получаем партию свечей из БД (с учетом дополнительного окна для расчета)
                let raw_candles = indicator_repo
                    .get_candles_batch(instrument_uid, offset, self.batch_size)
                    .await?;
                
                if raw_candles.is_empty() {
                    break;
                }
                
                let batch_count = raw_candles.len();
                
                // Конвертируем сырые данные свечей в более удобный формат
                let converted_candles: Vec<DbCandleConverted> = raw_candles
                    .into_iter()
                    .map(|raw| raw.into())
                    .collect();
                
                // Рассчитываем индикаторы для партии
                // В этом случае window_end_idx=0, так как мы собираем историю в каждой партии
                let indicators = self.calculate_indicators(&converted_candles, 0);
                
                // Вставляем рассчитанные индикаторы в БД
                let inserted = indicator_repo.insert_indicators(indicators).await?;
                
                processed_count += inserted as usize;
                
                info!(
                    "Обработана партия: {} свечей, всего для инструмента: {} (инструмент {}/{})",
                    batch_count, processed_count, index + 1, instrument_uids.len()
                );
                
                // Увеличиваем смещение для следующей партии
                offset += self.batch_size;
                
                // Если получили меньше свечей, чем размер партии, значит это последняя партия
                if batch_count < self.batch_size {
                    break;
                }
            }
            
            total_processed += processed_count;
            
            info!(
                "Завершена обработка инструмента {}/{}: {}, обработано {} свечей",
                index + 1, instrument_uids.len(), instrument_uid, processed_count
            );
        }
        
        info!(
            "Обработка всех инструментов завершена. Всего обработано: {} свечей",
            total_processed
        );
        
        Ok(total_processed)
    }
    
    /// Рассчитывает технические индикаторы на основе конвертированных свечей
    /// window_end_idx - индекс первой свечи после окна предварительных данных
    fn calculate_indicators(
        &self,
        candles: &[DbCandleConverted],
        window_end_idx: usize,
    ) -> Vec<DbIndicator> {
        if candles.len() <= self.window_size {
            debug!("Not enough candles for indicator calculation");
            return Vec::new();
        }
        
        let mut result = Vec::with_capacity(candles.len() - window_end_idx);
        
        // Окна для расчета скользящих средних и RSI
        let mut prices_window: VecDeque<f64> = VecDeque::with_capacity(self.window_size);
        let mut rsi_gains: VecDeque<f64> = VecDeque::with_capacity(14);
        let mut rsi_losses: VecDeque<f64> = VecDeque::with_capacity(14);
        
        // Предварительное заполнение окон данными для расчета
        for i in 0..window_end_idx {
            if i > 0 {
                // Рассчитываем изменение цены для RSI
                let price_change = candles[i].close_price - candles[i-1].close_price;
                if price_change >= 0.0 {
                    rsi_gains.push_back(price_change);
                    rsi_losses.push_back(0.0);
                } else {
                    rsi_gains.push_back(0.0);
                    rsi_losses.push_back(-price_change);
                }
                
                // Ограничиваем размер окна для RSI
                if rsi_gains.len() > 14 {
                    rsi_gains.pop_front();
                    rsi_losses.pop_front();
                }
            }
            
            prices_window.push_back(candles[i].close_price);
            if prices_window.len() > self.window_size {
                prices_window.pop_front();
            }
        }
        
        // Сохраняем предыдущую ма_10 и ма_30 для определения пересечений
        let mut prev_ma_10 = calculate_sma(prices_window.iter().cloned().collect::<Vec<f64>>(), 10);
        let mut prev_ma_30 = calculate_sma(prices_window.iter().cloned().collect::<Vec<f64>>(), 30);
        
        // Расчет стандартного отклонения объемов для определения аномалий
        let mut volume_stats = VolumeStatistics::new(50);
        for i in 0..window_end_idx {
            volume_stats.add(candles[i].volume as f64);
        }
        
        // Основной расчет индикаторов для каждой свечи
        for i in window_end_idx..candles.len() {
            let candle = &candles[i];
            
            // Расчет RSI
            if i > 0 {
                let price_change = candle.close_price - candles[i-1].close_price;
                if price_change >= 0.0 {
                    rsi_gains.push_back(price_change);
                    rsi_losses.push_back(0.0);
                } else {
                    rsi_gains.push_back(0.0);
                    rsi_losses.push_back(-price_change);
                }
                
                if rsi_gains.len() > 14 {
                    rsi_gains.pop_front();
                    rsi_losses.pop_front();
                }
            }
            
            // Обновление окна цен
            prices_window.push_back(candle.close_price);
            if prices_window.len() > self.window_size {
                prices_window.pop_front();
            }
            
            // Обновление статистики объемов
            volume_stats.add(candle.volume as f64);
            
            // Расчет скользящих средних
            let prices_vec = prices_window.iter().cloned().collect::<Vec<f64>>();
            let ma_10 = calculate_sma(prices_vec.clone(), 10);
            let ma_30 = calculate_sma(prices_vec, 30);
            
            // Расчет RSI
            let rsi_14 = calculate_rsi(&rsi_gains, &rsi_losses);
            
            // Расчет производных показателей
            let ma_diff = ma_10 - ma_30;
            
            // Определение пересечения MA
            let ma_cross = determine_ma_cross(prev_ma_10, prev_ma_30, ma_10, ma_30);
            
            // Обновление предыдущих значений MA
            prev_ma_10 = ma_10;
            prev_ma_30 = ma_30;
            
            // Определение зоны RSI
            let rsi_zone = if rsi_14 < 30.0 { 1 } else if rsi_14 > 70.0 { -1 } else { 0 };
            
            // Проверка аномалии объема
            let volume_norm = volume_stats.normalize(candle.volume as f64);
            let volume_anomaly = if volume_norm > 2.0 { 1 } else { 0 };
            
            // Расчет целевой переменной (будет обновлена при следующем проходе)
            let (price_change_15m, signal_15m) = if i + 15 < candles.len() {
                calculate_future_price_change(candle.close_price, candles[i + 15].close_price)
            } else {
                (0.0, 0)
            };
            
            // Получение признаков времени
            let dt = DateTime::<Utc>::from_timestamp(candle.time, 0).unwrap_or_default();
            let hour_of_day = dt.hour() as i8;
            let day_of_week = match dt.weekday() {
                Weekday::Mon => 1,
                Weekday::Tue => 2,
                Weekday::Wed => 3,
                Weekday::Thu => 4,
                Weekday::Fri => 5,
                Weekday::Sat => 6,
                Weekday::Sun => 7,
            };
            
            // Создание записи индикатора
            let indicator = DbIndicator {
                instrument_uid: candle.instrument_uid.clone(),
                time: candle.time,
                open_price: candle.open_price,
                high_price: candle.high_price,
                low_price: candle.low_price,
                close_price: candle.close_price,
                volume: candle.volume,
                rsi_14,
                ma_10,
                ma_30,
                volume_norm,
                ma_diff,
                ma_cross,
                rsi_zone,
                volume_anomaly,
                hour_of_day,
                day_of_week,
                price_change_15m,
                signal_15m,
            };
            
            result.push(indicator);
        }
        
        result
    }
}

/// Вспомогательная структура для хранения статистики объемов
struct VolumeStatistics {
    volumes: VecDeque<f64>,
    window_size: usize,
    sum: f64,
    sum_sq: f64,
}

impl VolumeStatistics {
    fn new(window_size: usize) -> Self {
        Self {
            volumes: VecDeque::with_capacity(window_size),
            window_size,
            sum: 0.0,
            sum_sq: 0.0,
        }
    }
    
    fn add(&mut self, volume: f64) {
        // Добавление нового значения
        self.volumes.push_back(volume);
        self.sum += volume;
        self.sum_sq += volume * volume;
        
        // Удаление старого значения, если превышен размер окна
        if self.volumes.len() > self.window_size {
            let old_value = self.volumes.pop_front().unwrap_or(0.0);
            self.sum -= old_value;
            self.sum_sq -= old_value * old_value;
        }
    }
    
    fn mean(&self) -> f64 {
        if self.volumes.is_empty() {
            return 0.0;
        }
        self.sum / self.volumes.len() as f64
    }
    
    fn stddev(&self) -> f64 {
        if self.volumes.len() <= 1 {
            return 0.0;
        }
        
        let n = self.volumes.len() as f64;
        let variance = (self.sum_sq - (self.sum * self.sum) / n) / (n - 1.0);
        
        if variance <= 0.0 {
            return 0.0;
        }
        
        variance.sqrt()
    }
    
    fn normalize(&self, value: f64) -> f64 {
        let mean = self.mean();
        let stddev = self.stddev();
        
        if stddev == 0.0 {
            return 0.0;
        }
        
        (value - mean) / stddev
    }
}

/// Вычисляет простую скользящую среднюю (SMA)
fn calculate_sma(prices: Vec<f64>, period: usize) -> f64 {
    if prices.is_empty() || period == 0 || prices.len() < period {
        return 0.0;
    }
    
    let start_idx = prices.len() - period;
    let sum: f64 = prices[start_idx..].iter().sum();
    
    sum / period as f64
}

/// Вычисляет RSI (Relative Strength Index)
fn calculate_rsi(gains: &VecDeque<f64>, losses: &VecDeque<f64>) -> f64 {
    if gains.len() < 14 || losses.len() < 14 {
        return 50.0; // Возвращаем нейтральное значение, если недостаточно данных
    }
    
    let avg_gain: f64 = gains.iter().sum::<f64>() / 14.0;
    let avg_loss: f64 = losses.iter().sum::<f64>() / 14.0;
    
    if avg_loss == 0.0 {
        return 100.0;
    }
    
    let rs = avg_gain / avg_loss;
    100.0 - (100.0 / (1.0 + rs))
}

/// Определяет пересечение скользящих средних
fn determine_ma_cross(prev_ma_fast: f64, prev_ma_slow: f64, curr_ma_fast: f64, curr_ma_slow: f64) -> i8 {
    // Пересечение снизу вверх (golden cross)
    if prev_ma_fast <= prev_ma_slow && curr_ma_fast > curr_ma_slow {
        return 1;
    }
    
    // Пересечение сверху вниз (death cross)
    if prev_ma_fast >= prev_ma_slow && curr_ma_fast < curr_ma_slow {
        return -1;
    }
    
    // Нет пересечения
    0
}

/// Расчет изменения цены в будущем и определение сигнала
fn calculate_future_price_change(current_price: f64, future_price: f64) -> (f64, i8) {
    if current_price == 0.0 {
        return (0.0, 0);
    }
    
    let price_change = ((future_price / current_price) - 1.0) * 100.0;
    
    let signal = if price_change > 0.2 {
        1 // Рост >0.2%
    } else if price_change < -0.2 {
        -1 // Падение >0.2%
    } else {
        0 // Боковик
    };
    
    (price_change, signal)
}