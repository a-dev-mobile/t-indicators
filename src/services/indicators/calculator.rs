// File: src/services/indicators/calculator.rs
use crate::app_state::models::AppState;
use crate::db::clickhouse::models::indicator::{DbCandleConverted, DbCandleRaw, DbIndicator};
use crate::db::clickhouse::repository::indicator_repository::IndicatorRepository;
use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc, Weekday};
use std::collections::VecDeque;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

pub struct IndicatorCalculator {
    app_state: Arc<AppState>,
    batch_size: usize,
    window_size: usize,
}

impl IndicatorCalculator {
    pub fn new(app_state: Arc<AppState>) -> Self {
        // Use moderate batch size to avoid memory issues entirely
        let batch_size = 1500; // Balanced batch size to avoid memory errors
        let window_size = 50;  // Size of window for moving averages and RSI

        Self {
            app_state,
            batch_size,
            window_size,
        }
    }

    /// Clear indicators table before recalculation
    pub async fn truncate_indicators_table(&self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Clearing indicators table before update");
        let client = self.app_state.clickhouse_service.connection.get_client();
        let query = "TRUNCATE TABLE market_data.tinkoff_indicators_1min";
        
        match client.query(query).execute().await {
            Ok(_) => {
                info!("Indicators table successfully cleared");
                Ok(())
            }
            Err(e) => {
                error!("Error clearing indicators table: {}", e);
                Err(Box::new(e))
            }
        }
    }

    /// Process all instruments and calculate technical indicators
    pub async fn process_all_instruments(&self) -> Result<usize, Box<dyn std::error::Error>> {
        info!("Starting processing for all instruments from last processed time");

        // Очищаем таблицу индикаторов перед обновлением
        // self.truncate_indicators_table().await?;

                // Get repositories
        let indicator_repo = &self.app_state.clickhouse_service.repository_indicator;
        let status_repo = &self.app_state.postgres_service.repository_indicator_status;

        // Get all instruments with candles
        let instrument_uids = indicator_repo.get_all_instrument_uids().await?;
        if instrument_uids.is_empty() {
            info!("No instruments found for processing");
            return Ok(0);
        }

        info!("Found {} instruments for processing", instrument_uids.len());

        let is_status_table_empty = self.is_status_table_empty().await?;
        if is_status_table_empty {
            info!("Status table is empty, performing full recalculation");
            self.truncate_indicators_table().await?;
        } else {
            info!("Status table has records, continuing from last processed times");
        }

        let mut total_processed = 0;

        // Process each instrument sequentially - no parallelism
        for (index, instrument_uid) in instrument_uids.iter().enumerate() {
            info!(
                "Processing instrument {}/{}: {}",
                index + 1,
                instrument_uids.len(),
                instrument_uid
            );

            // Get the last processed time for this instrument
            let mut last_processed_time = status_repo
                .get_last_processed_time(instrument_uid)
                .await?
                .unwrap_or(0); // If no record exists, start from the beginning (time 0)

            info!(
                "Last processed time for instrument {}: {}",
                instrument_uid, last_processed_time
            );

            let mut processed_count = 0;

            loop {
                // Fetch candles after the last processed time
                let raw_candles = indicator_repo
                    .get_candles_after_time(instrument_uid, last_processed_time, self.batch_size)
                    .await?;

                if raw_candles.is_empty() {
                    debug!(
                        "No more candles found for instrument {} after time {}",
                        instrument_uid, last_processed_time
                    );
                    break;
                }

                let batch_count = raw_candles.len();

                // Update the latest time for this batch
                let latest_time = if let Some(last_candle) = raw_candles.last() {
                    last_candle.time
                } else {
                    continue; // Should never happen as we just checked if empty, but just in case
                };

                debug!("Latest time in current batch: {}", latest_time);

                // Convert raw candles to a more convenient format
                let converted_candles: Vec<DbCandleConverted> =
                    raw_candles.into_iter().map(|raw| raw.into()).collect();

                let indicators = {
                    // Calculate indicators for the batch
                    let window_data = if processed_count == 0 && last_processed_time > 0 {
                        // We need historical data for the first batch to calculate indicators correctly
                        self.fetch_historical_window(
                            indicator_repo,
                            instrument_uid,
                            last_processed_time,
                        )
                        .await?
                    } else {
                        Vec::new()
                    };

                    // Get window size before moving window_data
                    let window_end_idx = if !window_data.is_empty() {
                        window_data.len()
                    } else {
                        0
                    };

                    // Combine historical window with new data if needed
                    let calculation_data = if !window_data.is_empty() {
                        let mut combined = window_data;
                        combined.extend(converted_candles.iter().cloned());
                        combined
                    } else {
                        converted_candles.clone()
                    };
                    
                    self.calculate_indicators(&calculation_data, window_end_idx)
                };
                
                // Insert calculated indicators
                if !indicators.is_empty() {
                    match indicator_repo.insert_indicators(indicators).await {
                        Ok(inserted) => {
                            processed_count += inserted as usize;
                            debug!("Inserted {} indicators for {}", inserted, instrument_uid);
                        }
                        Err(e) => {
                            // Just log the error and continue with the next batch
                            error!("Failed to insert indicators for {}: {}", instrument_uid, e);
                        }
                    }
                }
                
                // Update last processed time
                if let Err(e) = status_repo.update_last_processed_time(instrument_uid, latest_time).await {
                    error!("Failed to update last processed time for {}: {}", instrument_uid, e);
                }
                
                // Update last processed time for next iteration
                last_processed_time = latest_time;
                
                // If we received fewer candles than batch size, we're done with this instrument
                if batch_count < self.batch_size {
                    break;
                }
                
                // Very short pause between batches
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
            
            total_processed += processed_count;
            
            info!(
                "Completed processing for instrument {}/{}: {}, processed {} candles",
                index + 1, instrument_uids.len(), instrument_uid, processed_count
            );
        }
        
        info!(
            "All instrument processing completed. Total processed: {} candles",
            total_processed
        );

        Ok(total_processed)
    }
    
    /// Checks if the tinkoff_indicators_status table is empty
    async fn is_status_table_empty(&self) -> Result<bool, Box<dyn std::error::Error>> {
        let pool = self.app_state.postgres_service.connection.get_pool();
        
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_data.tinkoff_indicators_status")
            .fetch_one(pool)
            .await?;
        
        Ok(count == 0)
    }
    
    /// Fetches historical data for calculating indicators
    async fn fetch_historical_window(
        &self,
        repo: &Arc<IndicatorRepository>,
        instrument_uid: &str,
        current_time: i64,
    ) -> Result<Vec<DbCandleConverted>, Box<dyn std::error::Error>> {
        let window_size = self.window_size;
        
        debug!(
            "Fetching historical window of size {} for instrument {} before time {}",
            window_size, instrument_uid, current_time
        );
        
        // Query to get the last N candles before the current time
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
            WHERE instrument_uid = '{}' AND time <= {}
            ORDER BY time DESC
            LIMIT {}",
            instrument_uid, current_time, window_size
        );
        
        let client = repo.connection.get_client();
        let result = client.query(&query).fetch_all::<DbCandleRaw>().await?;
        
        debug!(
            "Retrieved {} historical candles for instrument {} before time {}",
            result.len(),
            instrument_uid,
            current_time
        );
        
        // Convert and reverse to get candles in ascending time order
        let mut converted: Vec<DbCandleConverted> =
            result.into_iter().map(|raw| raw.into()).collect();
        converted.reverse();
        
        Ok(converted)
    }

    /// Calculate technical indicators for candles
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
        // Windows for moving averages and RSI calculation
        let mut prices_window: VecDeque<f64> = VecDeque::with_capacity(self.window_size);
        let mut rsi_gains: VecDeque<f64> = VecDeque::with_capacity(14);
        let mut rsi_losses: VecDeque<f64> = VecDeque::with_capacity(14);
        
        // Pre-fill windows with data for calculation
        for i in 0..window_end_idx {
            if i > 0 {
                // Calculate price change for RSI
                let price_change = candles[i].close_price - candles[i - 1].close_price;
                if price_change >= 0.0 {
                    rsi_gains.push_back(price_change);
                    rsi_losses.push_back(0.0);
                } else {
                    rsi_gains.push_back(0.0);
                    rsi_losses.push_back(-price_change);
                }
                // Limit RSI window size
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
        
        // Save previous ma_10 and ma_30 for crossing detection
        let mut prev_ma_10 = calculate_sma(prices_window.iter().cloned().collect::<Vec<f64>>(), 10);
        let mut prev_ma_30 = calculate_sma(prices_window.iter().cloned().collect::<Vec<f64>>(), 30);
        
        // Calculate volume standard deviation for anomaly detection
        let mut volume_stats = VolumeStatistics::new(50);
        for i in 0..window_end_idx {
            volume_stats.add(candles[i].volume as f64);
        }
        
        // Main indicator calculation for each candle
        for i in window_end_idx..candles.len() {
            let candle = &candles[i];
            
            // RSI calculation
            if i > 0 {
                let price_change = candle.close_price - candles[i - 1].close_price;
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

            // Update price window
            prices_window.push_back(candle.close_price);
            if prices_window.len() > self.window_size {
                prices_window.pop_front();
            }

            // Update volume statistics
            volume_stats.add(candle.volume as f64);

            // Calculate moving averages
            let prices_vec = prices_window.iter().cloned().collect::<Vec<f64>>();
            let ma_10 = calculate_sma(prices_vec.clone(), 10);
            let ma_30 = calculate_sma(prices_vec, 30);

            // Calculate RSI
            let rsi_14 = calculate_rsi(&rsi_gains, &rsi_losses);

            // Calculate derived metrics
            let ma_diff = ma_10 - ma_30;

            // Detect MA crossing
            let ma_cross = determine_ma_cross(prev_ma_10, prev_ma_30, ma_10, ma_30);

            // Update previous MA values
            prev_ma_10 = ma_10;
            prev_ma_30 = ma_30;

            // Determine RSI zone
            let rsi_zone = if rsi_14 < 30.0 {
                1
            } else if rsi_14 > 70.0 {
                -1
            } else {
                0
            };

            // Check volume anomaly
            let volume_norm = volume_stats.normalize(candle.volume as f64);
            let volume_anomaly = if volume_norm > 2.0 { 1 } else { 0 };

            // Calculate target variable (will be updated on next pass)
            let (price_change_15m, signal_15m) = if i + 15 < candles.len() {
                calculate_future_price_change(candle.close_price, candles[i + 15].close_price)
            } else {
                (0.0, 0)
            };

            // Get time features
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

            // Create indicator record
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

/// Helper structure for volume statistics
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
        // Add new value
        self.volumes.push_back(volume);
        self.sum += volume;
        self.sum_sq += volume * volume;

        // Remove old value if window size is exceeded
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

/// Calculate Simple Moving Average (SMA)
fn calculate_sma(prices: Vec<f64>, period: usize) -> f64 {
    if prices.is_empty() || period == 0 || prices.len() < period {
        return 0.0;
    }

    let start_idx = prices.len() - period;
    let sum: f64 = prices[start_idx..].iter().sum();

    sum / period as f64
}

/// Calculate RSI (Relative Strength Index)
fn calculate_rsi(gains: &VecDeque<f64>, losses: &VecDeque<f64>) -> f64 {
    if gains.len() < 14 || losses.len() < 14 {
        return 50.0; // Return neutral value if insufficient data
    }

    let avg_gain: f64 = gains.iter().sum::<f64>() / 14.0;
    let avg_loss: f64 = losses.iter().sum::<f64>() / 14.0;

    if avg_loss == 0.0 {
        return 100.0;
    }

    let rs = avg_gain / avg_loss;
    100.0 - (100.0 / (1.0 + rs))
}

/// Determine moving average crossing
fn determine_ma_cross(
    prev_ma_fast: f64,
    prev_ma_slow: f64,
    curr_ma_fast: f64,
    curr_ma_slow: f64,
) -> i8 {
    // Crossing from below (golden cross)
    if prev_ma_fast <= prev_ma_slow && curr_ma_fast > curr_ma_slow {
        return 1;
    }

    // Crossing from above (death cross)
    if prev_ma_fast >= prev_ma_slow && curr_ma_fast < curr_ma_slow {
        return -1;
    }

    // No crossing
    0
}

/// Calculate future price change and determine signal
fn calculate_future_price_change(current_price: f64, future_price: f64) -> (f64, i8) {
    if current_price == 0.0 {
        return (0.0, 0);
    }

    let price_change = ((future_price / current_price) - 1.0) * 100.0;

    let signal = if price_change > 0.2 {
        1 // Rise >0.2%
    } else if price_change < -0.2 {
        -1 // Fall >0.2%
    } else {
        0 // Sideways
    };

    (price_change, signal)
}
