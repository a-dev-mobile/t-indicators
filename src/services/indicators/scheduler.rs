// File: src/services/indicators/scheduler.rs
use super::calculator::IndicatorCalculator;
use crate::app_state::models::AppState;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, warn};

pub struct IndicatorsScheduler {
    app_state: Arc<AppState>,
}

impl IndicatorsScheduler {
    pub fn new(app_state: Arc<AppState>) -> Self {
        Self { app_state }
    }

    // Modified to implement basic retry and backoff
    pub async fn trigger_update(&self) -> Result<usize, Box<dyn std::error::Error>> {
        info!("Starting indicators update for all instruments");
        
        // Create indicator calculator
        let calculator = IndicatorCalculator::new(self.app_state.clone());
        
        // Initial attempt
        match calculator.process_all_instruments().await {
            Ok(count) => {
                info!("Indicators update completed successfully. Processed {} candles", count);
                return Ok(count);
            },
            Err(e) => {
                // Check if the error is memory-related
                let err_string = e.to_string();
                if err_string.contains("MEMORY_LIMIT_EXCEEDED") {
                    warn!("Memory limit exceeded during indicators update. Retrying with backoff: {}", e);
                    
                    // Implement backoff and retry logic
                    for attempt in 1..=3 {
                        // Exponential backoff: 5s, 10s, 20s
                        let backoff = 5 * 2u64.pow(attempt - 1);
                        info!("Waiting for {}s before retry attempt {}/3", backoff, attempt);
                        time::sleep(Duration::from_secs(backoff)).await;
                        
                        // Retry with a fresh calculator (which will have smaller batch sizes)
                        let retry_calculator = IndicatorCalculator::new(self.app_state.clone());
                        
                        match retry_calculator.process_all_instruments().await {
                            Ok(count) => {
                                info!("Retry succeeded on attempt {}. Processed {} candles", attempt, count);
                                return Ok(count);
                            },
                            Err(retry_err) => {
                                if retry_err.to_string().contains("MEMORY_LIMIT_EXCEEDED") {
                                    warn!("Memory limit still exceeded on retry attempt {}: {}", attempt, retry_err);
                                    // Continue to next retry
                                } else {
                                    // Different error, return it
                                    error!("New error occurred during retry: {}", retry_err);
                                    return Err(retry_err);
                                }
                            }
                        }
                    }
                    
                    // All retries failed
                    error!("All retry attempts failed due to memory limitations");
                    return Err(e);
                } else {
                    // Not a memory related error, just return it
                    error!("Error during indicators update: {}", e);
                    return Err(e);
                }
            }
        }
    }
}