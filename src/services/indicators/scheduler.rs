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

    // Simplified implementation without unnecessary retries
    pub async fn trigger_update(&self) -> Result<usize, Box<dyn std::error::Error>> {
        info!("Starting indicators update for all instruments");
        
        // Create indicator calculator with conservative batch sizes
        let calculator = IndicatorCalculator::new(self.app_state.clone());
        
        // Process all instruments - no retries on memory errors since we use smaller batches by default
        match calculator.process_all_instruments().await {
            Ok(count) => {
                info!("Indicators update completed successfully. Processed {} candles", count);
                Ok(count)
            },
            Err(e) => {
                error!("Error during indicators update: {}", e);
                Err(e)
            }
        }
    }
    
    // Start a regular scheduled update process
    pub async fn start_scheduled_updates(&self) {
        info!("Starting scheduled indicator updates");
        
        // Get the update interval from settings
        let interval_seconds = self.app_state.settings.app_config.indicators_updater.interval_seconds;
        info!("Update interval set to {} seconds", interval_seconds);
        
        // Create a new task for the scheduler
        let app_state = self.app_state.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(interval_seconds));
            
            loop {
                interval.tick().await;
                
                // Check if updates are enabled in config
                if !app_state.settings.app_config.indicators_updater.enabled {
                    debug!("Indicator updates are disabled in config, skipping");
                    continue;
                }
                
                // Check if current time is within the allowed operation window
                if !app_state.settings.app_config.indicators_updater.is_operation_allowed() {
                    debug!("Outside operation window, skipping update");
                    continue;
                }
                
                info!("Executing scheduled indicator update");
                
                // Create a new scheduler and trigger the update
                let scheduler = IndicatorsScheduler::new(app_state.clone());
                match scheduler.trigger_update().await {
                    Ok(count) => {
                        info!("Scheduled indicators update completed: {} candles processed", count);
                    }
                    Err(e) => {
                        error!("Scheduled indicators update failed: {}", e);
                    }
                }
            }
        });
        
        info!("Scheduled update task started");
    }
}