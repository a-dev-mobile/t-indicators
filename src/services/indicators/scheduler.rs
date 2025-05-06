// File: src/services/indicators/scheduler.rs
use super::calculator::IndicatorCalculator;
use crate::app_state::models::AppState;
use chrono::{DateTime, Utc};
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
    // src/services/indicators/scheduler.rs (обновление метода trigger_update)

    pub async fn trigger_update(&self) -> Result<usize, Box<dyn std::error::Error>> {
        info!("Запуск обновления индикаторов для всех инструментов");

        // Создание калькулятора индикаторов
        let calculator = IndicatorCalculator::new(self.app_state.clone());

        // Обработка всех инструментов
        let total_processed = calculator.process_all_instruments().await?;

        info!(
            "Обновление индикаторов завершено. Обработано {} свечей",
            total_processed
        );

        Ok(total_processed)
    }
}
