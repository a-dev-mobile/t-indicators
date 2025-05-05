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
    
    /// Запускает обновление индикаторов для всех инструментов
    pub async fn trigger_update(&self) -> Result<usize, Box<dyn std::error::Error>> {
        info!("Starting indicators update for all instruments");
        
        // Получение репозитория индикаторов
        let indicator_repo = &self.app_state.clickhouse_service.repository_indicator;
        
        // Получение всех инструментов с свечами
        let instrument_uids = indicator_repo.get_all_instrument_uids().await?;
        
        if instrument_uids.is_empty() {
            info!("No instruments found for processing");
            return Ok(0);
        }
        
        info!("Found {} instruments for processing", instrument_uids.len());
        
        // Создание калькулятора индикаторов
        let calculator = IndicatorCalculator::new(self.app_state.clone());
        
        let mut total_processed = 0;
        let mut processed_instruments = 0;
        
        // Получение текущего времени (конец обработки)
        let now = Utc::now();
        let end_time = now.timestamp();
        
        // Обработка каждого инструмента
        for (index, instrument_uid) in instrument_uids.iter().enumerate() {
            // Получение статуса обработки для инструмента
            let status = indicator_repo.get_status(instrument_uid).await?;
            
            // Определение времени начала обработки
            let start_time = match status {
                Some(status) => status.last_processed_time,
                None => {
                    // Для новых инструментов начинаем с времени, которое было 7 дней назад
                    (now - chrono::Duration::days(7)).timestamp()
                }
            };
            
            info!(
                "Processing instrument {}/{}: {}, from {} to {} ({})",
                index + 1,
                instrument_uids.len(),
                instrument_uid,
                start_time,
                end_time,
                format_time_range(start_time, end_time)
            );
            
            // Если нет новых данных для обработки, пропускаем
            if start_time >= end_time {
                debug!("No new data to process for instrument {}", instrument_uid);
                continue;
            }
            
            // Обработка индикаторов для инструмента
            match calculator
                .process_instrument(instrument_uid, start_time, end_time)
                .await
            {
                Ok(count) => {
                    total_processed += count;
                    processed_instruments += 1;
                    info!(
                        "Successfully processed {} indicators for instrument {}",
                        count, instrument_uid
                    );
                }
                Err(e) => {
                    error!("Error processing indicators for instrument {}: {}", instrument_uid, e);
                    // Продолжаем с следующим инструментом
                }
            }
            
            // Небольшая задержка между обработкой инструментов
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        
        info!(
            "Completed indicators update: processed {} indicators for {} instruments",
            total_processed, processed_instruments
        );
        
        Ok(processed_instruments)
    }
    
    /// Запускает планировщик для регулярного обновления индикаторов
    pub async fn start(&self) {
        if !self
            .app_state
            .settings
            .app_config
            .indicators_updater
            .enabled
        {
            info!("Indicators scheduler is disabled in configuration");
            return;
        }
        
        let updater_config = &self.app_state.settings.app_config.indicators_updater;
        
        // Вывод информации об окне работы, если оно настроено
        if let (Some(start), Some(end)) = (&updater_config.start_time, &updater_config.end_time) {
            info!(
                "Scheduler operation window configured: {} to {} UTC",
                start, end
            );
        }
        
        info!(
            "Starting indicators scheduler with {} second interval",
            updater_config.interval_seconds,
        );
        
        // Клонирование app_state для использования в задаче
        let app_state = self.app_state.clone();
        
        // Создание интервала из конфигурации
        let interval_seconds = updater_config.interval_seconds;
        
        // Запуск цикла с интервалом
        let mut interval = time::interval(Duration::from_secs(interval_seconds));
        
        // Основной цикл планировщика
        tokio::spawn(async move {
            loop {
                interval.tick().await;
                
                // Проверка, находимся ли мы в разрешенном окне работы
                let updater_config = &app_state.settings.app_config.indicators_updater;
                let operation_allowed = is_operation_allowed(updater_config);
                
                if !operation_allowed {
                    debug!(
                        "Scheduler: skipping update - outside operation window (current time: {})",
                        chrono::Utc::now().format("%H:%M:%S")
                    );
                    continue;
                }
                
                info!("Scheduler: triggering indicators update");
                
                // Создание нового экземпляра планировщика
                let scheduler = IndicatorsScheduler::new(app_state.clone());
                
                // Запуск обновления индикаторов
                match scheduler.trigger_update().await {
                    Ok(count) => info!(
                        "Scheduler: successfully updated indicators for {} instruments",
                        count
                    ),
                    Err(e) => error!("Scheduler: failed to update indicators: {}", e),
                }
            }
        });
        
        // Возвращаемся сразу после запуска фоновой задачи
    }
}

// Вспомогательная функция для проверки, разрешена ли работа в текущий момент
fn is_operation_allowed(
    updater_config: &crate::env_config::models::app_config::IndicatorsUpdaterConfig,
) -> bool {
    // Если окно времени не настроено, всегда разрешаем работу
    if updater_config.start_time.is_none() || updater_config.end_time.is_none() {
        return true;
    }
    
    // Получение текущего времени UTC
    let now = chrono::Utc::now().time();
    
    // Парсинг начального и конечного времени
    if let (Some(start_str), Some(end_str)) = (&updater_config.start_time, &updater_config.end_time) {
        if let (Ok(start), Ok(end)) = (
            chrono::NaiveTime::parse_from_str(start_str, "%H:%M:%S"),
            chrono::NaiveTime::parse_from_str(end_str, "%H:%M:%S"),
        ) {
            // Проверка, находится ли текущее время в окне работы
            if start <= end {
                // Простой случай: начальное время до конечного
                return start <= now && now <= end;
            } else {
                // Случай, когда окно работы пересекает полночь
                // например, start=21:00:00, end=04:00:00
                return start <= now || now <= end;
            }
        }
    }
    
    // Если парсинг не удался, по умолчанию разрешаем работу
    true
}

// Форматирование временного диапазона в удобочитаемую строку
fn format_time_range(start_timestamp: i64, end_timestamp: i64) -> String {
    let start = DateTime::<Utc>::from_timestamp(start_timestamp, 0).unwrap_or_default();
    let end = DateTime::<Utc>::from_timestamp(end_timestamp, 0).unwrap_or_default();
    
    format!(
        "{} to {}",
        start.format("%Y-%m-%d %H:%M:%S"),
        end.format("%Y-%m-%d %H:%M:%S")
    )
}