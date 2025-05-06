
mod api;
mod app_state;
mod db;
mod env_config;
mod layers;
mod logger;
mod services;
mod utils;


use app_state::models::AppState;
use axum::{Router, routing::get};
use db::{
    clickhouse::clickhouse_service::{self, ClickhouseService},
    postgres::postgres_service::PostgresService,
};
use env_config::models::{app_config::AppConfig, app_env::AppEnv, app_setting::AppSettings};
use layers::{create_cors, create_trace};
use services::indicators::scheduler::IndicatorsScheduler;
use std::{net::SocketAddr, sync::Arc};
use tokio::{net::TcpListener, signal};
use tracing::{debug, error, info};

#[tokio::main]
async fn main() {
    // Инициализация приложения
    let settings: Arc<AppSettings> = Arc::new(initialize_application().await);
    
    // Подключение к базам данных
    let (clickhouse_service, postgres_service) =
        initialize_database_connections(settings.clone()).await;
    
    // Настройка адреса сервера
    let server_address: SocketAddr = format!(
        "{}:{}",
        settings.app_env.server_address, settings.app_env.server_port,
    )
    .parse()
    .expect("Invalid server address configuration");
    
    info!("Server will listen on: {}", server_address);
    
    // Создание глобального состояния приложения
    let app_state: Arc<AppState> = Arc::new(AppState {
        settings: settings.clone(),
        clickhouse_service: Arc::new(clickhouse_service),
        postgres_service: Arc::new(postgres_service),
    });
    
    // Инициализация и запуск фоновых сервисов
    initialize_background_services(app_state.clone()).await;
    
    // Создание API роутера
    let app_router = create_application_router(app_state.clone());
    
    // Запуск HTTP сервера
    start_http_server(app_router, server_address).await;
    
    info!("Application started successfully!");
}

/// Инициализирует настройки и логирование приложения
async fn initialize_application() -> AppSettings {
    // Загрузка переменных окружения и конфигурации
    let environment = AppEnv::new();
    let config = AppConfig::new(&environment.env);
    let app_settings = AppSettings {
        app_config: config,
        app_env: environment,
    };
    
    // Настройка логирования с уровнем и форматом из конфигурации
    logger::init_logger(
        &app_settings.app_config.log.level,
        &app_settings.app_config.log.format,
    )
    .expect("Failed to initialize logger");
    
    info!("Starting Indicators Service application...");
    info!("Current environment: {}", app_settings.app_env.env);
    
    // Добавление подробного логирования в режиме разработки
    if app_settings.app_env.is_local() {
        info!("Running in local development mode");
        debug!("Configuration details: {:#?}", app_settings);
    } else {
        info!("Running in production mode");
    }
    
    app_settings
}

/// Устанавливает соединения с базами данных
async fn initialize_database_connections(
    settings: Arc<AppSettings>,
) -> (ClickhouseService, PostgresService) {
    info!("Initializing database connections...");
    
    // Инициализация подключения к ClickHouse
    let clickhouse_service = match ClickhouseService::new(&settings).await {
        Ok(service) => {
            info!("ClickHouse connection established successfully");
            service
        }
        Err(err) => {
            error!("Failed to connect to ClickHouse: {}", err);
            panic!("Cannot continue without ClickHouse connection");
        }
    };
    
    // Инициализация подключения к PostgreSQL
    let postgres_service = match PostgresService::new(&settings).await {
        Ok(service) => {
            info!("PostgreSQL connection established successfully");
            service
        }
        Err(err) => {
            error!("Failed to connect to PostgreSQL: {}", err);
            panic!("Cannot continue without PostgreSQL connection");
        }
    };
    
    (clickhouse_service, postgres_service)
}

/// Создает API роутер со всеми эндпоинтами и middleware
fn create_application_router(app_state: Arc<AppState>) -> Router {
    Router::new()
        .layer(create_cors())
        .route("/api-health", get(api::health_api))
        .route("/db-health", get(api::health_db))
        .layer(axum::Extension(app_state.clone()))
        .layer(create_trace())
}

/// Запускает HTTP сервер на указанном адресе
async fn start_http_server(app: Router, addr: SocketAddr) {
    info!("Starting HTTP server on {}", addr);
    
    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(err) => {
            error!("Failed to bind to address {}: {}", addr, err);
            panic!("Cannot start server: {}", err);
        }
    };
    
    info!("Server started successfully, now accepting connections");
    
    if let Err(err) = axum::serve(listener, app).await {
        error!("Server error: {}", err);
        panic!("Server failed: {}", err);
    }
}

/// Инициализирует и запускает все фоновые сервисы
async fn initialize_background_services(app_state: Arc<AppState>) {
    // Инициализация планировщика индикаторов
    let indicators_scheduler = IndicatorsScheduler::new(app_state.clone());
    
    // Выполнение начального обновления индикаторов
    match indicators_scheduler.trigger_update().await {
        Ok(count) => info!("Initial indicators update completed: {} instruments processed", count),
        Err(err) => error!("Failed to perform initial indicators update: {}", err),
    }
    
    // Запуск планировщика для регулярных обновлений
    match indicators_scheduler.trigger_update().await {
        Ok(count) => info!("Scheduled indicators update completed: {} instruments processed", count),
        Err(err) => error!("Failed to perform scheduled indicators update: {}", err),
    }
    
    info!("Background services initialized successfully");
}