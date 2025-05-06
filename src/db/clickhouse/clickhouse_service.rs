use crate::db::clickhouse::connection::ClickhouseConnection;
use crate::db::clickhouse::repository::indicator_repository::IndicatorRepository;
use crate::env_config::models::app_setting::AppSettings;
use std::sync::Arc;
use tracing::{error, info};

pub struct ClickhouseService {
    // Соединения
    pub connection: Arc<ClickhouseConnection>,
    // Аналитические репозитории (ClickHouse)
    pub repository_indicator: Arc<IndicatorRepository>,
}

impl ClickhouseService {
    pub async fn new(settings: &Arc<AppSettings>) -> Result<Self, Box<dyn std::error::Error>> {
        info!("Initializing database service components");
        
        // Инициализация соединения с ClickHouse
        info!("Creating ClickHouse connection");
        let clickhouse_connection = match ClickhouseConnection::new(settings.clone()).await {
            Ok(conn) => {
                info!("ClickHouse connection established successfully");
                Arc::new(conn)
            }
            Err(e) => {
                error!("Failed to establish ClickHouse connection: {}", e);
                return Err(Box::new(e));
            }
        };
        
        // Инициализация аналитических репозиториев (ClickHouse)
        info!("Initialize repositories (ClickHouse)");
        
        let indicator_repository = Arc::new(IndicatorRepository::new(
            clickhouse_connection.clone(),
        ));
        
        info!("Database service initialized successfully");
        
        Ok(Self {
            connection: clickhouse_connection,

            repository_indicator: indicator_repository,
        })
    }
}