use crate::db::postgres::repository::health_check_repository::TraitHealthCheckRepository;
use crate::db::postgres::repository::tinkoff_candles_status_repository::TraitTinkoffCandlesStatusRepository;
use crate::db::postgres::{
    connection::PostgresConnection,
    repository::health_check_repository::StructHealthCheckRepository,
    repository::tinkoff_candles_status_repository::StructTinkoffCandlesStatusRepository,
};

use crate::env_config::models::app_setting::AppSettings;
use std::sync::Arc;
use tracing::{error, info};

pub struct PostgresService {
    // Connection
    pub connection: Arc<PostgresConnection>,

    // Operational repositories (PostgreSQL)
    pub repository_health_check: Arc<dyn TraitHealthCheckRepository + Send + Sync>,
    pub repository_tinkoff_candles_status: Arc<dyn TraitTinkoffCandlesStatusRepository + Send + Sync>,
    // Add other PostgreSQL repositories here as needed
    // Example: pub user_repository: Arc<dyn UserRepository + Send + Sync>,
    // Example: pub order_repository: Arc<dyn OrderRepository + Send + Sync>,
}

impl PostgresService {
    pub async fn new(settings: &Arc<AppSettings>) -> Result<Self, Box<dyn std::error::Error>> {
        info!("Initializing PostgreSQL service components");

        // Initialize PostgreSQL connection
        info!("Creating PostgreSQL connection");
        let postgres_connection = match PostgresConnection::new(settings.clone()).await {
            Ok(conn) => {
                info!("PostgreSQL connection established successfully");
                Arc::new(conn)
            }
            Err(e) => {
                error!("Failed to establish PostgreSQL connection: {}", e);
                return Err(Box::new(e));
            }
        };

        // Initialize repositories
        info!("Initializing repositories");
        let health_check_repository = Arc::new(StructHealthCheckRepository::new(
            postgres_connection.clone(),
        )) as Arc<dyn TraitHealthCheckRepository + Send + Sync>;

        let tinkoff_candles_status_repository = Arc::new(StructTinkoffCandlesStatusRepository::new(
            postgres_connection.clone(),
        )) as Arc<dyn TraitTinkoffCandlesStatusRepository + Send + Sync>;

        // Initialize any other repositories here
        // Example:
        // let user_repository = Arc::new(PgUserRepository::new(
        //    postgres_connection.clone(),
        // )) as Arc<dyn UserRepository + Send + Sync>;

        info!("PostgreSQL service initialized successfully");
        Ok(Self {
            connection: postgres_connection,
            repository_health_check: health_check_repository,
            repository_tinkoff_candles_status: tinkoff_candles_status_repository,
            // Add other repositories here as they are implemented
            // Example: user_repository,
        })
    }

    // Add any service-level methods here that might coordinate between repositories

  
}
