use crate::db::postgres::repository::health_check_repository::TraitHealthCheckRepository;


use crate::db::postgres::{
    connection::PostgresConnection,
    repository::health_check_repository::StructHealthCheckRepository,

};
use crate::env_config::models::app_setting::AppSettings;
use std::sync::Arc;
use tracing::{error, info};

pub struct PostgresService {
    // Connection
    pub connection: Arc<PostgresConnection>,

    // Operational repositories (PostgreSQL)
    pub repository_health_check: Arc<dyn TraitHealthCheckRepository + Send + Sync>,


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
        ))
            as Arc<dyn TraitHealthCheckRepository + Send + Sync>;

     

        info!("PostgreSQL service initialized successfully");
        Ok(Self {
            connection: postgres_connection,
            repository_health_check: health_check_repository,

          
        })
    }
}
