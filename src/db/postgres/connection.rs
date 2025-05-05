use crate::env_config::models::app_setting::AppSettings;
use sqlx::{Pool, Postgres, postgres::PgPoolOptions};
use std::sync::Arc;
use tracing::{debug, error, info};

#[derive(Clone)]
pub struct PostgresConnection {
    pool: Pool<Postgres>,
}

impl PostgresConnection {
    pub async fn new(settings: Arc<AppSettings>) -> Result<Self, sqlx::Error> {
        info!("Initializing PostgreSQL connection...");

        // Create connection pool with the settings
        let connection_string = format!(
            "postgres://{}:{}@{}/{}",
            settings.app_env.postgres_user,
            settings.app_env.postgres_password,
            settings.app_env.postgres_host,
            settings.app_env.postgres_database
        );

        let pool = PgPoolOptions::new()
            .max_connections(settings.app_config.postgres.max_connections)
            .min_connections(settings.app_config.postgres.min_connections)
            .max_lifetime(std::time::Duration::from_secs(
                settings.app_config.postgres.max_lifetime,
            ))
            .idle_timeout(std::time::Duration::from_secs(
                settings.app_config.postgres.idle_timeout,
            ))
            .acquire_timeout(std::time::Duration::from_secs(
                settings.app_config.postgres.timeout,
            ))
            .connect(&connection_string)
            .await?;

        // Test connection
        debug!("Executing test query on PostgreSQL");
        match sqlx::query("SELECT 1").execute(&pool).await {
            Ok(_) => info!("PostgreSQL connection successful"),
            Err(e) => {
                error!("Failed to connect to PostgreSQL: {}", e);
                return Err(e);
            }
        }

        Ok(Self { pool })
    }

    pub fn get_pool(&self) -> &Pool<Postgres> {
        &self.pool
    }
}
