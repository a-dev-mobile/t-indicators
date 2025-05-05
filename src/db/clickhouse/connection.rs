use crate::env_config::models::app_setting::AppSettings;
use clickhouse::Client;
use std::sync::Arc;
use tracing::{debug, error, info};

#[derive(Clone)]
pub struct ClickhouseConnection {
    client: Client,
}

impl ClickhouseConnection {
    pub async fn new(settings: Arc<AppSettings>) -> Result<Self, clickhouse::error::Error> {
        info!("Initializing ClickHouse connection...");
        
        // Using the current AppEnv structure which only has clickhouse_url
        // We'll embed credentials in the URL instead of using with_user/with_password
        


        
        // Create client with the authenticated URL
        let client = Client::default()
            .with_url(&settings.app_env.clickhouse_url)
            .with_user(&settings.app_env.clickhouse_user)
            .with_password(&settings.app_env.clickhouse_password)
            .with_database(&settings.app_env.clickhouse_database)
            .with_option("connect_timeout", settings.app_config.clickhouse.timeout.to_string())
            .with_option("receive_timeout", settings.app_config.clickhouse.timeout.to_string())
            .with_option("send_timeout", settings.app_config.clickhouse.timeout.to_string());
            
      
            
        // Test connection
        let test_query = "SELECT 1";
        debug!("Executing test query: {}", test_query);
        
        match client.query(test_query).execute().await {
            Ok(_) => info!("ClickHouse connection successful"),
            Err(e) => {
                error!("Failed to connect to ClickHouse: {}", e);
                return Err(e);
            }
        }
        
        Ok(Self { client })
    }
    
    pub fn get_client(&self) -> Client {
        self.client.clone()
    }
}