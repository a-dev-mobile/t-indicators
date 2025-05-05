// settings.rs
use super::models::app_config::AppConfig;
use super::models::app_env::Env;
use std::fs;
use std::path::Path;
use toml;

impl AppConfig {
    pub fn new(env: &Env) -> Self {
        

        Self::load_config(env).expect("Failed to load configuration")
    }

    fn load_config(env: &Env) -> Result<AppConfig, Box<dyn std::error::Error>> {
        let config_path = format!("config/{}.toml", env);
        let path = Path::new(&config_path);

        let content = fs::read_to_string(path)?;
        let config: AppConfig = toml::from_str(&content)?;

        Ok(config)
    }

}
