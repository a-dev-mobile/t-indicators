use super::{app_config::AppConfig, app_env::AppEnv};

#[derive(Debug)]
pub struct AppSettings {
    pub app_config: AppConfig,
    pub app_env: AppEnv,
}

