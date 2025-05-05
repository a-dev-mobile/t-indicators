use super::models::app_env::{AppEnv, Env};
use std::env;
use std::str::FromStr;

impl AppEnv {
    pub fn new() -> AppEnv {
        AppEnv {
            env: Env::from_str(&get_env_var("ENV")).expect("Unknown environment"),
            server_port: get_env_var("SERVER_PORT")
                .parse()
                .expect("PORT must be a number"),
            server_address: get_env_var("SERVER_ADDRESS"),
            clickhouse_url: get_env_var("CLICKHOUSE_HOST"),
            clickhouse_user: get_env_var("CLICKHOUSE_USER"),
            clickhouse_password: get_env_var("CLICKHOUSE_PASSWORD"),
            clickhouse_database: get_env_var("CLICKHOUSE_DATABASE"),
            postgres_host: get_env_var("POSTGRES_HOST"),
            postgres_user: get_env_var("POSTGRES_USER"),
            postgres_password: get_env_var("POSTGRES_PASSWORD"),
            postgres_database: get_env_var("POSTGRES_DATABASE"),
        }
    }
}

impl Default for AppEnv {
    fn default() -> Self {
        Self::new()
    }
}

fn get_env_var(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("ENV -> {} is not set", name))
}
