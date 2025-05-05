use std::fmt;

use std::io::{Error, ErrorKind};
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};
/// Supported log format types
#[derive(Debug, Clone, PartialEq)]
pub enum LogFormat {
    Plain,
    Json,
}

impl fmt::Display for LogFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogFormat::Plain => write!(f, "plain"),
            LogFormat::Json => write!(f, "json"),
        }
    }
}

impl From<&str> for LogFormat {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "json" => LogFormat::Json,
            _ => LogFormat::Plain,
        }
    }
}

/// # Examples
///
/// ```rust
///  Initialize with INFO level and plain text format
/// init_logger("info", "plain").expect("Failed to initialize logger");
///
///  Initialize with DEBUG level and JSON format
/// init_logger("debug", "json").expect("Failed to initialize logger");

/// ```

pub fn init_logger(log_level: &str, log_format: &str) -> Result<(), Error> {
    // Parse and validate the log level, falling back to "info" if invalid
    let filter = EnvFilter::try_new(log_level)
        .map_err(|_| Error::new(ErrorKind::InvalidInput, "Invalid log level"))?;
    
    // Create the base formatter with common configuration
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        // Disable target field in output for cleaner logs
        .with_target(false)
        // Track span lifecycle events (creation and closure)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE);
    
    // Initialize the logger with the specified format
    let format = LogFormat::from(log_format);
    match format {
        LogFormat::Json => builder.json().init(),
        LogFormat::Plain => builder.init(),
    }
    
    Ok(())
    // Err(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_format_from_str() {
        assert_eq!(LogFormat::from("json"), LogFormat::Json);
        assert_eq!(LogFormat::from("JSON"), LogFormat::Json);
        assert_eq!(LogFormat::from("plain"), LogFormat::Plain);
        assert_eq!(LogFormat::from("invalid"), LogFormat::Plain);
    }

    #[test]
    fn test_init_logger() {
        // Test with valid configurations
        assert!(init_logger("debug", "plain").is_ok());
        assert!(init_logger("info", "json").is_ok());

        // Test with invalid log level (should fallback to info)
        assert!(init_logger("invalid_level", "plain").is_ok());
    }
}
