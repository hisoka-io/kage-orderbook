use std::fmt;

use tracing_subscriber::{
    EnvFilter,
    fmt::{format::Writer, time::FormatTime},
};
use uuid::Uuid;

struct UtcTimer;

impl FormatTime for UtcTimer {
    fn format_time(&self, writer: &mut Writer<'_>) -> fmt::Result {
        write!(
            writer,
            "{}",
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ")
        )
    }
}

pub fn init() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,kage_registry=warn"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_timer(UtcTimer)
        .with_target(true)
        .compact()
        .init();
}

pub fn short_id(id: Uuid) -> String {
    id.to_string()[..5].to_owned()
}

#[macro_export]
macro_rules! service_log {
    ($service:expr, $($arg:tt)*) => {
        tracing::info!(target: $service, "{}", format_args!($($arg)*))
    };
}

#[macro_export]
macro_rules! service_warn {
    ($service:expr, $($arg:tt)*) => {
        tracing::warn!(target: $service, "{}", format_args!($($arg)*))
    };
}

#[macro_export]
macro_rules! service_error {
    ($service:expr, $($arg:tt)*) => {
        tracing::error!(target: $service, "{}", format_args!($($arg)*))
    };
}
