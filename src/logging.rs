use uuid::Uuid;

pub fn timestamp() -> String {
    chrono::Local::now().format("%H:%M:%S%.3f").to_string()
}

pub fn short_id(id: Uuid) -> String {
    id.to_string()[..5].to_owned()
}

#[macro_export]
macro_rules! service_log {
    ($service:expr, $($arg:tt)*) => {
        println!(
            "[{}][{}] {}",
            $crate::logging::timestamp(),
            $service,
            format_args!($($arg)*)
        )
    };
}

#[macro_export]
macro_rules! service_error {
    ($service:expr, $($arg:tt)*) => {
        eprintln!(
            "[{}][{}] {}",
            $crate::logging::timestamp(),
            $service,
            format_args!($($arg)*)
        )
    };
}
