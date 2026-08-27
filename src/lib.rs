pub mod api;
pub mod assignment;
pub mod config;
pub mod core;
pub mod logging;
pub mod pricing;
pub mod readiness;
pub mod solver;
pub mod storage;

// Keep the established public paths while organizing their implementations by domain.
pub use core::order;
pub use solver::{registry, session};
