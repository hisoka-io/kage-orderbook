mod handle;
mod maintenance;
mod operations;
mod runtime;

pub use super::state::{CreateOrderOutcome, OrderError, Orderbook, Transition, handle};
pub use handle::{OrderbookHandle, ServiceError};
pub use runtime::{
    start_orderbook, start_orderbook_with_repository, start_orderbook_with_repository_and_policy,
};

#[cfg(test)]
mod tests;
