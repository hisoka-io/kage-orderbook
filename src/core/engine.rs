mod admission;
mod handle;
mod maintenance;
mod operations;
mod runtime;

pub use super::state::{CreateOrderOutcome, OrderError, Orderbook, Transition, handle};
pub use admission::AdmissionGate;
pub use handle::{OrderbookHandle, ServiceError};
pub use runtime::{
    OrderbookRuntime, start_orderbook_with_admission, start_supervised_orderbook_with_admission,
};

#[cfg(test)]
pub(crate) use runtime::{
    start_orderbook_with_repository, start_orderbook_with_repository_and_policy,
    start_supervised_orderbook_with_repository,
};

#[cfg(test)]
mod tests;
