use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

pub use kage_types::health::ReadinessSnapshot;
use tokio::task::JoinHandle;

use crate::{
    core::engine::OrderbookHandle,
    pricing::{PricingHandle, PricingStatus},
    registry::SolverRegistry,
};

#[derive(Default)]
struct ReadinessState {
    pricing: AtomicBool,
    registry: AtomicBool,
    engine: AtomicBool,
    solvers: AtomicUsize,
    chain: AtomicBool,
    reported: AtomicBool,
    announced_ready: AtomicBool,
}

#[derive(Clone, Default)]
pub struct ServiceReadiness {
    state: Arc<ReadinessState>,
}

pub(crate) struct SolverConnection {
    readiness: ServiceReadiness,
}

impl ServiceReadiness {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> ReadinessSnapshot {
        let pricing = self.state.pricing.load(Ordering::Acquire);
        let registry = self.state.registry.load(Ordering::Acquire);
        let engine = self.state.engine.load(Ordering::Acquire);
        let solver = self.state.solvers.load(Ordering::Acquire) > 0;
        let chain = self.state.chain.load(Ordering::Acquire);
        let mut missing = Vec::new();
        if !pricing {
            missing.push("pricing".to_owned());
        }
        if !registry {
            missing.push("registry".to_owned());
        }
        if !solver {
            missing.push("solver".to_owned());
        }
        if !chain {
            missing.push("chain".to_owned());
        }
        if !engine {
            missing.push("orderbook_engine".to_owned());
        }

        ReadinessSnapshot {
            ready: missing.is_empty(),
            pricing,
            registry,
            solver,
            chain,
            actor: engine,
            missing,
        }
    }

    pub fn monitor_pricing(&self, pricing: PricingHandle, interval: Duration) -> JoinHandle<()> {
        self.set_pricing(pricing.status() == PricingStatus::Ready);
        let readiness = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                readiness.set_pricing(pricing.status() == PricingStatus::Ready);
            }
        })
    }

    pub fn monitor_embedded_pricing(
        &self,
        pricing: crate::pricing::EmbeddedPricing,
        interval: Duration,
    ) -> JoinHandle<()> {
        use kage_price_estimate::oracle::PricingStatus as EmbeddedStatus;

        self.set_pricing(matches!(pricing.status(), EmbeddedStatus::Ready));
        let readiness = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                readiness.set_pricing(matches!(pricing.status(), EmbeddedStatus::Ready));
            }
        })
    }

    pub fn monitor_registry(&self, registry: SolverRegistry, interval: Duration) -> JoinHandle<()> {
        self.set_registry(registry.health().is_ok());
        let readiness = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                readiness.set_registry(registry.health().is_ok());
            }
        })
    }

    pub fn monitor_engine(&self, orderbook: OrderbookHandle, interval: Duration) -> JoinHandle<()> {
        self.set_engine(orderbook.is_available());
        let readiness = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                readiness.set_engine(orderbook.is_available());
            }
        })
    }

    pub(crate) fn solver_connection(&self) -> SolverConnection {
        self.state.solvers.fetch_add(1, Ordering::AcqRel);
        self.state_changed();
        SolverConnection {
            readiness: self.clone(),
        }
    }

    #[cfg(test)]
    pub(crate) fn always_ready() -> Self {
        let readiness = Self::new();
        readiness.state.pricing.store(true, Ordering::Release);
        readiness.state.registry.store(true, Ordering::Release);
        readiness.state.engine.store(true, Ordering::Release);
        readiness.state.solvers.store(1, Ordering::Release);
        readiness.state.chain.store(true, Ordering::Release);
        readiness
    }

    pub(crate) fn set_pricing(&self, ready: bool) {
        self.set_dependency(&self.state.pricing, ready);
    }

    pub(crate) fn set_registry(&self, ready: bool) {
        self.set_dependency(&self.state.registry, ready);
    }

    pub(crate) fn set_engine(&self, ready: bool) {
        self.set_dependency(&self.state.engine, ready);
    }

    pub fn set_chain(&self, ready: bool) {
        self.set_dependency(&self.state.chain, ready);
    }

    pub fn report(&self) {
        self.state.reported.store(true, Ordering::Release);
        let snapshot = self.snapshot();
        self.state
            .announced_ready
            .store(snapshot.ready, Ordering::Release);
        if snapshot.ready {
            tracing::info!(target: "readiness", "accepting orders");
        } else {
            tracing::warn!(
                target: "readiness",
                missing = %snapshot.missing.join(","),
                "not accepting orders"
            );
        }
    }

    fn set_dependency(&self, status: &AtomicBool, ready: bool) {
        if status.swap(ready, Ordering::AcqRel) != ready {
            self.state_changed();
        }
    }

    fn state_changed(&self) {
        if !self.state.reported.load(Ordering::Acquire) {
            return;
        }
        let snapshot = self.snapshot();
        if snapshot.ready {
            if !self.state.announced_ready.swap(true, Ordering::AcqRel) {
                tracing::info!(target: "readiness", "accepting orders");
            }
        } else if self.state.announced_ready.swap(false, Ordering::AcqRel) {
            tracing::warn!(
                target: "readiness",
                missing = %snapshot.missing.join(","),
                "not accepting orders"
            );
        }
    }
}

impl Drop for SolverConnection {
    fn drop(&mut self) {
        self.readiness.state.solvers.fetch_sub(1, Ordering::AcqRel);
        self.readiness.state_changed();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_missing_dependencies_and_connection_lifecycle() {
        let readiness = ServiceReadiness::new();
        assert_eq!(
            readiness.snapshot().missing,
            vec!["pricing", "registry", "solver", "chain", "orderbook_engine"]
        );

        readiness.set_pricing(true);
        readiness.set_registry(true);
        readiness.set_engine(true);
        readiness.set_chain(true);
        let solver = readiness.solver_connection();
        assert!(readiness.snapshot().ready);

        readiness.set_chain(false);
        assert_eq!(readiness.snapshot().missing, vec!["chain"]);
        drop(solver);
        assert_eq!(readiness.snapshot().missing, vec!["solver", "chain"]);
    }
}
