use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use serde::{Deserialize, Serialize};
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
    actor: AtomicBool,
    solvers: AtomicUsize,
    chains: AtomicUsize,
}

#[derive(Clone, Default)]
pub struct ServiceReadiness {
    state: Arc<ReadinessState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadinessSnapshot {
    pub ready: bool,
    pub pricing: bool,
    pub registry: bool,
    pub solver: bool,
    pub chain: bool,
    pub actor: bool,
    pub missing: Vec<String>,
}

#[derive(Clone, Copy)]
enum ConnectionKind {
    Solver,
    Chain,
}

pub(crate) struct ConnectionGuard {
    readiness: ServiceReadiness,
    kind: ConnectionKind,
}

impl ServiceReadiness {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> ReadinessSnapshot {
        let pricing = self.state.pricing.load(Ordering::Acquire);
        let registry = self.state.registry.load(Ordering::Acquire);
        let actor = self.state.actor.load(Ordering::Acquire);
        let solver = self.state.solvers.load(Ordering::Acquire) > 0;
        let chain = self.state.chains.load(Ordering::Acquire) > 0;
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
        if !actor {
            missing.push("actor".to_owned());
        }

        ReadinessSnapshot {
            ready: missing.is_empty(),
            pricing,
            registry,
            solver,
            chain,
            actor,
            missing,
        }
    }

    pub fn monitor_pricing(&self, pricing: PricingHandle, interval: Duration) -> JoinHandle<()> {
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

    pub fn monitor_registry(&self, registry: SolverRegistry, interval: Duration) -> JoinHandle<()> {
        let readiness = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let healthy = tokio::time::timeout(interval.period(), registry.health())
                    .await
                    .is_ok_and(|result| result.is_ok());
                readiness.set_registry(healthy);
            }
        })
    }

    pub fn monitor_actor(&self, orderbook: OrderbookHandle, interval: Duration) -> JoinHandle<()> {
        let readiness = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                readiness.set_actor(orderbook.is_available());
            }
        })
    }

    pub(crate) fn solver_connection(&self) -> ConnectionGuard {
        self.state.solvers.fetch_add(1, Ordering::AcqRel);
        ConnectionGuard {
            readiness: self.clone(),
            kind: ConnectionKind::Solver,
        }
    }

    pub(crate) fn chain_connection(&self) -> ConnectionGuard {
        self.state.chains.fetch_add(1, Ordering::AcqRel);
        ConnectionGuard {
            readiness: self.clone(),
            kind: ConnectionKind::Chain,
        }
    }

    pub(crate) fn always_ready() -> Self {
        let readiness = Self::new();
        readiness.state.pricing.store(true, Ordering::Release);
        readiness.state.registry.store(true, Ordering::Release);
        readiness.state.actor.store(true, Ordering::Release);
        readiness.state.solvers.store(1, Ordering::Release);
        readiness.state.chains.store(1, Ordering::Release);
        readiness
    }

    pub(crate) fn set_pricing(&self, ready: bool) {
        set_dependency("pricing", &self.state.pricing, ready);
    }

    pub(crate) fn set_registry(&self, ready: bool) {
        set_dependency("registry", &self.state.registry, ready);
    }

    pub(crate) fn set_actor(&self, ready: bool) {
        set_dependency("actor", &self.state.actor, ready);
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        let counter = match self.kind {
            ConnectionKind::Solver => &self.readiness.state.solvers,
            ConnectionKind::Chain => &self.readiness.state.chains,
        };
        counter.fetch_sub(1, Ordering::AcqRel);
    }
}

fn set_dependency(name: &str, status: &AtomicBool, ready: bool) {
    if status.swap(ready, Ordering::AcqRel) != ready {
        crate::service_log!("readiness", "dependency={name} ready={ready}");
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
            vec!["pricing", "registry", "solver", "chain", "actor"]
        );

        readiness.set_pricing(true);
        readiness.set_registry(true);
        readiness.set_actor(true);
        let solver = readiness.solver_connection();
        let chain = readiness.chain_connection();
        assert!(readiness.snapshot().ready);

        drop(chain);
        assert_eq!(readiness.snapshot().missing, vec!["chain"]);
        drop(solver);
        assert_eq!(readiness.snapshot().missing, vec!["solver", "chain"]);
    }
}
