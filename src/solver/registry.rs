use std::{collections::HashMap, sync::Arc};

use alloy_primitives::B256;
use kage_registry::{RegistryIndexer, SyncState};
use thiserror::Error;

use crate::order::SolverId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SolverProfile {
    pub noise_public_key: B256,
    pub active: bool,
}

#[derive(Clone)]
pub struct SolverRegistry {
    backend: RegistryBackend,
}

#[derive(Clone)]
enum RegistryBackend {
    Chain(Arc<RegistryIndexer>),
    Static(Arc<HashMap<SolverId, SolverProfile>>),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RegistryError {
    #[error("registry mirror is still backfilling")]
    Syncing,
    #[error("registry mirror is stale: {0}")]
    Stale(String),
}

impl SolverRegistry {
    pub fn chain(indexer: Arc<RegistryIndexer>) -> Self {
        Self {
            backend: RegistryBackend::Chain(indexer),
        }
    }

    pub fn from_profiles(profiles: impl IntoIterator<Item = (SolverId, SolverProfile)>) -> Self {
        Self {
            backend: RegistryBackend::Static(Arc::new(profiles.into_iter().collect())),
        }
    }

    pub fn get(&self, solver_id: SolverId) -> Option<SolverProfile> {
        match &self.backend {
            RegistryBackend::Static(profiles) => profiles.get(&solver_id).copied(),
            RegistryBackend::Chain(indexer) => {
                indexer.get_solver(solver_id).map(|solver| SolverProfile {
                    noise_public_key: solver.noise_key,
                    active: solver.is_active(),
                })
            }
        }
    }

    pub fn health(&self) -> Result<(), RegistryError> {
        match &self.backend {
            RegistryBackend::Static(_) => Ok(()),
            RegistryBackend::Chain(indexer) => match indexer.state() {
                SyncState::Active => Ok(()),
                SyncState::Syncing => Err(RegistryError::Syncing),
                SyncState::Error(message) => Err(RegistryError::Stale(message)),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::Address;

    use super::*;

    #[test]
    fn an_unknown_solver_is_absent_rather_than_inactive() {
        let known = Address::repeat_byte(1);
        let registry = SolverRegistry::from_profiles([(
            known,
            SolverProfile {
                noise_public_key: B256::repeat_byte(2),
                active: true,
            },
        )]);

        assert!(registry.get(known).is_some_and(|profile| profile.active));
        assert!(registry.get(Address::repeat_byte(9)).is_none());
        assert_eq!(registry.health(), Ok(()));
    }
}
