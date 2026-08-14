use std::collections::HashMap;
use std::sync::Arc;

use alloy_primitives::B256;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::order::SolverId;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SolverProfile {
    pub noise_key: B256,
    pub active: bool,
}

#[derive(Clone)]
pub struct SolverRegistry {
    backend: Arc<RegistryBackend>,
}

enum RegistryBackend {
    Http {
        client: reqwest::Client,
        base_url: String,
    },
    Static(HashMap<SolverId, SolverProfile>),
}

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error(transparent)]
    Http(#[from] reqwest::Error),
}

impl SolverRegistry {
    pub fn http(base_url: impl Into<String>) -> Self {
        Self {
            backend: Arc::new(RegistryBackend::Http {
                client: reqwest::Client::new(),
                base_url: base_url.into().trim_end_matches('/').to_owned(),
            }),
        }
    }

    pub fn from_profiles(profiles: impl IntoIterator<Item = (SolverId, SolverProfile)>) -> Self {
        Self {
            backend: Arc::new(RegistryBackend::Static(profiles.into_iter().collect())),
        }
    }

    pub async fn get(&self, solver_id: SolverId) -> Result<Option<SolverProfile>, RegistryError> {
        match self.backend.as_ref() {
            RegistryBackend::Static(profiles) => Ok(profiles.get(&solver_id).copied()),
            RegistryBackend::Http { client, base_url } => {
                let response = client
                    .get(format!("{base_url}/solvers/{solver_id}"))
                    .send()
                    .await?;
                if response.status() == reqwest::StatusCode::NOT_FOUND {
                    return Ok(None);
                }
                Ok(Some(response.error_for_status()?.json().await?))
            }
        }
    }
}
