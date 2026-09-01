use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use alloy_primitives::{Address, B256};
use tokio::sync::OwnedMutexGuard;

use super::handle::ServiceError;
use crate::{
    config::AppConfig,
    order::TradeTerms,
    registry::SolverRegistry,
    session::SolverSessions,
    storage::{CapacityUsage, NewProofOrder, ProofOrderRepository, ReservationCandidate},
};

#[derive(Clone)]
pub struct AdmissionGate {
    sessions: SolverSessions,
    registry: SolverRegistry,
    allowed_solvers: Arc<HashSet<Address>>,
    movement_allowances: Arc<HashMap<(u64, Address, Address), u16>>,
    require_preview: bool,
}

pub(in crate::core::engine) struct AdmissionPermit {
    _guard: OwnedMutexGuard<()>,
    pub preview_valid_until_ms: i64,
}

pub(in crate::core::engine) struct FallbackPermit {
    _guard: OwnedMutexGuard<()>,
    pub next_solver: Option<Address>,
}

pub(in crate::core::engine) struct DisclosurePermit {
    _guard: OwnedMutexGuard<()>,
}

impl AdmissionGate {
    pub fn from_config(
        sessions: SolverSessions,
        registry: SolverRegistry,
        config: &AppConfig,
    ) -> Self {
        let movement_allowances = config
            .chains
            .iter()
            .flat_map(|chain| {
                chain.markets.iter().filter_map(move |market| {
                    let token_in = chain
                        .tokens
                        .iter()
                        .find(|token| token.symbol == market.token_in)?
                        .address;
                    let token_out = chain
                        .tokens
                        .iter()
                        .find(|token| token.symbol == market.token_out)?
                        .address;
                    Some((
                        (chain.chain_id, token_in, token_out),
                        market.movement_allowance_bps,
                    ))
                })
            })
            .collect();
        Self {
            sessions,
            registry,
            allowed_solvers: Arc::new(config.allowed_solvers.iter().copied().collect()),
            movement_allowances: Arc::new(movement_allowances),
            require_preview: true,
        }
    }

    #[cfg(test)]
    pub(in crate::core::engine) fn for_test(
        sessions: SolverSessions,
        registry: SolverRegistry,
        allowed_solvers: Arc<HashSet<Address>>,
        movement_allowance_bps: u16,
    ) -> Self {
        Self {
            sessions,
            registry,
            allowed_solvers,
            movement_allowances: Arc::new(HashMap::from([(
                (31_337, Address::repeat_byte(1), Address::repeat_byte(2)),
                movement_allowance_bps,
            )])),
            require_preview: false,
        }
    }

    pub(in crate::core::engine) async fn select_candidate(
        &self,
        proof_orders: &ProofOrderRepository,
        input: &mut NewProofOrder,
    ) -> Result<AdmissionPermit, ServiceError> {
        let guard = self.sessions.capacity_guard().await;
        self.require_healthy_registry()?;
        let preview_valid_until_ms = if self.require_preview {
            proof_orders
                .preview_valid_until(input.preview_id)
                .await
                .map_err(|error| {
                    crate::service_error!(
                        "orderbook",
                        "preview admission check failed error={error}"
                    );
                    ServiceError::AdmissionUnavailable
                })?
                .ok_or(ServiceError::PreviewExpired)?
        } else {
            i64::MAX
        };
        let usage_at_ms = super::maintenance::now_ms();
        let usage = self.capacity_usage(proof_orders, usage_at_ms).await?;
        let now_ms = super::maintenance::now_ms();
        if preview_valid_until_ms <= now_ms {
            return Err(ServiceError::PreviewExpired);
        }
        let now_ms_u64 = u64::try_from(now_ms).map_err(|_| ServiceError::AdmissionUnavailable)?;

        for index in 0..input.candidates.len() {
            let candidate = &input.candidates[index];
            if self.candidate_has_capacity(
                &usage,
                candidate.solver_id,
                candidate.encryption_key_id,
                Some(&candidate.encryption_public_key),
                &input.terms,
                input.fee_bps,
                now_ms_u64,
            )? {
                input.candidates.swap(0, index);
                return Ok(AdmissionPermit {
                    _guard: guard,
                    preview_valid_until_ms,
                });
            }
        }

        Err(ServiceError::RouteCapacityChanged)
    }

    pub(in crate::core::engine) async fn select_fallback(
        &self,
        proof_orders: &ProofOrderRepository,
        terms: &TradeTerms,
        fee_bps: u16,
        candidates: &[ReservationCandidate],
    ) -> Result<FallbackPermit, ServiceError> {
        let guard = self.sessions.capacity_guard().await;
        self.require_healthy_registry()?;
        let usage_at_ms = super::maintenance::now_ms();
        let usage = self.capacity_usage(proof_orders, usage_at_ms).await?;
        let now_ms = super::maintenance::now_ms();
        let now_ms_u64 = u64::try_from(now_ms).map_err(|_| ServiceError::AdmissionUnavailable)?;

        for candidate in candidates {
            if self.require_preview && candidate.encryption_public_key.is_none() {
                continue;
            }
            if self.candidate_has_capacity(
                &usage,
                candidate.solver_id,
                candidate.key_id,
                candidate.encryption_public_key.as_deref(),
                terms,
                fee_bps,
                now_ms_u64,
            )? {
                return Ok(FallbackPermit {
                    _guard: guard,
                    next_solver: Some(candidate.solver_id),
                });
            }
        }

        Ok(FallbackPermit {
            _guard: guard,
            next_solver: None,
        })
    }

    pub(in crate::core::engine) async fn authorize_disclosure(
        &self,
        solver_id: Address,
        session_token: Option<&str>,
        key_id: B256,
        public_key: Option<&[u8]>,
        terms: &TradeTerms,
    ) -> Result<DisclosurePermit, ServiceError> {
        let guard = self.sessions.capacity_guard().await;
        self.require_healthy_registry()?;
        let now_ms = super::maintenance::now_ms();
        let now_ms_u64 = u64::try_from(now_ms).map_err(|_| ServiceError::AdmissionUnavailable)?;
        if (self.require_preview && (session_token.is_none() || public_key.is_none()))
            || session_token
                .is_some_and(|token| self.sessions.resolve(token, now_ms_u64) != Some(solver_id))
            || !self.candidate_is_live(solver_id, key_id, public_key, terms, now_ms_u64)
        {
            return Err(ServiceError::RouteCapacityChanged);
        }
        Ok(DisclosurePermit { _guard: guard })
    }

    fn require_healthy_registry(&self) -> Result<(), ServiceError> {
        self.registry
            .health()
            .map_err(|_| ServiceError::AdmissionUnavailable)
    }

    #[allow(clippy::too_many_arguments)]
    fn candidate_has_capacity(
        &self,
        usage: &CapacityUsage,
        solver_id: Address,
        key_id: B256,
        public_key: Option<&[u8]>,
        terms: &TradeTerms,
        fee_bps: u16,
        now_ms_u64: u64,
    ) -> Result<bool, ServiceError> {
        if !self.candidate_is_live(solver_id, key_id, public_key, terms, now_ms_u64) {
            return Ok(false);
        }
        let capacity = self
            .sessions
            .capacity_for_market(
                solver_id,
                terms.chain_id,
                terms.token_in,
                terms.token_out,
                now_ms_u64,
            )
            .ok_or(ServiceError::RouteCapacityChanged)?;
        let movement_allowance = self
            .movement_allowances
            .get(&(terms.chain_id, terms.token_in, terms.token_out))
            .copied()
            .ok_or(ServiceError::AdmissionUnavailable)?;
        if capacity
            .minimum_margin_bps
            .checked_add(movement_allowance)
            .is_none_or(|required| required > fee_bps)
        {
            return Ok(false);
        }
        if usage.active_workload(solver_id) >= u64::from(capacity.max_jobs_total) {
            return Ok(false);
        }
        Ok(usage
            .held_output_amount(solver_id, terms.chain_id, terms.token_out)
            .checked_add(terms.amount_out)
            .is_some_and(|required| required <= capacity.amount_out_total))
    }

    async fn capacity_usage(
        &self,
        proof_orders: &ProofOrderRepository,
        now_ms: i64,
    ) -> Result<CapacityUsage, ServiceError> {
        proof_orders.capacity_usage(now_ms).await.map_err(|error| {
            crate::service_error!("orderbook", "capacity reconstruction failed error={error}");
            ServiceError::AdmissionUnavailable
        })
    }

    fn candidate_is_live(
        &self,
        solver_id: Address,
        key_id: B256,
        public_key: Option<&[u8]>,
        terms: &TradeTerms,
        now_ms: u64,
    ) -> bool {
        if !self.allowed_solvers.contains(&solver_id)
            || !self
                .registry
                .get(solver_id)
                .is_some_and(|profile| profile.active)
        {
            return false;
        }
        self.sessions
            .capacity_for_market(
                solver_id,
                terms.chain_id,
                terms.token_in,
                terms.token_out,
                now_ms,
            )
            .is_some_and(|capacity| {
                capacity.route.encryption_key_id == key_id
                    && public_key
                        .is_none_or(|expected| capacity.route.encryption_public_key == expected)
                    && capacity.route.key_expires_at_ms > terms.expires_at_ms
                    && proof_has_required_runway(
                        terms.expires_at_ms,
                        now_ms,
                        capacity.required_proof_lifetime_seconds,
                    )
                    && capacity.route.min_amount_in <= terms.amount_in
                    && terms.amount_in <= capacity.route.max_amount_in
            })
    }
}

fn proof_has_required_runway(
    expires_at_ms: i64,
    now_ms: u64,
    required_proof_lifetime_seconds: u64,
) -> bool {
    let Ok(now_ms) = i64::try_from(now_ms) else {
        return false;
    };
    let required_ms = i64::try_from(required_proof_lifetime_seconds)
        .unwrap_or(i64::MAX)
        .saturating_mul(1_000);
    expires_at_ms > now_ms.saturating_add(required_ms)
}
