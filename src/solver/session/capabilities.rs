use std::collections::HashSet;

use alloy_primitives::{Address, B256};
use kage_types::routing::{PreviewRoute, SolverCapabilities};

use super::{AuthError, CAPABILITY_TTL_MS, CapabilityLease, CapabilityRoute, SolverSessions};

impl SolverSessions {
    pub(crate) fn register_capabilities(
        &self,
        token: &str,
        capabilities: SolverCapabilities,
        now_ms: u64,
    ) -> Result<(), AuthError> {
        validate_capabilities(&capabilities, now_ms)?;
        let mut state = self.lock();
        let session = *state.tokens.get(token).ok_or(AuthError::UnknownSession)?;
        if session.expires_at_ms <= now_ms {
            return Err(AuthError::UnknownSession);
        }
        if state
            .capabilities
            .get(&session.solver_id)
            .is_some_and(|current| current.capabilities.revision >= capabilities.revision)
        {
            return Err(AuthError::StaleCapabilityRevision);
        }
        state.capabilities.insert(
            session.solver_id,
            CapabilityLease {
                capabilities,
                expires_at_ms: now_ms
                    .saturating_add(CAPABILITY_TTL_MS)
                    .min(session.expires_at_ms),
            },
        );
        Ok(())
    }

    pub fn routes_for_market(
        &self,
        chain_id: u64,
        token_in: Address,
        token_out: Address,
        now_ms: u64,
    ) -> Vec<CapabilityRoute> {
        let mut state = self.lock();
        prune_expired_authority(&mut state, now_ms);
        let mut routes = state
            .capabilities
            .iter()
            .filter_map(|(solver_id, lease)| {
                route_for_market(*solver_id, lease, chain_id, token_in, token_out)
            })
            .collect::<Vec<_>>();
        routes.sort_by_key(|route| {
            (
                route.minimum_margin_bps,
                std::cmp::Reverse(route.route.max_amount_in),
                route.route.solver_id,
            )
        });
        routes
    }

    pub fn capacity_for_market(
        &self,
        solver_id: Address,
        chain_id: u64,
        token_in: Address,
        token_out: Address,
        now_ms: u64,
    ) -> Option<CapabilityRoute> {
        let mut state = self.lock();
        prune_expired_authority(&mut state, now_ms);
        route_for_market(
            solver_id,
            state.capabilities.get(&solver_id)?,
            chain_id,
            token_in,
            token_out,
        )
    }
}

fn prune_expired_authority(state: &mut super::State, now_ms: u64) {
    let now_i64 = i64::try_from(now_ms).unwrap_or(i64::MAX);
    state
        .tokens
        .retain(|_, session| session.expires_at_ms > now_ms);
    let live_solvers = state
        .tokens
        .values()
        .map(|session| session.solver_id)
        .collect::<HashSet<_>>();
    state.capabilities.retain(|solver_id, lease| {
        live_solvers.contains(solver_id)
            && lease.expires_at_ms > now_ms
            && lease.capabilities.key_expires_at_ms > now_i64
    });
}

fn route_for_market(
    solver_id: Address,
    lease: &CapabilityLease,
    chain_id: u64,
    token_in: Address,
    token_out: Address,
) -> Option<CapabilityRoute> {
    let market = lease.capabilities.markets.iter().find(|market| {
        market.chain_id == chain_id && market.token_in == token_in && market.token_out == token_out
    })?;
    Some(CapabilityRoute {
        route: PreviewRoute {
            solver_id,
            min_amount_in: market.min_amount_in,
            max_amount_in: market.max_amount_in,
            encryption_key_id: lease.capabilities.encryption_key_id,
            encryption_public_key: lease.capabilities.encryption_public_key.clone(),
            key_expires_at_ms: lease.capabilities.key_expires_at_ms,
        },
        minimum_margin_bps: market.minimum_margin_bps,
        max_in_flight: lease.capabilities.max_in_flight,
        available_amount_out: market.available_amount_out,
    })
}

pub(super) fn validate_capabilities(
    capabilities: &SolverCapabilities,
    now_ms: u64,
) -> Result<(), AuthError> {
    let mut markets = HashSet::with_capacity(capabilities.markets.len());
    let valid = capabilities.revision > 0
        && capabilities.encryption_key_id != B256::ZERO
        && capabilities.encryption_public_key.len() == 32
        && capabilities
            .encryption_public_key
            .iter()
            .any(|byte| *byte != 0)
        && capabilities.key_expires_at_ms > now_ms as i64
        && ((capabilities.markets.is_empty() && capabilities.max_in_flight == 0)
            || (!capabilities.markets.is_empty() && capabilities.max_in_flight > 0))
        && capabilities.markets.iter().all(|market| {
            market.chain_id > 0
                && market.token_in != market.token_out
                && market.min_amount_in > alloy_primitives::U256::ZERO
                && market.max_amount_in >= market.min_amount_in
                && market.available_amount_out > alloy_primitives::U256::ZERO
                && market.minimum_margin_bps > 0
                && market.minimum_margin_bps <= 10_000
                && markets.insert((market.chain_id, market.token_in, market.token_out))
        });
    valid.then_some(()).ok_or(AuthError::InvalidCapabilities)
}
