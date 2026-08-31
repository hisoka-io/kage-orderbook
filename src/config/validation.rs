use std::collections::{HashMap, HashSet};

use alloy_primitives::Address;

use super::{AppConfig, ComplaintFinalityPolicy, ConfigError, MAX_TOKEN_DECIMALS, model::invalid};

#[derive(Debug, Clone, Copy)]
struct ConfiguredMarketPolicy {
    movement_allowance_bps: u16,
    max_total_deviation_bps: u16,
}

impl AppConfig {
    pub(super) fn validate(&self) -> Result<(), ConfigError> {
        let mut allowed_solvers = HashSet::new();
        if self.allowed_solvers.is_empty() {
            return Err(invalid(
                "allowed_solvers must contain at least one locally owned solver",
            ));
        }
        for solver in &self.allowed_solvers {
            if *solver == Address::ZERO || !allowed_solvers.insert(*solver) {
                return Err(invalid(
                    "allowed_solvers must contain unique, non-zero addresses",
                ));
            }
        }
        if self.api.request_timeout_ms == 0
            || self.api.max_body_bytes == 0
            || self.api.max_order_request_bytes == 0
            || self.api.max_ciphertext_bytes == 0
            || self.api.websocket_max_message_bytes == 0
            || self.api.websocket_max_subscriptions == 0
            || self.api.websocket_message_window_ms == 0
            || self.api.websocket_message_burst == 0
            || self.api.websocket_heartbeat_interval_ms == 0
            || self.api.websocket_idle_timeout_ms == 0
            || self.api.solver_auth_recheck_ms == 0
            || self.api.rate_limit_replenish_ms == 0
            || self.api.rate_limit_burst == 0
            || self.api.cors_max_age_seconds == 0
        {
            return Err(invalid(
                "API limits and durations must be greater than zero",
            ));
        }
        if self.api.websocket_idle_timeout_ms <= self.api.websocket_heartbeat_interval_ms {
            return Err(invalid(
                "api.websocket_idle_timeout_ms must exceed websocket_heartbeat_interval_ms",
            ));
        }
        if self.api.max_order_request_bytes > self.api.max_body_bytes
            || self.api.max_ciphertext_bytes >= self.api.max_order_request_bytes
        {
            return Err(invalid(
                "api.max_ciphertext_bytes must be smaller than max_order_request_bytes, which must not exceed max_body_bytes",
            ));
        }
        let mut origins = HashSet::new();
        for origin in &self.api.allowed_origins {
            let parsed = reqwest::Url::parse(origin)
                .map_err(|_| invalid(format!("invalid API allowed origin {origin}")))?;
            let normalized = parsed.origin().ascii_serialization();
            if !matches!(parsed.scheme(), "http" | "https")
                || parsed.host_str().is_none()
                || parsed.path() != "/"
                || parsed.query().is_some()
                || parsed.fragment().is_some()
                || normalized != *origin
                || !origins.insert(origin)
            {
                return Err(invalid(format!("invalid API allowed origin {origin}")));
            }
        }
        if self.database.max_connections == 0 || self.database.busy_timeout_ms == 0 {
            return Err(invalid("database values must be greater than zero"));
        }
        if self.runtime.command_capacity == 0 {
            return Err(invalid(
                "runtime.command_capacity must be greater than zero",
            ));
        }
        if self.pricing.max_age_ms == 0
            || self.pricing.reconnect_delay_ms == 0
            || self.pricing.idle_timeout_ms == 0
        {
            return Err(invalid("pricing durations must be greater than zero"));
        }
        if let Some(oracle) = &self.pricing_oracle {
            oracle
                .validate()
                .map_err(|error| invalid(format!("pricing oracle: {error}")))?;
        }
        if self.chains.is_empty() {
            return Err(invalid("at least one chain must be configured"));
        }

        let mut chain_ids = HashSet::new();
        let mut configured_markets: HashMap<String, Vec<ConfiguredMarketPolicy>> = HashMap::new();
        for chain in &self.chains {
            if chain.chain_id == 0 || !chain_ids.insert(chain.chain_id) {
                return Err(invalid(format!(
                    "chain_id {} must be non-zero and unique",
                    chain.chain_id
                )));
            }
            if chain.name.trim().is_empty() || chain.tokens.is_empty() {
                return Err(invalid(format!(
                    "chain {} requires a name and at least one token",
                    chain.chain_id
                )));
            }
            if chain.darkpool == Address::ZERO {
                return Err(invalid(format!(
                    "chain {} requires a non-zero darkpool address",
                    chain.chain_id
                )));
            }
            if chain.registry == Address::ZERO {
                return Err(invalid(format!(
                    "chain {} requires a non-zero registry address",
                    chain.chain_id
                )));
            }

            let mut symbols = HashSet::new();
            let mut token_bps = HashMap::new();
            let mut addresses = HashSet::new();
            for token in &chain.tokens {
                if token.symbol.is_empty() || token.symbol != token.symbol.to_uppercase() {
                    return Err(invalid(format!(
                        "token symbol {} must be non-empty uppercase",
                        token.symbol
                    )));
                }
                if token.pricing_asset.is_empty()
                    || token.pricing_asset != token.pricing_asset.to_uppercase()
                {
                    return Err(invalid(format!(
                        "pricing asset {} must be non-empty uppercase",
                        token.pricing_asset
                    )));
                }
                if token.address == Address::ZERO {
                    return Err(invalid(format!(
                        "token {} has a zero address",
                        token.symbol
                    )));
                }
                if token.decimals > MAX_TOKEN_DECIMALS {
                    return Err(invalid(format!(
                        "token {} decimals must not exceed {MAX_TOKEN_DECIMALS}",
                        token.symbol
                    )));
                }
                if !symbols.insert(token.symbol.as_str()) || !addresses.insert(token.address) {
                    return Err(invalid(format!(
                        "token symbols and addresses must be unique on chain {}",
                        chain.chain_id
                    )));
                }
                validate_bps(token.max_price_deviation_bps)?;
                token_bps.insert(token.symbol.as_str(), token.max_price_deviation_bps);
            }

            let mut markets = HashSet::new();
            for market in &chain.markets {
                if market.token_in == market.token_out
                    || !symbols.contains(market.token_in.as_str())
                    || !symbols.contains(market.token_out.as_str())
                {
                    return Err(invalid(format!(
                        "market {} -> {} is invalid on chain {}",
                        market.token_in, market.token_out, chain.chain_id
                    )));
                }
                if !markets.insert((market.token_in.as_str(), market.token_out.as_str())) {
                    return Err(invalid(format!(
                        "duplicate market {} -> {} on chain {}",
                        market.token_in, market.token_out, chain.chain_id
                    )));
                }
                let token_limit =
                    token_bps[market.token_in.as_str()].min(token_bps[market.token_out.as_str()]);
                let market_limit = market.max_price_deviation_bps.unwrap_or(token_limit);
                if let Some(bps) = market.max_price_deviation_bps {
                    validate_bps(bps)?;
                    if bps > token_limit {
                        return Err(invalid(format!(
                            "market BPS cannot exceed token limit {token_limit}"
                        )));
                    }
                }
                configured_markets
                    .entry(format!("{}/{}", market.token_in, market.token_out))
                    .or_default()
                    .push(ConfiguredMarketPolicy {
                        movement_allowance_bps: market.movement_allowance_bps,
                        max_total_deviation_bps: market_limit,
                    });
            }
        }
        self.validate_proof_order_policy(&allowed_solvers, &configured_markets)?;
        Ok(())
    }

    fn validate_proof_order_policy(
        &self,
        allowed_solvers: &HashSet<Address>,
        configured_markets: &HashMap<String, Vec<ConfiguredMarketPolicy>>,
    ) -> Result<(), ConfigError> {
        let policy = &self.proof_orders;
        if policy.proof_lifetime_seconds == 0
            || policy.minimum_remaining_seconds == 0
            || policy.preview_lifetime_seconds == 0
            || policy.reservation_attempt_timeout_ms == 0
            || policy.preview_cleanup_grace_seconds == 0
            || policy.ciphertext_cleanup_grace_seconds == 0
            || policy.complaint_window_seconds == 0
            || policy.evidence_retention_seconds == 0
            || policy.resolved_complaint_retention_seconds == 0
        {
            return Err(invalid(
                "proof_orders durations must all be greater than zero",
            ));
        }
        if policy.minimum_remaining_seconds >= policy.proof_lifetime_seconds {
            return Err(invalid(
                "proof_orders.minimum_remaining_seconds must be less than proof_lifetime_seconds",
            ));
        }
        if policy.preview_lifetime_seconds > policy.proof_lifetime_seconds {
            return Err(invalid(
                "proof_orders.preview_lifetime_seconds must not exceed proof_lifetime_seconds",
            ));
        }
        if policy.reservation_attempt_timeout_ms
            >= u64::from(policy.minimum_remaining_seconds).saturating_mul(1_000)
        {
            return Err(invalid(
                "proof_orders.reservation_attempt_timeout_ms must be less than the minimum remaining proof window",
            ));
        }
        if policy.max_recipients == 0
            || policy.max_recipients > kage_types::routing::MAX_PROOF_RECIPIENTS
        {
            return Err(invalid(format!(
                "proof_orders.max_recipients must be between 1 and {}",
                kage_types::routing::MAX_PROOF_RECIPIENTS
            )));
        }
        if policy.evidence_retention_seconds < policy.complaint_window_seconds
            || policy.resolved_complaint_retention_seconds < policy.complaint_window_seconds
        {
            return Err(invalid(
                "proof evidence retention must cover the complaint window",
            ));
        }
        if matches!(
            policy.complaint_finality,
            ComplaintFinalityPolicy::Confirmations { count: 0 }
        ) {
            return Err(invalid(
                "proof_orders.complaint_finality confirmations must be greater than zero",
            ));
        }
        if self.fee_categories.is_empty() {
            return Err(invalid("at least one fee category must be configured"));
        }

        let mut category_ids = HashSet::new();
        for category in &self.fee_categories {
            if !valid_category_id(&category.id) {
                return Err(invalid(format!(
                    "fee category id {} must be 1-64 lowercase letters, digits, hyphens, or underscores and start/end with a letter or digit",
                    category.id
                )));
            }
            if !category_ids.insert(category.id.as_str()) {
                return Err(invalid(format!(
                    "duplicate fee category id {}",
                    category.id
                )));
            }
            if category.fee_bps == 0 || category.fee_bps > 10_000 {
                return Err(invalid(format!(
                    "fee category {} fee_bps must be between 1 and 10000",
                    category.id
                )));
            }
            if category.markets.is_empty() {
                return Err(invalid(format!(
                    "fee category {} must contain at least one market",
                    category.id
                )));
            }
            let mut category_markets = HashSet::new();
            for market in &category.markets {
                if !category_markets.insert(market.as_str()) {
                    return Err(invalid(format!(
                        "fee category {} contains duplicate market {}",
                        category.id, market
                    )));
                }
                let Some(policies) = configured_markets.get(market) else {
                    return Err(invalid(format!(
                        "fee category {} references unknown market {}",
                        category.id, market
                    )));
                };
                for policy in policies {
                    if policy.movement_allowance_bps >= category.fee_bps {
                        return Err(invalid(format!(
                            "fee category {} must exceed the movement allowance for market {}",
                            category.id, market
                        )));
                    }
                    if policy.max_total_deviation_bps < category.fee_bps {
                        return Err(invalid(format!(
                            "fee category {} exceeds the deviation limit for market {}",
                            category.id, market
                        )));
                    }
                }
            }
            if category.solver_ids.is_empty() {
                return Err(invalid(format!(
                    "fee category {} must contain at least one solver",
                    category.id
                )));
            }
            if category.solver_ids.len() > policy.max_recipients {
                return Err(invalid(format!(
                    "fee category {} has more solvers than proof_orders.max_recipients",
                    category.id
                )));
            }
            let mut category_solvers = HashSet::new();
            for solver in &category.solver_ids {
                if !allowed_solvers.contains(solver) {
                    return Err(invalid(format!(
                        "fee category {} references solver {} outside allowed_solvers",
                        category.id, solver
                    )));
                }
                if !category_solvers.insert(*solver) {
                    return Err(invalid(format!(
                        "fee category {} contains duplicate solver {}",
                        category.id, solver
                    )));
                }
            }
        }
        for market in configured_markets.keys() {
            if !self
                .fee_categories
                .iter()
                .any(|category| category.markets.iter().any(|candidate| candidate == market))
            {
                return Err(invalid(format!(
                    "configured market {market} has no fee category"
                )));
            }
        }
        Ok(())
    }
}

fn valid_category_id(id: &str) -> bool {
    let bytes = id.as_bytes();
    (1..=64).contains(&bytes.len())
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
}

fn validate_bps(bps: u16) -> Result<(), ConfigError> {
    if bps == 0 || bps > 10_000 {
        return Err(invalid(format!(
            "basis-point value must be between 1 and 10000, got {bps}"
        )));
    }
    Ok(())
}
