use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use alloy_primitives::{Address, B256, U256};
use kage_types::{
    orders::TradeTerms,
    proof_orders::PreviewCategory,
    routing::{MultiRecipientProof, PreviewRequest, PreviewResponse},
};

use super::{
    calculation::{deviation_bps, output_amount, route_supports_category, solver_is_exposable},
    model::{Category, EligiblePreview, Market, PreviewError},
};
use crate::{
    config::{AppConfig, ProofOrderSettings},
    pricing::EmbeddedPricing,
    registry::SolverRegistry,
    session::SolverSessions,
    storage::{PreviewRepository, PreviewSnapshot, ProofOrderRepository},
};

#[derive(Clone)]
pub struct PreviewService {
    pricing: Option<EmbeddedPricing>,
    sessions: SolverSessions,
    registry: SolverRegistry,
    allowed_solvers: Arc<HashSet<Address>>,
    markets: Arc<HashMap<(u64, Address, Address), Market>>,
    expected_domains: Arc<HashMap<u64, B256>>,
    previews: PreviewRepository,
    proof_orders: ProofOrderRepository,
    policy: ProofOrderSettings,
}

impl PreviewService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pricing: EmbeddedPricing,
        sessions: SolverSessions,
        registry: SolverRegistry,
        previews: PreviewRepository,
        proof_orders: ProofOrderRepository,
        config: &AppConfig,
    ) -> Self {
        let markets = config
            .chains
            .iter()
            .flat_map(|chain| {
                chain.markets.iter().map(move |market| {
                    let input = chain
                        .tokens
                        .iter()
                        .find(|token| token.symbol == market.token_in)
                        .expect("validated input token");
                    let output = chain
                        .tokens
                        .iter()
                        .find(|token| token.symbol == market.token_out)
                        .expect("validated output token");
                    let market_name = format!("{}/{}", market.token_in, market.token_out);
                    let categories = config
                        .fee_categories
                        .iter()
                        .filter(|category| category.markets.contains(&market_name))
                        .map(|category| Category {
                            id: category.id.clone(),
                            fee_bps: category.fee_bps,
                            solvers: Arc::new(category.solver_ids.iter().copied().collect()),
                        })
                        .collect();
                    (
                        (chain.chain_id, input.address, output.address),
                        Market {
                            asset_in: input.pricing_asset.clone(),
                            asset_out: output.pricing_asset.clone(),
                            decimals_in: input.decimals,
                            decimals_out: output.decimals,
                            movement_allowance_bps: market.movement_allowance_bps,
                            max_total_deviation_bps: market.max_price_deviation_bps.unwrap_or_else(
                                || {
                                    input
                                        .max_price_deviation_bps
                                        .min(output.max_price_deviation_bps)
                                },
                            ),
                            categories,
                        },
                    )
                })
            })
            .collect();
        let expected_domains = config
            .chains
            .iter()
            .map(|chain| {
                (
                    chain.chain_id,
                    crate::proof_domain::proof_domain(chain.chain_id, chain.darkpool),
                )
            })
            .collect();
        Self {
            pricing: Some(pricing),
            sessions,
            registry,
            allowed_solvers: Arc::new(config.allowed_solvers.iter().copied().collect()),
            markets: Arc::new(markets),
            expected_domains: Arc::new(expected_domains),
            previews,
            proof_orders,
            policy: config.proof_orders.clone(),
        }
    }

    pub fn expected_domain(&self, chain_id: u64) -> Option<B256> {
        self.expected_domains.get(&chain_id).copied()
    }

    #[cfg(test)]
    pub(crate) fn admission_only(
        previews: PreviewRepository,
        proof_orders: ProofOrderRepository,
        config: &AppConfig,
    ) -> Self {
        let expected_domains = config
            .chains
            .iter()
            .map(|chain| {
                (
                    chain.chain_id,
                    crate::proof_domain::proof_domain(chain.chain_id, chain.darkpool),
                )
            })
            .collect();
        Self {
            pricing: None,
            sessions: SolverSessions::new("kage-orderbook:preview-admission-test"),
            registry: SolverRegistry::from_profiles([]),
            allowed_solvers: Arc::new(config.allowed_solvers.iter().copied().collect()),
            markets: Arc::new(HashMap::new()),
            expected_domains: Arc::new(expected_domains),
            previews,
            proof_orders,
            policy: config.proof_orders.clone(),
        }
    }

    pub async fn create(
        &self,
        request: PreviewRequest,
        now_ms: u64,
    ) -> Result<PreviewResponse, PreviewError> {
        let market = self
            .markets
            .get(&(request.chain_id, request.token_in, request.token_out))
            .ok_or(PreviewError::UnsupportedMarket)?;
        self.registry
            .health()
            .map_err(|error| PreviewError::Registry(error.to_string()))?;
        let pair = self
            .pricing
            .as_ref()
            .ok_or_else(|| PreviewError::Pricing("pricing service is unavailable".to_owned()))?
            .fresh_pair(&market.asset_in, &market.asset_out)
            .map_err(|error| PreviewError::Pricing(error.to_string()))?;

        let proof_lifetime_ms = i64::from(self.policy.proof_lifetime_seconds) * 1_000;
        let now_i64 = i64::try_from(now_ms).unwrap_or(i64::MAX);
        let required_key_expiry_ms = now_i64.saturating_add(proof_lifetime_ms);
        let mut live_routes = Vec::new();
        for route in self.sessions.routes_for_market(
            request.chain_id,
            request.token_in,
            request.token_out,
            now_ms,
        ) {
            let public = &route.route;
            if !solver_is_exposable(&self.allowed_solvers, &self.registry, public.solver_id)
                || public.key_expires_at_ms <= required_key_expiry_ms
                || public.min_amount_in > request.amount_in
                || request.amount_in > public.max_amount_in
            {
                continue;
            }
            let workload = self
                .proof_orders
                .active_workload(public.solver_id, now_i64)
                .await
                .map_err(|error| PreviewError::Storage(error.to_string()))?;
            if workload < u64::from(route.max_in_flight) {
                live_routes.push(route);
            }
        }
        if live_routes.is_empty() {
            return Err(PreviewError::NoRoute);
        }

        let midpoint_amount_out = output_amount(
            request.amount_in,
            market.decimals_in,
            market.decimals_out,
            U256::from(pair.from.price.raw()),
            U256::from(pair.to.price.raw()),
        )?;
        let confidence_amount_out = output_amount(
            request.amount_in,
            market.decimals_in,
            market.decimals_out,
            U256::from(pair.from.min_price.raw()),
            U256::from(pair.to.max_price.raw()),
        )?;
        if midpoint_amount_out == U256::ZERO || confidence_amount_out == U256::ZERO {
            return Err(PreviewError::Arithmetic);
        }
        let oracle_adjustment_amount = midpoint_amount_out.saturating_sub(confidence_amount_out);
        let oracle_adjustment_bps = deviation_bps(midpoint_amount_out, confidence_amount_out)?;

        let mut rejected_deviation = None;
        let mut categories = Vec::new();
        for category in &market.categories {
            let exact_amount_out = confidence_amount_out
                .checked_mul(U256::from(10_000_u16 - category.fee_bps))
                .ok_or(PreviewError::Arithmetic)?
                / U256::from(10_000_u16);
            if exact_amount_out == U256::ZERO {
                continue;
            }
            let routes = live_routes
                .iter()
                .filter(|route| route_supports_category(route, market, category, exact_amount_out))
                .map(|route| route.route.clone())
                .collect::<Vec<_>>();
            if routes.is_empty() {
                continue;
            }
            let total_deviation_bps = deviation_bps(midpoint_amount_out, exact_amount_out)?;
            if total_deviation_bps > market.max_total_deviation_bps {
                rejected_deviation = Some(total_deviation_bps);
                continue;
            }
            categories.push(PreviewCategory {
                id: category.id.clone(),
                fee_bps: category.fee_bps,
                exact_amount_out,
                fee_amount: confidence_amount_out - exact_amount_out,
                routes,
            });
        }
        if categories.is_empty() {
            if let Some(observed_bps) = rejected_deviation {
                return Err(PreviewError::DeviationExceeded {
                    observed_bps,
                    maximum_bps: market.max_total_deviation_bps,
                });
            }
            return Err(PreviewError::NoRoute);
        }

        let key_valid_until_ms = categories
            .iter()
            .flat_map(|category| &category.routes)
            .map(|route| route.key_expires_at_ms.saturating_sub(proof_lifetime_ms))
            .min()
            .ok_or(PreviewError::NoRoute)?;
        let valid_until_ms = [
            now_i64.saturating_add(i64::from(self.policy.preview_lifetime_seconds) * 1_000),
            i64::try_from(pair.from.valid_until_ms.min(pair.to.valid_until_ms)).unwrap_or(i64::MAX),
            key_valid_until_ms,
        ]
        .into_iter()
        .min()
        .ok_or(PreviewError::Arithmetic)?;
        if valid_until_ms <= now_i64 {
            return Err(PreviewError::NoRoute);
        }

        let response = PreviewResponse {
            preview_id: B256::random(),
            chain_id: request.chain_id,
            token_in: request.token_in,
            token_out: request.token_out,
            token_in_decimals: market.decimals_in,
            token_out_decimals: market.decimals_out,
            amount_in: request.amount_in,
            midpoint_amount_out,
            confidence_amount_out,
            oracle_adjustment_bps,
            oracle_adjustment_amount,
            valid_until_ms,
            recommended_proof_lifetime_seconds: self.policy.proof_lifetime_seconds,
            minimum_remaining_seconds: self.policy.minimum_remaining_seconds,
            categories,
        };
        let snapshot = PreviewSnapshot {
            response: response.clone(),
            price_in_e18: U256::from(pair.from.price.raw()),
            price_out_e18: U256::from(pair.to.price.raw()),
            price_in_lower_e18: U256::from(pair.from.min_price.raw()),
            price_out_upper_e18: U256::from(pair.to.max_price.raw()),
            pricing_sequence: pair.sequence,
            published_at_ms: pair.published_at_ms as i64,
            created_at_ms: now_i64,
            erase_after_ms: valid_until_ms.saturating_add(
                i64::try_from(self.policy.preview_cleanup_grace_seconds)
                    .unwrap_or(i64::MAX)
                    .saturating_mul(1_000),
            ),
        };
        self.previews
            .insert(&snapshot)
            .await
            .map_err(|error| PreviewError::Storage(error.to_string()))?;
        Ok(response)
    }

    pub async fn eligible_routes(
        &self,
        preview_id: B256,
        category_id: &str,
        terms: &TradeTerms,
        proof: &MultiRecipientProof,
        now_ms: u64,
    ) -> Result<EligiblePreview, PreviewError> {
        let snapshot = self
            .previews
            .get(preview_id)
            .await
            .map_err(|error| PreviewError::Storage(error.to_string()))?
            .ok_or(PreviewError::UnknownPreview)?;
        let preview = snapshot.response;
        if preview.valid_until_ms <= now_ms as i64 {
            return Err(PreviewError::UnknownPreview);
        }
        if preview.chain_id != terms.chain_id
            || preview.token_in != terms.token_in
            || preview.token_out != terms.token_out
            || preview.amount_in != terms.amount_in
            || terms.amount_in == U256::ZERO
            || terms.amount_out == U256::ZERO
            || terms.expires_at_ms <= now_ms as i64
        {
            return Err(PreviewError::TermsMismatch);
        }
        let category = preview
            .categories
            .into_iter()
            .find(|category| category.id == category_id)
            .ok_or(PreviewError::FeeCategoryUnavailable)?;
        let deterministic_output = preview
            .confidence_amount_out
            .checked_mul(U256::from(10_000_u16 - category.fee_bps))
            .ok_or(PreviewError::Arithmetic)?
            / U256::from(10_000_u16);
        if category.exact_amount_out != deterministic_output
            || deterministic_output != terms.amount_out
        {
            return Err(PreviewError::TermsMismatch);
        }
        let routes = category
            .routes
            .into_iter()
            .filter(|route| {
                route.min_amount_in <= terms.amount_in
                    && terms.amount_in <= route.max_amount_in
                    && route.key_expires_at_ms > terms.expires_at_ms
            })
            .collect::<Vec<_>>();
        if routes.is_empty() {
            return Err(PreviewError::FeeCategoryUnavailable);
        }
        let recipients_match = routes.len() == proof.recipients.len()
            && routes.iter().all(|route| {
                proof.recipients.iter().any(|recipient| {
                    recipient.solver_id == route.solver_id
                        && recipient.key_id == route.encryption_key_id
                })
            });
        if !recipients_match {
            return Err(PreviewError::InvalidRecipients);
        }
        Ok(EligiblePreview {
            category_id: category.id,
            fee_bps: category.fee_bps,
            routes,
        })
    }

    pub async fn cleanup(&self, now_ms: i64) -> Result<u64, PreviewError> {
        self.previews
            .cleanup(now_ms)
            .await
            .map_err(|error| PreviewError::Storage(error.to_string()))
    }
}
