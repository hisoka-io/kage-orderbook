use std::collections::HashSet;

use alloy_primitives::{Address, U256, U512};

use super::model::{Category, Market, PreviewError};
use crate::{registry::SolverRegistry, session::CapabilityRoute};

pub(super) fn route_supports_category(
    route: &CapabilityRoute,
    market: &Market,
    category: &Category,
    exact_amount_out: U256,
) -> bool {
    category.solvers.contains(&route.route.solver_id)
        && route.available_amount_out >= exact_amount_out
        && route
            .minimum_margin_bps
            .checked_add(market.movement_allowance_bps)
            .is_some_and(|required_fee| required_fee <= category.fee_bps)
}

pub(super) fn solver_is_exposable(
    allowed_solvers: &HashSet<Address>,
    registry: &SolverRegistry,
    solver_id: Address,
) -> bool {
    allowed_solvers.contains(&solver_id)
        && registry
            .get(solver_id)
            .is_some_and(|profile| profile.active)
}

pub(super) fn output_amount(
    amount_in: U256,
    decimals_in: u8,
    decimals_out: u8,
    price_in_e18: U256,
    price_out_e18: U256,
) -> Result<U256, PreviewError> {
    if amount_in == U256::ZERO || price_in_e18 == U256::ZERO || price_out_e18 == U256::ZERO {
        return Err(PreviewError::Arithmetic);
    }
    let numerator = U512::from(amount_in)
        .checked_mul(U512::from(price_in_e18))
        .and_then(|value| value.checked_mul(U512::from(10_u8).pow(U512::from(decimals_out))))
        .ok_or(PreviewError::Arithmetic)?;
    let denominator = U512::from(price_out_e18)
        .checked_mul(U512::from(10_u8).pow(U512::from(decimals_in)))
        .ok_or(PreviewError::Arithmetic)?;
    let output = numerator / denominator;
    if output > U512::from(U256::MAX) {
        return Err(PreviewError::Arithmetic);
    }
    Ok(output.to::<U256>())
}

pub(super) fn deviation_bps(reference: U256, value: U256) -> Result<u16, PreviewError> {
    if reference == U256::ZERO || value > reference {
        return Err(PreviewError::Arithmetic);
    }
    let difference = reference - value;
    let numerator = U512::from(difference)
        .checked_mul(U512::from(10_000_u16))
        .ok_or(PreviewError::Arithmetic)?;
    let bps = (numerator + U512::from(reference) - U512::from(1_u8)) / U512::from(reference);
    u16::try_from(bps).map_err(|_| PreviewError::Arithmetic)
}
