use std::{collections::HashSet, sync::Arc};

use alloy_primitives::Address;
use kage_types::routing::PreviewRoute;
use thiserror::Error;

#[derive(Clone)]
pub(super) struct Market {
    pub(super) asset_in: String,
    pub(super) asset_out: String,
    pub(super) decimals_in: u8,
    pub(super) decimals_out: u8,
    pub(super) movement_allowance_bps: u16,
    pub(super) max_total_deviation_bps: u16,
    pub(super) categories: Vec<Category>,
}

#[derive(Clone)]
pub(super) struct Category {
    pub(super) id: String,
    pub(super) fee_bps: u16,
    pub(super) solvers: Arc<HashSet<Address>>,
}

#[derive(Debug, Clone)]
pub struct EligiblePreview {
    pub category_id: String,
    pub fee_bps: u16,
    pub routes: Vec<PreviewRoute>,
}

#[derive(Debug, Error)]
pub enum PreviewError {
    #[error("market is not configured")]
    UnsupportedMarket,
    #[error("pricing is unavailable: {0}")]
    Pricing(String),
    #[error("solver registry is unavailable: {0}")]
    Registry(String),
    #[error("preview storage is unavailable: {0}")]
    Storage(String),
    #[error("no live solver route is available")]
    NoRoute,
    #[error("preview is unknown or expired")]
    UnknownPreview,
    #[error("order does not match its preview")]
    TermsMismatch,
    #[error("selected fee category is unavailable")]
    FeeCategoryUnavailable,
    #[error("quote deviation {observed_bps} bps exceeds market maximum {maximum_bps} bps")]
    DeviationExceeded { observed_bps: u16, maximum_bps: u16 },
    #[error("proof recipients do not exactly match the selected category routes")]
    InvalidRecipients,
    #[error("quote arithmetic overflow")]
    Arithmetic,
}
