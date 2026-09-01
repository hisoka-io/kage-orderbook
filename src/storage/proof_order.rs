use std::{
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use alloy_primitives::{Address, B256, U256, keccak256};
use kage_types::{
    api_types::{ComplaintResponse, ComplaintStatus},
    identifiers::OrderId,
    orders::TradeTerms,
    proof_orders::{
        AssignmentTicket, ComplaintEvidenceKind, CreateOrderRequest, ProofAcceptanceAck,
        ProofOrderBindings, ProofOrderResponse, ProofOrderState, ProofRejectionAck, ReservationAck,
        ReservationRequestClaims, assignment_ticket_digest, exact_terms_digest,
    },
    routing::{MultiRecipientProof, PreviewRoute, SolverProofDelivery},
};
use sqlx::{Row, Sqlite, SqlitePool, Transaction};

use crate::{
    complaint::EncryptedComplaintOpening,
    order::{Order, OrderAccessTokenHash},
};

use super::RepositoryError;

#[derive(Clone)]
pub struct ProofOrderRepository {
    pool: SqlitePool,
    retention_metrics: RetentionMetrics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    Created,
    Existing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdvanceOutcome {
    Advanced(Address),
    AwaitingCapacity,
    Exhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationalFailureKind {
    Proving,
    Submission,
    Transaction,
}

#[derive(Debug, Clone)]
pub struct NewProofOrder {
    pub order_id: OrderId,
    pub access_token_hash: OrderAccessTokenHash,
    pub preview_id: B256,
    pub category_id: String,
    pub terms: TradeTerms,
    pub domain_hash: B256,
    pub fee_bps: u16,
    pub settlement_commitment: B256,
    pub proof: MultiRecipientProof,
    pub candidates: Vec<PreviewRoute>,
    pub created_at_ms: i64,
    pub reservation_attempt_timeout_ms: u64,
    pub ciphertext_cleanup_grace_seconds: u64,
}

#[derive(Debug, Clone)]
pub struct ProofOrderBinding {
    pub bindings: ProofOrderBindings,
    pub domain_hash: B256,
    pub settlement_commitment: B256,
    pub assignment_digest: B256,
    pub disclosed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignedProofDecision {
    Accepted(ProofAcceptanceAck),
    Rejected(ProofRejectionAck),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingReservation {
    pub claims: ReservationRequestClaims,
    pub terms: TradeTerms,
    pub domain_hash: B256,
    pub fee_bps: u16,
    pub settlement_commitment: B256,
    pub key_id: B256,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReservationCandidate {
    pub solver_id: Address,
    pub key_id: B256,
    pub encryption_public_key: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct AccountabilityEvidence {
    pub settlement_commitment: B256,
    pub proof_expires_at_ms: i64,
    pub assigned_solver: Option<Address>,
    pub disclosed_at_ms: Option<i64>,
    pub acceptance: Option<ProofAcceptanceAck>,
    pub rejection: Option<ProofRejectionAck>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CleanupOutcome {
    pub payloads_erased: u64,
    pub complaints_erased: u64,
    pub orders_erased: u64,
}

#[derive(Clone, Default)]
pub struct RetentionMetrics {
    counters: Arc<RetentionMetricCounters>,
}

#[derive(Default)]
struct RetentionMetricCounters {
    cleanup_runs: AtomicU64,
    cleanup_failures: AtomicU64,
    payloads_erased: AtomicU64,
    complaints_erased: AtomicU64,
    orders_erased: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct RetentionMetricsSnapshot {
    pub cleanup_runs: u64,
    pub cleanup_failures: u64,
    pub payloads_erased: u64,
    pub complaints_erased: u64,
    pub orders_erased: u64,
}

impl RetentionMetrics {
    fn record(&self, result: &Result<CleanupOutcome, RepositoryError>) {
        match result {
            Ok(outcome) => {
                self.counters.cleanup_runs.fetch_add(1, Ordering::Relaxed);
                self.counters
                    .payloads_erased
                    .fetch_add(outcome.payloads_erased, Ordering::Relaxed);
                self.counters
                    .complaints_erased
                    .fetch_add(outcome.complaints_erased, Ordering::Relaxed);
                self.counters
                    .orders_erased
                    .fetch_add(outcome.orders_erased, Ordering::Relaxed);
            }
            Err(_) => {
                self.counters
                    .cleanup_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn snapshot(&self) -> RetentionMetricsSnapshot {
        RetentionMetricsSnapshot {
            cleanup_runs: self.counters.cleanup_runs.load(Ordering::Relaxed),
            cleanup_failures: self.counters.cleanup_failures.load(Ordering::Relaxed),
            payloads_erased: self.counters.payloads_erased.load(Ordering::Relaxed),
            complaints_erased: self.counters.complaints_erased.load(Ordering::Relaxed),
            orders_erased: self.counters.orders_erased.load(Ordering::Relaxed),
        }
    }
}

mod admission;
mod capacity;
mod cleanup;
mod evidence;
mod reservations;
mod rows;

pub use capacity::{CapacityUsage, OutputLiquidityKey};

#[cfg(test)]
mod tests;

impl ProofOrderRepository {
    pub(super) fn new(pool: SqlitePool, retention_metrics: RetentionMetrics) -> Self {
        Self {
            pool,
            retention_metrics,
        }
    }

    pub fn retention_metrics(&self) -> RetentionMetricsSnapshot {
        self.retention_metrics.snapshot()
    }
}
