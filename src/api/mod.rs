mod auth;
mod error;
mod health;
mod router;
mod solver;
mod state;
mod user;
mod websocket;

pub use kage_types::api_types::{
    ApiErrorResponse, ComplaintResponse, ComplaintStatus, CreateComplaintRequest,
    CreateOrderResponse, ORDER_ACCESS_TOKEN_HEADER, UserEventClientMessage, UserEventServerMessage,
};
pub use router::{ApiRuntime, router, supervised_router};

use state::{ApiState, now_ms};

#[cfg(test)]
use std::{collections::HashSet, sync::Arc};

#[cfg(test)]
use crate::{
    assignment::AssignmentIssuer,
    complaint::{ComplaintEvidenceCipher, ComplaintVerifier},
    config::{ApiSettings, ProofOrderSettings},
    order::OrderId,
    preview::PreviewService,
    registry::SolverRegistry,
    session::SolverSessions,
    storage::{
        NewProofOrder, PendingReservation, ProofOrderBinding, ProofOrderRepository,
        SignedProofDecision,
    },
};
#[cfg(test)]
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, header::CONTENT_TYPE},
    routing::post,
};
#[cfg(test)]
use kage_types::{
    proof_orders::{
        ComplaintEvidenceKind, ProofAcceptanceAck, ProofRejectionAck, ReservationAck,
        ReservationDecline, SolverProofDecisionRequest, settlement_commitment,
    },
    routing::PROOF_ENVELOPE_SUITE,
};
#[cfg(test)]
use router::router_with_components;
#[cfg(test)]
use solver::validation::{
    proof_acceptance_is_valid, proof_rejection_is_valid, reservation_ack_is_valid,
    reservation_decline_is_valid,
};
#[cfg(test)]
use user::{
    complaints::create_complaint_at,
    orders::{expected_proof_expiry_secs, proof_deadline_is_admissible, rotate_candidates},
};

#[cfg(test)]
mod tests;
