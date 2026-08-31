use alloy_primitives::Address;
use kage_types::{
    api_types::ComplaintStatus,
    proof_orders::{AssignmentTicket, ComplaintEvidenceKind, ReservationAck, ReservationDecline},
};
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::{
    complaint::EncryptedComplaintOpening,
    core::{
        events::OrderEvent,
        state::{CreateOrderOutcome, OrderError},
    },
    order::{Order, OrderId},
    storage::{AdvanceOutcome, NewProofOrder, RepositoryError, SignedProofDecision},
};

pub(in crate::core::engine) enum Request {
    CreateProofOrder {
        input: Box<NewProofOrder>,
        reply: oneshot::Sender<Result<CreateOrderOutcome, ServiceError>>,
    },
    AssignAndDiscloseProofOrder {
        order_id: OrderId,
        solver_id: Address,
        reservation_ack: ReservationAck,
        ticket: Box<AssignmentTicket>,
        reply: oneshot::Sender<Result<bool, ServiceError>>,
    },
    DeclineProofOrder {
        order_id: OrderId,
        solver_id: Address,
        decline: ReservationDecline,
        reply: oneshot::Sender<Result<Option<AdvanceOutcome>, ServiceError>>,
    },
    UpdateProofResult {
        order_id: OrderId,
        solver_id: Address,
        decision: SignedProofDecision,
        reply: oneshot::Sender<Result<bool, ServiceError>>,
    },
    InsertProofComplaint {
        order_id: OrderId,
        evidence_kind: ComplaintEvidenceKind,
        opening: EncryptedComplaintOpening,
        status: ComplaintStatus,
        reason: String,
        admitted_at_ms: i64,
        reply: oneshot::Sender<Result<bool, ServiceError>>,
    },
    ResolveProofComplaint {
        order_id: OrderId,
        reply: oneshot::Sender<Result<bool, ServiceError>>,
    },
    SetProofComplaintLegalHold {
        order_id: OrderId,
        held: bool,
        reply: oneshot::Sender<Result<bool, ServiceError>>,
    },
    GetOrder {
        order_id: OrderId,
        reply: oneshot::Sender<Result<Option<Order>, RepositoryError>>,
    },
}

#[derive(Clone)]
pub struct OrderbookHandle {
    pub(in crate::core::engine) requests: mpsc::Sender<Request>,
    pub(in crate::core::engine) events: broadcast::Sender<OrderEvent>,
}

#[derive(Debug)]
pub enum ServiceError {
    Closed,
    Order(OrderError),
    Repository(RepositoryError),
}

impl OrderbookHandle {
    pub fn is_available(&self) -> bool {
        !self.requests.is_closed()
    }

    pub async fn create_proof_order(
        &self,
        input: NewProofOrder,
    ) -> Result<CreateOrderOutcome, ServiceError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::CreateProofOrder {
                input: Box::new(input),
                reply,
            })
            .await
            .map_err(|_| ServiceError::Closed)?;
        result.await.map_err(|_| ServiceError::Closed)?
    }

    pub async fn assign_and_disclose_proof_order(
        &self,
        order_id: OrderId,
        solver_id: Address,
        reservation_ack: ReservationAck,
        ticket: AssignmentTicket,
    ) -> Result<bool, ServiceError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::AssignAndDiscloseProofOrder {
                order_id,
                solver_id,
                reservation_ack,
                ticket: Box::new(ticket),
                reply,
            })
            .await
            .map_err(|_| ServiceError::Closed)?;
        result.await.map_err(|_| ServiceError::Closed)?
    }

    pub async fn decline_proof_order(
        &self,
        order_id: OrderId,
        solver_id: Address,
        decline: ReservationDecline,
    ) -> Result<Option<AdvanceOutcome>, ServiceError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::DeclineProofOrder {
                order_id,
                solver_id,
                decline,
                reply,
            })
            .await
            .map_err(|_| ServiceError::Closed)?;
        result.await.map_err(|_| ServiceError::Closed)?
    }

    pub async fn update_proof_result(
        &self,
        order_id: OrderId,
        solver_id: Address,
        decision: SignedProofDecision,
    ) -> Result<bool, ServiceError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::UpdateProofResult {
                order_id,
                solver_id,
                decision,
                reply,
            })
            .await
            .map_err(|_| ServiceError::Closed)?;
        result.await.map_err(|_| ServiceError::Closed)?
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn insert_proof_complaint(
        &self,
        order_id: OrderId,
        evidence_kind: ComplaintEvidenceKind,
        opening: EncryptedComplaintOpening,
        status: ComplaintStatus,
        reason: String,
        admitted_at_ms: i64,
    ) -> Result<bool, ServiceError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::InsertProofComplaint {
                order_id,
                evidence_kind,
                opening,
                status,
                reason,
                admitted_at_ms,
                reply,
            })
            .await
            .map_err(|_| ServiceError::Closed)?;
        result.await.map_err(|_| ServiceError::Closed)?
    }

    pub async fn resolve_proof_complaint(&self, order_id: OrderId) -> Result<bool, ServiceError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::ResolveProofComplaint { order_id, reply })
            .await
            .map_err(|_| ServiceError::Closed)?;
        result.await.map_err(|_| ServiceError::Closed)?
    }

    pub async fn set_proof_complaint_legal_hold(
        &self,
        order_id: OrderId,
        held: bool,
    ) -> Result<bool, ServiceError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::SetProofComplaintLegalHold {
                order_id,
                held,
                reply,
            })
            .await
            .map_err(|_| ServiceError::Closed)?;
        result.await.map_err(|_| ServiceError::Closed)?
    }

    pub async fn get_order(&self, order_id: OrderId) -> Result<Option<Order>, ServiceError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::GetOrder { order_id, reply })
            .await
            .map_err(|_| ServiceError::Closed)?;

        result
            .await
            .map_err(|_| ServiceError::Closed)?
            .map_err(ServiceError::Repository)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<OrderEvent> {
        self.events.subscribe()
    }
}
