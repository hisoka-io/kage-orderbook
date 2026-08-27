use std::{env, sync::Arc};

use alloy::{
    primitives::{Address, B256},
    signers::{SignerSync, local::PrivateKeySigner},
};
use kage_types::assignment::{
    AssignmentTicketClaimsV1, AssignmentTicketV1, SolverAssignmentV1, assignment_order_digest,
};
use thiserror::Error;

use crate::order::{Order, OrderState};

const MAX_TICKET_TTL_MS: u64 = 15 * 60 * 1_000;

#[derive(Clone)]
pub struct AssignmentIssuer {
    signer: Arc<PrivateKeySigner>,
    ticket_ttl_ms: u64,
}

impl AssignmentIssuer {
    pub fn from_env() -> Result<Self, AssignmentConfigError> {
        let signer = required("KAGE_ASSIGNMENT_PRIVATE_KEY")?
            .parse::<PrivateKeySigner>()
            .map_err(|_| AssignmentConfigError::InvalidPrivateKey)?;
        let ticket_ttl_ms = optional_positive_u64("KAGE_ASSIGNMENT_TICKET_TTL_MS", 60_000)?;
        if ticket_ttl_ms > MAX_TICKET_TTL_MS {
            return Err(AssignmentConfigError::TicketTtlTooLong);
        }
        Ok(Self {
            signer: Arc::new(signer),
            ticket_ttl_ms,
        })
    }

    #[doc(hidden)]
    pub fn for_test(signer: PrivateKeySigner, ticket_ttl_ms: u64) -> Self {
        Self {
            signer: Arc::new(signer),
            ticket_ttl_ms,
        }
    }

    pub fn signer_address(&self) -> Address {
        self.signer.address()
    }

    pub fn issue(
        &self,
        order: &Order,
        solver_endpoint: &str,
        now_ms: u64,
    ) -> Result<SolverAssignmentV1, AssignmentIssueError> {
        if order.state != OrderState::AwaitingUserProof {
            return Err(AssignmentIssueError::NotReady);
        }
        let solver_id = order.solver.ok_or(AssignmentIssueError::NotReady)?;
        let noise_public_key = order
            .solver_noise_public_key
            .as_deref()
            .and_then(|key| <[u8; 32]>::try_from(key).ok())
            .map(B256::from)
            .filter(|key| *key != B256::ZERO)
            .ok_or(AssignmentIssueError::NotReady)?;
        let order_expires_at_ms = order
            .expires_at_ms
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(AssignmentIssueError::NotReady)?;
        let expires_at_ms = now_ms
            .checked_add(self.ticket_ttl_ms)
            .ok_or(AssignmentIssueError::Expired)?
            .min(order_expires_at_ms);
        if expires_at_ms <= now_ms {
            return Err(AssignmentIssueError::Expired);
        }
        let terms = kage_types::orders::TradeTerms {
            chain_id: order.chain_id,
            token_in: order.token_in,
            token_out: order.token_out,
            amount_in: order.amount_in,
            amount_out: order.amount_out,
            expires_at_ms: order.expires_at_ms.ok_or(AssignmentIssueError::NotReady)?,
        };
        let claims = AssignmentTicketClaimsV1 {
            order_id: order.id,
            order_version: order.version,
            solver_id,
            chain_id: order.chain_id,
            order_digest: assignment_order_digest(&terms),
            solver_endpoint: solver_endpoint.to_owned(),
            solver_noise_public_key: noise_public_key,
            issued_at_ms: i64::try_from(now_ms).map_err(|_| AssignmentIssueError::Expired)?,
            expires_at_ms: i64::try_from(expires_at_ms)
                .map_err(|_| AssignmentIssueError::Expired)?,
            nonce: B256::random(),
        };
        let signature = self
            .signer
            .sign_message_sync(&claims.signing_bytes())
            .map_err(|_| AssignmentIssueError::Signing)?;
        Ok(SolverAssignmentV1 {
            ticket: AssignmentTicketV1 {
                claims,
                signature: signature.as_bytes().to_vec(),
            },
        })
    }
}

fn required(name: &'static str) -> Result<String, AssignmentConfigError> {
    env::var(name).map_err(|_| AssignmentConfigError::Missing(name))
}

fn optional_positive_u64(name: &'static str, default: u64) -> Result<u64, AssignmentConfigError> {
    match env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or(AssignmentConfigError::InvalidPositiveInteger(name)),
        Err(_) => Ok(default),
    }
}

#[derive(Debug, Error)]
pub enum AssignmentConfigError {
    #[error("{0} must be set")]
    Missing(&'static str),
    #[error("{0} must be a positive integer")]
    InvalidPositiveInteger(&'static str),
    #[error("KAGE_ASSIGNMENT_PRIVATE_KEY is invalid")]
    InvalidPrivateKey,
    #[error("KAGE_ASSIGNMENT_TICKET_TTL_MS cannot exceed 900000")]
    TicketTtlTooLong,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AssignmentIssueError {
    #[error("order is not ready for direct proof delivery")]
    NotReady,
    #[error("order has expired")]
    Expired,
    #[error("failed to sign assignment ticket")]
    Signing,
}

#[cfg(test)]
mod tests {
    use alloy::signers::local::PrivateKeySigner;
    use alloy_primitives::U256;
    use kage_types::{
        assignment::assignment_order_digest,
        orders::{OrderState, TradeTerms},
    };
    use uuid::Uuid;

    use super::*;

    const NOW_MS: u64 = 1_800_000_000_000;

    fn signer() -> PrivateKeySigner {
        PrivateKeySigner::from_slice(&[7; 32]).unwrap()
    }

    fn assigned_order(solver_id: Address) -> Order {
        Order {
            id: Uuid::from_u128(1),
            state: OrderState::AwaitingUserProof,
            version: 5,
            chain_id: 31_337,
            token_in: Address::repeat_byte(1),
            token_out: Address::repeat_byte(2),
            amount_in: U256::from(1_000_u64),
            amount_out: U256::from(2_000_u64),
            expires_at_ms: Some(i64::try_from(NOW_MS + 120_000).unwrap()),
            solver: Some(solver_id),
            solver_noise_public_key: Some(vec![9; 32]),
        }
    }

    #[test]
    fn issues_a_short_lived_ticket_bound_to_the_complete_assignment() {
        let signer = signer();
        let solver_id = Address::repeat_byte(3);
        let issuer = AssignmentIssuer::for_test(signer.clone(), 60_000);
        let order = assigned_order(solver_id);

        let assignment = issuer
            .issue(&order, "https://solver.kage.test", NOW_MS)
            .unwrap();
        let claims = &assignment.ticket.claims;
        assert_eq!(claims.order_id, order.id);
        assert_eq!(claims.order_version, order.version);
        assert_eq!(claims.solver_id, solver_id);
        assert_eq!(claims.solver_endpoint, "https://solver.kage.test");
        assert_eq!(claims.solver_noise_public_key, B256::repeat_byte(9));
        assert_eq!(claims.issued_at_ms, NOW_MS as i64);
        assert_eq!(claims.expires_at_ms, (NOW_MS + 60_000) as i64);
        assert_ne!(claims.nonce, B256::ZERO);
        let terms = TradeTerms {
            chain_id: order.chain_id,
            token_in: order.token_in,
            token_out: order.token_out,
            amount_in: order.amount_in,
            amount_out: order.amount_out,
            expires_at_ms: order.expires_at_ms.unwrap(),
        };
        assert_eq!(claims.order_digest, assignment_order_digest(&terms));

        let signature =
            alloy_primitives::Signature::try_from(assignment.ticket.signature.as_slice()).unwrap();
        assert_eq!(
            signature
                .recover_address_from_msg(claims.signing_bytes())
                .unwrap(),
            signer.address()
        );
    }

    #[test]
    fn refuses_orders_that_are_not_ready() {
        let solver_id = Address::repeat_byte(3);
        let issuer = AssignmentIssuer::for_test(signer(), 60_000);
        let mut order = assigned_order(solver_id);
        order.state = OrderState::Reserving;
        assert_eq!(
            issuer
                .issue(&order, "https://solver.kage.test", NOW_MS)
                .unwrap_err(),
            AssignmentIssueError::NotReady
        );
    }

    #[test]
    fn ticket_never_outlives_the_order() {
        let solver_id = Address::repeat_byte(3);
        let issuer = AssignmentIssuer::for_test(signer(), 60_000);
        let mut order = assigned_order(solver_id);
        order.expires_at_ms = Some((NOW_MS + 10_000) as i64);
        assert_eq!(
            issuer
                .issue(&order, "https://solver.kage.test", NOW_MS)
                .unwrap()
                .ticket
                .claims
                .expires_at_ms,
            (NOW_MS + 10_000) as i64
        );
    }
}
