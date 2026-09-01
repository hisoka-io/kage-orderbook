use std::{env, sync::Arc};

use alloy::{
    primitives::{Address, B256},
    signers::{SignerSync, local::PrivateKeySigner},
};
use kage_types::proof_orders::{
    AssignmentTicket, AssignmentTicketClaims, ProofOrderBindings, ReservationRequest,
    ReservationRequestClaims,
};
use thiserror::Error;

#[derive(Clone)]
pub struct AssignmentIssuer {
    signer: Arc<PrivateKeySigner>,
}

impl AssignmentIssuer {
    pub fn from_env() -> Result<Self, AssignmentConfigError> {
        let signer = required("KAGE_ASSIGNMENT_PRIVATE_KEY")?
            .parse::<PrivateKeySigner>()
            .map_err(|_| AssignmentConfigError::InvalidPrivateKey)?;
        Ok(Self {
            signer: Arc::new(signer),
        })
    }

    #[doc(hidden)]
    pub fn for_test(signer: PrivateKeySigner) -> Self {
        Self {
            signer: Arc::new(signer),
        }
    }

    pub fn signer_address(&self) -> Address {
        self.signer.address()
    }

    pub fn issue_reservation_request(
        &self,
        claims: ReservationRequestClaims,
    ) -> Result<ReservationRequest, AssignmentIssueError> {
        let signature = self
            .signer
            .sign_message_sync(&claims.signing_bytes())
            .map_err(|_| AssignmentIssueError::Signing)?;
        Ok(ReservationRequest {
            claims,
            signature: signature.as_bytes().to_vec(),
        })
    }

    pub fn issue_proof_assignment(
        &self,
        bindings: ProofOrderBindings,
        settlement_commitment: B256,
        proof_encryption_key_id: B256,
        now_ms: u64,
    ) -> Result<AssignmentTicket, AssignmentIssueError> {
        let proof_expiry_ms = bindings
            .proof_expires_at_secs
            .checked_mul(1_000)
            .ok_or(AssignmentIssueError::Expired)?;
        let expires_at_ms = proof_expiry_ms;
        if expires_at_ms <= now_ms {
            return Err(AssignmentIssueError::Expired);
        }
        let claims = AssignmentTicketClaims {
            bindings,
            settlement_commitment,
            proof_encryption_key_id,
            issued_at_ms: i64::try_from(now_ms).map_err(|_| AssignmentIssueError::Expired)?,
            expires_at_ms: i64::try_from(expires_at_ms)
                .map_err(|_| AssignmentIssueError::Expired)?,
            nonce: B256::random(),
        };
        let signature = self
            .signer
            .sign_message_sync(&claims.signing_bytes())
            .map_err(|_| AssignmentIssueError::Signing)?;
        Ok(AssignmentTicket {
            claims,
            signature: signature.as_bytes().to_vec(),
        })
    }
}

fn required(name: &'static str) -> Result<String, AssignmentConfigError> {
    env::var(name).map_err(|_| AssignmentConfigError::Missing(name))
}

#[derive(Debug, Error)]
pub enum AssignmentConfigError {
    #[error("{0} must be set")]
    Missing(&'static str),
    #[error("KAGE_ASSIGNMENT_PRIVATE_KEY is invalid")]
    InvalidPrivateKey,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AssignmentIssueError {
    #[error("order has expired")]
    Expired,
    #[error("failed to sign assignment ticket")]
    Signing,
}

#[cfg(test)]
mod tests {
    use alloy::signers::local::PrivateKeySigner;
    use alloy_primitives::Signature;
    use kage_types::proof_orders::{ProofOrderBindings, ReservationRequestClaims};
    use uuid::Uuid;

    use super::*;

    const NOW_MS: u64 = 1_800_000_000_000;

    fn signer() -> PrivateKeySigner {
        PrivateKeySigner::from_slice(&[7; 32]).unwrap()
    }

    #[test]
    fn signs_reservation_requests_and_tickets_with_complete_bindings() {
        let signer = signer();
        let issuer = AssignmentIssuer::for_test(signer.clone());
        let bindings = ProofOrderBindings {
            order_id: Uuid::from_u128(9),
            preview_id: B256::repeat_byte(1),
            category_id: "major-50".to_owned(),
            solver_id: Address::repeat_byte(3),
            exact_terms_digest: B256::repeat_byte(4),
            ciphertext_digest: B256::repeat_byte(5),
            proof_expires_at_secs: (NOW_MS + 120_000) / 1_000,
        };
        let request_claims = ReservationRequestClaims {
            bindings: bindings.clone(),
            attempt_nonce: B256::repeat_byte(6),
            requested_at_ms: NOW_MS as i64,
            attempt_expires_at_ms: (NOW_MS + 2_000) as i64,
        };
        let request = issuer
            .issue_reservation_request(request_claims.clone())
            .unwrap();
        assert_eq!(
            request,
            issuer
                .issue_reservation_request(request_claims.clone())
                .unwrap()
        );
        let request_signature = Signature::try_from(request.signature.as_slice()).unwrap();
        assert_eq!(
            request_signature
                .recover_address_from_msg(request_claims.signing_bytes())
                .unwrap(),
            signer.address()
        );

        let ticket = issuer
            .issue_proof_assignment(
                bindings.clone(),
                B256::repeat_byte(7),
                B256::repeat_byte(8),
                NOW_MS,
            )
            .unwrap();
        assert_eq!(ticket.claims.bindings, bindings);
        assert_eq!(ticket.claims.expires_at_ms, (NOW_MS + 120_000) as i64);
        let ticket_signature = Signature::try_from(ticket.signature.as_slice()).unwrap();
        assert_eq!(
            ticket_signature
                .recover_address_from_msg(ticket.claims.signing_bytes())
                .unwrap(),
            signer.address()
        );
    }
}
