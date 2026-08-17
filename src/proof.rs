use serde::{Deserialize, Serialize};

pub const INTENT_PROOF_FIELDS: usize = 458;
pub const INTENT_PUBLIC_INPUTS: usize = 27;
pub const INTENT_VERIFICATION_KEY_FIELDS: usize = 115;
pub const INTENT_VERIFICATION_KEY_HASH: &str =
    "0x2e0b51ed4736571c1daa939f67d50684aa262bab910d8971e77c0d65f89efc50";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IntentProofV1 {
    pub version: u8,
    pub circuit: String,
    pub proof_system: String,
    pub verifier_target: String,
    pub proof: String,
    pub proof_as_fields: Vec<String>,
    pub public_inputs: Vec<String>,
    pub verification_key_fields: Vec<String>,
    pub verification_key_hash: String,
}

impl IntentProofV1 {
    pub fn validate(&self) -> Result<(), ProofValidationError> {
        if self.version != 1
            || self.circuit != "swap_intent"
            || self.proof_system != "ultra_honk"
            || self.verifier_target != "noir-recursive"
        {
            return Err(ProofValidationError::UnsupportedEnvelope);
        }

        validate_field_list("proof fields", &self.proof_as_fields, INTENT_PROOF_FIELDS)?;
        validate_field_list("public inputs", &self.public_inputs, INTENT_PUBLIC_INPUTS)?;
        validate_field_list(
            "verification-key fields",
            &self.verification_key_fields,
            INTENT_VERIFICATION_KEY_FIELDS,
        )?;

        if !is_canonical_bytes(&self.proof) {
            return Err(ProofValidationError::InvalidProofBytes);
        }
        let proof_body = &self.proof[2..];
        if !proof_body.len().is_multiple_of(64) {
            return Err(ProofValidationError::InvalidProofBytes);
        }
        if proof_body.len() / 64 != self.proof_as_fields.len()
            || proof_body
                .as_bytes()
                .chunks_exact(64)
                .zip(&self.proof_as_fields)
                .any(|(chunk, field)| field.as_bytes().get(2..) != Some(chunk))
        {
            return Err(ProofValidationError::MismatchedProofFields);
        }
        if !is_canonical_field(&self.verification_key_hash)
            || self.verification_key_hash != INTENT_VERIFICATION_KEY_HASH
        {
            return Err(ProofValidationError::VerificationKeyHash);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProofValidationError {
    #[error("unsupported intent proof envelope")]
    UnsupportedEnvelope,
    #[error("{name} length {actual}; expected {expected}")]
    FieldCount {
        name: &'static str,
        actual: usize,
        expected: usize,
    },
    #[error("{0} must contain canonical lowercase fields")]
    InvalidField(&'static str),
    #[error("proof must be non-empty, lowercase, byte-aligned hex")]
    InvalidProofBytes,
    #[error("proof bytes and proof fields do not match")]
    MismatchedProofFields,
    #[error("verification-key hash does not match the pinned circuit")]
    VerificationKeyHash,
}

fn validate_field_list(
    name: &'static str,
    values: &[String],
    expected: usize,
) -> Result<(), ProofValidationError> {
    if values.len() != expected {
        return Err(ProofValidationError::FieldCount {
            name,
            actual: values.len(),
            expected,
        });
    }
    if values.iter().any(|value| !is_canonical_field(value)) {
        return Err(ProofValidationError::InvalidField(name));
    }
    Ok(())
}

fn is_canonical_field(value: &str) -> bool {
    value.len() == 66
        && value.starts_with("0x")
        && value.as_bytes()[2..]
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
}

fn is_canonical_bytes(value: &str) -> bool {
    value.len() > 2
        && value.len().is_multiple_of(2)
        && value.starts_with("0x")
        && value.as_bytes()[2..]
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_a_pinned_proof_envelope() {
        valid_proof().validate().unwrap();
    }

    #[test]
    fn rejects_mismatched_proof_bytes() {
        let mut proof = valid_proof();
        proof.proof_as_fields[0] = field("01");
        assert!(matches!(
            proof.validate(),
            Err(ProofValidationError::MismatchedProofFields)
        ));
    }

    fn valid_proof() -> IntentProofV1 {
        IntentProofV1 {
            version: 1,
            circuit: "swap_intent".to_owned(),
            proof_system: "ultra_honk".to_owned(),
            verifier_target: "noir-recursive".to_owned(),
            proof: format!("0x{}", "0".repeat(INTENT_PROOF_FIELDS * 64)),
            proof_as_fields: vec![field("00"); INTENT_PROOF_FIELDS],
            public_inputs: vec![field("00"); INTENT_PUBLIC_INPUTS],
            verification_key_fields: vec![field("00"); INTENT_VERIFICATION_KEY_FIELDS],
            verification_key_hash: INTENT_VERIFICATION_KEY_HASH.to_owned(),
        }
    }

    fn field(suffix: &str) -> String {
        format!("0x{suffix:0>64}")
    }
}
