use kage_types::proof::IntentProofV1;

pub const INTENT_PROOF_FIELDS: usize = 458;
pub const INTENT_PUBLIC_INPUTS: usize = 27;
pub const INTENT_VERIFICATION_KEY_FIELDS: usize = 115;
pub const INTENT_VERIFICATION_KEY_HASH: &str =
    "0x2e0b51ed4736571c1daa939f67d50684aa262bab910d8971e77c0d65f89efc50";

pub fn validate(proof: &IntentProofV1) -> Result<(), ProofValidationError> {
    if proof.version != 1
        || proof.circuit != "swap_intent"
        || proof.proof_system != "ultra_honk"
        || proof.verifier_target != "noir-recursive"
    {
        return Err(ProofValidationError::UnsupportedEnvelope);
    }

    validate_field_list("proof fields", &proof.proof_as_fields, INTENT_PROOF_FIELDS)?;
    validate_field_list("public inputs", &proof.public_inputs, INTENT_PUBLIC_INPUTS)?;
    validate_field_list(
        "verification-key fields",
        &proof.verification_key_fields,
        INTENT_VERIFICATION_KEY_FIELDS,
    )?;

    if !is_canonical_bytes(&proof.proof) {
        return Err(ProofValidationError::InvalidProofBytes);
    }
    let proof_body = &proof.proof[2..];
    if !proof_body.len().is_multiple_of(64) {
        return Err(ProofValidationError::InvalidProofBytes);
    }
    if proof_body.len() / 64 != proof.proof_as_fields.len()
        || proof_body
            .as_bytes()
            .chunks_exact(64)
            .zip(&proof.proof_as_fields)
            .any(|(chunk, field)| field.as_bytes().get(2..) != Some(chunk))
    {
        return Err(ProofValidationError::MismatchedProofFields);
    }
    if !is_canonical_field(&proof.verification_key_hash)
        || proof.verification_key_hash != INTENT_VERIFICATION_KEY_HASH
    {
        return Err(ProofValidationError::VerificationKeyHash);
    }
    Ok(())
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
        validate(&valid_proof()).unwrap();
    }

    #[test]
    fn rejects_mismatched_proof_bytes() {
        let mut proof = valid_proof();
        proof.proof_as_fields[0] = field("01");
        assert!(matches!(
            validate(&proof),
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
