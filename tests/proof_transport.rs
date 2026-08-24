use kage_orderbook::proof::transport::{
    ProofTransportError, decrypt_from_user, encrypt_for_solver, public_key,
};
use uuid::Uuid;

const MOCK_PRIVATE_KEY: [u8; 32] = [0x33; 32];
const EXPECTED_MOCK_PUBLIC_KEY: [u8; 32] = [
    0x7b, 0x0d, 0x47, 0xd9, 0x34, 0x27, 0xf8, 0x31, 0x11, 0x60, 0x78, 0x1c, 0x7c, 0x73, 0x3f, 0xd8,
    0x9f, 0x88, 0x97, 0x0a, 0xef, 0x49, 0x0d, 0x8a, 0xa0, 0xee, 0x19, 0xa4, 0xcb, 0x8a, 0x1b, 0x14,
];

#[test]
fn round_trips_a_chunked_realistic_proof() {
    let order_id = Uuid::new_v4();
    let private_key = [0x33; 32];
    let public_key = public_key(&private_key).unwrap();
    let proof = vec![0x5a; 75_000];

    let envelope = encrypt_for_solver(order_id, &public_key, &proof).unwrap();
    assert!(!envelope.windows(32).any(|window| window == &proof[..32]));
    assert_eq!(
        decrypt_from_user(order_id, &private_key, &envelope).unwrap(),
        proof
    );
}

#[test]
fn derives_the_expected_mock_public_key() {
    assert_eq!(
        public_key(&MOCK_PRIVATE_KEY).unwrap(),
        EXPECTED_MOCK_PUBLIC_KEY
    );
}

#[test]
fn rejects_tampering_and_the_wrong_private_key() {
    let order_id = Uuid::new_v4();
    let private_key = [0x33; 32];
    let public_key = public_key(&private_key).unwrap();
    let mut envelope = encrypt_for_solver(order_id, &public_key, b"proof").unwrap();
    let last = envelope.len() - 1;
    envelope[last] ^= 1;

    assert!(decrypt_from_user(order_id, &private_key, &envelope).is_err());
    let valid = encrypt_for_solver(order_id, &public_key, b"proof").unwrap();
    assert!(decrypt_from_user(order_id, &[0x44; 32], &valid).is_err());
}

#[test]
fn binds_ciphertext_to_one_order_and_rejects_bad_framing() {
    let order_id = Uuid::new_v4();
    let private_key = [0x33; 32];
    let public_key = public_key(&private_key).unwrap();
    let envelope = encrypt_for_solver(order_id, &public_key, b"proof").unwrap();

    assert!(decrypt_from_user(Uuid::new_v4(), &private_key, &envelope).is_err());
    assert!(matches!(
        decrypt_from_user(order_id, &private_key, b"not-noise"),
        Err(ProofTransportError::MalformedEnvelope)
    ));
}
