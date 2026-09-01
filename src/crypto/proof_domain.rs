use alloy_primitives::{Address, B256, keccak256};
use ark_bn254::Fr;
use ark_ff::{AdditiveGroup, BigInteger, PrimeField};

const DOMAIN_LABEL: &str = "kage.proof.domain.v1";
const PROTOCOL_VERSION: u64 = 1;
const RATE: usize = 3;
const TWO_POW_64: u128 = 1 << 64;

pub fn proof_domain(chain_id: u64, darkpool: Address) -> B256 {
    let tag = Fr::from_be_bytes_mod_order(keccak256(DOMAIN_LABEL).as_slice());
    let address = Fr::from_be_bytes_mod_order(darkpool.into_word().as_slice());
    let domain = poseidon2(&[tag, Fr::from(PROTOCOL_VERSION), Fr::from(chain_id), address]);
    B256::from_slice(&domain.into_bigint().to_bytes_be())
}

fn poseidon2(inputs: &[Fr]) -> Fr {
    let iv = Fr::from(inputs.len() as u128 * TWO_POW_64);
    let mut state = [Fr::ZERO, Fr::ZERO, Fr::ZERO, iv];
    let mut cache = [Fr::ZERO; RATE];
    let mut cached = 0;
    for input in inputs {
        if cached == RATE {
            for (slot, absorbed) in state.iter_mut().zip(cache) {
                *slot += absorbed;
            }
            state = taceo_poseidon2::bn254::t4::permutation(&state);
            cache[0] = *input;
            cached = 1;
        } else {
            cache[cached] = *input;
            cached += 1;
        }
    }
    for (slot, absorbed) in state.iter_mut().zip(&cache[..cached]) {
        *slot += absorbed;
    }
    taceo_poseidon2::bn254::t4::permutation(&state)[0]
}

#[cfg(test)]
mod tests {
    use alloy_primitives::address;

    use super::*;

    #[test]
    fn matches_the_circuit_sdk_and_solver_vector() {
        assert_eq!(
            proof_domain(31_337, address!("3Aa5ebB10DC797CAC828524e59A333d0A371443c")),
            "0x218015d81e0358e2a28413bf26e312ca3535f4a771d81637c331d30847afa078"
                .parse::<B256>()
                .unwrap()
        );
    }
}
