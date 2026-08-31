use std::{collections::HashSet, sync::Arc};

use alloy_primitives::{Address, B256, U256};
use kage_price_estimate::domain::{PairSnapshot, PriceE18, Spot};
use kage_types::routing::{PreviewRequest, PreviewRoute, SolverCapabilities, SolverMarket};

use super::{
    PreviewService,
    calculation::{deviation_bps, output_amount, route_supports_category, solver_is_exposable},
    model::{Category, Market},
};
use crate::{
    config::AppConfig,
    pricing::EmbeddedPricing,
    registry::{SolverProfile, SolverRegistry},
    session::{CapabilityRoute, SolverSessions},
    storage::OrderRepository,
};

const CONFIG: &str = r#"{
      "allowed_solvers": [
        "0x1111111111111111111111111111111111111111",
        "0x2222222222222222222222222222222222222222"
      ],
      "proof_orders": {
        "proof_lifetime_seconds": 30,
        "minimum_remaining_seconds": 15,
        "preview_lifetime_seconds": 15,
        "reservation_attempt_timeout_ms": 2000,
        "max_recipients": 8,
        "preview_cleanup_grace_seconds": 300,
        "ciphertext_cleanup_grace_seconds": 300,
        "complaint_window_seconds": 2592000,
        "evidence_retention_seconds": 2592000,
        "resolved_complaint_retention_seconds": 2592000
      },
      "fee_categories": [
        {
          "id": "major-50",
          "fee_bps": 50,
          "markets": ["ETH/USDC"],
          "solver_ids": [
            "0x1111111111111111111111111111111111111111",
            "0x2222222222222222222222222222222222222222"
          ]
        },
        {
          "id": "priority-75",
          "fee_bps": 75,
          "markets": ["ETH/USDC"],
          "solver_ids": [
            "0x1111111111111111111111111111111111111111",
            "0x2222222222222222222222222222222222222222"
          ]
        }
      ],
      "database": { "max_connections": 1, "busy_timeout_ms": 5000 },
      "runtime": { "command_capacity": 256 },
      "pricing": {
        "max_age_ms": 5000,
        "reconnect_delay_ms": 50,
        "idle_timeout_ms": 1000
      },
      "chains": [{
        "chain_id": 31337,
        "name": "local",
        "darkpool": "0x3Aa5ebB10DC797CAC828524e59A333d0A371443c",
        "registry": "0x0404040404040404040404040404040404040404",
        "registry_deploy_block": 1,
        "confirmations": 0,
        "tokens": [
          {
            "symbol": "ETH",
            "address": "0x0101010101010101010101010101010101010101",
            "decimals": 18,
            "pricing_asset": "ETH",
            "max_price_deviation_bps": 200
          },
          {
            "symbol": "USDC",
            "address": "0x0202020202020202020202020202020202020202",
            "decimals": 6,
            "pricing_asset": "USDC",
            "max_price_deviation_bps": 200
          }
        ],
        "markets": [{
          "token_in": "ETH",
          "token_out": "USDC",
          "movement_allowance_bps": 10,
          "max_price_deviation_bps": 200
        }]
      }]
    }"#;

fn market() -> Market {
    Market {
        asset_in: "ETH".to_owned(),
        asset_out: "USDC".to_owned(),
        decimals_in: 18,
        decimals_out: 6,
        movement_allowance_bps: 15,
        max_total_deviation_bps: 100,
        categories: Vec::new(),
    }
}

fn route(minimum_margin_bps: u16) -> CapabilityRoute {
    CapabilityRoute {
        route: PreviewRoute {
            solver_id: Address::repeat_byte(1),
            min_amount_in: U256::from(1),
            max_amount_in: U256::from(100),
            encryption_key_id: B256::repeat_byte(2),
            encryption_public_key: vec![3; 32],
            key_expires_at_ms: 10_000,
        },
        minimum_margin_bps,
        max_in_flight: 2,
        available_amount_out: U256::MAX,
    }
}

fn category(fee_bps: u16) -> Category {
    Category {
        id: "test".to_owned(),
        fee_bps,
        solvers: Arc::new([Address::repeat_byte(1)].into_iter().collect()),
    }
}

#[test]
fn movement_allowance_must_fit_inside_the_category_fee() {
    assert!(route_supports_category(
        &route(35),
        &market(),
        &category(50),
        U256::from(100)
    ));
    assert!(!route_supports_category(
        &route(36),
        &market(),
        &category(50),
        U256::from(100)
    ));
    let mut insufficient = route(35);
    insufficient.available_amount_out = U256::from(99);
    assert!(!route_supports_category(
        &insufficient,
        &market(),
        &category(50),
        U256::from(100)
    ));
}

#[test]
fn arithmetic_handles_token_decimal_boundaries() {
    let price = U256::from(10_u64).pow(U256::from(18));
    for decimals_in in [0, 6, 8, 18, crate::config::MAX_TOKEN_DECIMALS] {
        for decimals_out in [0, 6, 8, 18, crate::config::MAX_TOKEN_DECIMALS] {
            let one_input = U256::from(10_u8).pow(U256::from(decimals_in));
            let one_output = U256::from(10_u8).pow(U256::from(decimals_out));
            assert_eq!(
                output_amount(one_input, decimals_in, decimals_out, price, price).unwrap(),
                one_output
            );
        }
    }
    assert!(output_amount(U256::ZERO, 18, 6, price, price).is_err());
    assert!(
        output_amount(
            U256::from(10_u8).pow(U256::from(18)),
            18,
            6,
            price,
            U256::ZERO
        )
        .is_err()
    );
}

#[test]
fn deviation_rounds_up_and_rejects_inverted_values() {
    assert_eq!(
        deviation_bps(U256::from(10_000), U256::from(9_999)).unwrap(),
        1
    );
    assert!(deviation_bps(U256::ZERO, U256::ZERO).is_err());
    assert!(deviation_bps(U256::from(1), U256::from(2)).is_err());
}

#[test]
fn quote_arithmetic_properties_hold_across_amounts_prices_and_decimals() {
    let unit = U256::from(10_u64).pow(U256::from(18));
    for decimals_in in [0, 1, 6, 8, 18, crate::config::MAX_TOKEN_DECIMALS] {
        for decimals_out in [0, 1, 6, 8, 18, crate::config::MAX_TOKEN_DECIMALS] {
            for amount in [1_u64, 2, 9, 10, 999, 1_000_000] {
                for price_in in [unit / U256::from(2), unit, unit * U256::from(3)] {
                    for price_out in [unit / U256::from(2), unit, unit * U256::from(5)] {
                        let amount = U256::from(amount);
                        let output =
                            output_amount(amount, decimals_in, decimals_out, price_in, price_out)
                                .unwrap();
                        let doubled = output_amount(
                            amount * U256::from(2),
                            decimals_in,
                            decimals_out,
                            price_in,
                            price_out,
                        )
                        .unwrap();
                        assert!(doubled >= output);
                        assert!(doubled <= output * U256::from(2) + U256::from(1));
                    }
                }
            }
        }
    }
}

#[test]
fn preview_solver_gate_requires_allowlist_and_active_registry_membership() {
    let active = Address::repeat_byte(1);
    let inactive = Address::repeat_byte(2);
    let unknown = Address::repeat_byte(3);
    let registry = SolverRegistry::from_profiles([
        (
            active,
            SolverProfile {
                noise_public_key: B256::repeat_byte(4),
                active: true,
            },
        ),
        (
            inactive,
            SolverProfile {
                noise_public_key: B256::repeat_byte(5),
                active: false,
            },
        ),
    ]);
    assert!(solver_is_exposable(
        &HashSet::from([active]),
        &registry,
        active
    ));
    assert!(!solver_is_exposable(
        &HashSet::from([active, inactive]),
        &registry,
        inactive
    ));
    assert!(!solver_is_exposable(
        &HashSet::from([active, unknown]),
        &registry,
        unknown
    ));
    assert!(!solver_is_exposable(&HashSet::new(), &registry, active));
}

#[tokio::test]
async fn preview_returns_every_category_and_route_without_internal_inputs() {
    let config = AppConfig::from_json(CONFIG).unwrap();
    let repository = OrderRepository::connect("sqlite::memory:").await.unwrap();
    let now = 1_000_000_u64;
    let spot = |price| Spot {
        price: PriceE18::new(price).unwrap(),
        observed_at_ms: now,
        valid_until_ms: now + 20_000,
        sequence: 7,
        min_price: PriceE18::new(price).unwrap(),
        max_price: PriceE18::new(price).unwrap(),
        spread_bps: 0,
        samples: Vec::new(),
    };
    let pricing = EmbeddedPricing::fixed(PairSnapshot {
        sequence: 7,
        published_at_ms: now,
        from: spot(2_000_000_000_000_000_000_000),
        to: spot(1_000_000_000_000_000_000),
    });
    let sessions = SolverSessions::new("kage-orderbook:preview-test");
    let solvers = [Address::repeat_byte(0x11), Address::repeat_byte(0x22)];
    for (index, solver_id) in solvers.into_iter().enumerate() {
        let opened = sessions.open(solver_id, now);
        sessions
            .register_capabilities(
                &opened.token,
                SolverCapabilities {
                    revision: 1,
                    max_in_flight: 2,
                    encryption_key_id: B256::repeat_byte(0x31 + index as u8),
                    encryption_public_key: vec![0x41 + index as u8; 32],
                    key_expires_at_ms: now as i64 + 60_000,
                    markets: vec![SolverMarket {
                        chain_id: 31_337,
                        token_in: Address::repeat_byte(1),
                        token_out: Address::repeat_byte(2),
                        min_amount_in: U256::from(1),
                        max_amount_in: U256::from(2_000_000_000_000_000_000_u64),
                        available_amount_out: U256::from(4_000_000_000_u64),
                        minimum_margin_bps: 20,
                    }],
                },
                now,
            )
            .unwrap();
    }
    let registry = SolverRegistry::from_profiles(solvers.map(|solver_id| {
        (
            solver_id,
            SolverProfile {
                noise_public_key: B256::repeat_byte(9),
                active: true,
            },
        )
    }));
    let service = PreviewService::new(
        pricing,
        sessions,
        registry,
        repository.previews(),
        repository.proof_orders(),
        &config,
    );
    let preview = service
        .create(
            PreviewRequest {
                chain_id: 31_337,
                token_in: Address::repeat_byte(1),
                token_out: Address::repeat_byte(2),
                amount_in: U256::from(1_000_000_000_000_000_000_u64),
            },
            now,
        )
        .await
        .unwrap();

    assert_eq!(preview.categories.len(), 2);
    assert!(
        preview
            .categories
            .iter()
            .all(|category| category.routes.len() == 2)
    );
    assert_eq!(
        preview.categories[0].exact_amount_out,
        U256::from(1_990_000_000_u64)
    );
    assert_eq!(
        preview.categories[1].exact_amount_out,
        U256::from(1_985_000_000_u64)
    );
    let public = serde_json::to_value(&preview).unwrap();
    assert!(public.get("price_in_e18").is_none());
    assert!(public.get("pricing_sequence").is_none());
    assert!(public.to_string().find("minimum_margin_bps").is_none());
    assert_eq!(
        repository
            .previews()
            .get(preview.preview_id)
            .await
            .unwrap()
            .unwrap()
            .response,
        preview
    );
}
