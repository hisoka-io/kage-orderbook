use std::path::Path;

use alloy_primitives::Address;

use super::*;

const VALID_CONFIG: &str = r#"
    {
      "allowed_solvers": [
        "0x0909090909090909090909090909090909090909",
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
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
      "fee_categories": [{
        "id": "eth-10",
        "fee_bps": 10,
        "markets": ["ETH/USDC"],
        "solver_ids": [
          "0x0909090909090909090909090909090909090909",
          "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ]
      }],
      "database": { "max_connections": 1, "busy_timeout_ms": 5000 },
      "runtime": { "command_capacity": 256 },
      "pricing": { "max_age_ms": 5000, "reconnect_delay_ms": 1000, "idle_timeout_ms": 30000 },
      "chains": [{
        "chain_id": 31337,
        "name": "local",
        "darkpool": "0x0303030303030303030303030303030303030303",
        "registry": "0x0404040404040404040404040404040404040404",
        "registry_deploy_block": 100,
        "confirmations": 0,
        "tokens": [
          {
            "symbol": "ETH",
            "address": "0x0101010101010101010101010101010101010101",
            "decimals": 18,
            "pricing_asset": "ETH",
            "max_price_deviation_bps": 50
          },
          {
            "symbol": "USDC",
            "address": "0x0202020202020202020202020202020202020202",
            "decimals": 6,
            "pricing_asset": "USDC",
            "max_price_deviation_bps": 20
          }
        ],
        "markets": [{
          "token_in": "ETH",
          "token_out": "USDC",
          "max_price_deviation_bps": 20
        }]
      }]
    }
    "#;

#[test]
fn loads_metadata_and_canonical_market_policy() {
    let config = AppConfig::from_json(VALID_CONFIG).unwrap();
    assert_eq!(config.pricing_assets(), vec!["ETH", "USDC"]);
    assert_eq!(config.chains[0].darkpool, Address::repeat_byte(3));
    assert_eq!(config.chains[0].registry, Address::repeat_byte(4));
    assert_eq!(config.chains[0].registry_deploy_block, 100);
    assert_eq!(config.api.request_timeout_ms, 10_000);
    assert_eq!(config.api.max_body_bytes, 8 * 1024 * 1024);
    assert_eq!(config.api.max_order_request_bytes, 8 * 1024 * 1024);
    assert_eq!(config.api.max_ciphertext_bytes, 7 * 1024 * 1024);
    assert!(config.api.allowed_origins.is_empty());
    assert_eq!(config.allowed_solvers.len(), 2);
    assert_eq!(config.runtime.shutdown_grace_ms, 15_000);
    assert_eq!(config.proof_orders, ProofOrderSettings::default());
    assert_eq!(config.fee_categories[0].id, "eth-10");
}

#[test]
fn validates_shutdown_grace_period() {
    let zero = VALID_CONFIG.replace(
        "\"runtime\": { \"command_capacity\": 256 }",
        "\"runtime\": { \"command_capacity\": 256, \"shutdown_grace_ms\": 0 }",
    );
    assert!(matches!(
        AppConfig::from_json(&zero),
        Err(ConfigError::Invalid(_))
    ));

    let excessive = VALID_CONFIG.replace(
        "\"runtime\": { \"command_capacity\": 256 }",
        "\"runtime\": { \"command_capacity\": 256, \"shutdown_grace_ms\": 300001 }",
    );
    assert!(matches!(
        AppConfig::from_json(&excessive),
        Err(ConfigError::Invalid(_))
    ));
}

#[test]
fn validates_api_limits_and_exact_origins() {
    let api = r#""api": {
          "request_timeout_ms": 10000,
          "max_body_bytes": 65536,
          "max_order_request_bytes": 65536,
          "max_ciphertext_bytes": 60000,
          "websocket_max_message_bytes": 16384,
          "rate_limit_replenish_ms": 100,
          "rate_limit_burst": 100,
          "cors_max_age_seconds": 600,
          "allowed_origins": ["https://app.example.com"]
        },
        "allowed_solvers":"#;
    let configured = VALID_CONFIG.replace("\"allowed_solvers\":", api);
    let config = AppConfig::from_json(&configured).unwrap();
    assert_eq!(config.api.allowed_origins, vec!["https://app.example.com"]);

    let zero_limit =
        configured.replace("\"request_timeout_ms\": 10000", "\"request_timeout_ms\": 0");
    assert!(matches!(
        AppConfig::from_json(&zero_limit),
        Err(ConfigError::Invalid(_))
    ));

    let origin_with_path =
        configured.replace("https://app.example.com", "https://app.example.com/path");
    assert!(matches!(
        AppConfig::from_json(&origin_with_path),
        Err(ConfigError::Invalid(_))
    ));
}

#[test]
fn rejects_zero_contract_addresses() {
    let configured = Address::repeat_byte(4).to_string();
    let zeroed = VALID_CONFIG.replace(&configured, &Address::ZERO.to_string());
    assert_ne!(zeroed, VALID_CONFIG, "{configured} is not in the fixture");
    assert!(matches!(
        AppConfig::from_json(&zeroed),
        Err(ConfigError::Invalid(_))
    ));
}

#[test]
fn rejects_unknown_fields_and_invalid_policy() {
    let unknown = VALID_CONFIG.replace(
        "\"command_capacity\": 256",
        "\"command_capacity\": 256, \"unexpected\": true",
    );
    assert!(matches!(
        AppConfig::from_json(&unknown),
        Err(ConfigError::Json(_))
    ));

    let bad_bps = VALID_CONFIG.replace(
        "\"max_price_deviation_bps\": 50",
        "\"max_price_deviation_bps\": 0",
    );
    assert!(matches!(
        AppConfig::from_json(&bad_bps),
        Err(ConfigError::Invalid(_))
    ));
}

#[test]
fn checked_in_config_is_valid() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config/localnet.json");
    let config = AppConfig::load_from(path).unwrap();

    assert_eq!(config.pricing_assets(), vec!["ETH", "USDC", "USDT"]);
    assert_eq!(config.chains[0].markets.len(), 6);
    assert_eq!(config.allowed_solvers.len(), 2);
    assert_eq!(config.fee_categories.len(), 2);
    assert_eq!(
        config.proof_orders,
        ProofOrderSettings {
            proof_lifetime_seconds: 120,
            minimum_remaining_seconds: 60,
            complaint_finality: ComplaintFinalityPolicy::Confirmations { count: 1 },
            ..ProofOrderSettings::default()
        }
    );
    assert_eq!(config.chains[0].markets[0].movement_allowance_bps, 15);
}

#[test]
fn validates_owned_solver_and_movement_policies() {
    let solver = Address::repeat_byte(9).to_string();
    let empty = VALID_CONFIG.replace(
        "\"0x0909090909090909090909090909090909090909\",\n        \"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"",
        "",
    );
    assert!(matches!(
        AppConfig::from_json(&empty),
        Err(ConfigError::Invalid(_))
    ));

    let duplicate = VALID_CONFIG.replace(
        "\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"\n      ],",
        &format!("\"{solver}\"\n      ],"),
    );
    assert!(matches!(
        AppConfig::from_json(&duplicate),
        Err(ConfigError::Invalid(_))
    ));

    let excessive_allowance = VALID_CONFIG.replace(
        "\"max_price_deviation_bps\": 20\n        }",
        "\"movement_allowance_bps\": 10, \"max_price_deviation_bps\": 20\n        }",
    );
    assert!(matches!(
        AppConfig::from_json(&excessive_allowance),
        Err(ConfigError::Invalid(_))
    ));
}

#[test]
fn validates_fee_categories_and_proof_order_bounds() {
    let unknown_category_field = VALID_CONFIG.replace(
        "\"id\": \"eth-10\"",
        "\"id\": \"eth-10\", \"unexpected\": false",
    );
    assert!(matches!(
        AppConfig::from_json(&unknown_category_field),
        Err(ConfigError::Json(_))
    ));

    let duplicate_category = VALID_CONFIG.replace(
        "      }],\n      \"database\":",
        "      }, {\n        \"id\": \"eth-10\",\n        \"fee_bps\": 10,\n        \"markets\": [\"ETH/USDC\"],\n        \"solver_ids\": [\"0x0909090909090909090909090909090909090909\"]\n      }],\n      \"database\":"
    );
    assert_ne!(duplicate_category, VALID_CONFIG);
    assert!(matches!(
        AppConfig::from_json(&duplicate_category),
        Err(ConfigError::Invalid(_))
    ));

    for (from, to) in [
        ("\"id\": \"eth-10\"", "\"id\": \"Invalid Category\""),
        ("\"fee_bps\": 10", "\"fee_bps\": 0"),
        ("\"markets\": [\"ETH/USDC\"]", "\"markets\": [\"USDC/ETH\"]"),
        (
            "\"minimum_remaining_seconds\": 15",
            "\"minimum_remaining_seconds\": 30",
        ),
        ("\"max_recipients\": 8", "\"max_recipients\": 1"),
        (
            "\"evidence_retention_seconds\": 2592000",
            "\"evidence_retention_seconds\": 1",
        ),
        (
            "\"preview_cleanup_grace_seconds\": 300",
            "\"preview_cleanup_grace_seconds\": 29",
        ),
    ] {
        let invalid_config = VALID_CONFIG.replace(from, to);
        assert_ne!(
            invalid_config, VALID_CONFIG,
            "fixture does not contain {from}"
        );
        assert!(matches!(
            AppConfig::from_json(&invalid_config),
            Err(ConfigError::Invalid(_))
        ));
    }

    let duplicate_market = VALID_CONFIG.replace(
        "\"markets\": [\"ETH/USDC\"]",
        "\"markets\": [\"ETH/USDC\", \"ETH/USDC\"]",
    );
    assert!(matches!(
        AppConfig::from_json(&duplicate_market),
        Err(ConfigError::Invalid(_))
    ));

    let duplicate_solver = VALID_CONFIG.replace(
        "\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"\n        ]",
        "\"0x0909090909090909090909090909090909090909\"\n        ]",
    );
    assert!(matches!(
        AppConfig::from_json(&duplicate_solver),
        Err(ConfigError::Invalid(_))
    ));

    let unowned_solver = VALID_CONFIG.replace(
        "\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"\n        ]",
        "\"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\"\n        ]",
    );
    assert!(matches!(
        AppConfig::from_json(&unowned_solver),
        Err(ConfigError::Invalid(_))
    ));
}

#[test]
fn complaint_finality_is_explicit_and_fail_closed() {
    assert_eq!(
        AppConfig::from_json(VALID_CONFIG)
            .unwrap()
            .proof_orders
            .complaint_finality,
        ComplaintFinalityPolicy::Finalized
    );
    let confirmed = VALID_CONFIG.replace(
        r#""resolved_complaint_retention_seconds": 2592000"#,
        r#""resolved_complaint_retention_seconds": 2592000,
             "complaint_finality": { "mode": "confirmations", "count": 6 }"#,
    );
    assert_eq!(
        AppConfig::from_json(&confirmed)
            .unwrap()
            .proof_orders
            .complaint_finality,
        ComplaintFinalityPolicy::Confirmations { count: 6 }
    );
    let ambiguous_latest = confirmed.replace(r#""count": 6"#, r#""count": 0"#);
    assert!(matches!(
        AppConfig::from_json(&ambiguous_latest),
        Err(ConfigError::Invalid(_))
    ));
}

#[test]
fn network_names_and_stamps_are_distinct() {
    for network in [Network::Localnet, Network::Testnet, Network::Mainnet] {
        assert_eq!(network.as_str().parse::<Network>().unwrap(), network);
        assert_eq!(Network::from_stamp(network.stamp()), Some(network));
    }
    assert!("mainet".parse::<Network>().is_err());
    assert!(Network::from_stamp(0).is_none());
}
