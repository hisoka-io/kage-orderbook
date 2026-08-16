use std::collections::HashMap;

use alloy_primitives::U256;
use thiserror::Error;

const MAX_FUTURE_SKEW_MS: u64 = 5_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PricePoint {
    pub price_e18: U256,
    pub observed_at_ms: u64,
    pub sequence: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PricingStatus {
    Connecting,
    Ready,
    Stale,
    Disconnected,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum PricingReadError {
    #[error("pricing feed is connecting")]
    Connecting,
    #[error("pricing feed is disconnected")]
    Disconnected,
    #[error("price is missing for {0}")]
    Missing(String),
    #[error("price is stale for {0}")]
    Stale(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Connection {
    Connecting,
    Connected,
    Disconnected,
}

#[derive(Clone, Debug)]
pub(crate) struct PricingSnapshot {
    connection: Connection,
    required_assets: Vec<String>,
    prices: HashMap<String, PricePoint>,
}

impl PricingSnapshot {
    pub(crate) fn new(required_assets: Vec<String>) -> Self {
        Self {
            connection: Connection::Connecting,
            required_assets,
            prices: HashMap::new(),
        }
    }

    pub(crate) fn set_connecting(&mut self) {
        self.connection = Connection::Connecting;
    }

    pub(crate) fn set_disconnected(&mut self) {
        self.connection = Connection::Disconnected;
    }

    pub(crate) fn replace_prices(&mut self, prices: HashMap<String, PricePoint>) {
        self.prices = prices
            .into_iter()
            .filter(|(asset, _)| self.required_assets.contains(asset))
            .collect();
        self.connection = Connection::Connected;
    }

    pub(crate) fn apply_tick(&mut self, asset: String, point: PricePoint) -> bool {
        if !self.required_assets.contains(&asset)
            || self
                .prices
                .get(&asset)
                .is_some_and(|current| current.sequence >= point.sequence)
        {
            return false;
        }
        self.prices.insert(asset, point);
        true
    }

    pub(crate) fn price(&self, asset: &str) -> Option<PricePoint> {
        self.prices.get(asset).cloned()
    }

    pub(crate) fn fresh_pair(
        &self,
        asset_in: &str,
        asset_out: &str,
        now_ms: u64,
        max_age_ms: u64,
    ) -> Result<(PricePoint, PricePoint), PricingReadError> {
        match self.connection {
            Connection::Connecting => return Err(PricingReadError::Connecting),
            Connection::Disconnected => return Err(PricingReadError::Disconnected),
            Connection::Connected => {}
        }

        let read = |asset: &str| {
            let point = self
                .prices
                .get(asset)
                .cloned()
                .ok_or_else(|| PricingReadError::Missing(asset.to_owned()))?;
            if point.observed_at_ms > now_ms.saturating_add(MAX_FUTURE_SKEW_MS)
                || now_ms.saturating_sub(point.observed_at_ms) > max_age_ms
            {
                return Err(PricingReadError::Stale(asset.to_owned()));
            }
            Ok(point)
        };

        Ok((read(asset_in)?, read(asset_out)?))
    }

    pub(crate) fn status(&self, now_ms: u64, max_age_ms: u64) -> PricingStatus {
        match self.connection {
            Connection::Connecting => PricingStatus::Connecting,
            Connection::Disconnected => PricingStatus::Disconnected,
            Connection::Connected => {
                let ready = self.required_assets.iter().all(|asset| {
                    self.prices.get(asset).is_some_and(|point| {
                        point.observed_at_ms <= now_ms.saturating_add(MAX_FUTURE_SKEW_MS)
                            && now_ms.saturating_sub(point.observed_at_ms) <= max_age_ms
                    })
                });
                if ready {
                    PricingStatus::Ready
                } else {
                    PricingStatus::Stale
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn point(sequence: u64, observed_at_ms: u64) -> PricePoint {
        PricePoint {
            price_e18: U256::from(sequence),
            observed_at_ms,
            sequence,
        }
    }

    #[test]
    fn requires_fresh_prices_for_every_asset() {
        let mut snapshot = PricingSnapshot::new(vec!["ETH".into(), "USDC".into()]);
        snapshot.replace_prices(HashMap::from([("ETH".into(), point(1, 1_000))]));
        assert_eq!(snapshot.status(1_100, 500), PricingStatus::Stale);

        assert!(snapshot.apply_tick("USDC".into(), point(2, 1_000)));
        assert_eq!(snapshot.status(1_100, 500), PricingStatus::Ready);
        assert_eq!(snapshot.status(1_600, 500), PricingStatus::Stale);

        snapshot.replace_prices(HashMap::from([("ETH".into(), point(3, 10_000))]));
        assert_eq!(snapshot.status(1_000, 500), PricingStatus::Stale);
    }

    #[test]
    fn ignores_duplicate_and_older_ticks() {
        let mut snapshot = PricingSnapshot::new(vec!["ETH".into()]);
        snapshot.replace_prices(HashMap::from([("ETH".into(), point(5, 1_000))]));

        assert!(!snapshot.apply_tick("ETH".into(), point(5, 2_000)));
        assert!(!snapshot.apply_tick("ETH".into(), point(4, 2_000)));
        assert!(snapshot.apply_tick("ETH".into(), point(6, 2_000)));
        assert_eq!(snapshot.price("ETH").unwrap().sequence, 6);
    }

    #[test]
    fn connection_state_overrides_cached_prices() {
        let mut snapshot = PricingSnapshot::new(vec!["ETH".into()]);
        snapshot.replace_prices(HashMap::from([("ETH".into(), point(1, 1_000))]));
        snapshot.set_disconnected();
        assert_eq!(snapshot.status(1_100, 500), PricingStatus::Disconnected);
        snapshot.set_connecting();
        assert_eq!(snapshot.status(1_100, 500), PricingStatus::Connecting);
    }

    #[test]
    fn reads_a_fresh_pair_from_one_snapshot() {
        let mut snapshot = PricingSnapshot::new(vec!["ETH".into(), "USDC".into()]);
        snapshot.replace_prices(HashMap::from([
            ("ETH".into(), point(1, 1_000)),
            ("USDC".into(), point(2, 1_000)),
        ]));

        let (eth, usdc) = snapshot.fresh_pair("ETH", "USDC", 1_100, 500).unwrap();
        assert_eq!(eth.sequence, 1);
        assert_eq!(usdc.sequence, 2);
        assert!(matches!(
            snapshot.fresh_pair("ETH", "LINK", 1_100, 500),
            Err(PricingReadError::Missing(asset)) if asset == "LINK"
        ));
        assert!(matches!(
            snapshot.fresh_pair("ETH", "USDC", 1_600, 500),
            Err(PricingReadError::Stale(_))
        ));
    }
}
