mod cache;
mod client;
mod validator;

use std::{sync::Arc, time::Duration};

use tokio::sync::watch;

pub use cache::{PricePoint, PricingReadError, PricingStatus};
pub use validator::{PriceValidationError, PricingValidator};

use cache::PricingSnapshot;

#[derive(Clone)]
pub struct PricingConfig {
    pub feed_url: String,
    pub token: String,
    pub assets: Vec<String>,
    pub max_age: Duration,
    pub reconnect_delay: Duration,
    pub idle_timeout: Duration,
}

#[derive(Clone)]
pub struct PricingHandle {
    receiver: watch::Receiver<Arc<PricingSnapshot>>,
    max_age: Duration,
}

impl PricingHandle {
    pub fn status(&self) -> PricingStatus {
        self.receiver
            .borrow()
            .status(now_ms(), self.max_age.as_millis() as u64)
    }

    pub fn price(&self, asset: &str) -> Option<PricePoint> {
        self.receiver.borrow().price(&asset.to_uppercase())
    }

    pub fn fresh_pair(
        &self,
        asset_in: &str,
        asset_out: &str,
    ) -> Result<(PricePoint, PricePoint), PricingReadError> {
        self.fresh_pair_at(asset_in, asset_out, now_ms())
    }

    fn fresh_pair_at(
        &self,
        asset_in: &str,
        asset_out: &str,
        now_ms: u64,
    ) -> Result<(PricePoint, PricePoint), PricingReadError> {
        let asset_in = asset_in.to_uppercase();
        let asset_out = asset_out.to_uppercase();
        self.receiver.borrow().fresh_pair(
            &asset_in,
            &asset_out,
            now_ms,
            self.max_age.as_millis() as u64,
        )
    }

    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.receiver.changed().await
    }
}

pub fn spawn(config: PricingConfig) -> PricingHandle {
    let mut assets: Vec<String> = config
        .assets
        .iter()
        .map(|asset| asset.trim().to_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect();
    assets.sort();
    assets.dedup();
    assert!(!assets.is_empty(), "pricing assets must not be empty");
    assert!(
        !config.token.is_empty(),
        "pricing feed token must not be empty"
    );

    let max_age = config.max_age;
    let (sender, receiver) = watch::channel(Arc::new(PricingSnapshot::new(assets.clone())));
    tokio::spawn(client::run(config, assets, sender));

    PricingHandle { receiver, max_age }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
