use kage_price_estimate::{
    domain::PairSnapshot,
    oracle::{PricingHandle, PricingReadError, PricingStatus},
};

#[derive(Clone)]
pub struct EmbeddedPricing {
    inner: EmbeddedPricingInner,
}

#[derive(Clone)]
enum EmbeddedPricingInner {
    Live(PricingHandle),
    #[cfg(test)]
    Fixed(Box<PairSnapshot>),
}

impl EmbeddedPricing {
    pub fn new(inner: PricingHandle) -> Self {
        Self {
            inner: EmbeddedPricingInner::Live(inner),
        }
    }

    #[cfg(test)]
    pub(crate) fn fixed(pair: PairSnapshot) -> Self {
        Self {
            inner: EmbeddedPricingInner::Fixed(Box::new(pair)),
        }
    }

    pub fn status(&self) -> PricingStatus {
        match &self.inner {
            EmbeddedPricingInner::Live(inner) => inner.status(),
            #[cfg(test)]
            EmbeddedPricingInner::Fixed(_) => PricingStatus::Ready,
        }
    }

    pub fn fresh_pair(&self, from: &str, to: &str) -> Result<PairSnapshot, PricingReadError> {
        match &self.inner {
            EmbeddedPricingInner::Live(inner) => inner.fresh_pair(from, to),
            #[cfg(test)]
            EmbeddedPricingInner::Fixed(pair) => {
                let _ = (from, to);
                Ok(pair.as_ref().clone())
            }
        }
    }
}
