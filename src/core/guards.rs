use thiserror::Error;

pub const DEFAULT_ORDER_TTL_SECONDS: u32 = 60;
pub const MIN_ORDER_TTL_SECONDS: u32 = 5;
pub const MAX_ORDER_TTL_SECONDS: u32 = 300;

#[derive(Debug, Clone, Copy)]
pub struct OrderPolicy {
    pub default_ttl_seconds: u32,
    pub min_ttl_seconds: u32,
    pub max_ttl_seconds: u32,
}

impl Default for OrderPolicy {
    fn default() -> Self {
        Self {
            default_ttl_seconds: DEFAULT_ORDER_TTL_SECONDS,
            min_ttl_seconds: MIN_ORDER_TTL_SECONDS,
            max_ttl_seconds: MAX_ORDER_TTL_SECONDS,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum GuardError {
    #[error("ttl_seconds must be between {min} and {max}")]
    InvalidTtl { min: u32, max: u32 },
    #[error("expiry timestamp overflow")]
    ExpiryOverflow,
}

pub fn resolve_expiry_ms(
    ttl_seconds: Option<u32>,
    received_at_ms: i64,
    policy: OrderPolicy,
) -> Result<i64, GuardError> {
    let ttl_seconds = ttl_seconds.unwrap_or(policy.default_ttl_seconds);
    if ttl_seconds < policy.min_ttl_seconds || ttl_seconds > policy.max_ttl_seconds {
        return Err(GuardError::InvalidTtl {
            min: policy.min_ttl_seconds,
            max: policy.max_ttl_seconds,
        });
    }

    received_at_ms
        .checked_add(i64::from(ttl_seconds) * 1_000)
        .ok_or(GuardError::ExpiryOverflow)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_default_and_explicit_ttl() {
        let policy = OrderPolicy::default();
        assert_eq!(resolve_expiry_ms(None, 1_000, policy), Ok(61_000));
        assert_eq!(resolve_expiry_ms(Some(5), 1_000, policy), Ok(6_000));
    }

    #[test]
    fn rejects_ttl_outside_policy() {
        let policy = OrderPolicy::default();
        assert!(matches!(
            resolve_expiry_ms(Some(4), 1_000, policy),
            Err(GuardError::InvalidTtl { .. })
        ));
        assert!(matches!(
            resolve_expiry_ms(Some(301), 1_000, policy),
            Err(GuardError::InvalidTtl { .. })
        ));
    }
}
