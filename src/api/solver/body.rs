use axum::http::{HeaderMap, StatusCode, header::CONTENT_TYPE};
use serde::de::DeserializeOwned;

pub(super) fn decode_solver_body<T: DeserializeOwned>(
    headers: &HeaderMap,
    body: &[u8],
) -> Result<T, StatusCode> {
    if headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/msgpack"))
    {
        rmp_serde::from_slice(body).map_err(|_| StatusCode::BAD_REQUEST)
    } else {
        serde_json::from_slice(body).map_err(|_| StatusCode::BAD_REQUEST)
    }
}
