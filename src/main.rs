use kage_orderbook::api;
use kage_orderbook::core::engine::start_orderbook;
use kage_orderbook::registry::SolverRegistry;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let database_url =
        std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://orderbook.db".to_owned());
    let registry_url =
        std::env::var("KAGE_REGISTRY_URL").unwrap_or_else(|_| "http://127.0.0.1:4000".to_owned());
    let orderbook = start_orderbook(&database_url).await.unwrap();
    let app = api::router(orderbook, SolverRegistry::http(registry_url));
    let listener = TcpListener::bind("127.0.0.1:3000").await.unwrap();

    kage_orderbook::service_log!("orderbook", "listening address=127.0.0.1:3000");
    axum::serve(listener, app).await.unwrap();
}
