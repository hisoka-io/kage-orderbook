use kage_orderbook::api;
use kage_orderbook::core::engine::start_orderbook;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let database_url =
        std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://orderbook.db".to_owned());
    let orderbook = start_orderbook(&database_url).await.unwrap();
    let app = api::router(orderbook);
    let listener = TcpListener::bind("127.0.0.1:3000").await.unwrap();

    kage_orderbook::service_log!("orderbook", "listening address=127.0.0.1:3000");
    axum::serve(listener, app).await.unwrap();
}
