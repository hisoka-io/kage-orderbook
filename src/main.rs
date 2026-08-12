use kage_orderbook::api;
use kage_orderbook::core::engine::start_orderbook;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let orderbook = start_orderbook();
    let app = api::router(orderbook);
    let listener = TcpListener::bind("127.0.0.1:3000").await.unwrap();

    kage_orderbook::service_log!("orderbook", "listening address=127.0.0.1:3000");
    axum::serve(listener, app).await.unwrap();
}
