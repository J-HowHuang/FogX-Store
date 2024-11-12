mod flight;
mod predatorfox;
mod catalog;

use arrow_flight::flight_service_server::FlightServiceServer;
use log::info;
use std::env;
use tonic::transport::Server;
use std::{future::IntoFuture, sync::{Arc, Mutex}};

#[tokio::main]
async fn main() -> Result<(), ()> {
    env_logger::init();
    let mut cat = catalog::Catalog::new("db/catalog").unwrap();
    cat.init_catalog().unwrap();
    let shared_cat = Arc::new(Mutex::new(cat));

    let addr: std::net::SocketAddr = format!("{}:50052", env::var("HOST_IP_ADDR").unwrap_or("0.0.0.0".to_string())).parse().unwrap();
    let flight = flight::FlightServiceImpl::new(shared_cat.clone());
    let svc = FlightServiceServer::new(flight);
    info!("Starting server on {}", addr);
    let flight_thread = Server::builder().add_service(svc).serve(addr);
    
    let cat_server = catalog::CatalogServer::new(shared_cat.clone()).unwrap();
    let cat_listener = tokio::net::TcpListener::bind(format!("{}:11632", env::var("HOST_IP_ADDR").unwrap_or("0.0.0.0".to_string()))).await.unwrap();
    
    let cat_thread = axum::serve(cat_listener, cat_server.app).into_future();

    let _ = futures::join!(flight_thread, cat_thread);

    Ok(())
}