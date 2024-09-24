mod flight;
mod predatorfox;
mod catalog;

use arrow_flight::flight_service_server::FlightServiceServer;
use log::info;
use tonic::transport::Server;
use std::{future::IntoFuture, sync::{Arc, Mutex, RwLock}};

#[tokio::main]
async fn main() -> Result<(), ()> {
    env_logger::init();
    let cat = catalog::Catalog::new("db/catalog").unwrap();
    let shared_cat = Arc::new(Mutex::new(cat));

    let addr: std::net::SocketAddr = "[::1]:50052".parse().unwrap();
    let flight = flight::FlightServiceImpl::new(shared_cat.clone());
    let svc = FlightServiceServer::new(flight);
    info!("Starting server on {}", addr);
    let flight_thread = Server::builder().add_service(svc).serve(addr);
    
    let cat_server = catalog::CatalogServer::new(shared_cat.clone()).unwrap();
    let cat_listener = tokio::net::TcpListener::bind("0.0.0.0:11632").await.unwrap();
    
    let cat_thread = axum::serve(cat_listener, cat_server.app).into_future();

    let _ = futures::join!(flight_thread, cat_thread);

    Ok(())
}