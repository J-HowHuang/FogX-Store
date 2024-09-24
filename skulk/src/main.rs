mod flight;
mod predatorfox;
mod catalog;

use arrow_flight::flight_service_server::FlightServiceServer;
use log::info;
use tonic::transport::Server;
use std::sync::{Arc, Mutex};

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
    let cat_thread = cat_server.rocket.launch();

    let _ = futures::join!(flight_thread, cat_thread);

    Ok(())
}