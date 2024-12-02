// Copyright 2024 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! This example demonstrates basic usage of LanceDb.
//!
//! Snippets from this example are used in the quickstart documentation.

mod predatorfox;
mod flight;
use std::net::SocketAddr;

use log::info;
use std::env;

use tonic::transport::Server;
use arrow_flight::flight_service_server::FlightServiceServer;


#[tokio::main]
async fn main() -> Result<(), ()> {
    env_logger::init();
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    let advertise_location = format!("{}:50051", env::var("ADVERTISE_IP_ADDR").unwrap_or("0.0.0.0".to_string()));
    let location = format!("{}:50051", env::var("HOST_IP_ADDR").unwrap_or("0.0.0.0".to_string()));
    let addr: SocketAddr = location.parse().unwrap();
    let service = flight::FlightServiceImpl::new(advertise_location, "./data/dataset_db".to_string(), health_reporter.clone()).await.unwrap();
    service.setup_predator().await;
    
    let svc = FlightServiceServer::new(service);
    info!("Starting server on {}", addr);
    Server::builder().add_service(health_service).add_service(svc).serve(addr).await.unwrap();

    Ok(())
}
