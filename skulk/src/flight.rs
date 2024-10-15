// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::catalog;
use crate::predatorfox::cmd::{Command, CommandType, SkulkQuery};

use arrow_flight::{FlightClient, FlightEndpoint};
use axum::body::Bytes;
use catalog::Catalog;
use futures::stream::BoxStream;
use log::info;
use prost::Message;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use tonic::transport::{channel, Channel};
use tonic::{Request, Response, Status, Streaming};

use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, Location, PollInfo,
    PutResult, SchemaResult, Ticket,
};

use std::convert::TryFrom;

pub struct FlightServiceImpl {
    catalog: Arc<Mutex<Catalog>>,
}

impl FlightServiceImpl {
    pub fn new(cat: Arc<Mutex<Catalog>>) -> Self {
        FlightServiceImpl { catalog: cat }
    }
    pub fn get_dataset_locs(&self, dataset: String) -> Vec<Location> {
        let cat = self.catalog.lock().unwrap();
        let locs = cat.get_dataset_locs(dataset);

        locs.iter()
            .map(|loc_str| Location {
                uri: loc_str.to_string(),
            })
            .collect()
    }
}

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Implement handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Implement list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = _request.into_inner();
        match DescriptorType::try_from(descriptor.r#type) {
            Ok(DescriptorType::Cmd) => {
                info!("Received command descriptor");
                let mut skulk_query = SkulkQuery::decode(descriptor.cmd.clone()).unwrap();
                info!("Requesting dataset {:?}", skulk_query.dataset);
                let mut new_descriptor = descriptor.clone();
                skulk_query.create_uuid();
                new_descriptor.cmd = Command{cmd_type: CommandType::Query.into(), query: skulk_query.clone()}.encode_to_vec().into();
                let locs = self.get_dataset_locs(skulk_query.dataset);

                // to collect available endpoints
                let mut flight_info = FlightInfo::new().with_descriptor(FlightDescriptor {
                    r#type: DescriptorType::Cmd as i32,
                    cmd: Vec::<u8>::new().into(),
                    path: vec!["".to_string()],
                });

                // relay poll request to all associated endpoints and collect responses
                for loc in locs {
                    let data_endpoint = Channel::from_shared(format!("http://{}:50051", loc.uri)).expect("invalid uri");
                    let channel = data_endpoint.connect().await.expect("error connecting");
                    let mut client = FlightClient::new(channel);
                    let sub_info = client.get_flight_info(new_descriptor.clone()).await?;
                    for endpoint in sub_info.endpoint {
                        flight_info = flight_info.with_endpoint(endpoint);
                    }
                    info!("Found endpoint {:?}", loc.uri);
                }
                return Ok(Response::new(flight_info));
            }
            Ok(DescriptorType::Path) => {
                return Err(Status::unimplemented(
                    "path is not supported, use command type descriptor",
                ))
            }
            Ok(DescriptorType::Unknown) => {
                return Err(Status::unimplemented("use command type descriptor"))
            }
            Err(_) => return Err(Status::unimplemented("Implement get_flight_info")),
        }
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        let descriptor = _request.into_inner();
        match DescriptorType::try_from(descriptor.r#type) {
            Ok(DescriptorType::Cmd) => {
                info!("Received command descriptor");
                let mut skulk_query = SkulkQuery::decode(descriptor.cmd.clone()).unwrap();
                let mut new_descriptor = descriptor.clone();
                skulk_query.create_uuid();
                new_descriptor.cmd = skulk_query.encode_to_vec().into();
                let locs = self.get_dataset_locs(skulk_query.dataset);

                // response to the client poll request
                let mut poll_info = PollInfo::new().with_descriptor(FlightDescriptor {
                    r#type: DescriptorType::Cmd as i32,
                    cmd: Vec::<u8>::new().into(),
                    path: vec!["".to_string()],
                });
                // to collect available endpoints
                let mut flight_info = FlightInfo::new();

                // relay poll request to all associated endpoints and collect responses
                for loc in locs {
                    let data_endpoint = Channel::from_shared(loc.uri).expect("invalid uri");
                    let channel = data_endpoint.connect().await.expect("error connecting");
                    let mut client = FlightClient::new(channel);
                    let sub_poll_info = client.poll_flight_info(new_descriptor.clone()).await?;
                    if let Some(info) = sub_poll_info.info {
                        for endpoint in info.endpoint {
                            flight_info = flight_info.with_endpoint(endpoint);
                        }
                    }
                }
                if !flight_info.endpoint.is_empty() {
                    poll_info = poll_info.with_info(flight_info);
                }
                return Ok(Response::new(poll_info));
            }
            Ok(DescriptorType::Path) => {
                return Err(Status::unimplemented(
                    "path is not supported, use command type descriptor",
                ))
            }
            Ok(DescriptorType::Unknown) => {
                return Err(Status::unimplemented("use command type descriptor"))
            }
            Err(_) => return Err(Status::unimplemented("Implement get_flight_info")),
        }
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Implement get_schema"))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("Implement do_get"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Implement do_put"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Implement do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Implement list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Implement do_exchange"))
    }
}
