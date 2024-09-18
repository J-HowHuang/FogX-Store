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

use crate::predatorfox;

use arrow_array::new_empty_array;
use arrow_flight::FlightEndpoint;
use futures::stream::BoxStream;
use prost::Message;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use arrow_flight::{
    flight_service_server::FlightService, flight_service_server::FlightServiceServer, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket, Location,
};
use arrow_flight::flight_descriptor::DescriptorType;

use std::convert::TryFrom;
#[derive(Clone)]
pub struct FlightServiceImpl {
    predator: predatorfox::Predator,
}

impl FlightServiceImpl {
    pub async fn new() -> Result<Self, ()> {
        Ok(Self { predator: predatorfox::Predator::new().await.unwrap() })
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
                let cmd = predatorfox::cmd::Command::decode(descriptor.cmd).unwrap();
                match predatorfox::cmd::CommandType::try_from(cmd.cmd_type) {
                    Ok(predatorfox::cmd::CommandType::Query) => {
                        let flight_info = FlightInfo {
                            schema: Vec::<u8>::new().into(),
                            flight_descriptor: None,
                            endpoint: vec![FlightEndpoint{
                                ticket: Some(Ticket { ticket: Vec::<u8>::new().into() }),
                                location: vec![Location{ uri: "[::1]:50051".to_string() }],
                                ..Default::default()
                            }],
                            total_records: 0,
                            total_bytes: 0,
                            ordered: false,
                            app_metadata: Vec::<u8>::new().into(),
                        };
                        return Ok(Response::new(flight_info));
                    },
                    Ok(predatorfox::cmd::CommandType::Unknown) => return Err(Status::unimplemented("unknown predatorfox command")),
                    Err(_) => return Err(Status::unimplemented("unknown predatorfox command")),
                }
            },
            Ok(DescriptorType::Path) => return Err(Status::unimplemented("path is not supported, use command type descriptor")),
            Ok(DescriptorType::Unknown) => return Err(Status::unimplemented("use command type descriptor")),
            Err(_) => return Err(Status::unimplemented("Implement get_flight_info")),
        }
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Implement poll_flight_info"))
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
