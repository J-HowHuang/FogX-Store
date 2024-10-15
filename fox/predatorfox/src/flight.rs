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
use std::collections::HashMap;
use std::sync::RwLock;
use std::pin::Pin;

use arrow_array::RecordBatch;
use arrow_flight::FlightEndpoint;
use arrow_flight::utils::batches_to_flight_data;
use arrow_schema::{Schema, Field, DataType};
use futures::stream::BoxStream;
use futures::{Stream, stream};
use log::info;
use prost::Message;
use lancedb::error::Error;
use tonic::{Request, Response, Status, Streaming};

use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::{
    flight_service_server::FlightService, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, Location, PollInfo, PutResult, SchemaResult, Ticket,
};

use std::convert::TryFrom;
pub struct FlightServiceImpl {
    predator: predatorfox::Predator,
    location: String,
    ticket_store: RwLock<HashMap<String, Vec<RecordBatch>>>,
}

impl FlightServiceImpl {
    pub async fn new(location: String, db_uri: String) -> Result<Self, ()> {
        Ok(Self {
            predator: predatorfox::Predator::new(db_uri).await.unwrap(),
            location,
            ticket_store: RwLock::new(HashMap::<String, Vec<RecordBatch>>::new()),
        })
    }

    pub async fn setup_predator(&self) {
        // Setup code for predator
        match self.predator
            .create_empty_table(Schema::new(vec![
                Field::new("c1", DataType::Int64, true),
                Field::new("c2", DataType::Utf8, false),
                Field::new("c3", DataType::Utf8, false),
            ]).into())
            .await {
                Ok(_) => info!("Successfully created empty table"),
                Err(e) => match e {
                    // Handle specific error kinds if needed
                    Error::TableAlreadyExists { .. } => info!("Table already exists"),
                    _ => info!("Failed to create empty table: {:?}", e),
                },
            }
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
                        info!("Received query command");
                        let stream = self.predator.execute_query(&cmd.query).await.expect("query failed");
                        let flight_info = FlightInfo::new()
                            .try_with_schema(
                                &stream[0].schema().clone()
                            )
                            .expect("schema failed")
                            .with_descriptor(FlightDescriptor {
                                r#type: DescriptorType::Cmd as i32,
                                cmd: Vec::<u8>::new().into(),
                                path: vec!["".to_string()],
                            })
                            .with_endpoint(FlightEndpoint {
                                ticket: Some(Ticket {
                                    ticket: cmd.query.uuid.clone().unwrap().into(),
                                }),
                                location: vec![Location {
                                    uri: format!("grpc://{}", self.location.clone()),
                                }],
                                ..Default::default()
                            })
                            // .with_total_bytes(0)
                            // .with_total_records(0)
                            .with_ordered(false);
                        {
                            let mut write_guard = self.ticket_store.write().unwrap();
                            write_guard.insert(cmd.query.uuid.unwrap(), stream);
                        }
                        return Ok(Response::new(flight_info));
                    }
                    Ok(predatorfox::cmd::CommandType::Unknown) => {
                        return Err(Status::unimplemented("unknown predatorfox command"))
                    }
                    Err(_) => return Err(Status::unimplemented("unknown predatorfox command")),
                }
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
                let cmd = predatorfox::cmd::Command::decode(descriptor.cmd).unwrap();
                match predatorfox::cmd::CommandType::try_from(cmd.cmd_type) {
                    Ok(predatorfox::cmd::CommandType::Query) => {
                        info!("Received query command");
                        let stream = self.predator.execute_query(&cmd.query).await.expect("query failed");
                        let flight_info = FlightInfo::new()
                            .try_with_schema(
                                &stream[0].schema().clone()
                            )
                            .expect("encoding failed")
                            .with_descriptor(FlightDescriptor {
                                r#type: DescriptorType::Cmd as i32,
                                cmd: Vec::<u8>::new().into(),
                                path: vec!["".to_string()],
                            })
                            .with_endpoint(FlightEndpoint {
                                ticket: Some(Ticket {
                                    ticket: cmd.query.uuid.unwrap().into(),
                                }),
                                location: vec![Location {
                                    uri: format!("grpc://{}", self.location.clone()),
                                }],
                                ..Default::default()
                            })
                            // .with_total_bytes(0)
                            // .with_total_records(0)
                            .with_ordered(false);
                        return Ok(Response::new(PollInfo::new().with_info(flight_info)));
                    }
                    Ok(predatorfox::cmd::CommandType::Unknown) => {
                        return Err(Status::unimplemented("unknown predatorfox command"))
                    }
                    Err(_) => return Err(Status::unimplemented("unknown predatorfox command")),
                }
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
        let ticket = _request.into_inner();
        let read_guard = self.ticket_store.read().unwrap();
        let batches = read_guard.get(&String::from_utf8(ticket.ticket.to_vec()).unwrap()).unwrap().clone();
        let first_batch = batches[0].clone();
        let schema = first_batch.schema_ref();
        let flight_data = batches_to_flight_data(schema, batches)
            .map_err(|e| Status::new(400.into(), "cannot convert record batches"))?
            .into_iter()
            .map(Ok);

        let stream: Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>> =
            Box::pin(stream::iter(flight_data));
        let resp = Response::new(stream);
        Ok(resp)
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
