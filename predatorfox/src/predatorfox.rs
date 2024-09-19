use lancedb::arrow::IntoArrow;
use lancedb::connection::Connection;
use lancedb::index::Index;
use lancedb::query::{ExecutableQuery, QueryBase};
use lancedb::{connect, Result, Table as LanceDbTable};
use std::sync::Arc;

use arrow_array::types::Float32Type;
use arrow_array::{FixedSizeListArray, Int32Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::{FlightData, FlightDescriptor, FlightInfo, SchemaResult, Ticket};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct Predator {
    pub lance_conn: Connection,
}

impl Predator {
    pub async fn new() -> Result<Self> {
        let uri = "data/sample-lancedb";
        let lance_conn = connect(uri).execute().await?;
        Ok(Self { lance_conn })
    }

    pub async fn get_schema(&self, table_name: &String) -> Result<SchemaRef> {
        // --8<-- [start:get_schema]
        self.lance_conn.open_table(table_name).execute().await?.schema().await
        // --8<-- [end:get_schema]
    }

    pub async fn create_index(table: &LanceDbTable) -> Result<()> {
        // --8<-- [start:create_index]
        table.create_index(&["vector"], Index::Auto).execute().await
        // --8<-- [end:create_index]
    }

    pub async fn search(&self, tbl: &LanceDbTable) -> Result<Vec<RecordBatch>> {
        tbl
            .query()
            .limit(2)
            .nearest_to(&[1.0; 128])?
            .execute()
            .await?
            .try_collect::<Vec<_>>()
            .await
    }
    
    pub async fn create_empty_table(&self, schema: Arc<Schema>) -> Result<LanceDbTable> {
        // --8<-- [start:create_empty_table]
        self.lance_conn.create_empty_table("empty_table", schema).execute().await
        // --8<-- [end:create_empty_table]
    }

    pub async fn delete(&self, tbl: &LanceDbTable) -> Result<()> {
        tbl.delete("id > 24").await
    }

    pub async fn drop_table(&self) -> Result<()> {
        self.lance_conn.drop_table("my_table").await
    }

    pub async fn open_existing_tbl(&self) -> Result<LanceDbTable> {
        self.lance_conn.open_table("my_table").execute().await
    }
}

pub mod cmd {
    include!(concat!(env!("OUT_DIR"), "/predatorfox.cmd.rs"));
}