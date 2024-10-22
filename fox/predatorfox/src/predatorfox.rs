use cmd::SkulkQuery;
use lancedb::connection::Connection;
use lancedb::query::{ExecutableQuery, QueryBase, Select};
use lancedb::{connect, Result, Table as LanceDbTable};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use futures::TryStreamExt;

#[derive(Clone)]
pub struct Predator {
    pub lance_conn: Connection,
}

impl Predator {
    pub async fn new(db_uri: String) -> Result<Self> {
        let lance_conn = connect(&db_uri).execute().await?;
        Ok(Self { lance_conn })
    }

    pub async fn execute_query(&self, query: &SkulkQuery) -> Result<Vec<RecordBatch>> {
        // --8<-- [start:execute_query]
        let tbl = self.lance_conn.open_table(query.dataset.clone()).execute().await?;
        let mut lance_query = tbl.query();
        if !query.columns.is_empty() {
            lance_query = lance_query.select(Select::Columns(query.columns.clone()));
        } else {
            lance_query = lance_query.select(Select::All);
        }
        if let Some(predicates) = query.predicates.clone() {
            lance_query = lance_query.only_if(predicates);
        }
        if let Some(limit) = query.limit {
            lance_query = lance_query.limit(limit.try_into().expect("limit must be positive"));
        }
        let stream = lance_query.execute().await.expect("query failed");
        stream.try_collect().await
        // --8<-- [end:execute_query]
    }
    
    pub async fn create_empty_table(&self, schema: Arc<Schema>) -> Result<LanceDbTable> {
        // --8<-- [start:create_empty_table]
        self.lance_conn.create_empty_table("empty_table", schema).execute().await
        // --8<-- [end:create_empty_table]
    }
}

pub mod cmd {
    include!(concat!(env!("OUT_DIR"), "/predatorfox.cmd.rs"));
}