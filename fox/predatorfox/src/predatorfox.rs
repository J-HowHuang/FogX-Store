use cmd::SkulkQuery;
use std::collections::HashMap;
use lancedb::connection::Connection;
use lancedb::index::Index;
use lancedb::query::{ExecutableQuery, QueryBase, Select};
use lancedb::{connect, Result, Table as LanceDbTable};
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use futures::TryStreamExt;

#[derive(Clone)]
pub struct Predator {
    pub lance_conn: Connection,
    pub ticket_store: HashMap<String, Vec<RecordBatch>>,
}

impl Predator {
    pub async fn new(db_uri: String) -> Result<Self> {
        let lance_conn = connect(&db_uri).execute().await?;
        Ok(Self { lance_conn, ticket_store: HashMap::<String, Vec<RecordBatch>>::new() })
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

    pub async fn sync_dataset(&self, tbl: &LanceDbTable, rlds_path: PathBuf) -> Result<()> {
        // --8<-- [start:sync_dataset]
        // read tensorflow dataset format file in `rlds_path` and iterate through each record
        // and insert into `tbl`
        
        // --8<-- [end:sync_dataset]
        Ok(())
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