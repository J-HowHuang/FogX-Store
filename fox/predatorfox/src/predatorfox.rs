use cmd::SkulkQuery;
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use lancedb::connection::Connection;
use lancedb::index::scalar::FullTextSearchQuery;
use lancedb::index::vector;
use lancedb::query::{ExecutableQuery, Query, QueryBase, Select, VectorQuery};
use lancedb::{connect, Result, Table as LanceDbTable};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use futures::TryStreamExt;

pub struct Predator {
    pub lance_conn: Connection,
    text_embed: TextEmbedding,
}

enum LanceQuery {
    Query(Query),
    VectorQuery(VectorQuery),
}

impl QueryBase for LanceQuery {
    fn limit(self, limit: usize) -> Self {
        match self {
            LanceQuery::Query(query) => LanceQuery::Query(query.limit(limit)),
            LanceQuery::VectorQuery(query) => LanceQuery::VectorQuery(query.limit(limit)),
        }
    }
    fn offset(self, offset: usize) -> Self {
        match self {
            LanceQuery::Query(query) => LanceQuery::Query(query.offset(offset)),
            LanceQuery::VectorQuery(query) => LanceQuery::VectorQuery(query.offset(offset)),
        }
    }
    fn only_if(self, filter: impl AsRef<str>) -> Self {
        match self {
            LanceQuery::Query(query) => LanceQuery::Query(query.only_if(filter)),
            LanceQuery::VectorQuery(query) => LanceQuery::VectorQuery(query.only_if(filter)),
        }
    }
    fn full_text_search(self, full_text_query: FullTextSearchQuery) -> Self {
        match self {
            LanceQuery::Query(query) => LanceQuery::Query(query.full_text_search(full_text_query)),
            LanceQuery::VectorQuery(query) => {
                LanceQuery::VectorQuery(query.full_text_search(full_text_query))
            }
        }
    }
    fn select(self, selection: Select) -> Self {
        match self {
            LanceQuery::Query(query) => LanceQuery::Query(query.select(selection)),
            LanceQuery::VectorQuery(query) => LanceQuery::VectorQuery(query.select(selection)),
        }
    }
    fn fast_search(self) -> Self {
        match self {
            LanceQuery::Query(query) => LanceQuery::Query(query.fast_search()),
            LanceQuery::VectorQuery(query) => LanceQuery::VectorQuery(query.fast_search()),
        }
    }
}

impl Predator {
    pub async fn new(db_uri: String) -> Result<Self> {
        let lance_conn = connect(&db_uri).execute().await?;
        let text_embed = TextEmbedding::try_new(
            InitOptions::new(EmbeddingModel::NomicEmbedTextV15).with_show_download_progress(true),
        )
        .expect("failed to initialize text embedding model");
        Ok(Self {
            lance_conn,
            text_embed,
        })
    }

    pub async fn execute_query(&self, query: &SkulkQuery) -> Result<Vec<RecordBatch>> {
        // --8<-- [start:execute_query]
        let tbl = self
            .lance_conn
            .open_table(query.dataset.clone())
            .execute()
            .await?;
        let mut lance_query: LanceQuery;
        if let Some(vector_query) = query.vector_query.clone() {
            let embedding = self
                .text_embed
                .embed(vec![vector_query.text_query], None)
                .expect("embed failed");
            let vec_query = tbl
                .query()
                .nearest_to(&embedding[0][..512])
                .expect("nearest_to failed");
            lance_query = LanceQuery::VectorQuery(vec_query.limit(vector_query.top_k as usize).column(&vector_query.column));
        } else {
            lance_query = LanceQuery::Query(tbl.query());
        }
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
        let stream = match lance_query {
            LanceQuery::Query(query) => query.execute().await.expect("query failed"),
            LanceQuery::VectorQuery(query) => query.execute().await.expect("query failed"),
        };
        stream.try_collect().await
        // --8<-- [end:execute_query]
    }

    pub async fn create_empty_table(&self, schema: Arc<Schema>) -> Result<LanceDbTable> {
        // --8<-- [start:create_empty_table]
        self.lance_conn
            .create_empty_table("empty_table", schema)
            .execute()
            .await
        // --8<-- [end:create_empty_table]
    }
}

pub mod cmd {
    include!(concat!(env!("OUT_DIR"), "/predatorfox.cmd.rs"));
}
