use cmd::SkulkQuery;
use embed_anything::{embed_query, embeddings::embed::Embedder};
use lancedb::connection::Connection;
use lancedb::index::scalar::FullTextSearchQuery;
use lancedb::query::{ExecutableQuery, Query, QueryBase, Select, VectorQuery};
use lancedb::{connect, Result, Table as LanceDbTable};
use std::collections::HashMap;
use std::sync::Arc;
use std::fs::read;
use std::vec;

use arrow_array::{RecordBatch, StringArray, LargeBinaryArray, ArrayRef};
use arrow_schema::{Schema, Field, DataType};
use futures::TryStreamExt;

pub struct Predator {
    pub lance_conn: Connection,
    embed_models: HashMap<String, Arc<Embedder>>,
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
        let clip =
            Embedder::from_pretrained_hf("clip", "openai/clip-vit-base-patch32", None).unwrap();
        let clip: Arc<Embedder> = Arc::new(clip);
        let all_mini =
            Embedder::from_pretrained_hf("bert", "sentence-transformers/all-MiniLM-L6-v2", None)
                .unwrap();
        let all_mini: Arc<Embedder> = Arc::new(all_mini);
        let embed_models = HashMap::from([
            ("openai/clip-vit-base-patch32".to_string(), clip),
            (
                "sentence-transformers/all-MiniLM-L6-v2".to_string(),
                all_mini,
            ),
        ]);
        Ok(Self {
            lance_conn,
            embed_models,
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
            let embedding = embed_query(
                vec![vector_query.text_query],
                &self.embed_models[&vector_query.embed_model.unwrap()],
                None,
            )
            .await
            .unwrap()[0]
                .embedding
                .to_dense()
                .unwrap();
            let vec_query = tbl
                .query()
                .nearest_to(embedding)
                .expect("nearest_to failed");
            lance_query = LanceQuery::VectorQuery(
                vec_query
                    .limit(vector_query.top_k as usize)
                    .column(&vector_query.column),
            );
        } else {
            lance_query = LanceQuery::Query(tbl.query());
        }
        if !query.columns.is_empty() {
            let mut required_columns = vec!["_episode_id".to_string()];
            if query.with_step_data {
                required_columns.push("_episode_path".to_string());
            }
            lance_query = lance_query.select(Select::Columns(query.columns.iter().cloned().chain(required_columns.iter().cloned()).collect()));
            
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
        let mut query_result = stream.try_collect().await.unwrap();
        if query.with_step_data {
            Self::collect_step_data(&mut query_result)
        } else {
            Ok(query_result)
        }
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

    fn collect_step_data(query_result: &mut Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        let mut step_data = Vec::<RecordBatch>::new();
        for batch in query_result {
            let episode_path_idx = batch.schema_ref().column_with_name("_episode_path").unwrap().0;
            let pq_paths = batch.remove_column(episode_path_idx);
            let pq_paths_iter = pq_paths
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter();
            
            let mut byte_array = Vec::new();
            for path in pq_paths_iter {
                if let Some(path) = path {
                    let content = read(path).unwrap();
                    byte_array.push(Some(content));
                } else {
                    byte_array.push(None);
                }
            }
            let binary_column = Arc::new(LargeBinaryArray::from_opt_vec(byte_array.iter().map(|opt: &Option<Vec<u8>>| opt.as_deref()).collect())) as ArrayRef;
            let new_field = Arc::new(Field::new("step_data", DataType::LargeBinary, true));
            let updated_schema = Arc::new(Schema::new(
                batch.schema().fields().iter().cloned().chain(std::iter::once(new_field)).collect::<Vec<Arc<Field>>>()
            ));
            let updated_record_batch = RecordBatch::try_new(
                updated_schema,
                batch.columns().iter().cloned().chain(std::iter::once(binary_column)).collect()
            )?;
            step_data.push(updated_record_batch);
        }
        Ok(step_data)
    }
}

pub mod cmd {
    include!(concat!(env!("OUT_DIR"), "/predatorfox.cmd.rs"));
}
