use axum::body::Bytes;
use axum::{
    extract::{Path, State},
    routing::{get, post},
    Router,
};
use duckdb::{params, Connection, Result};
use std::sync::{Arc, Mutex};
include!(concat!(env!("OUT_DIR"), "/catalog.cmd.rs"));

pub struct Catalog {
    db_conn: Connection,
}

pub struct CatalogServer {
    pub app: Router,
}

impl CatalogServer {
    pub fn new(catalog: Arc<Mutex<Catalog>>) -> Result<Self, ()> {
        let app = Router::new()
            .route("/", get(health_check))
            .route("/dataset/:name", post(register_dataset))
            .route("/dataset/:name/add", post(add_loc_to_ds))
            .route("/datasets", get(list_all_endpoints))
            .with_state(catalog);
        Ok(CatalogServer { app })
    }
}

impl Catalog {
    pub fn new(path: &str) -> Result<Self, String> {
        let db_conn = Connection::open(path).expect("Failed to open connection");
        Ok(Catalog { db_conn })
    }
    pub fn init_catalog(&mut self) -> Result<(), String> {
        let _ = self.db_conn.execute(
            "CREATE TABLE IF NOT EXISTS catalog (
                    dataset VARCHAR,
                    schema BINARY,
                );
                ",
            [],
        );
        let _ = self.db_conn.execute(
            "CREATE TABLE IF NOT EXISTS foxes (
                    dataset VARCHAR,
                    ip_addr VARCHAR,
                );
                ",
            [],
        );
        Ok(())
    }
    pub fn register_dataset(&mut self, name: String, schema: Vec<u8>) -> Result<(), String> {
        let _ = self.db_conn.execute(
            "INSERT INTO catalog (dataset, schema) VALUES ($1, $2);",
            params![name, schema],
        );
        Ok(())
    }
    pub fn add_loc_to_ds(&mut self, dataset: String, ip_addr: String) -> Result<(), String> {
        let _ = self.db_conn.execute(
            "INSERT INTO foxes (dataset, ip_addr) VALUES ($1, $2);",
            params![dataset, ip_addr],
        );
        Ok(())
    }
    pub fn get_dataset_locs(&self, dataset: String) -> Vec<String> {
        let mut locs = Vec::new();
        let mut stmt = self
            .db_conn
            .prepare("SELECT ip_addr FROM foxes WHERE dataset = $1;")
            .unwrap();
        let loc_iter = stmt
            .query_map(params![dataset], |row| row.get::<_, String>(0))
            .unwrap();
        for loc in loc_iter {
            locs.push(loc.unwrap());
        }
        locs
    }
    pub fn list_all_endpoints(&self) -> Vec<String> {
        let mut endpoints = Vec::new();
        let mut stmt = self.db_conn.prepare("SELECT * FROM foxes;").unwrap();
        let endpoint_iter = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0).unwrap(),
                    row.get::<_, String>(1).unwrap(),
                ))
            })
            .unwrap();
        for endpoint in endpoint_iter {
            let (dataset, ip_addr) = endpoint.unwrap();
            endpoints.push(dataset + " " + ip_addr.as_str());
        }
        endpoints
    }
}

async fn health_check() -> &'static str {
    "Healthy\n"
}

async fn register_dataset(
    State(catalog): State<Arc<Mutex<Catalog>>>,
    Path(name): Path<String>,
    body: Bytes,
) -> &'static str {
    catalog
        .lock()
        .unwrap()
        .register_dataset(name, body.to_vec())
        .unwrap();
    "Dataset registered\n"
}

async fn add_loc_to_ds(
    State(catalog): State<Arc<Mutex<Catalog>>>,
    Path(name): Path<String>,
    ip_addr: String,
) -> &'static str {
    catalog
        .lock()
        .unwrap()
        .add_loc_to_ds(name, ip_addr)
        .unwrap();
    "Location added\n"
}

async fn list_all_endpoints(State(catalog): State<Arc<Mutex<Catalog>>>) -> String {
    let endpoints = catalog.lock().unwrap().list_all_endpoints();
    let mut res = String::new();
    for endpoint in endpoints {
        res.push_str(endpoint.as_str());
        res.push('\n');
    }
    res
}

#[cfg(test)]
mod catalog_tests {
    use super::*;
    use arrow_ipc::writer::StreamWriter;

    #[test]
    fn init_dataset() {
        let mut cat = Catalog::new("db/temp_init_dataset").unwrap();
        cat.init_catalog().unwrap();

        let conn = Connection::open("db/temp_init_dataset").unwrap();
        // check if catalog table is created
        {
            let result = conn.query_row(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'catalog';",
                [],
                |row| {
                    let table_name: String = row.get(0)?;
                    assert_eq!(table_name, "catalog", "Incorrect table name");
                    Ok(())
                },
            );
            assert!(result.is_ok(), "catalog table not found");
        }
        // check if foxes table is created
        {
            let result = conn.query_row(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'foxes';",
                [],
                |row| {
                    let table_name: String = row.get(0)?;
                    assert_eq!(table_name, "foxes", "Incorrect table name");
                    Ok(())
                },
            );
            assert!(result.is_ok(), "foxes table not found");
        }
        // cleanup
        std::fs::remove_file("db/temp_init_dataset").unwrap();
    }
    #[test]
    fn register_dataset() {
        let mut cat = Catalog::new("db/temp_register_dataset").unwrap();
        cat.init_catalog().unwrap();
        let schema = arrow_schema::Schema::empty();
        let buffer: Vec<u8> = Vec::new();
        let stream_writer = StreamWriter::try_new(buffer, &schema).unwrap();
        cat.register_dataset("test_ds".to_string(), stream_writer.into_inner().unwrap())
            .unwrap();

        let conn = Connection::open("db/temp_register_dataset").unwrap();
        let result = conn.query_row(
            "SELECT dataset FROM catalog WHERE dataset = 'test_ds';",
            [],
            |row| {
                let dataset: String = row.get(0)?;
                assert_eq!(dataset, "test_ds", "Incorrect dataset name");
                Ok(())
            },
        );
        assert!(result.is_ok(), "Dataset test_ds not found");
        // cleanup
        std::fs::remove_file("db/temp_register_dataset").unwrap();
    }
    #[test]
    fn add_loc_to_ds() {
        let mut cat = Catalog::new("db/temp_add_loc_to_ds").unwrap();
        cat.init_catalog().unwrap();
        let schema = arrow_schema::Schema::empty();
        let buffer: Vec<u8> = Vec::new();
        let stream_writer = StreamWriter::try_new(buffer, &schema).unwrap();
        cat.register_dataset("test_ds".to_string(), stream_writer.into_inner().unwrap())
            .unwrap();
        cat.add_loc_to_ds("test_ds".to_string(), "0.0.0.0".to_string())
            .unwrap();

        let conn = Connection::open("db/temp_add_loc_to_ds").unwrap();
        let result = conn.query_row(
            "SELECT ip_addr FROM foxes WHERE dataset = 'test_ds';",
            [],
            |row| {
                let ip_addr: String = row.get(0)?;
                assert_eq!(ip_addr, "0.0.0.0", "Incorrect ip address");
                Ok(())
            },
        );
        assert!(result.is_ok(), "IP address not found");
        // cleanup
        std::fs::remove_file("db/temp_add_loc_to_ds").unwrap();
    }
}
