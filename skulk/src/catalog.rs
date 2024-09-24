use duckdb::{params, Connection, Result};
use prost::bytes::Bytes;
use rocket::{get, post, routes, Rocket, State, Build};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
include!(concat!(env!("OUT_DIR"), "/catalog.cmd.rs"));

pub struct Catalog {
    db_conn: Connection,
}

pub struct CatalogServer {
    pub rocket: Rocket<Build>,
}

impl CatalogServer {
    pub fn new(catalog: Arc<Mutex<Catalog>>) -> Result<Self, ()> {
        let rocket_port = "11632";
        let build = rocket::build()
            .configure(rocket::Config::figment().merge(("port", rocket_port)))
            .manage(catalog)
            .mount("/", routes![health_check, register_dataset]);
        Ok(CatalogServer {rocket: build})
    }
}

impl Catalog {
    pub fn new(path: &str) -> Result<Self, String> {
        let db_conn = Connection::open(path).expect("Failed to open connection");
        Ok(Catalog { db_conn })
    }
    pub fn init_catalog(&mut self) -> Result<(), String> {
        self.db_conn.execute("CREATE TYPE source_type AS ENUM ('fox', 's3');", []);
        self.db_conn.execute(
            "CREATE TABLE IF NOT EXISTS catalog (
                    dataset VARCHAR,
                    schema BINARY,
                );
                ",
            [],
        );
        self.db_conn.execute(
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
        self.db_conn.execute(
            "INSERT INTO catalog (dataset, schema) VALUES ($1, $2);",
            params![name, schema],
        );
        Ok(())
    }
}

#[get("/")]
fn health_check() -> &'static str {
    "Healthy\n"
}

#[post("/register/dataset/<name>", data = "<cmd_bytes>")]
fn register_dataset(name: String, cmd_bytes: Vec<u8>, catalog: &State<Arc<Mutex<Catalog>>>) -> &'static str {
    catalog.inner().lock().unwrap().register_dataset(name, cmd_bytes).unwrap();
    "Dataset registered\n"
}
