use rocksdb::{DB, Options, Error};
use std::collections::HashMap;

pub struct Catalog {
    db: DB,
    tables: HashMap<String, String>,
}

impl Catalog {
    pub fn new(path: &str) -> Result<Self, Error> {
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, path)?;
        Ok(Catalog {
            db,
            tables: HashMap::new(),
        })
    }

    pub fn create_table(&mut self, table_name: &str) -> Result<(), Error> {
        if self.tables.contains_key(table_name) {
            return Err(Error::new("Table already exists"));
        }
        self.tables.insert(table_name.to_string(), table_name.to_string());
        self.db.put(table_name, b"")?;
        Ok(())
    }

    pub fn get_table(&self, table_name: &str) -> Option<&String> {
        self.tables.get(table_name)
    }

    pub fn delete_table(&mut self, table_name: &str) -> Result<(), Error> {
        if self.tables.remove(table_name).is_none() {
            return Err(Error::new("Table does not exist"));
        }
        self.db.delete(table_name)?;
        Ok(())
    }
}

fn main() {
    let mut catalog = Catalog::new("/path/to/rocksdb").unwrap();
    catalog.create_table("users").unwrap();
    println!("{:?}", catalog.get_table("users"));
    catalog.delete_table("users").unwrap();
}