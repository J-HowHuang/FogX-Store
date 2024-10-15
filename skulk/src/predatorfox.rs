use uuid::Uuid;

pub mod cmd {
    include!(concat!(env!("OUT_DIR"), "/predatorfox.cmd.rs"));
}

impl cmd::SkulkQuery {
    pub fn create_uuid(&mut self) {
        let uuid = Uuid::new_v4();
        self.uuid = Some(uuid.to_string());
    }
}