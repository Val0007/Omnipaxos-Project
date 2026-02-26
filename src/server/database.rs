use omnipaxos_kv::common::kv::KVCommand;
use std::collections::HashMap;

pub struct Database {
    db: HashMap<String, String>,
}

impl Database {
    pub fn new() -> Self {
        Self { db: HashMap::new() }
    }

    pub fn handle_command(&mut self, command: KVCommand) -> Option<Option<String>> {
        match command {
            KVCommand::Put(key, value) => {
                self.db.insert(key, value);
                None
            }
            KVCommand::Delete(key) => {
                self.db.remove(&key);
                None
            }
            KVCommand::Get(key) => Some(self.db.get(&key).map(|v| v.clone())),
            KVCommand::Cas(key, expected, new_value) => {
                let current = self.db.get(&key).cloned();
                if current.as_deref() == Some(expected.as_str()) {
                    self.db.insert(key, new_value);
                    Some(Some("ok".to_string()))      // success
                } else {
                    Some(Some("conflict".to_string())) // failure
                }
            }
        }
    }
}
