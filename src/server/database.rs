use omnipaxos_kv::common::kv::KVCommand;
use std::collections::HashMap;
use std::fs;

pub struct Database {
    db: HashMap<String, String>,
    path: String,
}

impl Database {
    /// Create database and load persisted state if it exists
    pub fn new(path: String) -> Self {
        if let Ok(contents) = fs::read_to_string(&path) {
            if let Ok(map) = serde_json::from_str::<HashMap<String, String>>(&contents) {
                return Database { db: map, path };
            }
        }

        Database {
            db: HashMap::new(),
            path,
        }
    }

    /// Persist database to disk
    pub fn persist(&self) {
        if let Ok(json) = serde_json::to_string(&self.db) {
            let _ = fs::write(&self.path, json);
        }
    }

    /// Apply command to database
    pub fn handle_command(&mut self, command: KVCommand) -> Option<Option<String>> {
        match command {
            KVCommand::Put(key, value) => {
                self.db.insert(key, value);
                self.persist();
                None
            }

            KVCommand::Delete(key) => {
                self.db.remove(&key);
                self.persist();
                None
            }

            KVCommand::Get(key) => {
                Some(self.db.get(&key).map(|v| v.clone()))
            }

            KVCommand::Cas(key, expected, new_value) => {
                let current = self.db.get(&key).cloned();

                if current.as_deref() == Some(expected.as_str()) {
                    self.db.insert(key, new_value);
                    self.persist();
                    Some(Some("ok".to_string()))
                } else {
                    Some(Some("conflict".to_string()))
                }
            }
        }
    }
}