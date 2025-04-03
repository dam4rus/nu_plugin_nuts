mod commands;

use std::sync::{Arc, RwLock};

use async_nats::Client;
use commands::{Publish, Subscribe, connect::Connect, kv};
use nu_plugin::Plugin;
use tokio::runtime::Runtime;

#[derive(Debug)]
pub struct Nuts {
    pub(crate) runtime: Runtime,
    pub(crate) nats: Arc<RwLock<Option<Client>>>,
}

impl Nuts {
    pub fn new(runtime: Runtime) -> Self {
        Self {
            runtime,
            nats: Arc::new(RwLock::new(None)),
        }
    }
}

impl Plugin for Nuts {
    fn version(&self) -> String {
        env!("CARGO_PKG_VERSION").into()
    }

    fn commands(&self) -> Vec<Box<dyn nu_plugin::PluginCommand<Plugin = Self>>> {
        vec![
            Box::new(Connect),
            Box::new(Publish),
            Box::new(Subscribe),
            Box::new(kv::List),
            Box::new(kv::Get),
            Box::new(kv::Put),
            Box::new(kv::Watch),
            Box::new(kv::Delete),
        ]
    }
}
