mod commands;

use std::sync::{Arc, RwLock};

use async_nats::Client;
use commands::{connect::Connect, kv, publish::Publish};
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
            Box::new(Connect::default()),
            Box::new(Publish::default()),
            Box::new(kv::List),
        ]
    }
}
