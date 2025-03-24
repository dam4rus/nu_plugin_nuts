use std::sync::{Arc, atomic::AtomicBool};

use async_nats::{
    header::IntoHeaderName,
    jetstream::{self},
};
use futures::{StreamExt, TryStreamExt};
use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    IntoValue, LabeledError, ListStream, PipelineData, Signals, Signature, Span, SyntaxShape,
};
use tokio::sync::mpsc;

use crate::Nuts;

#[derive(Debug, Default)]
pub(crate) struct List;

impl PluginCommand for List {
    type Plugin = Nuts;

    fn name(&self) -> &str {
        "nuts kv list"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name()).required(
            "bucket",
            SyntaxShape::String,
            "Bucket to list keys for",
        )
    }

    fn description(&self) -> &str {
        "List keys in a bucket"
    }

    fn run(
        &self,
        plugin: &Self::Plugin,
        _engine: &EngineInterface,
        call: &EvaluatedCall,
        _input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let bucket: String = call.req(0)?;
        let client = plugin.nats.read().unwrap();
        match client.as_ref() {
            Some(client) => {
                let jetstream = jetstream::new(client.clone());
                let (tx, mut rx) = mpsc::unbounded_channel();
                plugin.runtime.spawn(async move {
                    let bucket = jetstream
                        .get_key_value(bucket)
                        .await
                        .map_err(|error| LabeledError::new(error.to_string()))
                        .unwrap();
                    let mut keys = bucket
                        .keys()
                        .await
                        .map_err(|error| LabeledError::new(error.to_string()))
                        .unwrap();
                    while let Some(key) = keys.next().await {
                        match key {
                            Ok(key) => tx.send(key).unwrap(),
                            Err(_) => break,
                        }
                    }
                });
                Ok(PipelineData::ListStream(
                    ListStream::new(
                        {
                            let handle = plugin.runtime.handle().clone();
                            std::iter::repeat_with(move || handle.block_on(rx.recv()))
                        }
                        .take_while(|item| item.is_some())
                        .map(|item| item.unwrap().into_value(Span::unknown())),
                        call.head,
                        Signals::new(Arc::new(AtomicBool::new(false))),
                    ),
                    None,
                ))
            }
            None => Err(LabeledError::new(
                "Not connected to NATS server. Call `nuts connect` first",
            )),
        }
    }
}
