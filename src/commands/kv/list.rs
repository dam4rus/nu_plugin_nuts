use async_nats::jetstream::{self};
use futures::TryStreamExt;

use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{IntoValue, LabeledError, PipelineData, Signature, SyntaxShape};

use crate::Nuts;

#[derive(Debug)]
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
                let keys = plugin.runtime.block_on(async move {
                    jetstream
                        .get_key_value(bucket)
                        .await
                        .map_err(|error| LabeledError::new(error.to_string()))?
                        .keys()
                        .await
                        .map_err(|error| LabeledError::new(error.to_string()))?
                        .try_collect::<Vec<String>>()
                        .await
                        .map_err(|error| LabeledError::new(error.to_string()))
                })?;
                Ok(PipelineData::Value(keys.into_value(call.head), None))
            }
            None => Err(LabeledError::new(
                "Not connected to NATS server. Call `nuts connect` first",
            )),
        }
    }
}
