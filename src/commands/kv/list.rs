use async_nats::jetstream::{self};
use futures::TryStreamExt;

use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{IntoValue, LabeledError, PipelineData, Signature, SyntaxShape, Type};

use crate::Nuts;

#[derive(Debug)]
pub(crate) struct List;

impl PluginCommand for List {
    type Plugin = Nuts;

    fn name(&self) -> &str {
        "nuts kv list"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .optional("bucket", SyntaxShape::String, "Bucket to list keys for")
            .input_output_type(Type::Any, Type::List(Type::String.into()))
            .search_terms(vec![
                "nats".to_owned(),
                "kv".to_owned(),
                "key".to_owned(),
                "value".to_owned(),
                "list".to_owned(),
            ])
    }

    fn description(&self) -> &str {
        "List buckets or keys in a bucket"
    }

    fn run(
        &self,
        plugin: &Self::Plugin,
        _engine: &EngineInterface,
        call: &EvaluatedCall,
        _input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let bucket: Option<String> = call.opt(0)?;
        let client = plugin.nats.read().unwrap();
        match client.as_ref() {
            Some(client) => {
                let jetstream = jetstream::new(client.clone());
                let keys = plugin.runtime.block_on(async move {
                    match bucket {
                        Some(bucket) => jetstream
                            .get_key_value(bucket)
                            .await
                            .map_err(|error| LabeledError::new(error.to_string()))?
                            .keys()
                            .await
                            .map_err(|error| LabeledError::new(error.to_string()))?
                            .try_collect::<Vec<String>>()
                            .await
                            .map_err(|error| LabeledError::new(error.to_string())),
                        None => jetstream
                            .stream_names()
                            .try_filter_map(|name| async move {
                                let result = if name.starts_with("KV_") {
                                    Some(name[3..].to_owned())
                                } else {
                                    None
                                };
                                Ok(result)
                            })
                            .try_collect::<Vec<String>>()
                            .await
                            .map_err(|error| LabeledError::new(error.to_string())),
                    }
                })?;
                Ok(PipelineData::Value(keys.into_value(call.head), None))
            }
            None => Err(LabeledError::new(
                "Not connected to NATS server. Call `nuts connect` first",
            )),
        }
    }
}
