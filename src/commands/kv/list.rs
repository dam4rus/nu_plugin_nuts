use async_nats::jetstream::{self};
use futures::TryStreamExt;

use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    Example, IntoValue, LabeledError, PipelineData, Signature, Span, SyntaxShape, Type,
};

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
    }

    fn description(&self) -> &str {
        "List buckets or keys in a bucket"
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["nats", "kv", "key", "value", "list"]
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![
            Example {
                example: "nuts kv list",
                description: "List all buckets",
                result: Some(["mybucket".into_value(Span::unknown())].into_value(Span::unknown())),
            },
            Example {
                example: "nuts kv list mybucket",
                description: "List all keys in a bucket",
                result: Some(["mykey".into_value(Span::unknown())].into_value(Span::unknown())),
            },
        ]
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
                                Ok(name.strip_prefix("KV_").map(String::from))
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
