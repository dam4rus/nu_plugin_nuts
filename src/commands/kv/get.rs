use async_nats::jetstream;
use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    IntoValue, LabeledError, PipelineData, Signature, Span, SyntaxShape, Type, Value,
};

use crate::Nuts;

pub(crate) struct Get;

impl PluginCommand for Get {
    type Plugin = Nuts;

    fn name(&self) -> &str {
        "nuts kv get"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .required(
                "bucket",
                SyntaxShape::String,
                "The bucket to get value from",
            )
            .required("key", SyntaxShape::String, "The key to get the value of")
            .switch("binary", "Return the value in binary format", Some('b'))
            .input_output_types(vec![(Type::Any, Type::String), (Type::Any, Type::Binary)])
    }

    fn description(&self) -> &str {
        "Get the value of a key in a bucket"
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["nats", "kv", "key", "value", "get"]
    }

    fn run(
        &self,
        plugin: &Self::Plugin,
        _engine: &EngineInterface,
        call: &EvaluatedCall,
        _input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let bucket: String = call.req(0)?;
        let key: String = call.req(1)?;
        let binary_output = call.has_flag("binary")?;
        let client = plugin.nats.read().unwrap();
        match client.as_ref() {
            Some(client) => {
                let value = plugin.runtime.block_on({
                    let client = client.clone();
                    async move {
                        let value = jetstream::new(client)
                            .get_key_value(&bucket)
                            .await
                            .map_err(|error| LabeledError::new(error.to_string()))?
                            .get(&key)
                            .await
                            .map_err(|error| LabeledError::new(error.to_string()))?
                            .ok_or_else(|| {
                                LabeledError::new(format!("Key {key} not found in bucket {bucket}"))
                            })?;
                        Ok::<Value, LabeledError>(if binary_output {
                            value.into_value(Span::unknown())
                        } else {
                            String::from_utf8_lossy(&value).into_value(Span::unknown())
                        })
                    }
                })?;
                Ok(PipelineData::Value(value, None))
            }
            None => Err(LabeledError::new(
                "Not connected to NATS server. Call `nuts connect` first",
            )),
        }
    }
}
