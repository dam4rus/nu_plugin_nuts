use async_nats::jetstream::{self, kv::Store};
use futures::future;
use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{LabeledError, PipelineData, Record, Signature, SyntaxShape, Value};

use crate::Nuts;

pub(crate) struct Put;

impl PluginCommand for Put {
    type Plugin = Nuts;

    fn name(&self) -> &str {
        "nuts kv put"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name()).required("bucket", SyntaxShape::String, "Bucket to put to")
    }

    fn description(&self) -> &str {
        "Put values into a key value bucket"
    }

    fn run(
        &self,
        plugin: &Self::Plugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let bucket: String = call.req(0)?;
        match plugin.nats.read().unwrap().as_ref() {
            Some(client) => {
                let jetstream = jetstream::new(client.clone());
                plugin.runtime.block_on(async move {
                    let store = jetstream
                        .get_key_value(bucket)
                        .await
                        .map_err(|error| LabeledError::new(error.to_string()))?;
                    match input {
                        PipelineData::Value(value, _) => {
                            Self::put_value(engine, call, &store, value)
                                .await
                                .map_err(|error| LabeledError::new(error.to_string()))?;
                        }
                        PipelineData::ListStream(value, _) => {
                            future::try_join_all(value.into_iter().map(|value| async {
                                Self::put_value(engine, call, &store, value).await
                            }))
                            .await
                            .map_err(|error| LabeledError::new(error.to_string()))?;
                        }
                        _ => (),
                    }
                    Ok::<(), LabeledError>(())
                })?;
                Ok(PipelineData::Empty)
            }
            None => Err(LabeledError::new(
                "Not connected to NATS server. Call `nuts connect` first",
            )),
        }
    }
}

impl Put {
    async fn put_value(
        engine: &EngineInterface,
        call: &EvaluatedCall,
        store: &Store,
        value: Value,
    ) -> Result<(), LabeledError> {
        match value {
            Value::Record { val, .. } => {
                future::try_join_all(Record::clone(&val).into_iter().map(
                    |(key, value)| async move {
                        store
                            .put(&key, value.coerce_into_binary()?.into())
                            .await
                            .map_err(|error| LabeledError::new(error.to_string()))
                    },
                ))
                .await?;
            }
            Value::List { vals, .. } => {
                future::try_join_all(
                    vals.into_iter()
                        .map(|value| async { Self::put_value(engine, call, store, value).await }),
                )
                .await
                .map_err(|error| LabeledError::new(error.to_string()))?;
            }
            value => {
                return Err(LabeledError::new("input must be a record")
                    .with_label("originating here", value.span()));
            }
        }
        Ok(())
    }
}
