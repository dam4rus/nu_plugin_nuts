use std::sync::Arc;

use async_nats::jetstream::{self};
use futures::future;
use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{Example, LabeledError, PipelineData, Signature, SyntaxShape, Type, Value};

use crate::Nuts;

pub(crate) struct Delete;

impl PluginCommand for Delete {
    type Plugin = Nuts;

    fn name(&self) -> &str {
        "nuts kv del"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .required(
                "bucket",
                SyntaxShape::String,
                "Bucket to delete or delete key from",
            )
            .input_output_types(vec![
                (Type::Nothing, Type::Nothing),
                (Type::String, Type::Nothing),
                (Type::List(Box::new(Type::String)), Type::Nothing),
            ])
    }

    fn description(&self) -> &str {
        "Delete keys from a bucket or the whole bucket"
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["nats", "kv", "key", "value", "delete"]
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![
            Example {
                example: "nuts kv del my-bucket",
                description: "Delete an entire bucket",
                result: None,
            },
            Example {
                example: "mykey | nuts kv del my-bucket",
                description: "Delete a single key from the bucket",
                result: None,
            },
            Example {
                example: "[mykey myotherkey] | nuts kv del my-bucket",
                description: "Delete multiple keys from the bucket",
                result: None,
            },
        ]
    }

    fn run(
        &self,
        plugin: &Self::Plugin,
        _engine: &EngineInterface,
        call: &EvaluatedCall,
        input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let bucket: String = call.req(0)?;
        let client = plugin.nats.read().unwrap();
        match client.as_ref() {
            Some(client) => {
                plugin.runtime.block_on({
                    let client = client.clone();
                    async move {
                        let jetstream = jetstream::new(client);
                        match input {
                            PipelineData::Empty => {
                                jetstream
                                    .delete_key_value(bucket)
                                    .await
                                    .map_err(|error| LabeledError::new(error.to_string()))?;
                            }
                            PipelineData::Value(value, ..) => {
                                Self::delete_value(&bucket, jetstream, value).await?;
                            }
                            PipelineData::ListStream(list_stream, ..) => {
                                future::try_join_all(
                                    list_stream.into_iter().map(|value| {
                                        let bucket = bucket.clone();
                                        let jetstream = jetstream.clone();
                                        async move {
                                            Self::delete_value(&bucket, jetstream, value).await
                                        }
                                    }),
                                )
                                .await?;
                            }
                            _ => (),
                        }
                        Ok::<(), LabeledError>(())
                    }
                })?;
                Ok(PipelineData::Empty)
            }
            None => Err(LabeledError::new(
                "Not connected to NATS server. Call `nuts connect` first",
            )),
        }
    }
}

impl Delete {
    async fn delete_value(
        bucket: &str,
        jetstream: jetstream::Context,
        value: Value,
    ) -> Result<(), LabeledError> {
        match value {
            Value::String {
                val, internal_span, ..
            } => jetstream
                .get_key_value(bucket)
                .await
                .map_err(|error| LabeledError::new(error.to_string()))?
                .delete(val)
                .await
                .map_err(|error| {
                    LabeledError::new(error.to_string())
                        .with_label("with key origiting from here", internal_span)
                })?,
            Value::List {
                vals,
                internal_span,
                ..
            } => {
                let key_value = Arc::new(
                    jetstream
                        .get_key_value(bucket)
                        .await
                        .map_err(|error| LabeledError::new(error.to_string()))?,
                );
                future::try_join_all(vals.iter().map(|value| {
                    let key_value = key_value.clone();
                    async move {
                        if let Value::String { val, .. } = value {
                            key_value.delete(val).await
                        } else {
                            unreachable!("Invalid list item type")
                        }
                    }
                }))
                .await
                .map_err(|error| {
                    LabeledError::new(error.to_string())
                        .with_label("with value originating from here", internal_span)
                })?;
            }
            _ => {
                return Err(LabeledError::new("Invalid value type")
                    .with_label("originating from here", value.span()));
            }
        }
        Ok(())
    }
}
