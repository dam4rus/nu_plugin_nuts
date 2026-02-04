use anyhow::Context;
use async_nats::{
    Client, HeaderMap, HeaderName, HeaderValue,
    header::{IntoHeaderName, IntoHeaderValue},
};
use futures::future;
use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    Example, LabeledError, PipelineData, Record, ShellError, Signature, Span, SyntaxShape, Type,
    Value,
};
use nu_utils::SharedCow;

use crate::Nuts;

#[derive(Debug)]
pub(crate) struct Publish;

impl PluginCommand for Publish {
    type Plugin = Nuts;

    fn name(&self) -> &str {
        "nuts pub"
    }

    fn signature(&self) -> Signature {
        let record_with_string_payload_type = Type::Record(
            [
                ("headers".to_owned(), Type::record()),
                ("payload".to_owned(), Type::String),
            ]
            .into(),
        );
        let record_with_binary_payload_type = Type::Record(
            [
                ("headers".to_owned(), Type::record()),
                ("payload".to_owned(), Type::Binary),
            ]
            .into(),
        );
        Signature::build(self.name())
            .required("subject", SyntaxShape::String, "Subject to publish to")
            .input_output_types(vec![
                (Type::String, Type::Nothing),
                (Type::Binary, Type::Nothing),
                (record_with_string_payload_type.clone(), Type::Nothing),
                (record_with_binary_payload_type.clone(), Type::Nothing),
                (Type::List(Type::String.into()), Type::Nothing),
                (Type::List(Type::Binary.into()), Type::Nothing),
                (
                    Type::List(record_with_string_payload_type.into()),
                    Type::Nothing,
                ),
                (
                    Type::List(record_with_binary_payload_type.into()),
                    Type::Nothing,
                ),
            ])
    }

    fn description(&self) -> &str {
        "Publish to a subject"
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["nats", "pub", "publish"]
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![
            Example {
                example: "'my message' | nuts pub subject",
                description: "Publish a string message",
                result: None,
            },
            Example {
                example: "{headers: {'Content-Type': 'text/plain'}, payload: 'my message'} | nuts pub subject",
                description: "Publish a message with headers",
                result: None,
            },
            Example {
                example: "['my message', 'my other message'] | nuts pub subject",
                description: "Publish multiple string messages",
                result: None,
            },
            Example {
                example: "[{headers: {'Content-Type': 'text/plain'}, payload: 'my message'}, {headers: {'Content-Type': 'text/plain'}, payload: 'my other message'}] | nuts pub subject",
                description: "Publish multiple messages with headers",
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
        let subject: String = call.req(0)?;
        let client = plugin.nats.read().unwrap();
        match client.as_ref() {
            Some(client) => {
                plugin.runtime.block_on(async move {
                    match input {
                        PipelineData::Value(Value::List { vals, .. }, ..) => {
                            future::try_join_all(vals.into_iter().map(|value| async {
                                Self::publish_value(client, &subject, value).await
                            }))
                            .await?;
                        }
                        PipelineData::Value(value, ..) => {
                            Self::publish_value(client, &subject, value).await?
                        }
                        PipelineData::ListStream(list_stream, ..) => {
                            future::try_join_all(list_stream.into_iter().map(|value| async {
                                Self::publish_value(client, &subject, value).await
                            }))
                            .await?;
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

impl Publish {
    async fn publish_record(
        client: &Client,
        subject: &str,
        record: SharedCow<Record>,
        internal_span: Span,
    ) -> Result<(), LabeledError> {
        let headers = if let Some(Value::Record { val: headers, .. }) = record.get("headers") {
            let headers = headers
                .iter()
                .map(|(key, value)| {
                    Ok((
                        key.clone().into_header_name(),
                        value.coerce_str().map(|value| value.into_header_value())?,
                    ))
                })
                .collect::<Result<Vec<(HeaderName, HeaderValue)>, ShellError>>()?;
            HeaderMap::from_iter(headers.into_iter())
        } else {
            HeaderMap::new()
        };
        let payload = record
            .get("payload")
            .cloned()
            .ok_or_else(|| {
                LabeledError::new("missing payload")
                    .with_label("input record must contain a payload field", internal_span)
            })?
            .coerce_into_binary()?;

        client
            .publish_with_headers(subject.to_owned(), headers, payload.into())
            .await
            .context("Failed to publish to NATS subject")
            .map_err(|error| LabeledError::new(error.to_string()))?;
        Ok(())
    }

    async fn publish_value(
        client: &Client,
        subject: &str,
        value: Value,
    ) -> Result<(), LabeledError> {
        match value {
            Value::Record {
                val, internal_span, ..
            } => Self::publish_record(client, subject, val, internal_span).await?,
            value => {
                let payload = value.clone().coerce_into_binary()?;
                client
                    .publish(subject.to_owned(), payload.into())
                    .await
                    .context("Failed to publish to NATS subject")
                    .map_err(|error| LabeledError::new(error.to_string()))?;
            }
        }
        Ok(())
    }
}
