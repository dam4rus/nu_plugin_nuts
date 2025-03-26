use anyhow::Context;
use async_nats::{
    Client, HeaderMap, HeaderName, HeaderValue,
    header::{IntoHeaderName, IntoHeaderValue},
};
use futures::future;
use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    LabeledError, PipelineData, Record, ShellError, Signature, Span, SyntaxShape, Value,
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
        Signature::build(self.name()).required(
            "subject",
            SyntaxShape::String,
            "Subject to publish to",
        )
    }

    fn description(&self) -> &str {
        "Publish to a subject"
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
        subject: &String,
        value: Value,
    ) -> Result<(), LabeledError> {
        match value {
            Value::Record { val, internal_span } => {
                Self::publish_record(client, subject, val, internal_span).await?
            }
            Value::List { vals, .. } => {
                future::try_join_all(
                    vals.into_iter()
                        .map(|value| async { Self::publish_value(client, subject, value).await }),
                )
                .await?;
            }
            value => {
                let payload = value.clone().coerce_into_binary()?;
                client
                    .publish(subject.clone(), payload.into())
                    .await
                    .context("Failed to publish to NATS subject")
                    .map_err(|error| LabeledError::new(error.to_string()))?;
            }
        }
        Ok(())
    }
}
