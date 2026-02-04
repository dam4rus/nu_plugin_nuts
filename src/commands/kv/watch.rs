use async_nats::jetstream;
use futures::StreamExt;
use log::info;
use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    Example, IntoValue, LabeledError, ListStream, PipelineData, Record, ShellError, Signals,
    Signature, Span, SyntaxShape, Type,
};
use tokio::{select, sync::mpsc};
use tokio_util::sync::CancellationToken;

use crate::Nuts;

pub(crate) struct Watch;

impl PluginCommand for Watch {
    type Plugin = Nuts;

    fn name(&self) -> &str {
        "nuts kv watch"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .required("bucket", SyntaxShape::String, "Bucket to watch")
            .optional("key", SyntaxShape::String, "The key to watch")
            .input_output_type(Type::Any, Type::List(Type::String.into()))
    }

    fn description(&self) -> &str {
        "Watch a bucket or key in a bucket"
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["nats", "kv", "key", "value", "watch"]
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![
            Example {
                example: "nuts kv watch mybucket",
                description: "Watch all keys in a bucket",
                result: Some(
                    Record::from_iter([
                        ("mykey".to_owned(), "myvalue".into_value(Span::unknown())),
                        (
                            "myotherkey".to_owned(),
                            "myothervalue".into_value(Span::unknown()),
                        ),
                    ])
                    .into_value(Span::unknown()),
                ),
            },
            Example {
                example: "nuts kv watch mybucket mykey",
                description: "Watch a single key in a bucket",
                result: Some(
                    Record::from_iter([(
                        "mykey".to_owned(),
                        "myvalue".into_value(Span::unknown()),
                    )])
                    .into_value(Span::unknown()),
                ),
            },
        ]
    }

    fn run(
        &self,
        plugin: &Self::Plugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        _input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let bucket: String = call.req(0)?;
        let key: Option<String> = call.opt(1)?;
        let client = plugin.nats.read().unwrap();
        match client.as_ref() {
            Some(client) => {
                let (tx, mut rx) = mpsc::unbounded_channel();
                plugin.runtime.spawn({
                    let client = client.clone();
                    let engine = engine.clone();
                    async move {
                        let jetstream = jetstream::new(client);
                        let key_value = jetstream
                            .get_key_value(bucket)
                            .await
                            .unwrap();
                        let mut watch = match key {
                            Some(key) => key_value
                                .watch(key)
                                .await
                                .unwrap(),
                            None => key_value
                                .watch_all()
                                .await
                            .unwrap(),
                        };

                        let cancellation = CancellationToken::new();
                        let _signal_guard = engine.register_signal_handler(Box::new({
                            let cancellation = cancellation.clone();
                            move |_| {
                                info!("Cancel");
                                cancellation.cancel();
                            }
                        })).expect("Failed to register signal handler");

                        loop {
                            select! {
                                _ = cancellation.cancelled() => {
                                    break;
                                }
                                Some(entry) = watch.next() => {
                                    tx.send(entry).expect("Failed to send key value entry through channel");
                                }
                            }
                        }
                    }
                });

                let handle = plugin.runtime.handle().clone();
                let stream_iter = std::iter::repeat_with(move || handle.block_on(rx.recv()))
                    .map_while(move |message| {
                        message.map(|message| match message {
                            Ok(entry) => Record::from_iter([(
                                entry.key,
                                String::from_utf8_lossy(&entry.value).into_value(Span::unknown()),
                            )])
                            .into_value(Span::unknown()),
                            Err(error) => IntoValue::into_value(
                                ShellError::LabeledError(
                                    LabeledError::new(error.to_string()).into(),
                                ),
                                Span::unknown(),
                            ),
                        })
                    });

                Ok(PipelineData::ListStream(
                    ListStream::new(stream_iter, call.head, Signals::empty()),
                    None,
                ))
            }
            None => Err(LabeledError::new(
                "Not connected to NATS server. Call `nuts connect` first",
            )),
        }
    }
}
