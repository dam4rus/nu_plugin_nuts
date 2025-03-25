use futures::StreamExt;
use log::info;
use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    IntoValue, LabeledError, ListStream, PipelineData, Signals, Signature, Span, SyntaxShape,
};
use tokio::{select, sync::mpsc};
use tokio_util::sync::CancellationToken;

use crate::Nuts;

pub(crate) struct Subscribe;

impl PluginCommand for Subscribe {
    type Plugin = Nuts;

    fn name(&self) -> &str {
        "nuts sub"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name()).required(
            "subject",
            SyntaxShape::String,
            "Subject to consume from",
        )
    }

    fn description(&self) -> &str {
        "Consume from a subject"
    }

    fn run(
        &self,
        plugin: &Self::Plugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        _input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let subject: String = call.req(0)?;
        let client = plugin.nats.read().unwrap();
        match client.as_ref() {
            Some(client) => {
                let (tx, mut rx) = mpsc::unbounded_channel();
                let cancellation = CancellationToken::new();
                plugin.runtime.spawn({
                    let client = client.clone();
                    let cancellation = cancellation.clone();
                    let engine = engine.clone();
                    async move {
                    info!("Spawned subscription");
                        let mut subscription = client
                            .subscribe(subject.clone())
                            .await
                            .unwrap_or_else(|_| panic!("Failed to subscribe to subject {}", subject));

                        info!("Subscribed");
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
                                Some(message) = subscription.next() => {
                                    tx.send(message).expect("Failed to send message through channel");
                                }
                            };
                        }
                    }
                });

                let handle = plugin.runtime.handle().clone();
                let stream_iter = std::iter::repeat_with(move || handle.block_on(rx.recv()))
                    .map_while(|message| {
                        message.map(|message| {
                            String::from_utf8_lossy(&message.payload).into_value(Span::unknown())
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
