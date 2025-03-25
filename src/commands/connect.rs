use anyhow::Context;
use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{LabeledError, PipelineData, Signature, SyntaxShape};

use crate::Nuts;

#[derive(Debug)]
pub(crate) struct Connect;

impl PluginCommand for Connect {
    type Plugin = Nuts;

    fn name(&self) -> &str {
        "nuts connect"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name()).required("url", SyntaxShape::String, "Connection URL")
    }

    fn description(&self) -> &str {
        "Connect to a NATS server"
    }

    fn run(
        &self,
        plugin: &Self::Plugin,
        _engine: &EngineInterface,
        call: &EvaluatedCall,
        input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let argument: String = call.req(0)?;
        let client = plugin
            .runtime
            .block_on(async move {
                async_nats::connect(argument)
                    .await
                    .context("Failed to connect to NATS server")
            })
            .map_err(|error| LabeledError::new(error.to_string()))?;
        *plugin.nats.write().unwrap() = Some(client);

        Ok(input)
    }
}
