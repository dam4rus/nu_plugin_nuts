use anyhow::Context;
use async_nats::ConnectOptions;
use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{LabeledError, PipelineData, ShellError, Signature, SyntaxShape};

use crate::Nuts;

#[derive(Debug)]
pub(crate) struct Connect;

impl PluginCommand for Connect {
    type Plugin = Nuts;

    fn name(&self) -> &str {
        "nuts connect"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .optional("url", SyntaxShape::String, "Connection URL")
            .named(
                "user",
                SyntaxShape::String,
                "Username or token to authenticate with",
                None,
            )
            .named(
                "password",
                SyntaxShape::String,
                "Password to authenticate with",
                None,
            )
            .named(
                "creds",
                SyntaxShape::String,
                "Token to authenticate with",
                None,
            )
            .named(
                "nkey",
                SyntaxShape::String,
                "NKEY to authenticate with",
                None,
            )
    }

    fn description(&self) -> &str {
        "Connect to a NATS server"
    }

    fn run(
        &self,
        plugin: &Self::Plugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let url: String = call.opt(0)?.unwrap_or_else(|| String::from("localhost"));
        let user = get_flag_or_env_var("user", "NATS_USER", call, engine)?;
        let password = get_flag_or_env_var("password", "NATS_PASSWORD", call, engine)?;
        let credentials = get_flag_or_env_var("creds", "NATS_CREDS", call, engine)?;
        let nkey = get_flag_or_env_var("nkey", "NATS_NKEY", call, engine)?;

        let options = ConnectOptions::new();
        let options = match (user, password) {
            (None, None) => options,
            (Some(user), Some(password)) => options.user_and_password(user, password),
            (Some(_), None) => {
                return Err(LabeledError::new(
                    "Missing `--password` argument for basic authentication",
                ));
            }
            (None, Some(_)) => {
                return Err(LabeledError::new(
                    "Missing `--user` argument for basic authentication",
                ));
            }
        };
        let options = match credentials {
            Some(credentials) => options
                .credentials(&credentials)
                .map_err(|err| LabeledError::new(err.to_string()))?,
            None => options,
        };
        let options = match nkey {
            Some(nkey) => options.nkey(nkey),
            None => options,
        };
        let client = plugin
            .runtime
            .block_on(async move {
                async_nats::connect_with_options(url, options)
                    .await
                    .context("Failed to connect to NATS server")
            })
            .map_err(|error| LabeledError::new(error.to_string()))?;
        *plugin.nats.write().unwrap() = Some(client);

        Ok(input)
    }
}

fn get_flag_or_env_var(
    flag: &str,
    env_var: impl Into<String>,
    call: &EvaluatedCall,
    engine: &EngineInterface,
) -> Result<Option<String>, ShellError> {
    let value: Option<String> = match call.get_flag(flag)? {
        Some(nkey) => Some(nkey),
        None => engine
            .get_env_var(env_var)?
            .map(|value| value.coerce_into_string())
            .transpose()?,
    };
    Ok(value)
}
