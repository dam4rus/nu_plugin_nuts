use anyhow::{Context, Result};
use nu_plugin::{MsgPackSerializer, serve_plugin};
use nu_plugin_nuts::Nuts;
use tokio::runtime::Runtime;

fn main() -> Result<()> {
    env_logger::init();
    serve_plugin(
        &Nuts::new(Runtime::new().context("Failed to create tokio runtime")?),
        MsgPackSerializer {},
    );
    Ok(())
}
