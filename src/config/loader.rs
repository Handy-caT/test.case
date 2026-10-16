use std::path::PathBuf;

use clap::Parser;
use config::{Config as ConfigBuilder, Environment, File, FileFormat};
use eyre::WrapErr;

use crate::config::Config;

#[derive(Parser)]
#[command(
    version,
    about = "Public Binance/Aster market depth and spread signals"
)]
struct CliArgs {
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,
}

impl Config {
    pub fn load() -> eyre::Result<Self> {
        let args = CliArgs::parse();
        let mut builder = ConfigBuilder::builder();
        if let Some(path) = args.config {
            builder = builder.add_source(File::from(path).format(FileFormat::Toml));
        }
        let mut config: Self = builder
            .add_source(
                Environment::with_prefix("TEST_CASE")
                    .prefix_separator("__")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()
            .wrap_err("Could not load configuration sources")?
            .try_deserialize()
            .wrap_err("Could not deserialize configuration")?;
        config.validate()?;
        Ok(config)
    }
}
