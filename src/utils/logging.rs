//! Logging utilities

use anyhow::Result;

/// Initialize logging based on verbosity
pub fn init(verbosity: u8) -> Result<()> {
    let level = match verbosity {
        0 => log::LevelFilter::Warn,
        1 => log::LevelFilter::Info,
        2 => log::LevelFilter::Debug,
        _ => log::LevelFilter::Trace,
    };

    env_logger::builder()
        .filter_level(level)
        .format_timestamp_millis()
        .try_init()?;

    Ok(())
}

