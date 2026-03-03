//! Progress bar utilities

use indicatif::{ProgressBar as IndicatifBar, ProgressStyle, MultiProgress};
use std::time::Duration;

pub enum ProgressBar {
    Bar(IndicatifBar),
    Multi(MultiProgress, IndicatifBar),
    None,
}

impl ProgressBar {
    pub fn new_spinner() -> Self {
        let bar = IndicatifBar::new_spinner();
        bar.set_style(
            ProgressStyle::default_spinner()
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
                .template("{spinner} {msg}")
                .unwrap(),
        );
        bar.enable_steady_tick(Duration::from_millis(100));
        ProgressBar::Bar(bar)
    }

    pub fn new_bar(len: u64) -> Self {
        let bar = IndicatifBar::new(len);
        bar.set_style(
            ProgressStyle::default_bar()
                .template("{spinner} [{elapsed_precise}] [{bar:40}] {pos}/{len} ({eta}) {msg}")
                .unwrap()
                .progress_chars("█▓▒░ "),
        );
        ProgressBar::Bar(bar)
    }

    /// Progress bar for backup/add
    pub fn new_backup_bar(total_files: u64, _total_size: u64) -> Self {
        let bar = IndicatifBar::new(total_files);
        bar.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner} [{elapsed_precise}] [{bar:40}] {pos}/{len} files ({percent}%)\n\
                     ⤷ {msg}\n\
                     ├─ Processed: {bytes} / {total_bytes}\n\
                     └─ Speed: {bytes_per_sec} | ETA: {eta}"
                )
                .unwrap()
                .progress_chars("█▓▒░ ")
        );
        
        bar.set_position(0);
        bar.set_message("Initializing...");
        
        ProgressBar::Bar(bar)
    }

    /// Progress bar for restore
    pub fn new_restore_bar(total_files: u64, _total_size: u64) -> Self {
        let bar = IndicatifBar::new(total_files);
        bar.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner} [{elapsed_precise}] [{bar:40}] {pos}/{len} files ({percent}%)\n\
                     ⤷ Restoring: {msg}\n\
                     ├─ Written: {bytes} / {total_bytes}\n\
                     └─ Speed: {bytes_per_sec} | ETA: {eta}"
                )
                .unwrap()
                .progress_chars("█▓▒░ ")
        );
        
        bar.set_position(0);
        bar.set_message("Preparing...");
        
        ProgressBar::Bar(bar)
    }

    /// Progress bar for garbage collection
    pub fn new_gc_bar(total_chunks: u64) -> Self {
        let bar = IndicatifBar::new(total_chunks);
        bar.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner} [{elapsed_precise}] [{bar:40}] {pos}/{len} chunks\n\
                     ⤷ {msg}\n\
                     └─ ETA: {eta}"
                )
                .unwrap()
                .progress_chars("█▓▒░ ")
        );
        
        bar.set_position(0);
        bar.set_message("Cleaning...");
        
        ProgressBar::Bar(bar)
    }

    /// Progress bar for integrity check
    pub fn new_check_bar(total_items: u64) -> Self {
        let bar = IndicatifBar::new(total_items);
        bar.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner} [{elapsed_precise}] [{bar:40}] {pos}/{len} items\n\
                     ⤷ {msg}\n\
                     └─ ETA: {eta}"
                )
                .unwrap()
                .progress_chars("█▓▒░ ")
        );
        
        bar.set_position(0);
        bar.set_message("Checking...");
        
        ProgressBar::Bar(bar)
    }

    pub fn set_message(&self, msg: String) {
        match self {
            ProgressBar::Bar(bar) => bar.set_message(msg),
            ProgressBar::Multi(_, bar) => bar.set_message(msg),
            ProgressBar::None => {}
        }
    }

    pub fn inc(&self, delta: u64) {
        match self {
            ProgressBar::Bar(bar) => bar.inc(delta),
            ProgressBar::Multi(_, bar) => bar.inc(delta),
            ProgressBar::None => {}
        }
    }

    pub fn set_position(&self, pos: u64) {
        match self {
            ProgressBar::Bar(bar) => bar.set_position(pos),
            ProgressBar::Multi(_, bar) => bar.set_position(pos),
            ProgressBar::None => {}
        }
    }

    pub fn set_length(&self, len: u64) {
        match self {
            ProgressBar::Bar(bar) => bar.set_length(len),
            ProgressBar::Multi(_, bar) => bar.set_length(len),
            ProgressBar::None => {}
        }
    }

    pub fn finish(&self) {
        match self {
            ProgressBar::Bar(bar) => bar.finish(),
            ProgressBar::Multi(multi, bar) => {
                bar.finish();
                multi.clear().ok();
            }
            ProgressBar::None => {}
        }
    }

    pub fn println(&self, msg: &str) {
        match self {
            ProgressBar::Bar(bar) => bar.println(msg),
            ProgressBar::Multi(multi, _) => {
                multi.println(msg).ok();
            }
            ProgressBar::None => println!("{}", msg),
        }
    }
}

impl Drop for ProgressBar {
    fn drop(&mut self) {
        self.finish();
    }
}

