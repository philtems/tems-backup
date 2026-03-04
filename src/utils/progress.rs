//! Progress bar utilities

use indicatif::{ProgressBar as IndicatifBar, ProgressStyle, MultiProgress};
use std::time::Duration;

pub enum ProgressBar {
    Bar(IndicatifBar),
    Multi(MultiProgress, IndicatifBar, IndicatifBar, IndicatifBar),
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

    /// Dual progress bar for backup/add (files and data)
    pub fn new_dual_backup_bar(total_files: u64, total_bytes: u64) -> Self {
        let multi = MultiProgress::new();
        
        // Files progress bar
        let files_bar = multi.add(IndicatifBar::new(total_files));
        files_bar.set_style(
            ProgressStyle::default_bar()
                .template("{spinner} [{elapsed_precise}] [{bar:40}] {pos}/{len} files ({percent}%)\n")
                .unwrap()
                .progress_chars("█▓▒░ ")
        );
        files_bar.set_message("Files");
        
        // Data progress bar
        let data_bar = multi.add(IndicatifBar::new(total_bytes));
        data_bar.set_style(
            ProgressStyle::default_bar()
                .template("├─ Data: {bytes} / {total_bytes} ({bytes_per_sec}) [{bar:40}] {msg}\n")
                .unwrap()
                .progress_chars("█▓▒░ ")
        );
        data_bar.set_message("Processing...");
        
        // Speed and ETA line
        let speed_bar = multi.add(IndicatifBar::new(100));
        speed_bar.set_style(
            ProgressStyle::default_bar()
                .template("└─ Files/sec: {msg} | ETA: {eta}")
                .unwrap()
        );
        speed_bar.set_message("0");
        speed_bar.set_position(0);
        
        ProgressBar::Multi(multi, files_bar, data_bar, speed_bar)
    }

    /// Dual progress bar for restore (files and data)
    pub fn new_dual_restore_bar(total_files: u64, total_bytes: u64) -> Self {
        let multi = MultiProgress::new();
        
        // Files progress bar
        let files_bar = multi.add(IndicatifBar::new(total_files));
        files_bar.set_style(
            ProgressStyle::default_bar()
                .template("{spinner} [{elapsed_precise}] [{bar:40}] {pos}/{len} files ({percent}%)\n")
                .unwrap()
                .progress_chars("█▓▒░ ")
        );
        files_bar.set_message("Files");
        
        // Data progress bar
        let data_bar = multi.add(IndicatifBar::new(total_bytes));
        data_bar.set_style(
            ProgressStyle::default_bar()
                .template("├─ Restored: {bytes} / {total_bytes} ({bytes_per_sec}) [{bar:40}] {msg}\n")
                .unwrap()
                .progress_chars("█▓▒░ ")
        );
        data_bar.set_message("Restoring...");
        
        // Speed and ETA line
        let speed_bar = multi.add(IndicatifBar::new(100));
        speed_bar.set_style(
            ProgressStyle::default_bar()
                .template("└─ Files/sec: {msg} | ETA: {eta}")
                .unwrap()
        );
        speed_bar.set_message("0");
        speed_bar.set_position(0);
        
        ProgressBar::Multi(multi, files_bar, data_bar, speed_bar)
    }

    /// Progress bar for garbage collection (chunks)
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

    /// Progress bar for integrity check (items)
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
            ProgressBar::Multi(_, files_bar, data_bar, _) => {
                files_bar.set_message(format!("File: {}", msg));
                data_bar.set_message(msg);
            }
            ProgressBar::None => {}
        }
    }

    pub fn set_files_message(&self, msg: String) {
        if let ProgressBar::Multi(_, files_bar, _, _) = self {
            files_bar.set_message(msg);
        }
    }

    pub fn set_data_message(&self, msg: String) {
        if let ProgressBar::Multi(_, _, data_bar, _) = self {
            data_bar.set_message(msg);
        }
    }

    pub fn set_files_speed(&self, files_per_sec: f64) {
        if let ProgressBar::Multi(_, _, _, speed_bar) = self {
            let speed_str = Self::format_files_per_sec(files_per_sec);
            speed_bar.set_message(speed_str);
        }
    }

    pub fn inc(&self, delta: u64) {
        match self {
            ProgressBar::Bar(bar) => bar.inc(delta),
            ProgressBar::Multi(_, files_bar, _, _) => files_bar.inc(delta),
            ProgressBar::None => {}
        }
    }

    pub fn inc_data(&self, delta: u64) {
        if let ProgressBar::Multi(_, _, data_bar, _) = self {
            data_bar.inc(delta);
        }
    }

    pub fn set_position(&self, pos: u64) {
        match self {
            ProgressBar::Bar(bar) => bar.set_position(pos),
            ProgressBar::Multi(_, files_bar, _, _) => files_bar.set_position(pos),
            ProgressBar::None => {}
        }
    }

    pub fn set_data_position(&self, pos: u64) {
        if let ProgressBar::Multi(_, _, data_bar, _) = self {
            data_bar.set_position(pos);
        }
    }

    pub fn set_length(&self, len: u64) {
        match self {
            ProgressBar::Bar(bar) => bar.set_length(len),
            ProgressBar::Multi(_, files_bar, _, _) => files_bar.set_length(len),
            ProgressBar::None => {}
        }
    }

    pub fn set_data_length(&self, len: u64) {
        if let ProgressBar::Multi(_, _, data_bar, _) = self {
            data_bar.set_length(len);
        }
    }

    pub fn finish(&self) {
        match self {
            ProgressBar::Bar(bar) => bar.finish(),
            ProgressBar::Multi(multi, files_bar, data_bar, speed_bar) => {
                files_bar.finish();
                data_bar.finish();
                speed_bar.finish();
                multi.clear().ok();
            }
            ProgressBar::None => {}
        }
    }

    pub fn println(&self, msg: &str) {
        match self {
            ProgressBar::Bar(bar) => bar.println(msg),
            ProgressBar::Multi(multi, _, _, _) => {
                multi.println(msg).ok();
            }
            ProgressBar::None => println!("{}", msg),
        }
    }

    /// Format files per second in human-readable form
    fn format_files_per_sec(fps: f64) -> String {
        if fps < 0.01 {
            format!("{:.2} files/hour", fps * 3600.0)
        } else if fps < 0.1 {
            format!("{:.2} files/min", fps * 60.0)
        } else if fps < 1.0 {
            format!("{:.1} files/min", fps * 60.0)
        } else if fps < 10.0 {
            format!("{:.2} files/s", fps)
        } else if fps < 100.0 {
            format!("{:.1} files/s", fps)
        } else {
            format!("{:.0} files/s", fps)
        }
    }
}

impl Drop for ProgressBar {
    fn drop(&mut self) {
        self.finish();
    }
}

