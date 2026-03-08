//! Utility modules

pub mod progress;
pub mod platform;
pub mod config;
pub mod logging;
pub mod retry;

use anyhow::Result;

/// Parse date string to timestamp
pub fn parse_date(date_str: &str) -> Result<u64> {
    // Try various formats
    let formats = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
    ];
    
    for format in &formats {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(date_str, format) {
            return Ok(dt.and_utc().timestamp() as u64);
        }
        if let Ok(d) = chrono::NaiveDate::parse_from_str(date_str, format) {
            if let Some(dt) = d.and_hms_opt(0, 0, 0) {
                return Ok(dt.and_utc().timestamp() as u64);
            }
        }
    }
    
    Err(anyhow::anyhow!("Unable to parse date: {}", date_str))
}

/// Parse duration string (e.g., "1d", "12h", "30m") to seconds
pub fn parse_duration(duration_str: &str) -> Result<u64> {
    let duration_str = duration_str.trim().to_lowercase();
    let len = duration_str.len();
    
    if len < 2 {
        return Err(anyhow::anyhow!("Invalid duration format: {}", duration_str));
    }
    
    let (num_str, unit) = duration_str.split_at(len - 1);
    let num: u64 = num_str.parse()?;
    
    match unit {
        "s" => Ok(num),
        "m" => Ok(num * 60),
        "h" => Ok(num * 60 * 60),
        "d" => Ok(num * 60 * 60 * 24),
        _ => Err(anyhow::anyhow!("Unknown duration unit: {}. Use s, m, h, d", unit)),
    }
}

/// Format size in human-readable form
pub fn format_size(size: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut size = size as f64;
    let mut unit = 0;
    
    while size >= 1024.0 && unit < 4 {
        size /= 1024.0;
        unit += 1;
    }
    
    format!("{:.2} {}", size, UNITS[unit])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("30s").unwrap(), 30);
        assert_eq!(parse_duration("5m").unwrap(), 300);
        assert_eq!(parse_duration("2h").unwrap(), 7200);
        assert_eq!(parse_duration("1d").unwrap(), 86400);
        
        assert!(parse_duration("10x").is_err());
        assert!(parse_duration("abc").is_err());
    }
}

