//! Retry utilities for handling transient errors

use std::thread;
use std::time::Duration;
use log::{warn, error};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RetryableError {
    #[error("Transient error: {0}")]
    Transient(String),

    #[error("Permanent error: {0}")]
    Permanent(String),
}

impl RetryableError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, RetryableError::Transient(_))
    }
}

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: i32,
    pub delay_seconds: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 0,
            delay_seconds: 5,
        }
    }
}

/// Execute an operation with retry logic
pub fn with_retry<F, T, E>(
    mut operation: F,
    max_retries: i32,
    delay_seconds: u64,
    context: &str,
) -> Result<T, E>
where
    F: FnMut() -> Result<T, E>,
    E: std::fmt::Display,
{
    let mut attempts = 0;
    
    loop {
        match operation() {
            Ok(result) => return Ok(result),
            Err(e) => {
                attempts += 1;
                
                // Check if we should abort
                if max_retries == 0 {
                    return Err(e);
                }
                
                if max_retries > 0 && attempts > max_retries {
                    error!("{} failed after {} attempts: {}", context, attempts - 1, e);
                    return Err(e);
                }
                
                // Log and wait
                if max_retries == -1 {
                    warn!("{} failed (attempt {}, will retry indefinitely): {}", 
                          context, attempts, e);
                } else {
                    warn!("{} failed (attempt {}/{}, retrying in {}s): {}", 
                          context, attempts, max_retries, delay_seconds, e);
                }
                
                thread::sleep(Duration::from_secs(delay_seconds));
            }
        }
    }
}

/// Check if an error is likely transient and should be retried
pub fn is_transient_error(error: &anyhow::Error) -> bool {
    let error_string = error.to_string().to_lowercase();
    
    // Common transient error patterns
    error_string.contains("lock") ||
    error_string.contains("temporarily") ||
    error_string.contains("timeout") ||
    error_string.contains("connection") ||
    error_string.contains("resource temporarily") ||
    error_string.contains("busy") ||
    error_string.contains("interrupted") ||
    error_string.contains("again") ||
    error_string.contains("would block")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_with_retry_success() {
        let mut counter = 0;
        let result = with_retry(
            || {
                counter += 1;
                if counter < 3 {
                    Err::<(), _>("Temporary error")
                } else {
                    Ok(())
                }
            },
            3,
            1,
            "test",
        );
        
        assert!(result.is_ok());
    }

    #[test]
    fn test_with_retry_failure() {
        let mut counter = 0;
        let result = with_retry(
            || {
                counter += 1;
                Err::<(), _>("Permanent error")
            },
            3,
            1,
            "test",
        );
        
        assert!(result.is_err());
        assert_eq!(counter, 4); // Initial + 3 retries
    }

    #[test]
    fn test_is_transient_error() {
        let err = anyhow::anyhow!("Resource temporarily unavailable");
        assert!(is_transient_error(&err));
        
        let err = anyhow::anyhow!("Permission denied");
        assert!(!is_transient_error(&err));
    }
}

