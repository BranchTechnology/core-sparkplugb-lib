use std::fmt;

#[derive(Debug, Clone)]
pub struct SparkplugError {
    details: String,
}

impl SparkplugError {
    pub(crate) fn new(msg: &str) -> SparkplugError {
        SparkplugError { details: msg.to_string() }
    }
}

impl fmt::Display for SparkplugError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "failed to parse metadata: {}", self.details)
    }
}
