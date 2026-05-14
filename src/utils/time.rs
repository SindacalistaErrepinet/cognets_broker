use std::time::Duration;

use chrono::{DateTime, Utc};

/// Returns current UTC timestamp in RFC3339 format.
pub fn now_timestamp() -> String {
    Utc::now().to_rfc3339()
}

/// Returns current UTC timestamp as nanoseconds since epoch.
pub fn now_timestamp_nanos() -> i64 {
    Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| Utc::now().timestamp_micros() * 1000)
}

/// Parses RFC3339 timestamp into nanoseconds since epoch.
pub fn timestamp_to_nanos(value: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(value).ok().map(|timestamp| {
        timestamp
            .timestamp_nanos_opt()
            .unwrap_or_else(|| timestamp.timestamp_micros() * 1000)
    })
}

/// Formats runtime duration as an ISO 8601 duration string.
pub fn duration_to_iso8601(duration: Duration) -> String {
    let mut seconds = duration.as_secs();
    let days = seconds / 86_400;
    seconds %= 86_400;
    let hours = seconds / 3_600;
    seconds %= 3_600;
    let minutes = seconds / 60;
    seconds %= 60;

    let mut formatted = String::from("P");
    if days > 0 {
        formatted.push_str(&format!("{days}D"));
    }

    if hours > 0 || minutes > 0 || seconds > 0 || days == 0 {
        formatted.push('T');
        if hours > 0 {
            formatted.push_str(&format!("{hours}H"));
        }
        if minutes > 0 {
            formatted.push_str(&format!("{minutes}M"));
        }
        if seconds > 0 || (hours == 0 && minutes == 0) {
            formatted.push_str(&format!("{seconds}S"));
        }
    }

    formatted
}
