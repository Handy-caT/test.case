use chrono::Utc;

pub fn utc_timestamp_nanoseconds() -> u64 {
    Utc::now()
        .timestamp_nanos_opt()
        .and_then(|timestamp| u64::try_from(timestamp).ok())
        .unwrap_or_default()
}
