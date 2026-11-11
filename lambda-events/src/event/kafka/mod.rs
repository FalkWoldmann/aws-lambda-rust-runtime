use crate::{custom_serde::deserialize_nullish, encodings::MillisecondTimestamp};
use base64::Engine;
#[cfg(feature = "builders")]
use bon::Builder;
use serde::{Deserialize, Serialize};
#[cfg(feature = "catch-all-fields")]
use serde_json::Value;
use std::{collections::HashMap, error::Error, fmt};

#[non_exhaustive]
#[cfg_attr(feature = "builders", derive(Builder))]
#[derive(Debug, Default, Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaEvent {
    #[serde(default)]
    pub event_source: Option<String>,
    #[serde(default)]
    pub event_source_arn: Option<String>,
    #[serde(deserialize_with = "deserialize_nullish")]
    #[serde(default)]
    pub records: HashMap<String, Vec<KafkaRecord>>,
    #[serde(default)]
    pub bootstrap_servers: Option<String>,
    /// Catchall to catch any additional fields that were present but not explicitly defined by this struct.
    /// Enabled with Cargo feature `catch-all-fields`.
    /// If `catch-all-fields` is disabled, any additional fields that are present will be ignored.
    #[cfg(feature = "catch-all-fields")]
    #[cfg_attr(docsrs, doc(cfg(feature = "catch-all-fields")))]
    #[serde(flatten)]
    #[cfg_attr(feature = "builders", builder(default))]
    pub other: serde_json::Map<String, Value>,
}

#[non_exhaustive]
#[cfg_attr(feature = "builders", derive(Builder))]
#[derive(Debug, Default, Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaRecord {
    #[serde(default)]
    pub topic: Option<String>,
    pub partition: i64,
    pub offset: i64,
    pub timestamp: MillisecondTimestamp,
    #[serde(default)]
    pub timestamp_type: Option<String>,
    pub key: Option<String>,
    pub value: Option<String>,
    pub headers: Vec<HashMap<String, Vec<i8>>>,
    /// Catchall to catch any additional fields that were present but not explicitly defined by this struct.
    /// Enabled with Cargo feature `catch-all-fields`.
    /// If `catch-all-fields` is disabled, any additional fields that are present will be ignored.
    #[cfg(feature = "catch-all-fields")]
    #[cfg_attr(docsrs, doc(cfg(feature = "catch-all-fields")))]
    #[serde(flatten)]
    #[cfg_attr(feature = "builders", builder(default))]
    pub other: serde_json::Map<String, Value>,
}

impl KafkaRecord {
    /// Decode the base64 encoded record key.
    ///
    /// Returns `Ok(None)` when the record has no key.
    pub fn decoded_key(&self) -> Result<Option<Vec<u8>>, KafkaRecordDecodeError> {
        decode_base64(self.key.as_deref())
    }

    /// Decode the base64 encoded record value.
    ///
    /// Returns `Ok(None)` when the record has no value, as is the case for tombstones.
    pub fn decoded_value(&self) -> Result<Option<Vec<u8>>, KafkaRecordDecodeError> {
        decode_base64(self.value.as_deref())
    }
}

fn decode_base64(encoded: Option<&str>) -> Result<Option<Vec<u8>>, KafkaRecordDecodeError> {
    encoded
        .map(|encoded| base64::engine::general_purpose::STANDARD.decode(encoded))
        .transpose()
        .map_err(KafkaRecordDecodeError)
}

/// Error returned when the key or value of a [`KafkaRecord`] is not valid base64.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct KafkaRecordDecodeError(base64::DecodeError);

impl fmt::Display for KafkaRecordDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid base64 in Kafka record: {}", self.0)
    }
}

impl Error for KafkaRecordDecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    #[cfg(feature = "kafka")]
    fn example_kafka_event() {
        let data = include_bytes!("../../fixtures/example-kafka-event.json");
        let parsed: KafkaEvent = serde_json::from_slice(data).unwrap();
        let output: String = serde_json::to_string(&parsed).unwrap();
        let reparsed: KafkaEvent = serde_json::from_slice(output.as_bytes()).unwrap();
        assert_eq!(parsed, reparsed);
    }

    #[test]
    #[cfg(feature = "kafka")]
    fn decodes_key_and_value() {
        let data = include_bytes!("../../fixtures/example-kafka-event.json");
        let parsed: KafkaEvent = serde_json::from_slice(data).unwrap();
        let record = &parsed.records["AWSKafkaTopic-0"][0];

        let expected = b"8d5596b4-1813-4238-b24b-6dad8e3d1c0c".to_vec();
        assert_eq!(record.decoded_key().unwrap(), Some(expected.clone()));
        assert_eq!(record.decoded_value().unwrap(), Some(expected));
    }

    #[test]
    #[cfg(feature = "kafka")]
    fn missing_key_and_value_decode_to_none() {
        let record = KafkaRecord::default();

        assert_eq!(record.decoded_key().unwrap(), None);
        assert_eq!(record.decoded_value().unwrap(), None);
    }

    #[test]
    #[cfg(feature = "kafka")]
    fn invalid_base64_is_an_error() {
        let record = KafkaRecord {
            key: Some("not base64!".to_string()),
            value: Some("not base64!".to_string()),
            ..Default::default()
        };

        assert!(record.decoded_key().is_err());
        assert!(record.decoded_value().is_err());
    }
}
