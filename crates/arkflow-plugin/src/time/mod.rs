/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

use serde::de::Unexpected;
use serde::{de, Deserialize, Deserializer, Serialize};
use std::time::Duration;

pub fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    humantime::parse_duration(&s).map_err(|_| {
        de::Error::invalid_value(Unexpected::Str(&s), &"a duration like '10ms' or '1s'")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[derive(Debug, Deserialize, Serialize)]
    struct TestConfig {
        #[serde(deserialize_with = "deserialize_duration")]
        duration: Duration,
    }

    #[test]
    fn test_deserialize_duration_seconds() {
        let json = r#"{"duration": "5s"}"#;
        let config: TestConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.duration, Duration::from_secs(5));
    }

    #[test]
    fn test_deserialize_duration_milliseconds() {
        let json = r#"{"duration": "500ms"}"#;
        let config: TestConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.duration, Duration::from_millis(500));
    }

    #[test]
    fn test_deserialize_duration_microseconds() {
        let json = r#"{"duration": "100us"}"#;
        let config: TestConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.duration, Duration::from_micros(100));
    }

    #[test]
    fn test_deserialize_duration_nanoseconds() {
        let json = r#"{"duration": "50ns"}"#;
        let config: TestConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.duration, Duration::from_nanos(50));
    }

    #[test]
    fn test_deserialize_duration_minutes() {
        let json = r#"{"duration": "3m"}"#;
        let config: TestConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.duration, Duration::from_secs(180));
    }

    #[test]
    fn test_deserialize_duration_hours() {
        let json = r#"{"duration": "2h"}"#;
        let config: TestConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.duration, Duration::from_secs(7200));
    }

    #[test]
    fn test_deserialize_duration_complex() {
        let json = r#"{"duration": "1h30m"}"#;
        let config: TestConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.duration, Duration::from_secs(5400));
    }

    #[test]
    fn test_deserialize_duration_invalid() {
        let json = r#"{"duration": "invalid"}"#;
        let result: Result<TestConfig, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_duration_empty() {
        let json = r#"{"duration": ""}"#;
        let result: Result<TestConfig, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_duration_equality() {
        let d1 = Duration::from_secs(5);
        let d2 = Duration::from_secs(5);
        assert_eq!(d1, d2);
    }

    #[test]
    fn test_duration_comparison() {
        let d1 = Duration::from_secs(1);
        let d2 = Duration::from_secs(2);
        assert!(d1 < d2);
    }

    #[test]
    fn test_duration_arithmetic() {
        let d1 = Duration::from_millis(500);
        let d2 = Duration::from_millis(500);
        assert_eq!(d1 + d2, Duration::from_secs(1));
    }
}
