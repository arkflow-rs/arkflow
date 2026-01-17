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

//! Integration tests for codec input functionality

use arkflow_core::config::EngineConfig;

#[test]
fn test_parse_input_config_with_json_codec() {
    let yaml = r#"
logging:
  level: info

streams:
  - input:
      type: generate
      context: '{"test": "data"}'
      interval: 1s
      codec:
        type: json
    pipeline:
      thread_num: 1
      processors: []
    output:
      type: stdout
"#;

    let config = EngineConfig::from_yaml_str(yaml);
    assert!(config.is_ok());

    let config = config.unwrap();
    assert_eq!(config.streams.len(), 1);

    let stream_config = &config.streams[0];
    assert!(stream_config.input.codec.is_some());

    let codec_config = stream_config.input.codec.as_ref().unwrap();
    assert_eq!(codec_config.codec_type, "json");
}

#[test]
fn test_parse_input_config_without_codec() {
    let yaml = r#"
logging:
  level: info

streams:
  - input:
      type: generate
      context: '{"test": "data"}'
      interval: 1s
    pipeline:
      thread_num: 1
      processors: []
    output:
      type: stdout
"#;

    let config = EngineConfig::from_yaml_str(yaml);
    assert!(config.is_ok());

    let config = config.unwrap();
    assert_eq!(config.streams.len(), 1);

    let stream_config = &config.streams[0];
    assert!(stream_config.input.codec.is_none());
}

#[test]
fn test_parse_input_config_with_protobuf_codec() {
    let yaml = r#"
logging:
  level: info

streams:
  - input:
      type: kafka
      brokers:
        - localhost:9092
      topics:
        - test-topic
      consumer_group: test-group
      codec:
        type: protobuf
    pipeline:
      thread_num: 1
      processors: []
    output:
      type: stdout
"#;

    let config = EngineConfig::from_yaml_str(yaml);
    assert!(config.is_ok());

    let config = config.unwrap();
    assert_eq!(config.streams.len(), 1);

    let stream_config = &config.streams[0];
    assert!(stream_config.input.codec.is_some());

    let codec_config = stream_config.input.codec.as_ref().unwrap();
    assert_eq!(codec_config.codec_type, "protobuf");
}

// Helper extension trait to parse from YAML string
trait EngineConfigExt {
    fn from_yaml_str(yaml: &str) -> Result<Self, Box<dyn std::error::Error>>
    where
        Self: Sized;
}

impl EngineConfigExt for EngineConfig {
    fn from_yaml_str(yaml: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(serde_yaml::from_str(yaml)?)
    }
}
