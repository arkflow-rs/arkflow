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

//! 增强状态管理和 S3 后端的集成测试

#[cfg(test)]
mod tests {
    use crate::state::{
        enhanced::{EnhancedStateConfig, EnhancedStateManager, StateBackendType},
        helper::{SimpleMemoryState, StateHelper},
    };

    #[tokio::test]
    async fn test_enhanced_state_manager_memory() {
        let config = EnhancedStateConfig {
            enabled: true,
            backend_type: StateBackendType::Memory,
            s3_config: None,
            checkpoint_interval_ms: 1000,
            retained_checkpoints: 3,
            exactly_once: false,
            state_timeout_ms: 60000,
        };

        let mut state_manager = EnhancedStateManager::new(config).await.unwrap();

        // 测试基本状态操作
        state_manager
            .set_state_value("test_op", &"counter", 42u64)
            .await
            .unwrap();
        let counter: Option<u64> = state_manager
            .get_state_value("test_op", &"counter")
            .await
            .unwrap();
        assert_eq!(counter, Some(42));

        // 测试检查点创建
        let checkpoint_id = state_manager.create_checkpoint().await.unwrap();
        assert!(checkpoint_id > 0);

        // 测试状态统计
        let stats = state_manager.get_state_stats().await;
        assert!(stats.enabled);
        assert_eq!(stats.backend_type, StateBackendType::Memory);
    }

    #[test]
    fn test_simple_memory_state() {
        let mut state = SimpleMemoryState::new();

        // 测试 put 和 get 操作
        state
            .put_typed("test_key", "test_value".to_string())
            .unwrap();
        let value: Option<String> = state.get_typed("test_key").unwrap();
        assert_eq!(value, Some("test_value".to_string()));

        // 测试数字
        state.put_typed("number", 123u64).unwrap();
        let number: Option<u64> = state.get_typed("number").unwrap();
        assert_eq!(number, Some(123));
    }
}
