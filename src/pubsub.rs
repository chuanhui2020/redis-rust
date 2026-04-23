// 发布订阅模块，基于 tokio broadcast channel 实现

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use tokio::sync::broadcast;

use crate::error::Result;
use crate::storage::StorageEngine;

/// 发布订阅管理器，管理频道和模式订阅
#[derive(Debug, Clone)]
pub struct PubSubManager {
    /// 内部状态，用 Arc + RwLock 保证线程安全
    inner: Arc<RwLock<PubSubInner>>,
}

#[derive(Debug)]
struct PubSubInner {
    /// 精确频道: 频道名 -> broadcast sender
    channels: HashMap<String, broadcast::Sender<Bytes>>,
    /// 模式订阅: 模式 -> broadcast sender，消息为 (频道名, 内容)
    patterns: HashMap<String, broadcast::Sender<(String, Bytes)>>,
    /// 分片频道: 频道名 -> broadcast sender
    shard_channels: HashMap<String, broadcast::Sender<Bytes>>,
}

impl PubSubManager {
    /// 创建新的 PubSub 管理器
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(PubSubInner {
                channels: HashMap::new(),
                patterns: HashMap::new(),
                shard_channels: HashMap::new(),
            })),
        }
    }

    /// 订阅指定频道，返回 receiver 用于接收消息
    pub fn subscribe(&self, channel: &str) -> broadcast::Receiver<Bytes> {
        let mut inner = self.inner.write().unwrap();
        let sender = inner
            .channels
            .entry(channel.to_string())
            .or_insert_with(|| broadcast::channel(256).0);
        sender.subscribe()
    }

    /// 模式订阅，返回 receiver 用于接收匹配的消息
    /// 每条消息为 (频道名, 内容)
    pub fn psubscribe(&self, pattern: &str) -> broadcast::Receiver<(String, Bytes)> {
        let mut inner = self.inner.write().unwrap();
        let sender = inner
            .patterns
            .entry(pattern.to_string())
            .or_insert_with(|| broadcast::channel(256).0);
        sender.subscribe()
    }

    /// 取消精确订阅（清理无活跃接收者的频道）
    pub fn unsubscribe(&self, channel: &str) {
        let mut inner = self.inner.write().unwrap();
        if let Some(sender) = inner.channels.get(channel)
            && sender.receiver_count() == 0 {
                inner.channels.remove(channel);
            }
    }

    /// 取消模式订阅（清理无活跃接收者的模式）
    pub fn punsubscribe(&self, pattern: &str) {
        let mut inner = self.inner.write().unwrap();
        if let Some(sender) = inner.patterns.get(pattern)
            && sender.receiver_count() == 0 {
                inner.patterns.remove(pattern);
            }
    }

    /// 获取活跃的频道名称，可选按 glob 模式过滤
    pub fn channels(&self, pattern: Option<&str>) -> Vec<String> {
        let inner = self.inner.read().unwrap();
        inner
            .channels
            .keys()
            .filter(|ch| {
                if let Some(pat) = pattern {
                    crate::storage::StorageEngine::glob_match(ch, pat)
                } else {
                    true
                }
            })
            .cloned()
            .collect()
    }

    /// 获取指定频道的订阅者数量
    pub fn numsub(&self, channels: &[String]) -> Vec<(String, usize)> {
        let inner = self.inner.read().unwrap();
        channels
            .iter()
            .map(|ch| {
                let count = inner
                    .channels
                    .get(ch.as_str())
                    .map(|sender| sender.receiver_count())
                    .unwrap_or(0);
                (ch.clone(), count)
            })
            .collect()
    }

    /// 获取活跃的模式订阅数量
    pub fn numpat(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.patterns.len()
    }

    /// 订阅分片频道，返回 receiver 用于接收消息
    pub fn ssubscribe(&self, channel: &str) -> broadcast::Receiver<Bytes> {
        let mut inner = self.inner.write().unwrap();
        let sender = inner
            .shard_channels
            .entry(channel.to_string())
            .or_insert_with(|| broadcast::channel(256).0);
        sender.subscribe()
    }

    /// 取消分片订阅（清理无活跃接收者的频道）
    pub fn sunsubscribe(&self, channel: &str) {
        let mut inner = self.inner.write().unwrap();
        if let Some(sender) = inner.shard_channels.get(channel)
            && sender.receiver_count() == 0 {
                inner.shard_channels.remove(channel);
            }
    }

    /// 向分片频道发布消息，返回收到消息的订阅者数量（无模式匹配）
    pub fn spublish(&self, channel: &str, message: Bytes) -> Result<usize> {
        let mut total = 0usize;

        {
            let inner = self.inner.read().unwrap();
            if let Some(sender) = inner.shard_channels.get(channel)
                && let Ok(n) = sender.send(message.clone()) { total += n }
        }

        // 清理无活跃接收者的空分片频道
        {
            let mut inner = self.inner.write().unwrap();
            let empty_channels: Vec<String> = inner
                .shard_channels
                .iter()
                .filter(|(_, s)| s.receiver_count() == 0)
                .map(|(k, _)| k.clone())
                .collect();
            for ch in empty_channels {
                inner.shard_channels.remove(&ch);
            }
        }

        Ok(total)
    }

    /// 获取活跃的分片频道名称，可选按 glob 模式过滤
    pub fn shard_channels(&self, pattern: Option<&str>) -> Vec<String> {
        let inner = self.inner.read().unwrap();
        inner
            .shard_channels
            .keys()
            .filter(|ch| {
                if let Some(pat) = pattern {
                    crate::storage::StorageEngine::glob_match(ch, pat)
                } else {
                    true
                }
            })
            .cloned()
            .collect()
    }

    /// 获取指定分片频道的订阅者数量
    pub fn shard_numsub(&self, channels: &[String]) -> Vec<(String, usize)> {
        let inner = self.inner.read().unwrap();
        channels
            .iter()
            .map(|ch| {
                let count = inner
                    .shard_channels
                    .get(ch.as_str())
                    .map(|sender| sender.receiver_count())
                    .unwrap_or(0);
                (ch.clone(), count)
            })
            .collect()
    }

    /// 向频道发布消息，返回收到消息的订阅者数量
    pub fn publish(&self, channel: &str, message: Bytes) -> Result<usize> {
        let mut total = 0usize;

        // 精确订阅者
        {
            let inner = self.inner.read().unwrap();
            if let Some(sender) = inner.channels.get(channel) {
                match sender.send(message.clone()) {
                    Ok(n) => total += n,
                    Err(_) => {
                        // 无活跃接收者，稍后清理
                    }
                }
            }
        }

        // 模式订阅者
        {
            let inner = self.inner.read().unwrap();
            for (pattern, sender) in inner.patterns.iter() {
                if StorageEngine::glob_match(channel, pattern)
                    && let Ok(n) = sender.send((channel.to_string(), message.clone())) { total += n }
            }
        }

        // 清理无活跃接收者的空频道
        {
            let mut inner = self.inner.write().unwrap();
            let empty_channels: Vec<String> = inner
                .channels
                .iter()
                .filter(|(_, s)| s.receiver_count() == 0)
                .map(|(k, _)| k.clone())
                .collect();
            for ch in empty_channels {
                inner.channels.remove(&ch);
            }
            let empty_patterns: Vec<String> = inner
                .patterns
                .iter()
                .filter(|(_, s)| s.receiver_count() == 0)
                .map(|(k, _)| k.clone())
                .collect();
            for pat in empty_patterns {
                inner.patterns.remove(&pat);
            }
        }

        Ok(total)
    }
}

impl Default for PubSubManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_publish_no_subscribers() {
        let pubsub = PubSubManager::new();
        let count = pubsub.publish("ch1", Bytes::from("hello")).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_subscribe_and_publish() {
        let pubsub = PubSubManager::new();
        let mut rx = pubsub.subscribe("ch1");

        let count = pubsub.publish("ch1", Bytes::from("hello")).unwrap();
        assert_eq!(count, 1);

        let msg = rx.try_recv().unwrap();
        assert_eq!(msg, Bytes::from("hello"));
    }

    #[test]
    fn test_multiple_subscribers() {
        let pubsub = PubSubManager::new();
        let mut rx1 = pubsub.subscribe("ch1");
        let mut rx2 = pubsub.subscribe("ch1");

        let count = pubsub.publish("ch1", Bytes::from("hello")).unwrap();
        assert_eq!(count, 2);

        assert_eq!(rx1.try_recv().unwrap(), Bytes::from("hello"));
        assert_eq!(rx2.try_recv().unwrap(), Bytes::from("hello"));
    }

    #[test]
    fn test_unsubscribe() {
        let pubsub = PubSubManager::new();
        {
            let _rx = pubsub.subscribe("ch1");
        }
        // rx 已 drop，receiver_count 应为 0
        pubsub.unsubscribe("ch1");

        let count = pubsub.publish("ch1", Bytes::from("hello")).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_psubscribe_and_publish() {
        let pubsub = PubSubManager::new();
        let mut rx = pubsub.psubscribe("news.*");

        let count = pubsub.publish("news.sport", Bytes::from("goal")).unwrap();
        assert_eq!(count, 1);

        let (ch, msg) = rx.try_recv().unwrap();
        assert_eq!(ch, "news.sport");
        assert_eq!(msg, Bytes::from("goal"));
    }

    #[test]
    fn test_psubscribe_no_match() {
        let pubsub = PubSubManager::new();
        let mut rx = pubsub.psubscribe("news.*");

        let count = pubsub.publish("weather.today", Bytes::from("sunny")).unwrap();
        assert_eq!(count, 0);

        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_punsubscribe() {
        let pubsub = PubSubManager::new();
        {
            let _rx = pubsub.psubscribe("test.*");
        }
        pubsub.punsubscribe("test.*");

        let count = pubsub.publish("test.a", Bytes::from("x")).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_channels() {
        let pubsub = PubSubManager::new();
        let _rx1 = pubsub.subscribe("news");
        let _rx2 = pubsub.subscribe("weather");
        let _rx3 = pubsub.subscribe("news.sport");

        let all = pubsub.channels(None);
        assert_eq!(all.len(), 3);
        assert!(all.contains(&"news".to_string()));
        assert!(all.contains(&"weather".to_string()));
        assert!(all.contains(&"news.sport".to_string()));

        let filtered = pubsub.channels(Some("news*"));
        assert_eq!(filtered.len(), 2);
        assert!(filtered.contains(&"news".to_string()));
        assert!(filtered.contains(&"news.sport".to_string()));
    }

    #[test]
    fn test_numsub() {
        let pubsub = PubSubManager::new();
        let _rx1 = pubsub.subscribe("news");
        let _rx2 = pubsub.subscribe("news");

        let result = pubsub.numsub(&["news".to_string(), "weather".to_string()]);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ("news".to_string(), 2));
        assert_eq!(result[1], ("weather".to_string(), 0));
    }

    #[test]
    fn test_numpat() {
        let pubsub = PubSubManager::new();
        assert_eq!(pubsub.numpat(), 0);

        let _rx1 = pubsub.psubscribe("news.*");
        assert_eq!(pubsub.numpat(), 1);

        let _rx2 = pubsub.psubscribe("weather.*");
        assert_eq!(pubsub.numpat(), 2);
    }

    #[test]
    fn test_ssubscribe_and_spublish() {
        let pubsub = PubSubManager::new();
        let mut rx = pubsub.ssubscribe("sh1");

        let count = pubsub.spublish("sh1", Bytes::from("hello")).unwrap();
        assert_eq!(count, 1);

        let msg = rx.try_recv().unwrap();
        assert_eq!(msg, Bytes::from("hello"));
    }

    #[test]
    fn test_spublish_no_pattern_match() {
        let pubsub = PubSubManager::new();
        // 分片发布不应匹配模式订阅
        let mut rx = pubsub.psubscribe("sh.*");
        let count = pubsub.spublish("sh.1", Bytes::from("hello")).unwrap();
        assert_eq!(count, 0);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_sunsubscribe() {
        let pubsub = PubSubManager::new();
        {
            let _rx = pubsub.ssubscribe("sh1");
        }
        pubsub.sunsubscribe("sh1");

        let count = pubsub.spublish("sh1", Bytes::from("hello")).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_shard_channels() {
        let pubsub = PubSubManager::new();
        let _rx1 = pubsub.ssubscribe("news");
        let _rx2 = pubsub.ssubscribe("weather");
        let _rx3 = pubsub.ssubscribe("news.sport");

        let all = pubsub.shard_channels(None);
        assert_eq!(all.len(), 3);

        let filtered = pubsub.shard_channels(Some("news*"));
        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn test_shard_numsub() {
        let pubsub = PubSubManager::new();
        let _rx1 = pubsub.ssubscribe("news");
        let _rx2 = pubsub.ssubscribe("news");

        let result = pubsub.shard_numsub(&["news".to_string(), "weather".to_string()]);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ("news".to_string(), 2));
        assert_eq!(result[1], ("weather".to_string(), 0));
    }
}
