// 延迟追踪模块
// 记录各种事件的延迟，用于 LATENCY 命令

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use crate::error::{AppError, Result};

/// 单条延迟记录
#[derive(Debug, Clone)]
pub struct LatencyEntry {
    /// 时间戳（Unix 时间戳，秒）
    pub timestamp: u64,
    /// 延迟（毫秒）
    pub latency_ms: u64,
}

/// 延迟追踪器
#[derive(Debug, Clone)]
pub struct LatencyTracker {
    inner: Arc<Mutex<LatencyTrackerInner>>,
}

#[derive(Debug)]
struct LatencyTrackerInner {
    /// 事件名 → 延迟记录队列
    events: HashMap<String, VecDeque<LatencyEntry>>,
    /// 每个事件最大保留条数
    max_len: usize,
}

impl LatencyTracker {
    /// 创建新的延迟追踪器
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(LatencyTrackerInner {
                events: HashMap::new(),
                max_len: 160,
            })),
        }
    }

    /// 记录一条延迟事件
    pub fn record(&self, event_name: &str, latency_ms: u64) -> Result<()> {
        let mut inner = self.inner.lock().map_err(|e| {
            AppError::Storage(format!("延迟追踪器锁中毒: {}", e))
        })?;
        let entry = LatencyEntry {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            latency_ms,
        };
        let max_len = inner.max_len;
        let queue = inner.events.entry(event_name.to_string()).or_insert_with(VecDeque::new);
        queue.push_back(entry);
        while queue.len() > max_len {
            queue.pop_front();
        }
        Ok(())
    }

    /// 获取所有事件的最新延迟记录
    /// 返回 (event_name, latest_latency_ms, timestamp, all_time_max_ms)
    pub fn latest(&self) -> Result<Vec<(String, u64, u64, u64)>> {
        let inner = self.inner.lock().map_err(|e| {
            AppError::Storage(format!("延迟追踪器锁中毒: {}", e))
        })?;
        let mut result = Vec::new();
        for (name, queue) in inner.events.iter() {
            if let Some(entry) = queue.back() {
                let all_time_max = queue.iter().map(|e| e.latency_ms).max().unwrap_or(0);
                result.push((name.clone(), entry.latency_ms, entry.timestamp, all_time_max));
            }
        }
        Ok(result)
    }

    /// 获取指定事件的历史延迟记录
    pub fn history(&self, event_name: &str) -> Result<Vec<(u64, u64)>> {
        let inner = self.inner.lock().map_err(|e| {
            AppError::Storage(format!("延迟追踪器锁中毒: {}", e))
        })?;
        Ok(inner
            .events
            .get(event_name)
            .map(|q| q.iter().map(|e| (e.timestamp, e.latency_ms)).collect())
            .unwrap_or_default())
    }

    /// 重置指定事件的延迟记录
    pub fn reset(&self, event_names: &[&str]) -> Result<usize> {
        let mut inner = self.inner.lock().map_err(|e| {
            AppError::Storage(format!("延迟追踪器锁中毒: {}", e))
        })?;
        let mut count = 0;
        for name in event_names {
            if let Some(queue) = inner.events.get_mut(*name) {
                count += queue.len();
                queue.clear();
            }
        }
        Ok(count)
    }

    /// 重置所有延迟记录
    pub fn reset_all(&self) -> Result<usize> {
        let mut inner = self.inner.lock().map_err(|e| {
            AppError::Storage(format!("延迟追踪器锁中毒: {}", e))
        })?;
        let count: usize = inner.events.values().map(|q| q.len()).sum();
        inner.events.clear();
        Ok(count)
    }
}

impl Default for LatencyTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latency_record_and_latest() {
        let tracker = LatencyTracker::new();
        tracker.record("command", 10).unwrap();
        tracker.record("command", 20).unwrap();
        tracker.record("aof-write", 5).unwrap();

        let latest = tracker.latest().unwrap();
        assert_eq!(latest.len(), 2);

        let cmd = latest.iter().find(|(n, _, _, _)| n == "command").unwrap();
        assert_eq!(cmd.1, 20); // latest latency
        assert_eq!(cmd.3, 20); // all-time max
    }

    #[test]
    fn test_latency_history() {
        let tracker = LatencyTracker::new();
        tracker.record("command", 10).unwrap();
        tracker.record("command", 20).unwrap();

        let history = tracker.history("command").unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].1, 10);
        assert_eq!(history[1].1, 20);
    }

    #[test]
    fn test_latency_reset() {
        let tracker = LatencyTracker::new();
        tracker.record("command", 10).unwrap();
        tracker.record("command", 20).unwrap();
        tracker.record("aof-write", 5).unwrap();

        let count = tracker.reset(&["command"]).unwrap();
        assert_eq!(count, 2);

        let latest = tracker.latest().unwrap();
        assert_eq!(latest.len(), 1);
        assert_eq!(latest[0].0, "aof-write");
    }

    #[test]
    fn test_latency_reset_all() {
        let tracker = LatencyTracker::new();
        tracker.record("command", 10).unwrap();
        tracker.record("aof-write", 5).unwrap();

        let count = tracker.reset_all().unwrap();
        assert_eq!(count, 2);
        assert_eq!(tracker.latest().unwrap().len(), 0);
    }

    #[test]
    fn test_latency_max_len() {
        let tracker = LatencyTracker::new();
        for i in 0..200 {
            tracker.record("command", i).unwrap();
        }
        let history = tracker.history("command").unwrap();
        assert_eq!(history.len(), 160);
    }
}
