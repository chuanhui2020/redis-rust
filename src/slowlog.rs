//! 慢查询日志模块，记录执行时间超过阈值的命令
// 慢查询日志模块

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::protocol::RespValue;

/// 单条慢查询记录
#[derive(Debug, Clone)]
pub struct SlowLogEntry {
    /// 唯一 ID
    pub id: u64,
    /// 时间戳（Unix 时间戳，秒）
    pub timestamp: u64,
    /// 执行耗时（微秒）
    pub duration_us: u64,
    /// 命令名
    pub cmd_name: String,
    /// 命令参数
    pub args: Vec<String>,
}

/// 慢查询日志管理器
#[derive(Debug, Clone)]
pub struct SlowLog {
    /// 日志队列（双端队列，保留最近的数据）
    entries: Arc<Mutex<VecDeque<SlowLogEntry>>>,
    /// 下一条日志 ID
    next_id: Arc<Mutex<u64>>,
    /// 最大保留条数
    max_len: Arc<AtomicUsize>,
    /// 慢查询阈值（微秒）
    threshold_us: u64,
}

impl SlowLog {
    /// 创建新的慢查询日志管理器
    pub fn new() -> Self {
        Self {
            entries: Arc::new(Mutex::new(VecDeque::new())),
            next_id: Arc::new(Mutex::new(1)),
            max_len: Arc::new(AtomicUsize::new(128)),
            threshold_us: 10_000,
        }
    }

    /// 设置最大保留条数
    pub fn set_max_len(&self, len: usize) {
        let mut entries = self.entries.lock().unwrap();
        while entries.len() > len {
            entries.pop_front();
        }
        drop(entries);
        self.max_len.store(len, Ordering::SeqCst);
    }

    /// 设置慢查询阈值（微秒）
    pub fn set_threshold(&mut self, us: u64) {
        self.threshold_us = us;
    }

    /// 获取当前阈值
    pub fn threshold_us(&self) -> u64 {
        self.threshold_us
    }

    /// 记录一条慢查询
    pub fn record(&self, cmd_name: &str, args: Vec<String>, duration_us: u64) {
        if duration_us < self.threshold_us {
            return;
        }

        let mut entries = self.entries.lock().unwrap();
        let mut next_id = self.next_id.lock().unwrap();

        let entry = SlowLogEntry {
            id: *next_id,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            duration_us,
            cmd_name: cmd_name.to_string(),
            args,
        };

        entries.push_back(entry);
        *next_id += 1;

        while entries.len() > self.max_len.load(Ordering::SeqCst) {
            entries.pop_front();
        }
    }

    /// 获取最近 count 条慢查询
    pub fn get(&self, count: usize) -> Vec<SlowLogEntry> {
        let entries = self.entries.lock().unwrap();
        entries.iter().rev().take(count).cloned().collect()
    }

    /// 获取慢查询数量
    pub fn len(&self) -> usize {
        let entries = self.entries.lock().unwrap();
        entries.len()
    }

    /// 检查慢查询日志是否为空
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 清空慢查询日志
    pub fn reset(&self) {
        let mut entries = self.entries.lock().unwrap();
        entries.clear();
    }

    /// 将慢查询条目转换为 RESP 值
    pub fn entry_to_resp(entry: &SlowLogEntry) -> RespValue {
        let mut parts = Vec::new();
        parts.push(RespValue::Integer(entry.id as i64));
        parts.push(RespValue::Integer(entry.timestamp as i64));
        parts.push(RespValue::Integer(entry.duration_us as i64));

        let mut cmd_parts = Vec::new();
        cmd_parts.push(RespValue::BulkString(Some(Bytes::from(entry.cmd_name.clone()))));
        for arg in &entry.args {
            cmd_parts.push(RespValue::BulkString(Some(Bytes::from(arg.clone()))));
        }
        parts.push(RespValue::Array(cmd_parts));

        RespValue::Array(parts)
    }
}

impl Default for SlowLog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slowlog_record_and_get() {
        let log = SlowLog::new();
        log.record("GET", vec!["key".to_string()], 15_000);
        log.record("SET", vec!["key".to_string(), "value".to_string()], 20_000);

        let entries = log.get(10);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].cmd_name, "SET");
        assert_eq!(entries[0].duration_us, 20_000);
        assert_eq!(entries[1].cmd_name, "GET");
    }

    #[test]
    fn test_slowlog_threshold() {
        let log = SlowLog::new();
        // 低于阈值，不应记录
        log.record("GET", vec!["key".to_string()], 5_000);
        assert_eq!(log.len(), 0);

        // 高于阈值，应记录
        log.record("SET", vec!["key".to_string(), "value".to_string()], 15_000);
        assert_eq!(log.len(), 1);
    }

    #[test]
    fn test_slowlog_reset() {
        let log = SlowLog::new();
        log.record("GET", vec!["key".to_string()], 15_000);
        assert_eq!(log.len(), 1);

        log.reset();
        assert_eq!(log.len(), 0);
    }

    #[test]
    fn test_slowlog_max_len() {
        let log = SlowLog::new();
        log.set_max_len(3);
        log.record("A", vec![], 15_000);
        log.record("B", vec![], 15_000);
        log.record("C", vec![], 15_000);
        log.record("D", vec![], 15_000);

        assert_eq!(log.len(), 3);
        let entries = log.get(10);
        assert_eq!(entries[0].cmd_name, "D");
        assert_eq!(entries[2].cmd_name, "B");
    }
}
