//! 内存使用统计模块（MEMORY USAGE/MEMORY DOCTOR）

use std::borrow::Cow;
use crate::error::{AppError, Result};
use crate::storage::StorageEngine;

impl StorageEngine {
    /// 估算所有数据库的内存使用量（字节）
    pub fn memory_usage(&self) -> Result<usize> {
        let mut total = 0usize;
        for db in self.dbs.iter() {
            total += db.inner.memory_usage();
        }
        Ok(total)
    }

    /// 估算单个 key 的内存使用量（字节）
    pub fn memory_key_usage(&self, key: &str, _samples: Option<usize>) -> Result<Option<usize>> {
        let db = self.db();
        let map = db
            .inner
            .get_shard(key)
            .read()
            .map_err(|e| AppError::Storage(Cow::Owned(format!("锁中毒: {}", e))))?;
        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    return Ok(None);
                }
                let size = crate::storage::estimate_entry_size(key, v);
                Ok(Some(size))
            }
            None => Ok(None),
        }
    }

    /// 返回内存诊断信息
    pub fn memory_doctor(&self) -> Result<String> {
        let total_keys = self.dbsize()?;
        let total_memory = self.memory_usage()?;
        let policy = self.get_eviction_policy();
        let policy_str = format!("{:?}", policy);
        let maxmemory = self.get_maxmemory();

        let mut lines = Vec::new();
        lines.push("redis-rust 内存诊断报告".to_string());
        lines.push("========================".to_string());
        lines.push(format!("总 key 数量: {}", total_keys));
        lines.push(format!(
            "估算内存使用: {} bytes ({:.2} MB)",
            total_memory,
            total_memory as f64 / 1024.0 / 1024.0
        ));
        lines.push(format!("内存上限: {} bytes", maxmemory));
        lines.push(format!("淘汰策略: {}", policy_str));

        if total_keys == 0 {
            lines.push("建议: 数据库为空，可以开始导入数据".to_string());
        } else if total_memory as f64 > maxmemory as f64 * 0.8 {
            lines.push("警告: 内存使用超过上限的 80%，考虑增加内存或调整淘汰策略".to_string());
        } else {
            lines.push("状态: 内存使用正常".to_string());
        }

        Ok(lines.join("\n"))
    }


}
