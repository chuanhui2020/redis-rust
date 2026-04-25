//! 内存使用统计模块（MEMORY USAGE/MEMORY DOCTOR）

use bytes::Bytes;
use ordered_float::OrderedFloat;
use crate::error::{AppError, Result};
use crate::storage::{StorageEngine, StorageValue, StreamId};

impl StorageEngine {
    /// 估算所有数据库的内存使用量（字节）
    pub fn memory_usage(&self) -> Result<usize> {
        let mut total = 0usize;
        const ENTRY_OVERHEAD: usize = 64;
        for db in self.dbs.iter() {
            for shard in db.inner.all_shards() {
                let map = shard.read().map_err(|e| {
                    AppError::Storage(format!("锁中毒: {}", e))
                })?;
                for (key, value) in map.iter() {
                    total += ENTRY_OVERHEAD;
                    total += key.len();
                    total += Self::estimate_value_size(value);
                }
            }
        }
        Ok(total)
    }

    /// 估算单个 key 的内存使用量（字节）
    pub fn memory_key_usage(&self, key: &str, _samples: Option<usize>) -> Result<Option<usize>> {
        let db = self.db();
        let map = db.inner.get_shard(key).read().map_err(|e| {
            AppError::Storage(format!("锁中毒: {}", e))
        })?;
        match map.get(key) {
            Some(v) => {
                if Self::is_key_expired(&db, key) {
                    return Ok(None);
                }
                let size = 64 + key.len() + Self::estimate_value_size(v);
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
        lines.push(format!("估算内存使用: {} bytes ({:.2} MB)", total_memory, total_memory as f64 / 1024.0 / 1024.0));
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

    /// 估算单个 StorageValue 的内存大小
    fn estimate_value_size(value: &StorageValue) -> usize {
        match value {
            StorageValue::String(b) => b.len(),
            StorageValue::List(list) => {
                list.iter().map(|b| b.len()).sum::<usize>() + list.len() * std::mem::size_of::<Bytes>()
            }
            StorageValue::Hash(hash) => {
                hash.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() + hash.len() * std::mem::size_of::<(String, Bytes)>()
            }
            StorageValue::Set(set) => {
                set.iter().map(|b| b.len()).sum::<usize>() + set.len() * std::mem::size_of::<Bytes>()
            }
            StorageValue::ZSet(zset) => {
                zset.member_to_score.keys().map(|m| m.len()).sum::<usize>()
                    + zset.member_to_score.len() * std::mem::size_of::<(String, f64)>()
                    + zset.score_to_member.len() * std::mem::size_of::<((OrderedFloat<f64>, String), ())>()
            }
            StorageValue::HyperLogLog(_) => 16384,
            StorageValue::Stream(stream) => {
                stream.entries.values().map(|fields| fields.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>())
                    .sum::<usize>()
                    + stream.entries.len() * std::mem::size_of::<(StreamId, Vec<(String, String)>)>()
            }
        }
    }
}
