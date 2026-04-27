//! 内存淘汰模块，支持 LRU/LFU/Random/TTL 策略
//!
//! 实现采用 Redis 风格的随机采样算法：
//! 从所有 shard 中随机选取最多 16 个 key，然后在样本中根据策略选出最优者淘汰。
//! 这样避免了 O(N) 全表扫描，在大数据量下性能稳定。

use crate::error::{AppError, Result};
use crate::storage::{Entry, EvictionPolicy, StorageEngine};
use rand::Rng;
use std::sync::atomic::Ordering;

/// 每次淘汰时随机采样的 key 数量（与 Redis 默认一致）
const EVICTION_SAMPLE_SIZE: usize = 16;
/// 从单个 shard 采样时，最多尝试次数（避免在无 volatile key 的 shard 上无限重试）
const EVICTION_MAX_ATTEMPTS_PER_SHARD: usize = 5;

impl StorageEngine {
    pub(crate) fn evict_if_needed(&self) -> Result<()> {
        let max = self.maxmemory.load(Ordering::SeqCst);
        if max == 0 {
            return Ok(());
        }

        let policy = *self.eviction_policy.read().unwrap();

        while let Ok(usage) = self.memory_usage() {
            if usage < max as usize {
                break;
            }

            match policy {
                EvictionPolicy::NoEviction => {
                    return Err(AppError::Storage(
                        "OOM command not allowed when used memory > 'maxmemory'.".to_string(),
                    ));
                }
                EvictionPolicy::AllKeysLru => {
                    self.evict_lru(false)?;
                }
                EvictionPolicy::AllKeysRandom => {
                    self.evict_random(false)?;
                }
                EvictionPolicy::AllKeysLfu => {
                    self.evict_lfu(false)?;
                }
                EvictionPolicy::VolatileLru => {
                    if !self.evict_lru(true)? {
                        self.evict_lru(false)?;
                    }
                }
                EvictionPolicy::VolatileTtl => {
                    if !self.evict_ttl()? {
                        self.evict_random(false)?;
                    }
                }
                EvictionPolicy::VolatileRandom => {
                    if !self.evict_random(true)? {
                        self.evict_random(false)?;
                    }
                }
                EvictionPolicy::VolatileLfu => {
                    if !self.evict_lfu(true)? {
                        self.evict_lfu(false)?;
                    }
                }
            }
        }

        Ok(())
    }

    /// 通用随机采样函数
    ///
    /// 从所有 db 的所有 shard 中随机采样最多 `sample_size` 个满足条件的 key。
    ///
    /// # 参数
    /// - `sample_size` - 最大采样数量
    /// - `volatile_only` - 是否只采样带有过期时间的 key
    /// - `extract` - 从 Entry 中提取用于比较的值，返回 `None` 表示跳过该 entry
    ///
    /// # 返回值
    /// 候选列表，每个元素为 `(db_idx, key, extracted_value)`
    fn sample_keys<F, R>(&self, sample_size: usize, volatile_only: bool, mut extract: F) -> Vec<(usize, String, R)>
    where
        F: FnMut(&Entry) -> Option<R>,
    {
        use rand::seq::SliceRandom;
        let mut rng = rand::thread_rng();
        let mut candidates = Vec::with_capacity(sample_size);

        // 收集所有 shard 的索引，然后随机打乱
        let mut shards: Vec<(usize, usize)> = Vec::new();
        for (db_idx, db) in self.dbs.iter().enumerate() {
            for (shard_idx, _) in db.inner.all_shards().iter().enumerate() {
                shards.push((db_idx, shard_idx));
            }
        }
        shards.shuffle(&mut rng);

        for (db_idx, shard_idx) in shards {
            if candidates.len() >= sample_size {
                break;
            }

            let shard = &self.dbs[db_idx].inner.all_shards()[shard_idx];
            let map = match shard.try_read() {
                Ok(guard) => guard,
                Err(_) => continue,
            };

            if map.is_empty() {
                continue;
            }

            let len = map.len();
            for _ in 0..EVICTION_MAX_ATTEMPTS_PER_SHARD {
                if candidates.len() >= sample_size {
                    break;
                }
                let idx = rng.gen_range(0..len);
                if let Some((key, entry)) = map.iter().nth(idx) {
                    if volatile_only && entry.expire_at.is_none() {
                        continue;
                    }
                    if let Some(val) = extract(entry) {
                        candidates.push((db_idx, key.clone(), val));
                        break;
                    }
                }
            }
        }

        candidates
    }

    fn evict_lru(&self, volatile_only: bool) -> Result<bool> {
        let candidates = self.sample_keys(EVICTION_SAMPLE_SIZE, volatile_only, |e| Some(e.last_access));
        if let Some((db_idx, key, _)) = candidates.into_iter().min_by_key(|(_, _, t)| *t) {
            self.remove_key_from_db(db_idx, &key);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn evict_random(&self, volatile_only: bool) -> Result<bool> {
        let candidates = self.sample_keys(1, volatile_only, |_e| Some(()));
        if let Some((db_idx, key, _)) = candidates.into_iter().next() {
            self.remove_key_from_db(db_idx, &key);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn evict_lfu(&self, volatile_only: bool) -> Result<bool> {
        let candidates = self.sample_keys(EVICTION_SAMPLE_SIZE, volatile_only, |e| Some(e.access_count));
        if let Some((db_idx, key, _)) = candidates.into_iter().min_by_key(|(_, _, c)| *c) {
            self.remove_key_from_db(db_idx, &key);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn evict_ttl(&self) -> Result<bool> {
        let candidates = self.sample_keys(EVICTION_SAMPLE_SIZE, true, |e| e.expire_at);
        if let Some((db_idx, key, _)) = candidates.into_iter().min_by_key(|(_, _, t)| *t) {
            self.remove_key_from_db(db_idx, &key);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn remove_key_from_db(&self, db_idx: usize, key: &str) {
        let dbs = &self.dbs;
        let db = dbs[db_idx].clone();
        let _ = dbs;
        let mut map = db.inner.get_shard(key).write().unwrap();
        map.remove(key);
        let mut waiters = db.blocking_waiters.write().unwrap();
        waiters.remove(key);
        let mut hash_exp = db.hash_field_expirations.write().unwrap();
        hash_exp.remove(key);

        if let Some(ref notifier) = self.keyspace_notifier {
            notifier.notify_evicted(db_idx, key);
        }
    }
}
