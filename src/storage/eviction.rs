use std::sync::atomic::Ordering;
use std::time::Instant;
use crate::error::{AppError, Result};
use crate::storage::{EvictionPolicy, StorageEngine, StorageValue};

impl StorageEngine {
    /// 如果需要则淘汰 key，直到内存使用低于 maxmemory
    /// NoEviction 策略下，如果内存已满则返回 OOM 错误
    pub(crate) fn evict_if_needed(&self) -> Result<()> {
        let max = self.maxmemory.load(Ordering::SeqCst);
        if max == 0 {
            return Ok(());
        }

        let policy = *self.eviction_policy.read().unwrap();

        loop {
            let usage = match self.memory_usage() {
                Ok(u) => u,
                Err(_) => break,
            };
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

    /// LRU 淘汰：返回 true 表示成功淘汰了一个 key
    fn evict_lru(&self, volatile_only: bool) -> Result<bool> {
        let mut lru_key: Option<(usize, String)> = None;
        let mut lru_time = Instant::now();

        let dbs = &self.dbs;
        for (db_idx, db) in dbs.iter().enumerate() {
            if let Ok(times) = db.access_times.read() {
                for (key, time) in times.iter() {
                    if let Ok(map) = db.inner.get_shard(key).read() {
                        if !map.contains_key(key) {
                            continue;
                        }
                        if volatile_only {
                            if let Some(StorageValue::ExpiringString(_, _)) = map.get(key) {
                                if *time < lru_time {
                                    lru_time = *time;
                                    lru_key = Some((db_idx, key.clone()));
                                }
                            }
                        } else if *time < lru_time {
                            lru_time = *time;
                            lru_key = Some((db_idx, key.clone()));
                        }
                    }
                }
            }
        }

        match lru_key {
            Some((db_idx, key)) => {
                self.remove_key_from_db(db_idx, &key);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Random 淘汰：返回 true 表示成功淘汰了一个 key
    fn evict_random(&self, volatile_only: bool) -> Result<bool> {
        use rand::seq::IteratorRandom;
        let mut rng = rand::thread_rng();

        let dbs = &self.dbs;
        for (db_idx, db) in dbs.iter().enumerate() {
            for shard in db.inner.all_shards() {
                if let Ok(map) = shard.read() {
                    let keys: Vec<String> = if volatile_only {
                        map.iter()
                            .filter(|(_, v)| matches!(v, StorageValue::ExpiringString(_, _)))
                            .map(|(k, _)| k.clone())
                            .collect()
                    } else {
                        map.keys().cloned().collect()
                    };

                    if let Some(key) = keys.iter().choose(&mut rng) {
                        drop(map);
                        self.remove_key_from_db(db_idx, key);
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    /// LFU 淘汰：返回 true 表示成功淘汰了一个 key
    fn evict_lfu(&self, volatile_only: bool) -> Result<bool> {
        let mut lfu_key: Option<(usize, String)> = None;
        let mut min_count = u64::MAX;

        let dbs = &self.dbs;
        for (db_idx, db) in dbs.iter().enumerate() {
            if let Ok(counts) = db.access_counts.read() {
                for (key, count) in counts.iter() {
                    if let Ok(map) = db.inner.get_shard(key).read() {
                        if !map.contains_key(key) {
                            continue;
                        }
                        if volatile_only {
                            if let Some(StorageValue::ExpiringString(_, _)) = map.get(key) {
                                if *count < min_count {
                                    min_count = *count;
                                    lfu_key = Some((db_idx, key.clone()));
                                }
                            }
                        } else if *count < min_count {
                            min_count = *count;
                            lfu_key = Some((db_idx, key.clone()));
                        }
                    }
                }
            }
        }

        match lfu_key {
            Some((db_idx, key)) => {
                self.remove_key_from_db(db_idx, &key);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// TTL 淘汰：淘汰 TTL 最短的 key，返回 true 表示成功淘汰了一个 key
    fn evict_ttl(&self) -> Result<bool> {
        let mut ttl_key: Option<(usize, String)> = None;
        let mut min_ttl = u64::MAX;
        let now = Self::now_millis();

        let dbs = &self.dbs;
        for (db_idx, db) in dbs.iter().enumerate() {
            for shard in db.inner.all_shards() {
                if let Ok(map) = shard.read() {
                    for (key, value) in map.iter() {
                        if let StorageValue::ExpiringString(_, expire_at) = value {
                            let ttl = expire_at.saturating_sub(now);
                            if ttl < min_ttl {
                                min_ttl = ttl;
                                ttl_key = Some((db_idx, key.clone()));
                            }
                        }
                    }
                }
            }
        }

        match ttl_key {
            Some((db_idx, key)) => {
                self.remove_key_from_db(db_idx, &key);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// 从指定 db 中移除 key（包括所有辅助数据结构）
    fn remove_key_from_db(&self, db_idx: usize, key: &str) {
        let dbs = &self.dbs;
        let db = dbs[db_idx].clone();
        drop(dbs);
        let mut map = db.inner.get_shard(key).write().unwrap();
        map.remove(key);
        let mut versions = db.versions.get_shard(key).write().unwrap();
        versions.remove(key);
        let mut times = db.access_times.write().unwrap();
        times.remove(key);
        let mut counts = db.access_counts.write().unwrap();
        counts.remove(key);
        let mut waiters = db.blocking_waiters.write().unwrap();
        waiters.remove(key);
        let mut hash_exp = db.hash_field_expirations.write().unwrap();
        hash_exp.remove(key);

        // 发送淘汰通知
        if let Some(ref notifier) = self.keyspace_notifier {
            notifier.notify_evicted(db_idx, key);
        }
    }
}
