//! 内存淘汰模块，支持 LRU/LFU/Random/TTL 策略

use crate::error::{AppError, Result};
use crate::storage::{EvictionPolicy, StorageEngine};
use std::sync::atomic::Ordering;
use std::time::Instant;

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

    fn evict_lru(&self, volatile_only: bool) -> Result<bool> {
        let mut lru_key: Option<(usize, String)> = None;
        let mut lru_time = Instant::now();

        let dbs = &self.dbs;
        for (db_idx, db) in dbs.iter().enumerate() {
            for shard in db.inner.all_shards() {
                if let Ok(map) = shard.read() {
                    for (key, entry) in map.iter() {
                        if volatile_only && entry.expire_at.is_none() {
                            continue;
                        }
                        if entry.last_access < lru_time {
                            lru_time = entry.last_access;
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

    fn evict_random(&self, volatile_only: bool) -> Result<bool> {
        use rand::seq::IteratorRandom;
        let mut rng = rand::thread_rng();

        let dbs = &self.dbs;
        for (db_idx, db) in dbs.iter().enumerate() {
            for shard in db.inner.all_shards() {
                if let Ok(map) = shard.read() {
                    let keys: Vec<String> = if volatile_only {
                        map.iter()
                            .filter(|(_, entry)| entry.expire_at.is_some())
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

    fn evict_lfu(&self, volatile_only: bool) -> Result<bool> {
        let mut lfu_key: Option<(usize, String)> = None;
        let mut min_count = u64::MAX;

        let dbs = &self.dbs;
        for (db_idx, db) in dbs.iter().enumerate() {
            for shard in db.inner.all_shards() {
                if let Ok(map) = shard.read() {
                    for (key, entry) in map.iter() {
                        if volatile_only && entry.expire_at.is_none() {
                            continue;
                        }
                        if entry.access_count < min_count {
                            min_count = entry.access_count;
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

    fn evict_ttl(&self) -> Result<bool> {
        let mut ttl_key: Option<(usize, String)> = None;
        let mut min_ttl = u64::MAX;
        let now = Self::now_millis();

        let dbs = &self.dbs;
        for (db_idx, db) in dbs.iter().enumerate() {
            for shard in db.inner.all_shards() {
                if let Ok(map) = shard.read() {
                    for (key, entry) in map.iter() {
                        if let Some(expire_at) = entry.expire_at {
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
