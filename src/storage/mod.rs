//! 存储引擎核心模块，基于 64 分片 RwLock HashMap 实现高并发内存存储

// 内存存储引擎，提供键值对的增删改查能力

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use ordered_float::OrderedFloat;
use rand::seq::SliceRandom;

use crate::error::{AppError, Result};
use crate::keyspace::KeyspaceNotifier;

/// SET 命令的过期选项
#[derive(Debug, Clone, PartialEq)]
pub enum SetExpireOption {
    /// 相对秒级过期
    Ex(u64),
    /// 相对毫秒级过期
    Px(u64),
    /// 绝对秒级时间戳过期
    ExAt(u64),
    /// 绝对毫秒级时间戳过期
    PxAt(u64),
}

/// SET 命令的完整选项
#[derive(Debug, Clone, PartialEq)]
#[derive(Default)]
pub struct SetOptions {
    /// 仅当 key 不存在时才设置
    pub nx: bool,
    /// 仅当 key 存在时才设置
    pub xx: bool,
    /// 返回旧值
    pub get: bool,
    /// 保留原有 TTL
    pub keepttl: bool,
    /// 过期选项
    pub expire: Option<SetExpireOption>,
}


/// 内存淘汰策略
#[derive(Debug, Clone, Copy, PartialEq)]
#[derive(Default)]
pub enum EvictionPolicy {
    /// 不淘汰，内存不足时返回 OOM 错误
    NoEviction,
    /// 从所有 key 中淘汰最久未访问的（LRU）
    #[default]
    AllKeysLru,
    /// 从所有 key 中随机淘汰
    AllKeysRandom,
    /// 从所有 key 中淘汰访问次数最少的（LFU）
    AllKeysLfu,
    /// 只从有过期时间的 key 中按 LRU 淘汰
    VolatileLru,
    /// 淘汰 TTL 最短的 key
    VolatileTtl,
    /// 从有过期时间的 key 中随机淘汰
    VolatileRandom,
    /// 从有过期时间的 key 中按 LFU 淘汰
    VolatileLfu,
}




#[derive(Debug, Clone)]
pub enum StorageValue {
    /// 普通字符串值
    String(Bytes),
    /// 列表值
    List(VecDeque<Bytes>),
    /// 哈希值
    Hash(HashMap<String, Bytes>),
    /// 集合值
    Set(HashSet<Bytes>),
    /// 有序集合值
    ZSet(ZSetData),
    /// HyperLogLog 值
    HyperLogLog(HyperLogLog),
    /// Stream 值
    Stream(StreamData),
}

const NUM_SHARDS: usize = 64;

/// 分片哈希表，将数据分散到多个独立的 RwLock<HashMap> 中以减少锁竞争
#[derive(Debug)]
pub struct ShardedMap {
    shards: Vec<RwLock<HashMap<String, StorageValue>>>,
}

impl Default for ShardedMap {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardedMap {
    pub fn new() -> Self {
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(RwLock::new(HashMap::new()));
        }
        Self { shards }
    }

    /// 根据 key 的 hash 返回对应的分片锁
    pub fn get_shard(&self, key: &str) -> &RwLock<HashMap<String, StorageValue>> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish() as usize;
        &self.shards[hash % NUM_SHARDS]
    }

    /// 返回所有分片的引用（用于遍历操作）
    pub fn all_shards(&self) -> &[RwLock<HashMap<String, StorageValue>>] {
        &self.shards
    }
}

/// 分片版本号表，与 ShardedMap 使用相同的分片策略
#[derive(Debug)]
pub struct ShardedVersions {
    shards: Vec<RwLock<HashMap<String, u64>>>,
}

impl Default for ShardedVersions {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardedVersions {
    pub fn new() -> Self {
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(RwLock::new(HashMap::new()));
        }
        Self { shards }
    }

    pub fn get_shard(&self, key: &str) -> &RwLock<HashMap<String, u64>> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish() as usize;
        &self.shards[hash % NUM_SHARDS]
    }

    pub fn all_shards(&self) -> &[RwLock<HashMap<String, u64>>] {
        &self.shards
    }
}

/// 分片过期时间表，与 ShardedMap 使用相同的分片策略
#[derive(Debug)]
pub struct ShardedExpires {
    shards: Vec<RwLock<HashMap<String, u64>>>,
}

impl Default for ShardedExpires {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardedExpires {
    pub fn new() -> Self {
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(RwLock::new(HashMap::new()));
        }
        Self { shards }
    }

    pub fn get_shard(&self, key: &str) -> &RwLock<HashMap<String, u64>> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish() as usize;
        &self.shards[hash % NUM_SHARDS]
    }

    pub fn all_shards(&self) -> &[RwLock<HashMap<String, u64>>] {
        &self.shards
    }
}

/// 单个数据库实例的数据
#[derive(Debug, Clone)]
pub struct Db {
    /// 内部使用分片读写锁保护的数据存储
    pub inner: Arc<ShardedMap>,
    /// 每个 key 的版本号（用于 WATCH 乐观锁）
    pub versions: Arc<ShardedVersions>,
    /// 每个 key 的最后访问时间（用于 LRU 淘汰）
    pub access_times: Arc<RwLock<HashMap<String, Instant>>>,
    /// 每个 key 的访问次数（用于 LFU 淘汰）
    pub access_counts: Arc<RwLock<HashMap<String, u64>>>,
    /// 阻塞弹出等待者（用于 BLPOP/BRPOP）
    pub blocking_waiters: Arc<RwLock<HashMap<String, Vec<Arc<tokio::sync::Notify>>>>>,
    /// Hash 字段级过期时间（key → field → expire_at_ms）
    pub hash_field_expirations: Arc<RwLock<HashMap<String, HashMap<String, u64>>>>,
    /// key-level expiration time (Unix timestamp in milliseconds)
    pub expires: Arc<ShardedExpires>,
}

impl Db {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ShardedMap::new()),
            versions: Arc::new(ShardedVersions::new()),
            access_times: Arc::new(RwLock::new(HashMap::new())),
            access_counts: Arc::new(RwLock::new(HashMap::new())),
            blocking_waiters: Arc::new(RwLock::new(HashMap::new())),
            hash_field_expirations: Arc::new(RwLock::new(HashMap::new())),
            expires: Arc::new(ShardedExpires::new()),
        }
    }
}

impl Default for Db {
    fn default() -> Self {
        Self::new()
    }
}

/// 内存存储引擎，线程安全，支持 16 个数据库
#[derive(Debug)]
pub struct StorageEngine {
    /// 16 个独立的数据库实例
    pub(crate) dbs: Arc<Vec<Db>>,
    /// 当前选中的数据库索引（0-15），每个 Clone 的实例有自己独立的值
    current_db: AtomicUsize,
    /// 全局版本号计数器
    version_counter: Arc<AtomicU64>,
    /// 最大内存限制（字节，0 表示不限制）
    maxmemory: Arc<AtomicU64>,
    /// 内存淘汰策略
    eviction_policy: Arc<RwLock<EvictionPolicy>>,
    /// 主动过期清理开关（DEBUG SET-ACTIVE-EXPIRE 控制）
    active_expire_enabled: Arc<AtomicBool>,
    /// 上次 RDB 保存的 Unix 时间戳（秒）
    last_save_time: Arc<AtomicU64>,
    /// Keyspace 通知器（可选）
    keyspace_notifier: Option<Arc<KeyspaceNotifier>>,
}

impl Clone for StorageEngine {
    fn clone(&self) -> Self {
        Self {
            dbs: self.dbs.clone(),
            current_db: AtomicUsize::new(self.current_db.load(Ordering::Relaxed)),
            version_counter: self.version_counter.clone(),
            maxmemory: self.maxmemory.clone(),
            eviction_policy: self.eviction_policy.clone(),
            active_expire_enabled: self.active_expire_enabled.clone(),
            last_save_time: self.last_save_time.clone(),
            keyspace_notifier: self.keyspace_notifier.clone(),
        }
    }
}

/// LINSERT 的插入位置选项
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LInsertPosition {
    Before,
    After,
}

/// GETEX 的过期选项
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GetExOption {
    /// 移除过期时间
    Persist,
    /// 秒级相对过期
    Ex(u64),
    /// 毫秒级相对过期
    Px(u64),
    /// 秒级绝对过期时间戳
    ExAt(u64),
    /// 毫秒级绝对过期时间戳
    PxAt(u64),
}


impl StorageEngine {
    /// 创建新的存储引擎实例，初始化 16 个数据库
    pub fn new() -> Self {
        Self {
            dbs: Arc::new((0..16).map(|_| Db::new()).collect()),
            current_db: AtomicUsize::new(0),
            version_counter: Arc::new(AtomicU64::new(0)),
            maxmemory: Arc::new(AtomicU64::new(0)),
            eviction_policy: Arc::new(RwLock::new(EvictionPolicy::default())),
            active_expire_enabled: Arc::new(AtomicBool::new(true)),
            last_save_time: Arc::new(AtomicU64::new(0)),
            keyspace_notifier: None,
        }
    }

    /// 设置 Keyspace 通知器
    pub fn set_keyspace_notifier(&mut self, notifier: Arc<KeyspaceNotifier>) {
        self.keyspace_notifier = Some(notifier);
    }

    /// 获取当前选中的数据库（返回 clone，因为内部持有读锁）
    fn db(&self) -> Db {
        self.dbs[self.current_db.load(Ordering::Relaxed).min(15)].clone()
    }

    /// 切换数据库（0-15）
    pub fn select(&self, db: usize) -> Result<()> {
        if db > 15 {
            return Err(AppError::Command(
                "SELECT 的数据库索引必须在 0-15 之间".to_string(),
            ));
        }
        self.current_db.store(db, Ordering::Relaxed);
        Ok(())
    }

    /// 获取当前数据库索引
    pub fn current_db(&self) -> usize {
        self.current_db.load(Ordering::Relaxed)
    }

    /// 获取所有数据库实例（用于 RDB 持久化）
    pub fn all_dbs(&self) -> Vec<Db> {
        self.dbs.iter().cloned().collect()
    }

    /// 获取指定索引的数据库实例
    pub fn db_at(&self, index: usize) -> Option<Db> {
        self.dbs.get(index).cloned()
    }

    /// 设置最大内存限制（字节，0 表示不限制）
    pub fn set_maxmemory(&self, bytes: u64) {
        self.maxmemory.store(bytes, Ordering::SeqCst);
    }

    /// 获取当前最大内存限制
    pub fn get_maxmemory(&self) -> u64 {
        self.maxmemory.load(Ordering::SeqCst)
    }

    /// 更新指定 key 的访问时间和访问次数
    fn touch(&self, key: &str) {
        if self.maxmemory.load(Ordering::Relaxed) == 0 {
            return;
        }
        let db = self.db();
        let mut times = db.access_times.write().unwrap();
        times.insert(key.to_string(), Instant::now());
        let mut counts = db.access_counts.write().unwrap();
        *counts.entry(key.to_string()).or_insert(0) += 1;
    }


    /// 设置内存淘汰策略
    pub fn set_eviction_policy(&self, policy: EvictionPolicy) {
        let mut p = self.eviction_policy.write().unwrap();
        *p = policy;
    }

    /// 获取当前内存淘汰策略
    pub fn get_eviction_policy(&self) -> EvictionPolicy {
        *self.eviction_policy.read().unwrap()
    }

    /// 设置主动过期清理开关
    pub fn set_active_expire(&self, enabled: bool) {
        self.active_expire_enabled.store(enabled, Ordering::SeqCst);
    }

    /// 获取主动过期清理开关状态
    pub fn active_expire_enabled(&self) -> bool {
        self.active_expire_enabled.load(Ordering::SeqCst)
    }

    /// 返回 key 的内部编码类型字符串
    /// - String: 可解析为整数 → "int"；len ≤ 44 → "embstr"；否则 "raw"
    /// - List: 元素 ≤512 且每个元素 len ≤64 → "listpack"；否则 "quicklist"
    /// - Hash: 字段 ≤512 且每个字段名和值 len ≤64 → "listpack"；否则 "hashtable"
    /// - Set: 元素 ≤512 且每个元素可解析为整数 → "intset"；否则 "hashtable"
    /// - ZSet: 元素 ≤128 → "listpack"；否则 "skiplist"
    /// - HyperLogLog → "raw"
    pub fn object_encoding(&self, key: &str) -> Result<Option<String>> {
        let db = self.db();
        let map = db
            .inner
            .get_shard(key)
            .read()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let value = match map.get(key) {
            Some(v) => {
                if Self::is_key_expired(&db, key) {
                    drop(map);
                    let mut map = db.inner.get_shard(key).write().map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
                    self.check_and_remove_expired(&db, &mut map, key);
                    return Ok(None);
                }
                v
            }
            None => return Ok(None),
        };

        let encoding = match value {
            StorageValue::String(b) => {
                if std::str::from_utf8(b).ok().and_then(|s| s.parse::<i64>().ok()).is_some() {
                    "int".to_string()
                } else if b.len() <= 44 {
                    "embstr".to_string()
                } else {
                    "raw".to_string()
                }
            }
            StorageValue::List(list) => {
                if list.len() <= 512 && list.iter().all(|b| b.len() <= 64) {
                    "listpack".to_string()
                } else {
                    "quicklist".to_string()
                }
            }
            StorageValue::Hash(hash) => {
                if hash.len() <= 512
                    && hash.iter().all(|(k, v)| k.len() <= 64 && v.len() <= 64)
                {
                    "listpack".to_string()
                } else {
                    "hashtable".to_string()
                }
            }
            StorageValue::Set(set) => {
                if set.len() <= 512
                    && set.iter().all(|b| {
                        std::str::from_utf8(b)
                            .ok()
                            .and_then(|s| s.parse::<i64>().ok())
                            .is_some()
                    })
                {
                    "intset".to_string()
                } else {
                    "hashtable".to_string()
                }
            }
            StorageValue::ZSet(zset) => {
                if zset.member_to_score.len() <= 128 {
                    "listpack".to_string()
                } else {
                    "skiplist".to_string()
                }
            }
            StorageValue::HyperLogLog(_) => "raw".to_string(),
            StorageValue::Stream(_) => "stream".to_string(),
        };

        Ok(Some(encoding))
    }

    /// 返回 key 的引用计数（始终返回 1，因为不做引用计数）
    pub fn object_refcount(&self, key: &str) -> Result<Option<i64>> {
        let db = self.db();
        let map = db
            .inner
            .get_shard(key)
            .read()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(_) => {
                if Self::is_key_expired(&db, key) {
                    drop(map);
                    let mut map = db.inner.get_shard(key).write().map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
                    self.check_and_remove_expired(&db, &mut map, key);
                    Ok(None)
                } else {
                    Ok(Some(1))
                }
            }
            None => Ok(None),
        }
    }

    /// 返回 key 距离上次访问的秒数
    pub fn object_idletime(&self, key: &str) -> Result<Option<u64>> {
        let db = self.db();
        let map = db
            .inner
            .get_shard(key)
            .read()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        if map.get(key).is_some() {
            if Self::is_key_expired(&db, key) {
                drop(map);
                let mut map = db.inner.get_shard(key).write().map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
                self.check_and_remove_expired(&db, &mut map, key);
                return Ok(None);
            }
        } else {
            return Ok(None);
        }
        drop(map);

        let times = db
            .access_times
            .read()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match times.get(key) {
            Some(time) => {
                let elapsed = time.elapsed().as_secs();
                Ok(Some(elapsed))
            }
            None => Ok(Some(0)),
        }
    }

    /// 返回 key 的 LFU 访问频率计数
    pub fn object_freq(&self, key: &str) -> Result<Option<u64>> {
        let db = self.db();
        let map = db
            .inner
            .get_shard(key)
            .read()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        if map.get(key).is_some() {
            if Self::is_key_expired(&db, key) {
                drop(map);
                let mut map = db.inner.get_shard(key).write().map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
                self.check_and_remove_expired(&db, &mut map, key);
                return Ok(None);
            }
        } else {
            return Ok(None);
        }
        drop(map);

        let counts = db
            .access_counts
            .read()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match counts.get(key) {
            Some(count) => Ok(Some(*count)),
            None => Ok(Some(0)),
        }
    }

    /// 返回 OBJECT 命令帮助信息
    pub fn object_help() -> Vec<String> {
        vec![
            "OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands:".to_string(),
            "ENCODING <key>".to_string(),
            "    Return the encoding of <key>.".to_string(),
            "REFCOUNT <key>".to_string(),
            "    Return the refcount (number of references) of <key>.".to_string(),
            "IDLETIME <key>".to_string(),
            "    Return the idle time of <key>.".to_string(),
            "FREQ <key>".to_string(),
            "    Return the access frequency of <key>.".to_string(),
            "HELP".to_string(),
            "    Print this help.".to_string(),
        ]
    }

    /// 随机返回当前数据库中的一个 key
    pub fn randomkey(&self) -> Result<Option<String>> {
        let db = self.db();
        let mut all_keys: Vec<String> = Vec::new();
        for shard in db.inner.all_shards() {
            let map = shard.read().map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            for (k, _v) in map.iter() {
                if !Self::is_key_expired(&db, k) {
                    all_keys.push(k.clone());
                }
            }
        }
        let keys = all_keys;
        if keys.is_empty() {
            Ok(None)
        } else {
            let mut rng = rand::thread_rng();
            use rand::seq::SliceRandom;
            Ok(keys.choose(&mut rng).cloned())
        }
    }

    /// 更新多个 key 的最后访问时间，返回实际存在的 key 数量
    pub fn touch_keys(&self, keys: &[String]) -> Result<usize> {
        let db = self.db();
        let mut count = 0;
        for key in keys {
            let map = db.inner.get_shard(key).read()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            if map.get(key).is_some() && !Self::is_key_expired(&db, key) {
                count += 1;
            }
        }
        let mut times = db
            .access_times
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
        for key in keys {
            times.insert(key.clone(), Instant::now());
        }
        Ok(count)
    }

    /// 设置 key 在指定 Unix 时间戳（秒）过期
    pub fn expire_at(&self, key: &str, timestamp_secs: u64) -> Result<bool> {
        let expire_at_ms = timestamp_secs.saturating_mul(1000);
        self.set_expire_at_ms(key, expire_at_ms)
    }

    /// 设置 key 在指定 Unix 时间戳（毫秒）过期
    pub fn pexpire_at(&self, key: &str, timestamp_ms: u64) -> Result<bool> {
        self.set_expire_at_ms(key, timestamp_ms)
    }

    /// 内部辅助：设置 key 的绝对过期时间（毫秒）
    fn set_expire_at_ms(&self, key: &str, expire_at_ms: u64) -> Result<bool> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(_) => {
                if Self::is_key_expired(&db, key) {
                    map.remove(key);
                    let mut expires = db.expires.get_shard(key).write().unwrap();
                    expires.remove(key);
                    Ok(false)
                } else {
                    let mut expires = db.expires.get_shard(key).write().unwrap();
                    expires.insert(key.to_string(), expire_at_ms);
                    Ok(true)
                }
            }
            None => Ok(false),
        }
    }

    /// 返回 key 的过期时间戳（秒）
    /// - 返回正数：过期时间戳
    /// - 返回 -1：键存在但没有设置过期时间
    /// - 返回 -2：键不存在或已过期
    pub fn expire_time(&self, key: &str) -> Result<i64> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(_) => {
                if Self::is_key_expired(&db, key) {
                    map.remove(key);
                    let mut expires = db.expires.get_shard(key).write().unwrap();
                    expires.remove(key);
                    Ok(-2)
                } else {
                    let expires = db.expires.get_shard(key).read().unwrap();
                    match expires.get(key) {
                        Some(&expire_at) => Ok((expire_at / 1000) as i64),
                        None => Ok(-1),
                    }
                }
            }
            None => Ok(-2),
        }
    }

    /// 返回 key 的过期时间戳（毫秒）
    pub fn pexpire_time(&self, key: &str) -> Result<i64> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(_) => {
                if Self::is_key_expired(&db, key) {
                    map.remove(key);
                    let mut expires = db.expires.get_shard(key).write().unwrap();
                    expires.remove(key);
                    Ok(-2)
                } else {
                    let expires = db.expires.get_shard(key).read().unwrap();
                    match expires.get(key) {
                        Some(&expire_at) => Ok(expire_at as i64),
                        None => Ok(-1),
                    }
                }
            }
            None => Ok(-2),
        }
    }

    /// 仅当 newkey 不存在时重命名 key
    pub fn renamenx(&self, key: &str, newkey: &str) -> Result<bool> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        // 检查源 key 是否存在
        let value = match map.get(key) {
            Some(_) if Self::is_key_expired(&db, key) => {
                map.remove(key);
                let mut expires = db.expires.get_shard(key).write().unwrap();
                expires.remove(key);
                return Err(AppError::Storage("键不存在或已过期".to_string()));
            }
            Some(v) => v.clone(),
            None => return Err(AppError::Storage("键不存在".to_string())),
        };

        // 检查目标 key 是否存在（可能在不同分片，需要额外检查）
        drop(map);
        let mut new_map = db.inner.get_shard(newkey).write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
        if new_map.contains_key(newkey) {
            return Ok(false);
        }

        let mut map = db.inner.get_shard(key).write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
        map.remove(key);
        new_map.insert(newkey.to_string(), value);

        // 迁移过期时间
        let mut expires = db.expires.get_shard(key).write().unwrap();
        if let Some(expire_at) = expires.remove(key) {
            let mut new_expires = db.expires.get_shard(newkey).write().unwrap();
            new_expires.insert(newkey.to_string(), expire_at);
        }

        self.bump_version(key);
        self.bump_version(newkey);
        Ok(true)
    }

    /// 交换两个数据库的数据
    pub fn swap_db(&self, index1: usize, index2: usize) -> Result<()> {
        if index1 > 15 || index2 > 15 {
            return Err(AppError::Command(
                "SWAPDB 的数据库索引必须在 0-15 之间".to_string(),
            ));
        }
        if index1 == index2 {
            return Ok(());
        }
        let db1 = &self.dbs[index1];
        let db2 = &self.dbs[index2];
        for i in 0..NUM_SHARDS {
            let mut s1 = db1.inner.all_shards()[i].write().unwrap();
            let mut s2 = db2.inner.all_shards()[i].write().unwrap();
            std::mem::swap(&mut *s1, &mut *s2);
        }
        for i in 0..NUM_SHARDS {
            let mut v1 = db1.versions.all_shards()[i].write().unwrap();
            let mut v2 = db2.versions.all_shards()[i].write().unwrap();
            std::mem::swap(&mut *v1, &mut *v2);
        }
        for i in 0..NUM_SHARDS {
            let mut e1 = db1.expires.all_shards()[i].write().unwrap();
            let mut e2 = db2.expires.all_shards()[i].write().unwrap();
            std::mem::swap(&mut *e1, &mut *e2);
        }
        {
            let mut t1 = db1.access_times.write().unwrap();
            let mut t2 = db2.access_times.write().unwrap();
            std::mem::swap(&mut *t1, &mut *t2);
        }
        {
            let mut c1 = db1.access_counts.write().unwrap();
            let mut c2 = db2.access_counts.write().unwrap();
            std::mem::swap(&mut *c1, &mut *c2);
        }
        {
            let mut w1 = db1.blocking_waiters.write().unwrap();
            let mut w2 = db2.blocking_waiters.write().unwrap();
            std::mem::swap(&mut *w1, &mut *w2);
        }
        {
            let mut h1 = db1.hash_field_expirations.write().unwrap();
            let mut h2 = db2.hash_field_expirations.write().unwrap();
            std::mem::swap(&mut *h1, &mut *h2);
        }
        Ok(())
    }

    /// 清空指定数据库
    pub fn flush_db(&self, db_index: usize) -> Result<()> {
        if db_index > 15 {
            return Err(AppError::Command(
                "FLUSHDB 的数据库索引必须在 0-15 之间".to_string(),
            ));
        }
        let db = self.db_at(db_index).ok_or_else(|| {
            AppError::Command("无效的数据库索引".to_string())
        })?;
        for shard in db.inner.all_shards() {
            let mut map = shard.write().map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            map.clear();
        }
        for shard in db.versions.all_shards() {
            let mut v = shard.write().map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            v.clear();
        }
        for shard in db.expires.all_shards() {
            let mut e = shard.write().map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            e.clear();
        }
        let mut times = db
            .access_times
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
        times.clear();
        let mut counts = db
            .access_counts
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
        counts.clear();
        Ok(())
    }

    /// 记录上次 RDB 保存时间
    pub fn set_last_save_time(&self, timestamp_secs: u64) {
        self.last_save_time.store(timestamp_secs, Ordering::SeqCst);
    }

    /// 获取上次 RDB 保存时间（秒）
    pub fn get_last_save_time(&self) -> u64 {
        self.last_save_time.load(Ordering::SeqCst)
    }


    /// 惰性清理 Hash 中已过期的字段，返回被清理的字段数量
    fn cleanup_hash_expired_fields(&self, key: &str) -> usize {
        let db = self.db();
        let mut removed = 0usize;
        let now = Self::now_millis();

        // 先收集需要删除的字段
        let expired_fields: Vec<String> = {
            let exp_map = db.hash_field_expirations.read().unwrap();
            if let Some(field_map) = exp_map.get(key) {
                field_map
                    .iter()
                    .filter(|(_, expire_at)| **expire_at <= now)
                    .map(|(field, _)| field.clone())
                    .collect()
            } else {
                return 0;
            }
        };

        if expired_fields.is_empty() {
            return 0;
        }

        // 删除过期字段
        let mut inner = db.inner.get_shard(key).write().unwrap();
        if let Some(StorageValue::Hash(hash)) = inner.get_mut(key) {
            for field in &expired_fields {
                hash.remove(field);
            }
            removed = expired_fields.len();
            // 如果 hash 为空，删除整个 key
            if hash.is_empty() {
                inner.remove(key);
                drop(inner);
                let mut versions = db.versions.get_shard(key).write().unwrap();
                versions.remove(key);
                let mut exp_map = db.hash_field_expirations.write().unwrap();
                exp_map.remove(key);
                return removed;
            }
        }

        // 清理过期记录
        let mut exp_map = db.hash_field_expirations.write().unwrap();
        if let Some(field_map) = exp_map.get_mut(key) {
            for field in &expired_fields {
                field_map.remove(field);
            }
            if field_map.is_empty() {
                exp_map.remove(key);
            }
        }

        removed
    }

    /// 递增指定 key 的版本号
    fn bump_version(&self, key: &str) {
        let db = self.db();
        let mut versions = db.versions.get_shard(key).write().unwrap();
        let new_ver = self.version_counter.fetch_add(1, Ordering::SeqCst).saturating_add(1);
        versions.insert(key.to_string(), new_ver);
    }

    /// 获取指定 key 的当前版本号（key 不存在返回 0）
    pub fn get_version(&self, key: &str) -> Result<u64> {
        let db = self.db();
        let versions = db.versions.get_shard(key).read().map_err(|e| {
            AppError::Storage(format!("版本号锁中毒: {}", e))
        })?;
        Ok(*versions.get(key).unwrap_or(&0))
    }

    /// 检查所有被监视的 key 版本号是否未发生变化
    /// watched: key -> 之前记录的旧版本号
    pub fn watch_check(&self, watched: &HashMap<String, u64>) -> Result<bool> {
        let db = self.db();
        for (key, old_version) in watched {
            let versions = db.versions.get_shard(key).read().map_err(|e| {
                AppError::Storage(format!("版本号锁中毒: {}", e))
            })?;
            let current = *versions.get(key).unwrap_or(&0);
            if current != *old_version {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// 获取当前 Unix 时间戳（毫秒）
    fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("系统时间不应早于 Unix 纪元")
            .as_millis() as u64
    }

    /// 获取指定 key 所在分片的写锁
    pub(crate) fn write_shard(
        &self,
        key: &str,
    ) -> Result<std::sync::RwLockWriteGuard<'_, HashMap<String, StorageValue>>> {
        let db = &self.dbs[self.current_db.load(Ordering::Relaxed).min(15)];
        db.inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))
    }

    /// 获取指定 key 所在分片的读锁
    pub(crate) fn read_shard(
        &self,
        key: &str,
    ) -> Result<std::sync::RwLockReadGuard<'_, HashMap<String, StorageValue>>> {
        let db = &self.dbs[self.current_db.load(Ordering::Relaxed).min(15)];
        db.inner
            .get_shard(key)
            .read()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))
    }

    /// 检查指定 key 是否已过期
    fn is_key_expired(db: &Db, key: &str) -> bool {
        let expires = db.expires.get_shard(key).read().unwrap();
        expires.get(key).map_or(false, |&expire_at| Self::now_millis() >= expire_at)
    }

    /// 检查并清理过期的 key
    pub(crate) fn check_and_remove_expired(&self, db: &Db, map: &mut HashMap<String, StorageValue>, key: &str) {
        if Self::is_key_expired(db, key) {
            map.remove(key);
            let mut expires = db.expires.get_shard(key).write().unwrap();
            expires.remove(key);
            if let Some(ref notifier) = self.keyspace_notifier {
                let db_idx = self.current_db.load(Ordering::Relaxed);
                notifier.notify_expired(db_idx, key);
            }
        }
    }

    /// 检查值是否为列表类型，若不是则返回 WRONGTYPE 错误
    fn check_list_type(value: &StorageValue) -> Result<()> {
        match value {
            StorageValue::List(_) => Ok(()),
            _ => Err(AppError::Storage(
                "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
            )),
        }
    }

    /// 从 StorageValue 中获取列表的可变引用
    fn as_list_mut(value: &mut StorageValue) -> Option<&mut VecDeque<Bytes>> {
        match value {
            StorageValue::List(list) => Some(list),
            _ => None,
        }
    }

    /// 检查值是否为哈希类型，若不是则返回 WRONGTYPE 错误
    fn check_hash_type(value: &StorageValue) -> Result<()> {
        match value {
            StorageValue::Hash(_) => Ok(()),
            _ => Err(AppError::Storage(
                "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
            )),
        }
    }

    /// 从 StorageValue 中获取哈希的可变引用
    fn as_hash_mut(value: &mut StorageValue) -> Option<&mut HashMap<String, Bytes>> {
        match value {
            StorageValue::Hash(h) => Some(h),
            _ => None,
        }
    }

    /// 检查值是否为集合类型，若不是则返回 WRONGTYPE 错误
    fn check_set_type(value: &StorageValue) -> Result<()> {
        match value {
            StorageValue::Set(_) => Ok(()),
            _ => Err(AppError::Storage(
                "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
            )),
        }
    }

    /// 从 StorageValue 中获取集合的可变引用
    fn as_set_mut(value: &mut StorageValue) -> Option<&mut HashSet<Bytes>> {
        match value {
            StorageValue::Set(s) => Some(s),
            _ => None,
        }
    }
}

impl StorageEngine {
    fn check_hll_type(value: &StorageValue) -> Result<()> {
        match value {
            StorageValue::HyperLogLog(_) => Ok(()),
            _ => Err(AppError::Storage(
                "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
            )),
        }
    }
}

impl StorageEngine {
    fn check_zset_type(value: &StorageValue) -> Result<()> {
        match value {
            StorageValue::ZSet(_) => Ok(()),
            _ => Err(AppError::Storage(
                "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
            )),
        }
    }

    /// 从 StorageValue 中获取有序集合的可变引用
    fn as_zset_mut(value: &mut StorageValue) -> Option<&mut ZSetData> {
        match value {
            StorageValue::ZSet(z) => Some(z),
            _ => None,
        }
    }

    /// 检查值是否为 Stream 类型，若不是则返回 WRONGTYPE 错误
    fn check_stream_type(value: &StorageValue) -> Result<()> {
        match value {
            StorageValue::Stream(_) => Ok(()),
            _ => Err(AppError::Storage(
                "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
            )),
        }
    }

    /// 从 StorageValue 中获取 Stream 的可变引用
    #[allow(dead_code)]
    fn as_stream_mut(value: &mut StorageValue) -> Option<&mut StreamData> {
        match value {
            StorageValue::Stream(s) => Some(s),
            _ => None,
        }
    }
}

impl Default for StorageEngine {
    fn default() -> Self {
        Self::new()
    }
}

fn format_float(v: f64) -> String {
    let s = format!("{:.12}", v);
    let s = s.trim_end_matches('0').to_string();
    if s.ends_with('.') {
        s + "0"
    } else {
        s
    }
}

pub mod eviction;
pub mod memory;
pub mod bitmap_ops;
pub mod geo_ops;
pub mod hash_ops;
pub mod hll_ops;
pub mod key_ops;
pub mod list_ops;
pub mod set_ops;
pub mod stream_group;
pub mod stream_ops;
pub mod string_ops;
pub mod tests;
pub mod zset_advanced;
pub mod zset_ops;

// Re-export public types
#[allow(unused_imports)]
pub use bitmap_ops::*;
pub use hll_ops::*;
pub use stream_ops::*;
pub use zset_ops::*;
