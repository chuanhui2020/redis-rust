//! String 数据类型操作（对标 Redis String 命令族）
use super::*;

impl StorageEngine {
    /// 获取字符串值（对标 Redis GET 命令）
    ///
    /// 返回 key 对应的字符串值。如果 key 不存在，返回 None。
    /// 如果 key 已过期，会在读取时自动删除并返回 None。
    /// 如果 key 不是字符串类型，返回 WRONGTYPE 错误。
    ///
    /// # 参数
    /// - `key` - 键名
    ///
    /// # 返回值
    /// - `Ok(Some(Bytes))` - 键存在且未过期
    /// - `Ok(None)` - 键不存在或已过期
    /// - `Err(AppError::Storage)` - 类型错误或锁中毒
    ///
    /// # 性能说明
    /// 先使用读锁检查，仅在需要删除过期键时才升级为写锁，减少锁竞争。
    pub fn get(&self, key: &str) -> Result<Option<Bytes>> {
        let dbs = &self.dbs;
        let db = &dbs[self.current_db.load(Ordering::Relaxed).min(15)];
        let shard = db.inner.get_shard(key);

        // 先使用读锁检查键，避免不必要的写锁竞争
        {
            let map = shard
                .read()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

            if let Some(value) = map.get(key) {
                if !Self::is_key_expired(db, key) {
                    // 键存在且未过期，直接返回值
                    let result = match value {
                        StorageValue::String(v) => Ok(Some(v.clone())),
                        _ => {
                            Err(AppError::Storage(
                                "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                            ))
                        }
                    };
                    drop(map);
                    // 内联 touch，只在 maxmemory > 0 时执行
                    if result.is_ok() && self.maxmemory.load(Ordering::Relaxed) > 0 {
                        db.access_times.write().unwrap().insert(key.to_string(), Instant::now());
                        *db.access_counts.write().unwrap().entry(key.to_string()).or_insert(0) += 1;
                    }
                    return result;
                }
                // 键已过期，需要写锁删除
            } else {
                // 键不存在
                return Ok(None);
            }
        }

        // 键已过期，获取写锁删除
        let mut map = shard
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
        // 双重检查：在获取写锁期间可能已被其他线程删除或更新
        if Self::is_key_expired(db, key) {
            map.remove(key);
            let mut expires = db.expires.get_shard(key).write().unwrap();
            expires.remove(key);
            if let Some(ref notifier) = self.keyspace_notifier {
                let db_idx = self.current_db.load(Ordering::Relaxed);
                notifier.notify_expired(db_idx, key);
            }
        }
        Ok(None)
    }

    /// 设置键值对（对标 Redis SET 命令）
    ///
    /// 将 key 设置为指定的字符串值。如果 key 已存在则覆盖，无论其类型为何。
    /// 新设置的键没有过期时间（持久键）。
    /// 如果启用了 maxmemory，会在设置前后检查并执行内存淘汰。
    ///
    /// # 参数
    /// - `key` - 键名
    /// - `value` - 字符串值
    ///
    /// # 返回值
    /// - `Ok(())` - 设置成功
    /// - `Err(AppError::Storage)` - 锁中毒或内存淘汰失败
    pub fn set(&self, key: String, value: Bytes) -> Result<()> {
        let has_maxmem = self.maxmemory.load(Ordering::Relaxed) > 0;
        if has_maxmem {
            self.evict_if_needed()?;
        }
        let db = &self.dbs[self.current_db.load(Ordering::Relaxed).min(15)];
        {
            let mut map = db
                .inner
                .get_shard(&key)
                .write()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            map.insert(key.clone(), StorageValue::String(value));
            let mut expires = db.expires.get_shard(&key).write().unwrap();
            expires.remove(&key);
            // version bump 合并到同一个 shard 锁作用域内减少锁竞争
            let new_ver = self.version_counter.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
            if let Ok(mut versions) = db.versions.get_shard(&key).write() {
                versions.insert(key.clone(), new_ver);
            }
        }
        if has_maxmem {
            db.access_times.write().unwrap().insert(key.clone(), Instant::now());
            *db.access_counts.write().unwrap().entry(key).or_insert(0) += 1;
            self.evict_if_needed()?;
        }
        Ok(())
    }

    /// 设置带过期时间的键值对（单位：毫秒）
    ///
    /// 将 key 设置为指定的字符串值，并指定其过期时间（毫秒）。
    /// 过期时间通过绝对时间戳（毫秒级 Unix 时间）存储。
    /// 如果 key 已存在则覆盖。
    ///
    /// # 参数
    /// - `key` - 键名
    /// - `value` - 字符串值
    /// - `ttl_ms` - 过期时间，单位为毫秒
    ///
    /// # 返回值
    /// - `Ok(())` - 设置成功
    /// - `Err(AppError::Storage)` - 锁中毒或内存淘汰失败
    ///
    /// # Redis 兼容性
    /// 对标 `SET key value PX ttl_ms` 和 `PSETEX` 命令。
    pub fn set_with_ttl(&self, key: String, value: Bytes, ttl_ms: u64) -> Result<()> {
        self.evict_if_needed()?;
        let expire_at = Self::now_millis().saturating_add(ttl_ms);
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(&key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
        map.insert(key.clone(), StorageValue::String(value));
        let mut expires = db.expires.get_shard(&key).write().unwrap();
        expires.insert(key.clone(), expire_at);
        self.bump_version(&key);
        self.touch(&key);
        drop(map);
        self.evict_if_needed()?;
        Ok(())
    }

    /// 带完整选项的 SET 操作（对标 Redis SET 命令全选项）
    ///
    /// 支持 NX（仅当不存在时设置）、XX（仅当存在时设置）、
    /// GET（返回旧值）、KEEPTTL（保留原有过期时间）、
    /// EX/PX/EXAT/PXAT（指定过期时间）等全部选项组合。
    ///
    /// # 参数
    /// - `key` - 键名
    /// - `value` - 要设置的字符串值
    /// - `options` - SET 选项，包含 nx/xx/get/keepttl/expire 等字段
    ///
    /// # 返回值
    /// - `Ok((true, old_value))` - 设置成功，old_value 仅在 get=true 时有意义
    /// - `Ok((false, old_value))` - 因 NX/XX 条件未设置，old_value 仅在 get=true 时有意义
    /// - `Err(AppError::Storage)` - 类型错误或锁中毒
    ///
    /// # 行为说明
    /// - `nx=true` 且 key 存在 → 不设置，返回 `(false, old_value)`
    /// - `xx=true` 且 key 不存在 → 不设置，返回 `(false, old_value)`
    /// - `get=true` → 无论是否设置成功，都返回旧值（如果 key 存在且未过期）
    /// - `keepttl=true` → 保留原有 TTL，忽略其他过期选项
    pub fn set_with_options(
        &self,
        key: String,
        value: Bytes,
        options: &SetOptions,
    ) -> Result<(bool, Option<Bytes>)> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(&key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let old_value = if options.get {
            match map.get(&key) {
                Some(v) if !Self::is_key_expired(&db, &key) => Some(match v {
                    StorageValue::String(b) => b.clone(),
                    _ => return Err(AppError::Storage(
                        "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                    )),
                }),
                _ => {
                    map.remove(&key);
                    let mut expires = db.expires.get_shard(&key).write().unwrap();
                    expires.remove(&key);
                    None
                }
            }
        } else {
            None
        };

        // 检查 NX / XX 条件
        let exists = match map.get(&key) {
            Some(_) if !Self::is_key_expired(&db, &key) => true,
            _ => {
                map.remove(&key);
                let mut expires = db.expires.get_shard(&key).write().unwrap();
                expires.remove(&key);
                false
            }
        };

        if options.nx && exists {
            return Ok((false, old_value));
        }
        if options.xx && !exists {
            return Ok((false, old_value));
        }

        // 计算过期时间
        let expire_at: Option<u64> = if options.keepttl {
            // 保留原有 TTL
            let expires = db.expires.get_shard(&key).read().unwrap();
            expires.get(&key).copied()
        } else {
            match &options.expire {
                Some(SetExpireOption::Ex(seconds)) => {
                    Some(Self::now_millis().saturating_add(*seconds * 1000))
                }
                Some(SetExpireOption::Px(millis)) => {
                    Some(Self::now_millis().saturating_add(*millis))
                }
                Some(SetExpireOption::ExAt(timestamp_secs)) => Some(*timestamp_secs * 1000),
                Some(SetExpireOption::PxAt(timestamp_ms)) => Some(*timestamp_ms),
                None => None,
            }
        };

        map.insert(key.clone(), StorageValue::String(value));
        {
            let mut expires = db.expires.get_shard(&key).write().unwrap();
            match expire_at {
                Some(at) => { expires.insert(key.clone(), at); }
                None => { expires.remove(&key); }
            }
        }
        self.bump_version(&key);
        self.touch(&key);
        drop(map);
        self.evict_if_needed()?;
        Ok((true, old_value))
    }

    /// 最长公共子串（LCS，对标 Redis LCS 命令）
    ///
    /// 计算并返回两个字符串值的最长公共子串（Longest Common Substring）。
    /// 使用动态规划算法实现，时间复杂度 O(N×M)。
    ///
    /// # 参数
    /// - `key1` - 第一个键名
    /// - `key2` - 第二个键名
    ///
    /// # 返回值
    /// - `Ok(Some(String))` - 两个键都存在，返回最长公共子串（可能为空字符串）
    /// - `Ok(None)` - 任一键不存在或已过期
    /// - `Err(AppError::Storage)` - 键不是字符串类型或包含非法 UTF-8
    pub fn lcs(&self, key1: &str, key2: &str) -> Result<Option<String>> {
        let v1 = match self.get(key1)? {
            Some(b) => match std::str::from_utf8(&b) {
                Ok(s) => s.to_string(),
                Err(_) => return Err(AppError::Storage(
                    "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                )),
            },
            None => return Ok(None),
        };
        let v2 = match self.get(key2)? {
            Some(b) => match std::str::from_utf8(&b) {
                Ok(s) => s.to_string(),
                Err(_) => return Err(AppError::Storage(
                    "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                )),
            },
            None => return Ok(None),
        };

        if v1.is_empty() || v2.is_empty() {
            return Ok(Some("".to_string()));
        }

        let s1: Vec<char> = v1.chars().collect();
        let s2: Vec<char> = v2.chars().collect();
        let mut dp = vec![vec![0usize; s2.len() + 1]; s1.len() + 1];
        let mut max_len = 0usize;
        let mut end_pos = 0usize;

        for i in 1..=s1.len() {
            for j in 1..=s2.len() {
                if s1[i - 1] == s2[j - 1] {
                    dp[i][j] = dp[i - 1][j - 1] + 1;
                    if dp[i][j] > max_len {
                        max_len = dp[i][j];
                        end_pos = i;
                    }
                }
            }
        }

        if max_len == 0 {
            return Ok(Some("".to_string()));
        }

        let lcs_str: String = s1[end_pos - max_len..end_pos].iter().collect();
        Ok(Some(lcs_str))
    }

    /// 为已有键设置过期时间（对标 Redis EXPIRE 命令）
    ///
    /// 为已存在的键设置秒级过期时间。如果键已设置过期时间，则覆盖。
    /// 仅支持字符串类型（包括普通字符串和带过期时间的字符串）。
    ///
    /// # 参数
    /// - `key` - 键名
    /// - `seconds` - 过期时间，单位为秒
    ///
    /// # 返回值
    /// - `Ok(true)` - 设置成功
    /// - `Ok(false)` - 键不存在或已过期
    /// - `Err(AppError::Storage)` - 键不是字符串类型或锁中毒
    pub fn expire(&self, key: &str, seconds: u64) -> Result<bool> {
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
                    let new_expire_at = Self::now_millis().saturating_add(seconds * 1000);
                    let mut expires = db.expires.get_shard(key).write().unwrap();
                    expires.insert(key.to_string(), new_expire_at);
                    Ok(true)
                }
            }
            None => Ok(false),
        }
    }

    /// 返回键的剩余生存时间（对标 Redis TTL 命令）
    ///
    /// 查询 key 的剩余生存时间，单位为毫秒。
    /// 如果 key 已过期，会在查询时自动删除。
    ///
    /// # 参数
    /// - `key` - 键名
    ///
    /// # 返回值
    /// - `Ok(正数)` - 剩余毫秒数
    /// - `Ok(-1)` - 键存在但没有设置过期时间
    /// - `Ok(-2)` - 键不存在或已过期
    /// - `Err(AppError::Storage)` - 键不是字符串类型或锁中毒
    ///
    /// # Redis 兼容性
    /// 注意：Redis TTL 命令返回秒，此函数返回毫秒，对应 Redis PTTL。
    pub fn ttl(&self, key: &str) -> Result<i64> {
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
                        Some(&expire_at) => {
                            let remaining = expire_at as i64 - Self::now_millis() as i64;
                            Ok(remaining.max(0))
                        }
                        None => Ok(-1),
                    }
                }
            }
            None => Ok(-2),
        }
    }

    /// 删除指定键（对标 Redis DEL 命令）
    ///
    /// 删除指定的 key。如果 key 已过期，会先清理再返回 false。
    /// 支持删除任意类型的键（String、List、Hash、Set、ZSet、HyperLogLog、Stream）。
    /// 删除时会同步清理 Hash 字段级过期记录和阻塞等待者。
    ///
    /// # 参数
    /// - `key` - 要删除的键名
    ///
    /// # 返回值
    /// - `Ok(true)` - 键存在且未过期，删除成功
    /// - `Ok(false)` - 键不存在或已过期
    /// - `Err(AppError::Storage)` - 锁中毒
    pub fn del(&self, key: &str) -> Result<bool> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        // 先检查键是否存在且未过期
        let exists_and_not_expired = match map.get(key) {
            Some(_) => !Self::is_key_expired(&db, key),
            None => false,
        };

        if !exists_and_not_expired {
            // 若已过期则一并清理
            map.remove(key);
            let mut expires = db.expires.get_shard(key).write().unwrap();
            expires.remove(key);
            return Ok(false);
        }

        // 删除键并更新版本号
        map.remove(key);
        let mut expires = db.expires.get_shard(key).write().unwrap();
        expires.remove(key);
        drop(expires);
        self.bump_version(key);

        // 清理 Hash 字段级过期记录
        let mut hash_exp = db.hash_field_expirations.write().unwrap();
        hash_exp.remove(key);
        drop(hash_exp);

        // 清理阻塞等待者
        let mut waiters = db.blocking_waiters.write().unwrap();
        waiters.remove(key);

        Ok(true)
    }

    /// 检查键是否存在（对标 Redis EXISTS 命令）
    ///
    /// 检查 key 是否存在且未过期。如果 key 已过期，会在检查时自动删除并返回 false。
    ///
    /// # 参数
    /// - `key` - 键名
    ///
    /// # 返回值
    /// - `Ok(true)` - 键存在且未过期
    /// - `Ok(false)` - 键不存在或已过期
    /// - `Err(AppError::Storage)` - 锁中毒
    pub fn exists(&self, key: &str) -> Result<bool> {
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
                    Ok(true)
                }
            }
            None => Ok(false),
        }
    }

    /// 清空所有数据（对标 Redis FLUSHALL 命令）
    ///
    /// 清空所有 16 个数据库中的全部数据，包括键值对、版本号、访问时间和访问次数。
    /// 此操作不可撤销，请谨慎使用。
    ///
    /// # 返回值
    /// - `Ok(())` - 清空成功
    /// - `Err(AppError::Storage)` - 锁中毒
    pub fn flush(&self) -> Result<()> {
        for db in self.dbs.iter() {
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
            let mut times = db.access_times.write().unwrap();
            times.clear();
            let mut counts = db.access_counts.write().unwrap();
            counts.clear();
        }
        Ok(())
    }

    /// 批量获取多个键的值（对标 Redis MGET 命令）
    ///
    /// 同时获取多个 key 的字符串值。对于不存在的键或已过期键，对应位置返回 None。
    /// 非字符串类型的键对应位置也返回 None（不会返回 WRONGTYPE 错误）。
    ///
    /// # 参数
    /// - `keys` - 键名列表
    ///
    /// # 返回值
    /// - `Ok(Vec<Option<Bytes>>)` - 与输入 keys 顺序对应的结果列表
    /// - `Err(AppError::Storage)` - 锁中毒
    pub fn mget(&self, keys: &[String]) -> Result<Vec<Option<Bytes>>> {
        let db = self.db();
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let mut map = db
                .inner
                .get_shard(key)
                .write()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            match map.get(key.as_str()) {
                Some(value) => {
                    if Self::is_key_expired(&db, key) {
                        map.remove(key);
                        let mut expires = db.expires.get_shard(key).write().unwrap();
                        expires.remove(key);
                        results.push(None);
                    } else {
                        match value {
                            StorageValue::String(v) => results.push(Some(v.clone())),

                            StorageValue::List(_) | StorageValue::Hash(_) | StorageValue::Set(_) | StorageValue::ZSet(_) | StorageValue::HyperLogLog(_) | StorageValue::Stream(_) => {
                                results.push(None)
                            }
                        }
                    }
                }
                None => results.push(None),
            }
        }
        Ok(results)
    }

    /// 批量设置多个键值对（对标 Redis MSET 命令）
    ///
    /// 同时设置多个 key-value 对。如果某个 key 已存在则覆盖。
    /// 所有新设置的键均为持久键（无过期时间）。
    ///
    /// # 参数
    /// - `pairs` - 键值对列表，每个元素为 `(key, value)`
    ///
    /// # 返回值
    /// - `Ok(())` - 设置成功
    /// - `Err(AppError::Storage)` - 锁中毒
    pub fn mset(&self, pairs: &[(String, Bytes)]) -> Result<()> {
        let db = self.db();
        for (key, value) in pairs {
            let mut map = db
                .inner
                .get_shard(key)
                .write()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            map.insert(key.clone(), StorageValue::String(value.clone()));
        }
        Ok(())
    }

    /// 将存储值解析为 i64，不存在则返回 0
    ///
    /// 内部辅助函数，用于 INCR/DECR/INCRBY/DECRBY 等命令。
    /// 支持普通字符串和带过期时间的字符串。键不存在时视为 0。
    ///
    /// # 参数
    /// - `value` - 存储值引用，None 表示键不存在
    ///
    /// # 返回值
    /// - `Ok(i64)` - 解析成功或键不存在（返回 0）
    /// - `Err(AppError::Storage)` - 值不是有效的整数字符串或类型错误
    fn parse_int_value(value: Option<&StorageValue>) -> Result<i64> {
        match value {
            Some(StorageValue::String(v)) => {
                String::from_utf8_lossy(v)
                    .parse()
                    .map_err(|_| {
                        AppError::Storage("值不是有效的整数字符串".to_string())
                    })
            }

            Some(StorageValue::List(_))
            | Some(StorageValue::Hash(_))
            | Some(StorageValue::Set(_))
            | Some(StorageValue::ZSet(_))
            | Some(StorageValue::HyperLogLog(_))
            | Some(StorageValue::Stream(_)) => {
                Err(AppError::Storage(
                    "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                ))
            }
            None => Ok(0),
        }
    }

    /// 递增键的值（对标 Redis INCR 命令）
    ///
    /// 将 key 中存储的整数值增 1。如果 key 不存在，则先将其设为 0 再执行递增。
    /// 使用饱和加法，不会溢出。
    ///
    /// # 参数
    /// - `key` - 键名
    ///
    /// # 返回值
    /// - `Ok(i64)` - 递增后的新值
    /// - `Err(AppError::Storage)` - 值不是有效整数、类型错误或锁中毒
    pub fn incr(&self, key: &str) -> Result<i64> {
        self.incrby(key, 1)
    }

    /// 递减键的值（对标 Redis DECR 命令）
    ///
    /// 将 key 中存储的整数值减 1。如果 key 不存在，则先将其设为 0 再执行递减。
    /// 使用饱和减法，不会下溢。
    ///
    /// # 参数
    /// - `key` - 键名
    ///
    /// # 返回值
    /// - `Ok(i64)` - 递减后的新值
    /// - `Err(AppError::Storage)` - 值不是有效整数、类型错误或锁中毒
    pub fn decr(&self, key: &str) -> Result<i64> {
        self.decrby(key, 1)
    }

    /// 按指定步长递增键的值（对标 Redis INCRBY 命令）
    ///
    /// 将 key 中存储的整数值增加 delta。如果 key 不存在，则先将其设为 0 再执行递增。
    /// 使用饱和加法，不会溢出。
    ///
    /// # 参数
    /// - `key` - 键名
    /// - `delta` - 增量（可为负数，实现递减效果）
    ///
    /// # 返回值
    /// - `Ok(i64)` - 递增后的新值
    /// - `Err(AppError::Storage)` - 值不是有效整数、类型错误或锁中毒
    pub fn incrby(&self, key: &str, delta: i64) -> Result<i64> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let current = match map.get(key) {
            Some(value) => {
                if Self::is_key_expired(&db, key) {
                    map.remove(key);
                    let mut expires = db.expires.get_shard(key).write().unwrap();
                    expires.remove(key);
                    0i64
                } else {
                    match value {
                        StorageValue::List(_) => {
                            return Err(AppError::Storage(
                                "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                            ))
                        }
                        _ => Self::parse_int_value(Some(value))?,
                    }
                }
            }
            None => 0i64,
        };

        let new_val = current.saturating_add(delta);
        map.insert(
            key.to_string(),
            StorageValue::String(Bytes::from(new_val.to_string())),
        );
        let mut expires = db.expires.get_shard(key).write().unwrap();
        expires.remove(key);
        self.bump_version(key);
        self.touch(key);
        Ok(new_val)
    }

    /// 按指定步长递减键的值（对标 Redis DECRBY 命令）
    ///
    /// 将 key 中存储的整数值减少 delta。如果 key 不存在，则先将其设为 0 再执行递减。
    /// 内部通过调用 `incrby(key, -delta)` 实现。
    ///
    /// # 参数
    /// - `key` - 键名
    /// - `delta` - 减量
    ///
    /// # 返回值
    /// - `Ok(i64)` - 递减后的新值
    /// - `Err(AppError::Storage)` - 值不是有效整数、类型错误或锁中毒
    pub fn decrby(&self, key: &str, delta: i64) -> Result<i64> {
        self.incrby(key, -delta)
    }

    /// 追加值到字符串末尾（对标 Redis APPEND 命令）
    ///
    /// 将 value 追加到 key 原有值的末尾。如果 key 不存在，则将其设为空字符串后再追加。
    /// 追加后返回新字符串的长度。
    ///
    /// # 参数
    /// - `key` - 键名
    /// - `value` - 要追加的字节值
    ///
    /// # 返回值
    /// - `Ok(usize)` - 追加后字符串的新长度
    /// - `Err(AppError::Storage)` - 键不是字符串类型或锁中毒
    pub fn append(&self, key: &str, value: Bytes) -> Result<usize> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let existing = match map.get(key) {
            Some(v) => {
                if Self::is_key_expired(&db, key) {
                    map.remove(key);
                    let mut expires = db.expires.get_shard(key).write().unwrap();
                    expires.remove(key);
                    Bytes::new()
                } else {
                    match v {
                        StorageValue::String(b) => b.clone(),

                        StorageValue::List(_) | StorageValue::Hash(_) | StorageValue::Set(_) | StorageValue::ZSet(_) | StorageValue::HyperLogLog(_) | StorageValue::Stream(_) => {
                            return Err(AppError::Storage(
                                "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                            ))
                        }
                    }
                }
            }
            None => Bytes::new(),
        };

        let mut new_value = existing.to_vec();
        new_value.extend_from_slice(&value);
        let new_len = new_value.len();
        map.insert(
            key.to_string(),
            StorageValue::String(Bytes::from(new_value)),
        );
        let mut expires = db.expires.get_shard(key).write().unwrap();
        expires.remove(key);
        self.bump_version(key);
        self.touch(key);
        Ok(new_len)
    }
}

impl StorageEngine {
    /// 仅在键不存在时设置值（对标 Redis SETNX 命令）
    ///
    /// 将 key 的值设为 value，仅当 key 不存在时生效。
    /// 如果 key 已过期，会先删除过期键再设置。
    ///
    /// # 参数
    /// - `key` - 键名
    /// - `value` - 要设置的字符串值
    ///
    /// # 返回值
    /// - `Ok(true)` - 设置成功（key 原本不存在或已过期）
    /// - `Ok(false)` - key 已存在且未过期，未执行设置
    /// - `Err(AppError::Storage)` - 锁中毒
    pub fn setnx(&self, key: String, value: Bytes) -> Result<bool> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(&key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(&key) {
            Some(_v) => {
                if Self::is_key_expired(&db, &key) {
                    map.remove(&key);
                    let mut expires = db.expires.get_shard(&key).write().unwrap();
                    expires.remove(&key);
                    map.insert(key.clone(), StorageValue::String(value));
                    self.bump_version(&key);
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            None => {
                map.insert(key.clone(), StorageValue::String(value));
                let mut expires = db.expires.get_shard(&key).write().unwrap();
                expires.remove(&key);
                self.bump_version(&key);
                Ok(true)
            }
        }
    }

    /// 设置秒级过期时间的键值对（对标 Redis SETEX 命令）
    ///
    /// 将 key 设置为 value，并指定秒级过期时间。
    /// 如果 key 已存在则覆盖。
    ///
    /// # 参数
    /// - `key` - 键名
    /// - `seconds` - 过期时间，单位为秒
    /// - `value` - 要设置的字符串值
    ///
    /// # 返回值
    /// - `Ok(())` - 设置成功
    /// - `Err(AppError::Storage)` - 锁中毒或内存淘汰失败
    pub fn setex(&self, key: String, seconds: u64, value: Bytes) -> Result<()> {
        self.set_with_ttl(key, value, seconds * 1000)
    }

    /// 设置毫秒级过期时间的键值对（对标 Redis PSETEX 命令）
    ///
    /// 将 key 设置为 value，并指定毫秒级过期时间。
    /// 如果 key 已存在则覆盖。
    ///
    /// # 参数
    /// - `key` - 键名
    /// - `ms` - 过期时间，单位为毫秒
    /// - `value` - 要设置的字符串值
    ///
    /// # 返回值
    /// - `Ok(())` - 设置成功
    /// - `Err(AppError::Storage)` - 锁中毒或内存淘汰失败
    pub fn psetex(&self, key: String, ms: u64, value: Bytes) -> Result<()> {
        self.set_with_ttl(key, value, ms)
    }

    /// 设置新值并返回旧值（对标 Redis GETSET 命令）
    ///
    /// 将 key 设为 value，并返回 key 的旧值。
    /// 如果 key 不存在，则返回 None 并设置新值。
    /// 如果 key 已过期，会先删除再返回 None 并设置新值。
    /// 新值不会保留原有过期时间（变为持久键）。
    ///
    /// # 参数
    /// - `key` - 键名
    /// - `value` - 新值
    ///
    /// # 返回值
    /// - `Ok(Some(Bytes))` - 返回旧值
    /// - `Ok(None)` - 键不存在或已过期
    /// - `Err(AppError::Storage)` - 键不是字符串类型或锁中毒
    pub fn getset(&self, key: &str, value: Bytes) -> Result<Option<Bytes>> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let old = match map.get(key) {
            Some(v) => {
                if Self::is_key_expired(&db, key) {
                    map.remove(key);
                    let mut expires = db.expires.get_shard(key).write().unwrap();
                    expires.remove(key);
                    None
                } else {
                    match v {
                        StorageValue::String(b) => {
                            Some(b.clone())
                        }
                        _ => {
                            return Err(AppError::Storage(
                                "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                            ))
                        }
                    }
                }
            }
            None => None,
        };

        map.insert(key.to_string(), StorageValue::String(value));
        let mut expires = db.expires.get_shard(key).write().unwrap();
        expires.remove(key);
        self.bump_version(key);
        self.touch(key);
        drop(map);
        self.evict_if_needed()?;
        Ok(old)
    }

    /// 获取值并删除键（对标 Redis GETDEL 命令，Redis 6.2+）
    ///
    /// 返回 key 的值，并在返回前删除该 key。
    /// 如果 key 不存在或已过期，返回 None。
    ///
    /// # 参数
    /// - `key` - 键名
    ///
    /// # 返回值
    /// - `Ok(Some(Bytes))` - 键存在，返回其值并删除
    /// - `Ok(None)` - 键不存在或已过期
    /// - `Err(AppError::Storage)` - 键不是字符串类型或锁中毒
    pub fn getdel(&self, key: &str) -> Result<Option<Bytes>> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let result = match map.get(key) {
            Some(v) => {
                if Self::is_key_expired(&db, key) {
                    map.remove(key);
                    let mut expires = db.expires.get_shard(key).write().unwrap();
                    expires.remove(key);
                    None
                } else {
                    match v {
                        StorageValue::String(b) => {
                            Some(b.clone())
                        }
                        _ => {
                            return Err(AppError::Storage(
                                "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                            ))
                        }
                    }
                }
            }
            None => None,
        };

        if result.is_some() {
            map.remove(key);
            let mut expires = db.expires.get_shard(key).write().unwrap();
            expires.remove(key);
            self.bump_version(key);
        }
        Ok(result)
    }

    /// 获取值并修改过期时间（对标 Redis GETEX 命令，Redis 6.2+）
    ///
    /// 返回 key 的值，并根据选项修改其过期时间。
    /// 支持 Persist（移除过期时间）、Ex/Px（设置相对过期时间）、ExAt/PxAt（设置绝对过期时间）。
    ///
    /// # 参数
    /// - `key` - 键名
    /// - `option` - 过期时间选项
    ///
    /// # 返回值
    /// - `Ok(Some(Bytes))` - 键存在，返回其值并修改过期时间
    /// - `Ok(None)` - 键不存在或已过期
    /// - `Err(AppError::Storage)` - 键不是字符串类型或锁中毒
    pub fn getex(&self, key: &str, option: GetExOption) -> Result<Option<Bytes>> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let result = match map.get(key) {
            Some(v) => {
                if Self::is_key_expired(&db, key) {
                    map.remove(key);
                    let mut expires = db.expires.get_shard(key).write().unwrap();
                    expires.remove(key);
                    None
                } else {
                    match v {
                        StorageValue::String(b) => {
                            Some(b.clone())
                        }
                        _ => {
                            return Err(AppError::Storage(
                                "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                            ))
                        }
                    }
                }
            }
            None => None,
        };

        if result.is_some() {
            let mut expires = db.expires.get_shard(key).write().unwrap();
            match option {
                GetExOption::Persist => {
                    expires.remove(key);
                }
                GetExOption::Ex(seconds) => {
                    let expire_at = Self::now_millis().saturating_add(seconds * 1000);
                    expires.insert(key.to_string(), expire_at);
                }
                GetExOption::Px(ms) => {
                    let expire_at = Self::now_millis().saturating_add(ms);
                    expires.insert(key.to_string(), expire_at);
                }
                GetExOption::ExAt(timestamp) => {
                    let expire_at = timestamp * 1000;
                    expires.insert(key.to_string(), expire_at);
                }
                GetExOption::PxAt(ms_timestamp) => {
                    expires.insert(key.to_string(), ms_timestamp);
                }
            }
            self.bump_version(key);
            self.touch(key);
        }

        Ok(result)
    }

    /// 仅当所有 key 都不存在时才批量设置（对标 Redis MSETNX 命令）
    ///
    /// 原子性地同时设置多个 key-value 对，仅当所有给定 key 都不存在时才执行。
    /// 如果任一 key 已存在且未过期，则不做任何设置，返回 0。
    ///
    /// # 参数
    /// - `pairs` - 键值对列表
    ///
    /// # 返回值
    /// - `Ok(1)` - 所有 key 都不存在，设置成功
    /// - `Ok(0)` - 至少一个 key 已存在，未执行设置
    /// - `Err(AppError::Storage)` - 锁中毒
    pub fn msetnx(&self, pairs: &[(String, Bytes)]) -> Result<i64> {
        self.evict_if_needed()?;
        let db = self.db();
        // 检查所有 key 是否都不存在（或已过期）
        for (key, _) in pairs {
            let map = db
                .inner
                .get_shard(key)
                .write()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            if map.get(key).is_some()
                && !Self::is_key_expired(&db, key) {
                    return Ok(0);
                }
        }

        // 全部设置
        for (key, value) in pairs {
            let mut map = db
                .inner
                .get_shard(key)
                .write()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            map.insert(key.clone(), StorageValue::String(value.clone()));
            let mut expires = db.expires.get_shard(key).write().unwrap();
            expires.remove(key);
            self.bump_version(key);
        }
        self.evict_if_needed()?;
        Ok(1)
    }

    /// 浮点数递增（对标 Redis INCRBYFLOAT 命令）
    ///
    /// 将 key 中存储的浮点数值增加 delta。如果 key 不存在，则先将其设为 0.0 再执行递增。
    /// 返回新值的字符串表示，去除末尾多余的 0 以兼容 Redis 行为。
    ///
    /// # 参数
    /// - `key` - 键名
    /// - `delta` - 浮点增量
    ///
    /// # 返回值
    /// - `Ok(String)` - 递增后的新值字符串
    /// - `Err(AppError::Storage)` - 值不是有效浮点数、类型错误或锁中毒
    pub fn incrbyfloat(&self, key: &str, delta: f64) -> Result<String> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let current = match map.get(key) {
            Some(value) => {
                if Self::is_key_expired(&db, key) {
                    map.remove(key);
                    let mut expires = db.expires.get_shard(key).write().unwrap();
                    expires.remove(key);
                    0.0
                } else {
                    match value {
                        StorageValue::String(b) => {
                            String::from_utf8_lossy(b)
                                .parse::<f64>()
                                .map_err(|_| {
                                    AppError::Storage("值不是有效的浮点数字符串".to_string())
                                })?
                        }
                        _ => {
                            return Err(AppError::Storage(
                                "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                            ))
                        }
                    }
                }
            }
            None => 0.0,
        };

        let new_val = current + delta;
        // 去除末尾多余的 0，与 Redis 行为一致
        let new_str = format_float(new_val);
        map.insert(
            key.to_string(),
            StorageValue::String(Bytes::from(new_str.clone())),
        );
        let mut expires = db.expires.get_shard(key).write().unwrap();
        expires.remove(key);
        self.bump_version(key);
        self.touch(key);
        Ok(new_str)
    }

    /// 从指定偏移覆盖写入（对标 Redis SETRANGE 命令）
    ///
    /// 从 offset 开始用 value 覆盖 key 原有值的内容。
    /// 如果 offset 超过原值长度，中间不足部分用 `\x00` 填充。
    /// 如果 key 不存在，则将其视为空字符串后执行覆盖。
    ///
    /// # 参数
    /// - `key` - 键名
    /// - `offset` - 起始偏移位置（从 0 开始）
    /// - `value` - 要覆盖写入的字节值
    ///
    /// # 返回值
    /// - `Ok(usize)` - 修改后字符串的新长度
    /// - `Err(AppError::Storage)` - 键不是字符串类型或锁中毒
    pub fn setrange(&self, key: &str, offset: usize, value: Bytes) -> Result<usize> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let mut existing = match map.get(key) {
            Some(v) => {
                if Self::is_key_expired(&db, key) {
                    map.remove(key);
                    let mut expires = db.expires.get_shard(key).write().unwrap();
                    expires.remove(key);
                    Vec::new()
                } else {
                    match v {
                        StorageValue::String(b) => {
                            b.to_vec()
                        }
                        _ => {
                            return Err(AppError::Storage(
                                "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                            ))
                        }
                    }
                }
            }
            None => Vec::new(),
        };

        // 确保 existing 长度至少到 offset + value.len()
        let end = offset + value.len();
        if existing.len() < end {
            existing.resize(end, 0);
        }

        // 覆盖写入
        existing[offset..end].copy_from_slice(&value);

        let new_len = existing.len();
        map.insert(
            key.to_string(),
            StorageValue::String(Bytes::from(existing)),
        );
        let mut expires = db.expires.get_shard(key).write().unwrap();
        expires.remove(key);
        self.bump_version(key);
        self.touch(key);
        Ok(new_len)
    }

    /// 获取字符串的子范围（对标 Redis GETRANGE 命令）
    ///
    /// 返回 key 对应字符串值从 start 到 end 之间的子串。
    /// 支持负数索引（-1 表示最后一个字符）。
    /// 如果 start 大于 end，返回空字符串。
    ///
    /// # 参数
    /// - `key` - 键名
    /// - `start` - 起始位置（包含），支持负数
    /// - `end` - 结束位置（包含），支持负数
    ///
    /// # 返回值
    /// - `Ok(Some(Bytes))` - 返回子范围字节值（可能为空）
    /// - `Ok(None)` - 键不存在或已过期
    /// - `Err(AppError::Storage)` - 键不是字符串类型或锁中毒
    pub fn getrange(&self, key: &str, start: i64, end: i64) -> Result<Option<Bytes>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let bytes = match map.get(key) {
            Some(v) => {
                if Self::is_key_expired(&db, key) {
                    map.remove(key);
                    let mut expires = db.expires.get_shard(key).write().unwrap();
                    expires.remove(key);
                    return Ok(None);
                }
                match v {
                    StorageValue::String(b) => b.clone(),
                    StorageValue::List(_) | StorageValue::Hash(_) | StorageValue::Set(_) | StorageValue::ZSet(_) | StorageValue::HyperLogLog(_) | StorageValue::Stream(_) => {
                        return Err(AppError::Storage(
                            "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                        ))
                    }
                }
            }
            None => return Ok(None),
        };

        let len = bytes.len() as i64;
        // 处理负数索引
        let mut s = start;
        let mut e = end;
        if s < 0 {
            s += len;
        }
        if e < 0 {
            e += len;
        }
        //  clamp
        s = s.max(0);
        e = e.min(len - 1);

        if s > e {
            return Ok(Some(Bytes::new()));
        }

        let start_idx = s as usize;
        let end_idx = e as usize;
        Ok(Some(Bytes::copy_from_slice(
            &bytes[start_idx..=end_idx],
        )))
    }

    /// 返回字符串值的长度（对标 Redis STRLEN 命令）
    ///
    /// 返回 key 对应字符串值的字节长度。
    /// 如果 key 不存在或已过期，返回 0。
    ///
    /// # 参数
    /// - `key` - 键名
    ///
    /// # 返回值
    /// - `Ok(usize)` - 字符串的字节长度
    /// - `Err(AppError::Storage)` - 键不是字符串类型或锁中毒
    pub fn strlen(&self, key: &str) -> Result<usize> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if Self::is_key_expired(&db, key) {
                    map.remove(key);
                    let mut expires = db.expires.get_shard(key).write().unwrap();
                    expires.remove(key);
                    Ok(0)
                } else {
                    match v {
                        StorageValue::String(b) => Ok(b.len()),
                        StorageValue::List(_) | StorageValue::Hash(_) | StorageValue::Set(_) | StorageValue::ZSet(_) | StorageValue::HyperLogLog(_) | StorageValue::Stream(_) => {
                            Err(AppError::Storage(
                                "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                            ))
                        }
                    }
                }
            }
            None => Ok(0),
        }
    }
}
