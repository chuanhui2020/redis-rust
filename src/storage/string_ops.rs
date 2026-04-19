use super::*;

impl StorageEngine {
    pub fn get(&self, key: &str) -> Result<Option<Bytes>> {
        let dbs = &self.dbs;
        let db = &dbs[self.current_db.load(Ordering::Relaxed).min(15)];
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        // 先检查键是否存在以及是否过期
        if let Some(value) = map.get(key) {
            if Self::is_expired(value) {
                map.remove(key);
                if let Some(ref notifier) = self.keyspace_notifier {
                    let db_idx = self.current_db.load(Ordering::Relaxed);
                    notifier.notify_expired(db_idx, key);
                }
                return Ok(None);
            }
        }

        // 取出值
        let result = match map.get(key) {
            Some(StorageValue::String(v)) => Ok(Some(v.clone())),
            Some(StorageValue::ExpiringString(v, _)) => Ok(Some(v.clone())),
            Some(StorageValue::List(_))
            | Some(StorageValue::Hash(_))
            | Some(StorageValue::Set(_))
            | Some(StorageValue::ZSet(_))
            | Some(StorageValue::HyperLogLog(_))
            | Some(StorageValue::Stream(_))
            | Some(StorageValue::Stream(_)) => {
                Err(AppError::Storage(
                    "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                ))
            }
            None => Ok(None),
        };
        // 内联 touch，只在 maxmemory > 0 时执行，复用 db 引用
        if result.is_ok() && self.maxmemory.load(Ordering::Relaxed) > 0 {
            db.access_times.write().unwrap().insert(key.to_string(), Instant::now());
            *db.access_counts.write().unwrap().entry(key.to_string()).or_insert(0) += 1;
        }
        result
    }

    /// 设置键值对
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
        }
        let new_ver = self.version_counter.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        if let Ok(mut versions) = db.versions.get_shard(&key).write() {
            versions.insert(key.clone(), new_ver);
        }
        if has_maxmem {
            db.access_times.write().unwrap().insert(key.clone(), Instant::now());
            *db.access_counts.write().unwrap().entry(key).or_insert(0) += 1;
            self.evict_if_needed()?;
        }
        Ok(())
    }

    /// 设置带过期时间的键值对（单位：毫秒）
    pub fn set_with_ttl(&self, key: String, value: Bytes, ttl_ms: u64) -> Result<()> {
        self.evict_if_needed()?;
        let expire_at = Self::now_millis().saturating_add(ttl_ms);
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(&key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
        map.insert(key.clone(), StorageValue::ExpiringString(value, expire_at));
        self.bump_version(&key);
        self.touch(&key);
        drop(map);
        self.evict_if_needed()?;
        Ok(())
    }

    /// 带完整选项的 SET 操作
    /// 返回 (设置成功, 旧值)
    /// - nx=true 且 key 存在 → (false, None)
    /// - xx=true 且 key 不存在 → (false, None)
    /// - get=true → 返回旧值（即使未设置也可能返回旧值）
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
                Some(v) if !Self::is_expired(v) => Some(match v {
                    StorageValue::String(b) | StorageValue::ExpiringString(b, _) => b.clone(),
                    _ => return Err(AppError::Storage(
                        "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                    )),
                }),
                _ => {
                    map.remove(&key);
                    None
                }
            }
        } else {
            None
        };

        // 检查 NX / XX 条件
        let exists = match map.get(&key) {
            Some(v) if !Self::is_expired(v) => true,
            _ => {
                map.remove(&key);
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
            match map.get(&key) {
                Some(StorageValue::ExpiringString(_, ea)) => Some(*ea),
                _ => None,
            }
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

        let new_value = match expire_at {
            Some(at) => StorageValue::ExpiringString(value, at),
            None => StorageValue::String(value),
        };

        map.insert(key.clone(), new_value);
        self.bump_version(&key);
        self.touch(&key);
        drop(map);
        self.evict_if_needed()?;
        Ok((true, old_value))
    }

    /// 最长公共子串（LCS）
    /// 返回两个字符串值的最长公共子串
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

    /// 为已有键设置过期时间（单位：秒）
    /// 成功返回 true，键不存在或已过期返回 false
    pub fn expire(&self, key: &str, seconds: u64) -> Result<bool> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(value) => {
                if Self::is_expired(value) {
                    map.remove(key);
                    Ok(false)
                } else {
                    let new_expire_at = Self::now_millis().saturating_add(seconds * 1000);
                    let new_value = match value {
                        StorageValue::String(v) => {
                            StorageValue::ExpiringString(v.clone(), new_expire_at)
                        }
                        StorageValue::ExpiringString(v, _) => {
                            StorageValue::ExpiringString(v.clone(), new_expire_at)
                        }
                        StorageValue::List(_) | StorageValue::Hash(_) | StorageValue::Set(_) | StorageValue::ZSet(_) | StorageValue::HyperLogLog(_) | StorageValue::Stream(_) => {
                            return Err(AppError::Storage(
                                "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                            ))
                        }
                    };
                    map.insert(key.to_string(), new_value);
                    Ok(true)
                }
            }
            None => Ok(false),
        }
    }

    /// 返回键的剩余生存时间（单位：毫秒）
    /// - 返回正数：剩余毫秒数
    /// - 返回 -1：键存在但没有设置过期时间
    /// - 返回 -2：键不存在或已过期
    pub fn ttl(&self, key: &str) -> Result<i64> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(value) => {
                if Self::is_expired(value) {
                    map.remove(key);
                    Ok(-2)
                } else {
                    match value {
                        StorageValue::String(_) => Ok(-1),
                        StorageValue::ExpiringString(_, expire_at) => {
                            let remaining =
                                *expire_at as i64 - Self::now_millis() as i64;
                            Ok(remaining.max(0))
                        }
                        StorageValue::List(_) | StorageValue::Hash(_) | StorageValue::Set(_) | StorageValue::ZSet(_) | StorageValue::HyperLogLog(_) | StorageValue::Stream(_) => {
                            Err(AppError::Storage(
                                "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                            ))
                        }
                    }
                }
            }
            None => Ok(-2),
        }
    }

    /// 删除指定键，返回该键是否存在（未过期的才算存在）
    pub fn del(&self, key: &str) -> Result<bool> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(value) => {
                if Self::is_expired(value) {
                    map.remove(key);
                    Ok(false)
                } else {
                    match value {
                        StorageValue::List(_) | StorageValue::Hash(_) | StorageValue::Set(_) | StorageValue::ZSet(_) | StorageValue::HyperLogLog(_) | StorageValue::Stream(_) => Err(AppError::Storage(
                            "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                        )),
                        _ => {
                            map.remove(key);
                            self.bump_version(key);
                            self.touch(key);
                            Ok(true)
                        }
                    }
                }
            }
            None => Ok(false),
        }
    }

    /// 检查键是否存在（过期的不算）
    pub fn exists(&self, key: &str) -> Result<bool> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(value) => {
                if Self::is_expired(value) {
                    map.remove(key);
                    Ok(false)
                } else {
                    Ok(true)
                }
            }
            None => Ok(false),
        }
    }

    /// 清空所有数据
    pub fn flush(&self) -> Result<()> {
        let db = self.db();
        for shard in db.inner.all_shards() {
            let mut map = shard.write().map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            map.clear();
        }
        let db = self.db();
        for shard in db.versions.all_shards() {
            let mut v = shard.write().map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            v.clear();
        }
        let db = self.db();
        let mut times = db.access_times.write().unwrap();
        times.clear();
        Ok(())
    }

    /// 批量获取多个键的值
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
                    if Self::is_expired(value) {
                        map.remove(key);
                        results.push(None);
                    } else {
                        match value {
                            StorageValue::String(v) => results.push(Some(v.clone())),
                            StorageValue::ExpiringString(v, _) => {
                                results.push(Some(v.clone()))
                            }
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

    /// 批量设置多个键值对
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

    /// 将当前值解析为 i64，不存在则返回 0
    fn parse_int_value(value: Option<&StorageValue>) -> Result<i64> {
        match value {
            Some(StorageValue::String(v)) => {
                String::from_utf8_lossy(v)
                    .parse()
                    .map_err(|_| {
                        AppError::Storage("值不是有效的整数字符串".to_string())
                    })
            }
            Some(StorageValue::ExpiringString(v, _)) => {
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

    /// 递增键的值（+1），键不存在则从 0 开始
    pub fn incr(&self, key: &str) -> Result<i64> {
        self.incrby(key, 1)
    }

    /// 递减键的值（-1），键不存在则从 0 开始
    pub fn decr(&self, key: &str) -> Result<i64> {
        self.decrby(key, 1)
    }

    /// 递增键的值（+delta），键不存在则从 0 开始
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
                if Self::is_expired(value) {
                    map.remove(key);
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
        self.bump_version(key);
        self.touch(key);
        Ok(new_val)
    }

    /// 递减键的值（-delta），键不存在则从 0 开始
    pub fn decrby(&self, key: &str, delta: i64) -> Result<i64> {
        self.incrby(key, -delta)
    }

    /// 追加值到键的末尾，键不存在则视为空字符串
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
                if Self::is_expired(v) {
                    map.remove(key);
                    Bytes::new()
                } else {
                    match v {
                        StorageValue::String(b) => b.clone(),
                        StorageValue::ExpiringString(b, _) => b.clone(),
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
        self.bump_version(key);
        self.touch(key);
        Ok(new_len)
    }
}

impl StorageEngine {
    pub fn setnx(&self, key: String, value: Bytes) -> Result<bool> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(&key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(&key) {
            Some(v) => {
                if Self::is_expired(v) {
                    map.remove(&key);
                    map.insert(key.clone(), StorageValue::String(value));
                    self.bump_version(&key);
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            None => {
                map.insert(key.clone(), StorageValue::String(value));
                self.bump_version(&key);
                Ok(true)
            }
        }
    }

    /// 设置秒级过期时间的键值对
    pub fn setex(&self, key: String, seconds: u64, value: Bytes) -> Result<()> {
        self.set_with_ttl(key, value, seconds * 1000)
    }

    /// 设置毫秒级过期时间的键值对
    pub fn psetex(&self, key: String, ms: u64, value: Bytes) -> Result<()> {
        self.set_with_ttl(key, value, ms)
    }

    /// 设置新值并返回旧值，键不存在返回 None
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
                if Self::is_expired(v) {
                    map.remove(key);
                    None
                } else {
                    match v {
                        StorageValue::String(b) | StorageValue::ExpiringString(b, _) => {
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
        self.bump_version(key);
        self.touch(key);
        drop(map);
        self.evict_if_needed()?;
        Ok(old)
    }

    /// 获取值并删除键，键不存在返回 None
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
                if Self::is_expired(v) {
                    map.remove(key);
                    None
                } else {
                    match v {
                        StorageValue::String(b) | StorageValue::ExpiringString(b, _) => {
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
            self.bump_version(key);
        }
        Ok(result)
    }

    /// 获取值并修改过期时间，键不存在返回 None
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
                if Self::is_expired(v) {
                    map.remove(key);
                    None
                } else {
                    match v {
                        StorageValue::String(b) | StorageValue::ExpiringString(b, _) => {
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
            match option {
                GetExOption::Persist => {
                    // 将 ExpiringString 转为 String
                    if let Some(StorageValue::ExpiringString(v, _)) = map.get(key) {
                        let v = v.clone();
                        map.insert(key.to_string(), StorageValue::String(v));
                    }
                }
                GetExOption::Ex(seconds) => {
                    let expire_at = Self::now_millis().saturating_add(seconds * 1000);
                    if let Some(v) = map.get(key) {
                        let value = match v {
                            StorageValue::String(b) | StorageValue::ExpiringString(b, _) => b.clone(),
                            _ => unreachable!(),
                        };
                        map.insert(key.to_string(), StorageValue::ExpiringString(value, expire_at));
                    }
                }
                GetExOption::Px(ms) => {
                    let expire_at = Self::now_millis().saturating_add(ms);
                    if let Some(v) = map.get(key) {
                        let value = match v {
                            StorageValue::String(b) | StorageValue::ExpiringString(b, _) => b.clone(),
                            _ => unreachable!(),
                        };
                        map.insert(key.to_string(), StorageValue::ExpiringString(value, expire_at));
                    }
                }
                GetExOption::ExAt(timestamp) => {
                    let expire_at = timestamp * 1000;
                    if let Some(v) = map.get(key) {
                        let value = match v {
                            StorageValue::String(b) | StorageValue::ExpiringString(b, _) => b.clone(),
                            _ => unreachable!(),
                        };
                        map.insert(key.to_string(), StorageValue::ExpiringString(value, expire_at));
                    }
                }
                GetExOption::PxAt(ms_timestamp) => {
                    if let Some(v) = map.get(key) {
                        let value = match v {
                            StorageValue::String(b) | StorageValue::ExpiringString(b, _) => b.clone(),
                            _ => unreachable!(),
                        };
                        map.insert(key.to_string(), StorageValue::ExpiringString(value, ms_timestamp));
                    }
                }
            }
            self.bump_version(key);
            self.touch(key);
        }

        Ok(result)
    }

    /// 仅当所有 key 都不存在时才批量设置，原子操作，返回 1 成功 0 失败
    pub fn msetnx(&self, pairs: &[(String, Bytes)]) -> Result<i64> {
        self.evict_if_needed()?;
        let db = self.db();
        // 检查所有 key 是否都不存在（或已过期）
        for (key, _) in pairs {
            let mut map = db
                .inner
                .get_shard(key)
                .write()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            if let Some(v) = map.get(key) {
                if !Self::is_expired(v) {
                    return Ok(0);
                }
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
            self.bump_version(key);
        }
        self.evict_if_needed()?;
        Ok(1)
    }

    /// 浮点数递增，键不存在视为 0.0，返回新值字符串
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
                if Self::is_expired(value) {
                    map.remove(key);
                    0.0
                } else {
                    match value {
                        StorageValue::String(b) | StorageValue::ExpiringString(b, _) => {
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
        self.bump_version(key);
        self.touch(key);
        Ok(new_str)
    }

    /// 从 offset 位置覆盖写入，不足部分用 \x00 填充，返回新长度
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
                if Self::is_expired(v) {
                    map.remove(key);
                    Vec::new()
                } else {
                    match v {
                        StorageValue::String(b) | StorageValue::ExpiringString(b, _) => {
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
        self.bump_version(key);
        self.touch(key);
        Ok(new_len)
    }

    /// 获取字符串的子范围（支持负数索引）
    pub fn getrange(&self, key: &str, start: i64, end: i64) -> Result<Option<Bytes>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let bytes = match map.get(key) {
            Some(v) => {
                if Self::is_expired(v) {
                    map.remove(key);
                    return Ok(None);
                }
                match v {
                    StorageValue::String(b) => b.clone(),
                    StorageValue::ExpiringString(b, _) => b.clone(),
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
            s = len + s;
        }
        if e < 0 {
            e = len + e;
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

    /// 返回字符串值的长度，键不存在返回 0
    pub fn strlen(&self, key: &str) -> Result<usize> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if Self::is_expired(v) {
                    map.remove(key);
                    Ok(0)
                } else {
                    match v {
                        StorageValue::String(b) => Ok(b.len()),
                        StorageValue::ExpiringString(b, _) => Ok(b.len()),
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

