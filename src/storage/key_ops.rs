use super::*;

impl StorageEngine {
    pub fn keys(&self, pattern: &str) -> Result<Vec<String>> {
        let db = self.db();
        let mut result = Vec::new();
        for shard in db.inner.all_shards() {
            let map = shard.read().map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            for key in map.keys() {
                if let Some(value) = map.get(key) {
                    if !Self::is_expired(value) && Self::glob_match(key, pattern) {
                        result.push(key.clone());
                    }
                }
            }
        }
        result.sort();
        Ok(result)
    }

    /// 简单的 glob 模式匹配（支持 * 和 ?）
    pub(crate) fn glob_match(text: &str, pattern: &str) -> bool {
        let text_chars: Vec<char> = text.chars().collect();
        let pat_chars: Vec<char> = pattern.chars().collect();
        let n = text_chars.len();
        let m = pat_chars.len();

        let mut dp = vec![vec![false; m + 1]; n + 1];
        dp[0][0] = true;

        for j in 1..=m {
            if pat_chars[j - 1] == '*' {
                dp[0][j] = dp[0][j - 1];
            }
        }

        for i in 1..=n {
            for j in 1..=m {
                match pat_chars[j - 1] {
                    '*' => dp[i][j] = dp[i][j - 1] || dp[i - 1][j],
                    '?' => dp[i][j] = dp[i - 1][j - 1],
                    c => dp[i][j] = dp[i - 1][j - 1] && text_chars[i - 1] == c,
                }
            }
        }

        dp[n][m]
    }

    /// 增量迭代键
    /// cursor 为上一次迭代的游标，0 表示从头开始
    /// 返回 (新游标, 键列表)，新游标为 0 表示迭代结束
    pub fn scan(&self, cursor: usize, pattern: &str, count: usize) -> Result<(usize, Vec<String>)> {
        let db = self.db();
        let mut all_keys: Vec<String> = Vec::new();
        for shard in db.inner.all_shards() {
            let map = shard.read().map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            for key in map.keys() {
                if let Some(value) = map.get(key) {
                    if !Self::is_expired(value) {
                        all_keys.push(key.clone());
                    }
                }
            }
        }
        all_keys.sort();

        // 过滤掉已过期的键
        let mut keys = Vec::new();
        for key in &all_keys {
            if Self::glob_match(key, pattern) {
                keys.push(key.clone());
            }
        }

        if cursor >= keys.len() {
            return Ok((0, vec![]));
        }

        let count = if count == 0 { 10 } else { count };
        let end = (cursor + count).min(keys.len());
        let result = keys[cursor..end].to_vec();
        let new_cursor = if end >= keys.len() { 0 } else { end };

        Ok((new_cursor, result))
    }

    /// 重命名键，newkey 已存在则覆盖
    pub fn rename(&self, key: &str, newkey: &str) -> Result<()> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.remove(key) {
            Some(value) => {
                if Self::is_expired(&value) {
                    return Err(AppError::Storage(
                        "键不存在或已过期".to_string(),
                    ));
                }
                drop(map);
                let mut new_map = db.inner.get_shard(newkey).write()
                    .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
                new_map.insert(newkey.to_string(), value);
                self.bump_version(key);
                self.bump_version(newkey);
                self.touch(key);
                self.touch(newkey);
                Ok(())
            }
            None => Err(AppError::Storage(
                "键不存在".to_string(),
            )),
        }
    }

    /// 返回键的类型字符串
    pub fn key_type(&self, key: &str) -> Result<String> {
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
                    Ok("none".to_string())
                } else {
                    let type_str = match value {
                        StorageValue::String(_) | StorageValue::ExpiringString(_, _) => "string",
                        StorageValue::List(_) => "list",
                        StorageValue::Hash(_) => "hash",
                        StorageValue::Set(_) => "set",
                        StorageValue::ZSet(_) => "zset",
                        StorageValue::HyperLogLog(_) => "string",
                        StorageValue::Stream(_) => "stream",
                    };
                    Ok(type_str.to_string())
                }
            }
            None => Ok("none".to_string()),
        }
    }

    /// 移除键的过期时间，返回是否成功
    pub fn persist(&self, key: &str) -> Result<bool> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get_mut(key) {
            Some(value) => {
                if Self::is_expired(value) {
                    map.remove(key);
                    Ok(false)
                } else {
                    match value {
                        StorageValue::ExpiringString(v, _) => {
                            *value = StorageValue::String(v.clone());
                            Ok(true)
                        }
                        _ => Ok(false), // 无过期时间的键
                    }
                }
            }
            None => Ok(false),
        }
    }

    /// 设置键的毫秒级过期时间
    pub fn pexpire(&self, key: &str, ms: u64) -> Result<bool> {
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
                    let expire_at = Self::now_millis().saturating_add(ms);
                    let new_value = match value {
                        StorageValue::String(v) => {
                            StorageValue::ExpiringString(v.clone(), expire_at)
                        }
                        StorageValue::ExpiringString(v, _) => {
                            StorageValue::ExpiringString(v.clone(), expire_at)
                        }
                        StorageValue::List(_) | StorageValue::Hash(_)
                        | StorageValue::Set(_) | StorageValue::ZSet(_)
                        | StorageValue::HyperLogLog(_) | StorageValue::Stream(_) => {
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

    /// 返回键的剩余毫秒生存时间
    /// - 返回正数：剩余毫秒数
    /// - 返回 -1：键存在但没有过期时间
    /// - 返回 -2：键不存在或已过期
    pub fn pttl(&self, key: &str) -> Result<i64> {
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
                        StorageValue::List(_) | StorageValue::Hash(_)
                        | StorageValue::Set(_) | StorageValue::ZSet(_) | StorageValue::HyperLogLog(_) | StorageValue::Stream(_) => Ok(-1),
                        StorageValue::ExpiringString(_, expire_at) => {
                            let remaining =
                                *expire_at as i64 - Self::now_millis() as i64;
                            Ok(remaining.max(0))
                        }
                    }
                }
            }
            None => Ok(-2),
        }
    }

    /// 返回当前数据库的键数量
    pub fn dbsize(&self) -> Result<usize> {
        let db = self.db();
        let mut count = 0;
        for shard in db.inner.all_shards() {
            let mut map = shard.write().map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            let expired_keys: Vec<String> = map
                .iter()
                .filter(|(_, v)| Self::is_expired(v))
                .map(|(k, _)| k.clone())
                .collect();
            for key in expired_keys {
                map.remove(&key);
            }
            count += map.len();
        }
        Ok(count)
    }

    /// 返回服务器信息字符串
    pub fn info(&self, _section: Option<&str>) -> Result<String> {
        let size = self.dbsize()?;
        let info = format!(
            "# Server\r\nredis_version:0.1.0\r\nredis_mode:standalone\r\n\r\n# Keyspace\r\ndb0:keys={}\r\n",
            size
        );
        Ok(info)
    }

    /// 清理所有已过期的键
    fn cleanup_expired_keys(&self) -> Result<()> {
        if !self.active_expire_enabled.load(Ordering::SeqCst) {
            return Ok(());
        }
        let db = self.db();
        for shard in db.inner.all_shards() {
            let mut map = shard.write().map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            let expired_keys: Vec<String> = map
                .iter()
                .filter(|(_, v)| Self::is_expired(v))
                .map(|(k, _)| k.clone())
                .collect();
            for key in expired_keys {
                map.remove(&key);
            }
        }
        Ok(())
    }

    /// 启动后台过期键清理任务，每隔 interval_ms 毫秒执行一次
    pub fn start_cleanup_task(&self, interval_ms: u64) -> tokio::task::JoinHandle<()> {
        let storage = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(
                tokio::time::Duration::from_millis(interval_ms),
            );
            loop {
                interval.tick().await;
                if let Err(e) = storage.cleanup_expired_keys() {
                    log::error!("后台清理过期键失败: {}", e);
                }
            }
        })
    }

    /// 对 List/Set/ZSet 的元素进行排序
    /// 返回排序后的元素列表（字符串形式）
    /// by_pattern/get_patterns 暂不支持外部 key 引用
    pub fn sort(
        &self,
        key: &str,
        _by_pattern: Option<String>,
        _get_patterns: Vec<String>,
        limit_offset: Option<isize>,
        limit_count: Option<isize>,
        asc: bool,
        alpha: bool,
        store_key: Option<String>,
    ) -> Result<Vec<String>> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let value = match map.get(key) {
            Some(v) => {
                if Self::is_expired(v) {
                    map.remove(key);
                    return Ok(Vec::new());
                }
                v.clone()
            }
            None => return Ok(Vec::new()),
        };

        let mut elements: Vec<String> = match value {
            StorageValue::List(list) => {
                list.iter().map(|b| String::from_utf8_lossy(b).to_string()).collect()
            }
            StorageValue::Set(set) => {
                set.iter().map(|b| String::from_utf8_lossy(b).to_string()).collect()
            }
            StorageValue::ZSet(zset) => {
                zset.member_to_score.keys().cloned().collect()
            }
            _ => {
                return Err(AppError::Storage(
                    "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                ));
            }
        };

        // 排序
        if alpha {
            elements.sort_by(|a, b| a.cmp(b));
        } else {
            let mut keyed: Vec<(String, f64)> = elements
                .into_iter()
                .map(|s| {
                    let num = s.parse::<f64>().unwrap_or(0.0);
                    (s, num)
                })
                .collect();
            keyed.sort_by(|a, b| {
                a.1.partial_cmp(&b.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            elements = keyed.into_iter().map(|(s, _)| s).collect();
        }

        if !asc {
            elements.reverse();
        }

        // LIMIT
        if limit_offset.is_some() || limit_count.is_some() {
            let offset = limit_offset.unwrap_or(0).max(0) as usize;
            let count = limit_count.unwrap_or(-1);
            let end = if count < 0 {
                elements.len()
            } else {
                offset + count as usize
            };
            elements = elements.into_iter().skip(offset).take(end - offset).collect();
        }

        // STORE
        if let Some(dest) = store_key {
            let list: VecDeque<Bytes> = elements.iter().map(|s| Bytes::from(s.clone())).collect();
            drop(map);
            let mut dst_map = db.inner.get_shard(&dest).write()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            dst_map.insert(dest.clone(), StorageValue::List(list));
            self.bump_version(&dest);
            self.touch(&dest);
            // 返回存入的元素数量
            return Ok(Vec::new());
        }

        Ok(elements)
    }

    /// 异步删除多个 key（先从 HashMap 中移除，内存释放交给后台）
    pub fn unlink(&self, keys: &[String]) -> Result<usize> {
        self.evict_if_needed()?;
        let db = self.db();

        let mut removed = Vec::new();
        let mut count = 0usize;
        for key in keys {
            let mut map = db
                .inner
                .get_shard(key)
                .write()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            if let Some(value) = map.remove(key) {
                if !Self::is_expired(&value) {
                    count += 1;
                }
                removed.push(value);
            }
        }

        // 后台释放复杂类型的内存（如有 tokio 运行时则异步释放，否则同步释放）
        if !removed.is_empty() {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let _ = handle.spawn(async move {
                    drop(removed);
                });
            } else {
                drop(removed);
            }
        }

        Ok(count)
    }

    /// 复制 key 的值到新 key
    /// replace=true 时覆盖已有 destination
    /// 返回 1 表示复制成功，0 表示 destination 已存在且 replace=false
    pub fn copy(&self, source: &str, destination: &str, replace: bool) -> Result<bool> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut src_map = db
            .inner
            .get_shard(source)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let source_value = match src_map.get(source) {
            Some(v) => {
                if Self::is_expired(v) {
                    src_map.remove(source);
                    return Ok(false);
                }
                v.clone()
            }
            None => return Ok(false),
        };
        drop(src_map);

        let mut dst_map = db.inner.get_shard(destination).write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
        if !replace && dst_map.contains_key(destination) {
            return Ok(false);
        }

        dst_map.insert(destination.to_string(), source_value);
        self.bump_version(destination);
        self.touch(destination);
        Ok(true)
    }

    /// 将 key 的值序列化为字节
    /// 格式：魔数 'R' + 版本 1 + 类型标记 + 数据
    pub fn dump(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let db = self.db();
        let map = db
            .inner
            .get_shard(key)
            .read()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let value = match map.get(key) {
            Some(v) => {
                if Self::is_expired(v) {
                    return Ok(None);
                }
                v.clone()
            }
            None => return Ok(None),
        };

        let mut result = Vec::new();
        result.push(b'R'); // 魔数
        result.push(1);    // 版本

        match value {
            StorageValue::String(bytes) => {
                result.push(1); // 类型标记
                write_u32(&mut result, bytes.len() as u32);
                result.extend_from_slice(&bytes);
            }
            StorageValue::ExpiringString(bytes, expire_at) => {
                result.push(1); // 恢复为普通 String（restore 时通过 ttl 控制过期）
                write_u32(&mut result, bytes.len() as u32);
                result.extend_from_slice(&bytes);
                let _ = expire_at; // dump 不保留过期信息，由 restore 的 ttl 参数控制
            }
            StorageValue::Stream(_) => {
                // Stream 类型暂不支持 DUMP/RESTORE
                return Err(AppError::Storage("Stream 类型暂不支持 DUMP".to_string()));
            }
            StorageValue::List(list) => {
                result.push(3);
                write_u32(&mut result, list.len() as u32);
                for item in list {
                    write_u32(&mut result, item.len() as u32);
                    result.extend_from_slice(&item);
                }
            }
            StorageValue::Hash(hash) => {
                result.push(4);
                write_u32(&mut result, hash.len() as u32);
                for (k, v) in hash {
                    write_u32(&mut result, k.len() as u32);
                    result.extend_from_slice(k.as_bytes());
                    write_u32(&mut result, v.len() as u32);
                    result.extend_from_slice(&v);
                }
            }
            StorageValue::Set(set) => {
                result.push(5);
                write_u32(&mut result, set.len() as u32);
                for item in set {
                    write_u32(&mut result, item.len() as u32);
                    result.extend_from_slice(&item);
                }
            }
            StorageValue::ZSet(zset) => {
                result.push(6);
                write_u32(&mut result, zset.member_to_score.len() as u32);
                for (member, score) in zset.member_to_score {
                    write_f64(&mut result, score);
                    write_u32(&mut result, member.len() as u32);
                    result.extend_from_slice(member.as_bytes());
                }
            }
            StorageValue::HyperLogLog(hll) => {
                result.push(7);
                let data = hll.dump();
                write_u32(&mut result, data.len() as u32);
                result.extend_from_slice(&data);
            }
        }

        Ok(Some(result))
    }

    /// 从序列化数据恢复 key
    /// ttl_ms=0 表示不设置过期时间
    pub fn restore(&self, key: &str, ttl_ms: u64, serialized: &[u8], replace: bool) -> Result<()> {
        if serialized.len() < 2 {
            return Err(AppError::Storage("DUMP 数据格式错误".to_string()));
        }
        if serialized[0] != b'R' || serialized[1] != 1 {
            return Err(AppError::Storage("DUMP 数据版本不兼容".to_string()));
        }
        if serialized.len() < 3 {
            return Err(AppError::Storage("DUMP 数据格式错误".to_string()));
        }

        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        if !replace && map.contains_key(key) {
            return Err(AppError::Storage("键已存在".to_string()));
        }

        let mut pos = 3usize;
        let value = match serialized[2] {
            1 => {
                let len = read_u32(serialized, &mut pos)? as usize;
                let bytes = read_bytes(serialized, &mut pos, len)?;
                if ttl_ms > 0 {
                    let expire_at = Self::now_millis() + ttl_ms;
                    StorageValue::ExpiringString(bytes, expire_at)
                } else {
                    StorageValue::String(bytes)
                }
            }
            3 => {
                let count = read_u32(serialized, &mut pos)? as usize;
                let mut list = VecDeque::new();
                for _ in 0..count {
                    let len = read_u32(serialized, &mut pos)? as usize;
                    let bytes = read_bytes(serialized, &mut pos, len)?;
                    list.push_back(bytes);
                }
                StorageValue::List(list)
            }
            4 => {
                let count = read_u32(serialized, &mut pos)? as usize;
                let mut hash = HashMap::new();
                for _ in 0..count {
                    let klen = read_u32(serialized, &mut pos)? as usize;
                    let k = String::from_utf8_lossy(&read_bytes(serialized, &mut pos, klen)?).to_string();
                    let vlen = read_u32(serialized, &mut pos)? as usize;
                    let v = read_bytes(serialized, &mut pos, vlen)?;
                    hash.insert(k, v);
                }
                StorageValue::Hash(hash)
            }
            5 => {
                let count = read_u32(serialized, &mut pos)? as usize;
                let mut set = HashSet::new();
                for _ in 0..count {
                    let len = read_u32(serialized, &mut pos)? as usize;
                    let bytes = read_bytes(serialized, &mut pos, len)?;
                    set.insert(bytes);
                }
                StorageValue::Set(set)
            }
            6 => {
                let count = read_u32(serialized, &mut pos)? as usize;
                let mut zset = ZSetData::new();
                for _ in 0..count {
                    let score = read_f64(serialized, &mut pos)?;
                    let mlen = read_u32(serialized, &mut pos)? as usize;
                    let member = String::from_utf8_lossy(&read_bytes(serialized, &mut pos, mlen)?).to_string();
                    zset.add(member, score);
                }
                StorageValue::ZSet(zset)
            }
            7 => {
                let len = read_u32(serialized, &mut pos)? as usize;
                let data = read_bytes(serialized, &mut pos, len)?;
                let hll = HyperLogLog::load(&data)?;
                StorageValue::HyperLogLog(hll)
            }
            _ => {
                return Err(AppError::Storage("未知的 DUMP 数据类型".to_string()));
            }
        };

        map.insert(key.to_string(), value);
        self.bump_version(key);
        self.touch(key);
        Ok(())
    }
}

fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_be_bytes());
}

/// DUMP/RESTORE 辅助函数：写入 f64（大端序）
fn write_f64(buf: &mut Vec<u8>, v: f64) {
    buf.extend_from_slice(&v.to_be_bytes());
}

/// DUMP/RESTORE 辅助函数：读取 u32（大端序）
fn read_u32(data: &[u8], pos: &mut usize) -> Result<u32> {
    if *pos + 4 > data.len() {
        return Err(AppError::Storage("DUMP 数据截断".to_string()));
    }
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&data[*pos..*pos + 4]);
    *pos += 4;
    Ok(u32::from_be_bytes(buf))
}

/// DUMP/RESTORE 辅助函数：读取 f64（大端序）
fn read_f64(data: &[u8], pos: &mut usize) -> Result<f64> {
    if *pos + 8 > data.len() {
        return Err(AppError::Storage("DUMP 数据截断".to_string()));
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[*pos..*pos + 8]);
    *pos += 8;
    Ok(f64::from_be_bytes(buf))
}

/// DUMP/RESTORE 辅助函数：读取指定长度的字节
fn read_bytes(data: &[u8], pos: &mut usize, len: usize) -> Result<Bytes> {
    if *pos + len > data.len() {
        return Err(AppError::Storage("DUMP 数据截断".to_string()));
    }
    let result = Bytes::copy_from_slice(&data[*pos..*pos + len]);
    *pos += len;
    Ok(result)
}

