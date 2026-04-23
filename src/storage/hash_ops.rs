use super::*;

impl StorageEngine {
    pub fn hset(&self, key: &str, field: String, value: Bytes) -> Result<i64> {
        self.evict_if_needed()?;
        self.cleanup_hash_expired_fields(key);
        let mut map = self.write_shard(key)?;

        Self::check_and_remove_expired(&mut map, key);
        match map.get_mut(key) {
            Some(v) => {
                                Self::check_hash_type(v)?;

                                let h = Self::as_hash_mut(v).unwrap();

                                let is_new = if h.contains_key(&field) { 0 } else { 1 };

                                h.insert(field, value);

                                Ok(is_new)

            }
            None => {

                            let mut h = HashMap::new();

                            h.insert(field, value);

                            map.insert(key.to_string(), StorageValue::Hash(h));

                            Ok(1)


            }
        }
    }

    /// 获取哈希字段的值
    pub fn hget(&self, key: &str, field: &str) -> Result<Option<Bytes>> {
        self.cleanup_hash_expired_fields(key);
        let mut map = self.write_shard(key)?;

        Self::check_and_remove_expired(&mut map, key);
        match map.get(key) {
            Some(v) => {
                                Self::check_hash_type(v)?;

                                match v {

                                    StorageValue::Hash(h) => Ok(h.get(field).cloned()),

                                    _ => unreachable!(),

                                }

            }
            None => Ok(None),
        }
    }

    /// 删除哈希中的一个或多个字段，返回删除的字段数量
    pub fn hdel(&self, key: &str, fields: &[String]) -> Result<i64> {
        self.evict_if_needed()?;
        self.cleanup_hash_expired_fields(key);
        let mut map = self.write_shard(key)?;

        Self::check_and_remove_expired(&mut map, key);
        match map.get_mut(key) {
            Some(v) => {
                                Self::check_hash_type(v)?;

                                let h = Self::as_hash_mut(v).unwrap();

                                let mut count = 0i64;

                                for field in fields {

                                    if h.remove(field).is_some() {

                                        count += 1;

                                    }

                                }

                                if h.is_empty() {

                                    map.remove(key);

                                }

                                Ok(count)

            }
            None => Ok(0),
        }
    }

    /// 检查哈希字段是否存在
    pub fn hexists(&self, key: &str, field: &str) -> Result<bool> {
        self.cleanup_hash_expired_fields(key);
        let mut map = self.write_shard(key)?;

        Self::check_and_remove_expired(&mut map, key);
        match map.get(key) {
            Some(v) => {
                                Self::check_hash_type(v)?;

                                match v {

                                    StorageValue::Hash(h) => Ok(h.contains_key(field)),

                                    _ => unreachable!(),

                                }

            }
            None => Ok(false),
        }
    }

    /// 返回哈希中的所有字段和值
    pub fn hgetall(&self, key: &str) -> Result<Vec<(String, Bytes)>> {
        self.cleanup_hash_expired_fields(key);
        let mut map = self.write_shard(key)?;

        Self::check_and_remove_expired(&mut map, key);
        match map.get(key) {
            Some(v) => {
                                Self::check_hash_type(v)?;

                                match v {

                                    StorageValue::Hash(h) => {

                                        let result: Vec<(String, Bytes)> = h

                                            .iter()

                                            .map(|(k, v)| (k.clone(), v.clone()))

                                            .collect();

                                        Ok(result)

                                    }

                                    _ => unreachable!(),

                                }

            }
            None => Ok(vec![]),
        }
    }

    /// 返回哈希中字段的数量
    pub fn hlen(&self, key: &str) -> Result<usize> {
        self.cleanup_hash_expired_fields(key);
        let mut map = self.write_shard(key)?;

        Self::check_and_remove_expired(&mut map, key);
        match map.get(key) {
            Some(v) => {
                                Self::check_hash_type(v)?;

                                match v {

                                    StorageValue::Hash(h) => Ok(h.len()),

                                    _ => unreachable!(),

                                }

            }
            None => Ok(0),
        }
    }

    /// 哈希字段值加整数，返回新值
    pub fn hincrby(&self, key: &str, field: String, delta: i64) -> Result<i64> {
        self.evict_if_needed()?;
        self.cleanup_hash_expired_fields(key);
        let mut map = self.write_shard(key)?;

        Self::check_and_remove_expired(&mut map, key);
        match map.get_mut(key) {
            Some(v) => {
                                Self::check_hash_type(v)?;

                                let h = Self::as_hash_mut(v).unwrap();

                                let current = match h.get(&field) {

                                    Some(b) => {

                                        String::from_utf8_lossy(b)

                                            .parse::<i64>()

                                            .map_err(|_| {

                                                AppError::Storage("hash value is not an integer".to_string())

                                            })?

                                    }

                                    None => 0i64,

                                };

                                let new_val = current.saturating_add(delta);

                                h.insert(field, Bytes::from(new_val.to_string()));

                                self.bump_version(key);

                                Ok(new_val)

            }
            None => {

                            let mut h = HashMap::new();

                            h.insert(field, Bytes::from(delta.to_string()));

                            map.insert(key.to_string(), StorageValue::Hash(h));

                            self.bump_version(key);

                            Ok(delta)


            }
        }
    }

    /// 哈希字段值加浮点数，返回新值字符串
    pub fn hincrbyfloat(&self, key: &str, field: String, delta: f64) -> Result<String> {
        self.evict_if_needed()?;
        self.cleanup_hash_expired_fields(key);
        let mut map = self.write_shard(key)?;

        Self::check_and_remove_expired(&mut map, key);
        match map.get_mut(key) {
            Some(v) => {
                                Self::check_hash_type(v)?;

                                let h = Self::as_hash_mut(v).unwrap();

                                let current = match h.get(&field) {

                                    Some(b) => {

                                        String::from_utf8_lossy(b)

                                            .parse::<f64>()

                                            .map_err(|_| {

                                                AppError::Storage("hash value is not a valid float".to_string())

                                            })?

                                    }

                                    None => 0.0,

                                };

                                let new_val = current + delta;

                                let new_str = format_float(new_val);

                                h.insert(field, Bytes::from(new_str.clone()));

                                self.bump_version(key);

                                Ok(new_str)

            }
            None => {

                            let mut h = HashMap::new();

                            let new_str = format_float(delta);

                            h.insert(field, Bytes::from(new_str.clone()));

                            map.insert(key.to_string(), StorageValue::Hash(h));

                            self.bump_version(key);

                            Ok(new_str)


            }
        }
    }

    /// 返回哈希中所有字段名
    pub fn hkeys(&self, key: &str) -> Result<Vec<String>> {
        self.cleanup_hash_expired_fields(key);
        let mut map = self.write_shard(key)?;

        Self::check_and_remove_expired(&mut map, key);
        match map.get(key) {
            Some(v) => {
                                Self::check_hash_type(v)?;

                                match v {

                                    StorageValue::Hash(h) => {

                                        let mut keys: Vec<String> = h.keys().cloned().collect();

                                        keys.sort();

                                        Ok(keys)

                                    }

                                    _ => unreachable!(),

                                }

            }
            None => Ok(vec![]),
        }
    }

    /// 返回哈希中所有字段值
    pub fn hvals(&self, key: &str) -> Result<Vec<Bytes>> {
        self.cleanup_hash_expired_fields(key);
        let mut map = self.write_shard(key)?;

        Self::check_and_remove_expired(&mut map, key);
        match map.get(key) {
            Some(v) => {
                                Self::check_hash_type(v)?;

                                match v {

                                    StorageValue::Hash(h) => {

                                        // 按字段名排序以保持确定性

                                        let mut pairs: Vec<(String, Bytes)> = h.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

                                        pairs.sort_by(|a, b| a.0.cmp(&b.0));

                                        let result: Vec<Bytes> = pairs.into_iter().map(|(_, v)| v).collect();

                                        Ok(result)

                                    }

                                    _ => unreachable!(),

                                }

            }
            None => Ok(vec![]),
        }
    }

    /// 仅当字段不存在时才设置，返回 1 成功 0 失败
    pub fn hsetnx(&self, key: &str, field: String, value: Bytes) -> Result<i64> {
        self.evict_if_needed()?;
        self.cleanup_hash_expired_fields(key);
        let mut map = self.write_shard(key)?;

        Self::check_and_remove_expired(&mut map, key);
        match map.get_mut(key) {
            Some(v) => {
                                Self::check_hash_type(v)?;

                                let h = Self::as_hash_mut(v).unwrap();

                                if h.contains_key(&field) {

                                    Ok(0)

                                } else {

                                    h.insert(field, value);

                                    self.bump_version(key);

                                    Ok(1)

                                }

            }
            None => {

                            let mut h = HashMap::new();

                            h.insert(field, value);

                            map.insert(key.to_string(), StorageValue::Hash(h));

                            self.bump_version(key);

                            Ok(1)


            }
        }
    }

    /// 随机返回 count 个字段
    /// count > 0：不重复；count < 0：可重复
    /// with_values：是否同时返回值
    pub fn hrandfield(&self, key: &str, count: i64, with_values: bool) -> Result<Vec<(String, Option<Bytes>)>> {
        let mut map = self.write_shard(key)?;

        Self::check_and_remove_expired(&mut map, key);
        match map.get(key) {
            Some(v) => {
                                Self::check_hash_type(v)?;

                                match v {

                                    StorageValue::Hash(h) => {

                                        let fields: Vec<(String, Bytes)> = h

                                            .iter()

                                            .map(|(k, v)| (k.clone(), v.clone()))

                                            .collect();

                                        if fields.is_empty() {

                                            return Ok(vec![]);

                                        }


                                        use rand::seq::SliceRandom;

                                        let mut rng = rand::thread_rng();


                                        if count > 0 {

                                            let n = count as usize;

                                            if n >= fields.len() {

                                                // 返回全部，打乱顺序

                                                let mut shuffled = fields.clone();

                                                shuffled.shuffle(&mut rng);

                                                Ok(shuffled.into_iter().map(|(k, v)| {

                                                    if with_values {

                                                        (k, Some(v))

                                                    } else {

                                                        (k, None)

                                                    }

                                                }).collect())

                                            } else {

                                                let selected: Vec<_> = fields.choose_multiple(&mut rng, n).cloned().collect();

                                                Ok(selected.into_iter().map(|(k, v)| {

                                                    if with_values {

                                                        (k, Some(v))

                                                    } else {

                                                        (k, None)

                                                    }

                                                }).collect())

                                            }

                                        } else {

                                            let n = (-count) as usize;

                                            let mut result = Vec::with_capacity(n);

                                            for _ in 0..n {

                                                let (k, v) = fields.choose(&mut rng).unwrap().clone();

                                                if with_values {

                                                    result.push((k, Some(v)));

                                                } else {

                                                    result.push((k, None));

                                                }

                                            }

                                            Ok(result)

                                        }

                                    }

                                    _ => unreachable!(),

                                }

            }
            None => Ok(vec![]),
        }
    }

    /// 增量迭代哈希字段
    /// 返回 (新游标, 字段值对列表)
    pub fn hscan(&self, key: &str, cursor: usize, pattern: &str, count: usize) -> Result<(usize, Vec<(String, Bytes)>)> {
        let mut map = self.write_shard(key)?;

        Self::check_and_remove_expired(&mut map, key);
        match map.get(key) {
            Some(v) => {
                            Self::check_hash_type(v)?;

                            match v {

                                StorageValue::Hash(h) => {

                                    let mut fields: Vec<(String, Bytes)> = h

                                        .iter()

                                        .map(|(k, v)| (k.clone(), v.clone()))

                                        .collect();

                                    fields.sort_by(|a, b| a.0.cmp(&b.0));


                                    let mut filtered = Vec::new();

                                    for (field, value) in fields {

                                        if Self::glob_match(&field, pattern) {

                                            filtered.push((field, value));

                                        }

                                    }


                                    if cursor >= filtered.len() {

                                        return Ok((0, vec![]));

                                    }


                                    let count = if count == 0 { 10 } else { count };

                                    let end = (cursor + count).min(filtered.len());

                                    let result = filtered[cursor..end].to_vec();

                                    let new_cursor = if end >= filtered.len() { 0 } else { end };


                                    Ok((new_cursor, result))

                                }

                                _ => unreachable!(),

                            }

            }
            None => Ok((0, vec![])),
        }
    }

    /// 批量设置哈希字段
    pub fn hmset(&self, key: &str, pairs: &[(String, Bytes)]) -> Result<()> {
        self.evict_if_needed()?;
        let mut map = self.write_shard(key)?;

        Self::check_and_remove_expired(&mut map, key);
        match map.get_mut(key) {
            Some(v) => {
                                Self::check_hash_type(v)?;

                                let h = Self::as_hash_mut(v).unwrap();

                                for (field, value) in pairs {

                                    h.insert(field.clone(), value.clone());

                                }

            }
            None => {

                            let mut h = HashMap::new();

                            for (field, value) in pairs {

                                h.insert(field.clone(), value.clone());

                            }

                            map.insert(key.to_string(), StorageValue::Hash(h));


            }
        }
        Ok(())
    }

    /// 批量获取哈希字段的值
    pub fn hmget(&self, key: &str, fields: &[String]) -> Result<Vec<Option<Bytes>>> {
        self.cleanup_hash_expired_fields(key);
        let mut map = self.write_shard(key)?;

        Self::check_and_remove_expired(&mut map, key);
        let h = match map.get(key) {
            Some(v) => {
                Self::check_hash_type(v)?;
                match v {
                    StorageValue::Hash(h) => h,
                    _ => unreachable!(),
                }
            }
            None => return Ok(vec![None; fields.len()]),
        };

        let results: Vec<Option<Bytes>> = fields
            .iter()
            .map(|f| h.get(f).cloned())
            .collect();
        Ok(results)
    }

    // ---------- Hash 字段级过期操作 ----------

    /// 为 Hash 字段设置过期时间（秒），返回每个字段的状态码
    /// 1=设置成功, 0=字段不存在, -1=key不存在或类型错误
    pub fn hexpire(&self, key: &str, fields: &[String], seconds: u64) -> Result<Vec<i64>> {
        self.hpexpire(key, fields, seconds * 1000)
    }

    /// 为 Hash 字段设置过期时间（毫秒），返回每个字段的状态码
    pub fn hpexpire(&self, key: &str, fields: &[String], milliseconds: u64) -> Result<Vec<i64>> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = self.write_shard(key)?;

        Self::check_and_remove_expired(&mut map, key);
        match map.get_mut(key) {
            Some(v) => {
                                Self::check_hash_type(v)?;

                                let h = Self::as_hash_mut(v).unwrap();

                                let expire_at = Self::now_millis().saturating_add(milliseconds);

                                let mut result = Vec::with_capacity(fields.len());

                                let mut exp_map = db.hash_field_expirations.write().unwrap();

                                let field_map = exp_map.entry(key.to_string()).or_insert_with(HashMap::new);

                                for field in fields {

                                    if h.contains_key(field) {

                                        field_map.insert(field.clone(), expire_at);

                                        result.push(1);

                                    } else {

                                        result.push(0);

                                    }

                                }

                                if field_map.is_empty() {

                                    exp_map.remove(key);

                                }

                                self.bump_version(key);

                                Ok(result)

            }
            None => Ok(vec![-1; fields.len()]),
        }
    }

    /// 为 Hash 字段设置绝对过期时间（秒 Unix 时间戳），返回每个字段的状态码
    pub fn hexpireat(&self, key: &str, fields: &[String], timestamp_sec: u64) -> Result<Vec<i64>> {
        self.hpexpireat(key, fields, timestamp_sec * 1000)
    }

    /// 为 Hash 字段设置绝对过期时间（毫秒 Unix 时间戳），返回每个字段的状态码
    pub fn hpexpireat(&self, key: &str, fields: &[String], timestamp_ms: u64) -> Result<Vec<i64>> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = self.write_shard(key)?;

        Self::check_and_remove_expired(&mut map, key);
        match map.get_mut(key) {
            Some(v) => {
                                Self::check_hash_type(v)?;

                                let h = Self::as_hash_mut(v).unwrap();

                                let mut result = Vec::with_capacity(fields.len());

                                let mut exp_map = db.hash_field_expirations.write().unwrap();

                                let field_map = exp_map.entry(key.to_string()).or_insert_with(HashMap::new);

                                for field in fields {

                                    if h.contains_key(field) {

                                        field_map.insert(field.clone(), timestamp_ms);

                                        result.push(1);

                                    } else {

                                        result.push(0);

                                    }

                                }

                                if field_map.is_empty() {

                                    exp_map.remove(key);

                                }

                                self.bump_version(key);

                                Ok(result)

            }
            None => Ok(vec![-1; fields.len()]),
        }
    }

    /// 返回 Hash 字段剩余 TTL（秒）
    /// 正数=剩余秒数, -1=字段无过期, -2=字段不存在或 key 不存在/类型错误
    pub fn httl(&self, key: &str, fields: &[String]) -> Result<Vec<i64>> {
        let ttls = self.hpttl(key, fields)?;
        Ok(ttls.into_iter().map(|t| if t > 0 { t / 1000 } else { t }).collect())
    }

    /// 返回 Hash 字段剩余 TTL（毫秒）
    pub fn hpttl(&self, key: &str, fields: &[String]) -> Result<Vec<i64>> {
        self.cleanup_hash_expired_fields(key);
        let db = self.db();
        let map = self.read_shard(key)?;

        match map.get(key) {
            Some(v) => {
                if Self::is_expired(v) {
                    return Ok(vec![-2; fields.len()]);
                }
                Self::check_hash_type(v)?;
                let h = match v {
                    StorageValue::Hash(h) => h,
                    _ => unreachable!(),
                };
                let exp_map = db.hash_field_expirations.read().unwrap();
                let field_map = exp_map.get(key);
                let now = Self::now_millis();
                let mut result = Vec::with_capacity(fields.len());
                for field in fields {
                    if !h.contains_key(field) {
                        result.push(-2);
                    } else if let Some(fm) = field_map {
                        if let Some(&expire_at) = fm.get(field) {
                            if expire_at > now {
                                result.push((expire_at - now) as i64);
                            } else {
                                result.push(-1);
                            }
                        } else {
                            result.push(-1);
                        }
                    } else {
                        result.push(-1);
                    }
                }
                Ok(result)
            }
            None => Ok(vec![-2; fields.len()]),
        }
    }

    /// 返回 Hash 字段绝对过期时间（秒 Unix 时间戳）
    /// 正数=过期时间, -1=字段无过期, -2=字段不存在或 key 不存在/类型错误
    pub fn hexpiretime(&self, key: &str, fields: &[String]) -> Result<Vec<i64>> {
        let times = self.hpexpiretime(key, fields)?;
        Ok(times.into_iter().map(|t| if t > 0 { t / 1000 } else { t }).collect())
    }

    /// 返回 Hash 字段绝对过期时间（毫秒 Unix 时间戳）
    pub fn hpexpiretime(&self, key: &str, fields: &[String]) -> Result<Vec<i64>> {
        self.cleanup_hash_expired_fields(key);
        let db = self.db();
        let map = self.read_shard(key)?;

        match map.get(key) {
            Some(v) => {
                if Self::is_expired(v) {
                    return Ok(vec![-2; fields.len()]);
                }
                Self::check_hash_type(v)?;
                let h = match v {
                    StorageValue::Hash(h) => h,
                    _ => unreachable!(),
                };
                let exp_map = db.hash_field_expirations.read().unwrap();
                let field_map = exp_map.get(key);
                let mut result = Vec::with_capacity(fields.len());
                for field in fields {
                    if !h.contains_key(field) {
                        result.push(-2);
                    } else if let Some(fm) = field_map {
                        if let Some(&expire_at) = fm.get(field) {
                            result.push(expire_at as i64);
                        } else {
                            result.push(-1);
                        }
                    } else {
                        result.push(-1);
                    }
                }
                Ok(result)
            }
            None => Ok(vec![-2; fields.len()]),
        }
    }

    /// 移除 Hash 字段的过期时间，返回每个字段的状态码
    /// 1=成功移除, 0=字段无过期或不存在, -1=key不存在或类型错误
    pub fn hpersist(&self, key: &str, fields: &[String]) -> Result<Vec<i64>> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = self.write_shard(key)?;

        Self::check_and_remove_expired(&mut map, key);
        match map.get_mut(key) {
            Some(v) => {
                                Self::check_hash_type(v)?;

                                let h = Self::as_hash_mut(v).unwrap();

                                let mut exp_map = db.hash_field_expirations.write().unwrap();

                                let mut result = Vec::with_capacity(fields.len());

                                if let Some(field_map) = exp_map.get_mut(key) {

                                    for field in fields {

                                        if h.contains_key(field) {

                                            if field_map.remove(field).is_some() {

                                                result.push(1);

                                            } else {

                                                result.push(0);

                                            }

                                        } else {

                                            result.push(0);

                                        }

                                    }

                                    if field_map.is_empty() {

                                        exp_map.remove(key);

                                    }

                                } else {

                                    for field in fields {

                                        if h.contains_key(field) {

                                            result.push(0);

                                        } else {

                                            result.push(0);

                                        }

                                    }

                                }

                                self.bump_version(key);

                                Ok(result)

            }
            None => Ok(vec![-1; fields.len()]),
        }
    }

    // ---------- Stream 操作 ----------

}

