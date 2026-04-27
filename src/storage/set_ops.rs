//! Set 数据类型操作（对标 Redis Set 命令族）

use super::*;

impl StorageEngine {
    pub fn sadd(&self, key: &str, members: Vec<Bytes>) -> Result<i64> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get_mut(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    let mut set = HashSet::new();
                    let mut count = 0i64;
                    for m in members {
                        if set.insert(m) {
                            count += 1;
                        }
                    }
                    map.insert(key.to_string(), Entry::new(StorageValue::Set(set)));
                    Ok(count)
                } else {
                    Self::check_set_type(&v.value)?;
                    let set = Self::as_set_mut(&mut v.value).unwrap();
                    let mut count = 0i64;
                    for m in members {
                        if set.insert(m) {
                            count += 1;
                        }
                    }
                    Ok(count)
                }
            }
            None => {
                let mut set = HashSet::new();
                let mut count = 0i64;
                for m in members {
                    if set.insert(m) {
                        count += 1;
                    }
                }
                map.insert(key.to_string(), Entry::new(StorageValue::Set(set)));
                Ok(count)
            }
        }
    }

    /// 从集合中删除一个或多个成员
    /// 返回实际删除的成员数量
    pub fn srem(&self, key: &str, members: &[Bytes]) -> Result<i64> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get_mut(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(0)
                } else {
                    Self::check_set_type(&v.value)?;
                    let set = Self::as_set_mut(&mut v.value).unwrap();
                    let mut count = 0i64;
                    for m in members {
                        if set.remove(m) {
                            count += 1;
                        }
                    }
                    if set.is_empty() {
                        map.remove(key);
                    }
                    Ok(count)
                }
            }
            None => Ok(0),
        }
    }

    /// 返回集合中的所有成员
    pub fn smembers(&self, key: &str) -> Result<Vec<Bytes>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(vec![])
                } else {
                    Self::check_set_type(&v.value)?;
                    match &v.value {
                        StorageValue::Set(s) => Ok(s.iter().cloned().collect()),
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(vec![]),
        }
    }

    /// 检查成员是否在集合中
    pub fn sismember(&self, key: &str, member: &Bytes) -> Result<bool> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(false)
                } else {
                    Self::check_set_type(&v.value)?;
                    match &v.value {
                        StorageValue::Set(s) => Ok(s.contains(member)),
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(false),
        }
    }

    /// 返回集合的成员数量
    pub fn scard(&self, key: &str) -> Result<usize> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(0)
                } else {
                    Self::check_set_type(&v.value)?;
                    match &v.value {
                        StorageValue::Set(s) => Ok(s.len()),
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(0),
        }
    }

    /// 返回多个集合的交集
    pub fn sinter(&self, keys: &[String]) -> Result<Vec<Bytes>> {
        let db = self.db();

        let mut sets: Vec<HashSet<Bytes>> = Vec::new();
        for key in keys {
            let mut map = db
                .inner
                .get_shard(key)
                .write()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            match map.get(key) {
                Some(v) => {
                    if v.is_expired() {
                        map.remove(key);
                        return Ok(vec![]);
                    }
                    Self::check_set_type(&v.value)?;
                    match &v.value {
                        StorageValue::Set(s) => sets.push(s.clone()),
                        _ => unreachable!(),
                    }
                }
                None => return Ok(vec![]),
            }
        }

        if sets.is_empty() {
            return Ok(vec![]);
        }

        let mut result = sets[0].clone();
        for set in &sets[1..] {
            result = result.intersection(set).cloned().collect();
        }
        Ok(result.into_iter().collect())
    }

    /// 返回多个集合的并集
    pub fn sunion(&self, keys: &[String]) -> Result<Vec<Bytes>> {
        let db = self.db();

        let mut result = HashSet::new();
        for key in keys {
            let mut map = db
                .inner
                .get_shard(key)
                .write()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            match map.get(key) {
                Some(v) => {
                    if v.is_expired() {
                        map.remove(key);
                        continue;
                    }
                    Self::check_set_type(&v.value)?;
                    match &v.value {
                        StorageValue::Set(s) => {
                            for m in s {
                                result.insert(m.clone());
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                None => continue,
            }
        }
        Ok(result.into_iter().collect())
    }

    /// 返回第一个集合与其他集合的差集
    pub fn sdiff(&self, keys: &[String]) -> Result<Vec<Bytes>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        let db = self.db();

        let mut first_set: Option<HashSet<Bytes>> = None;
        for (i, key) in keys.iter().enumerate() {
            let mut map = db
                .inner
                .get_shard(key)
                .write()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            match map.get(key) {
                Some(v) => {
                    if v.is_expired() {
                        map.remove(key);
                        if i == 0 {
                            return Ok(vec![]);
                        }
                        continue;
                    }
                    Self::check_set_type(&v.value)?;
                    match &v.value {
                        StorageValue::Set(s) => {
                            if i == 0 {
                                first_set = Some(s.clone());
                            } else if let Some(ref mut fs) = first_set {
                                fs.retain(|m| !s.contains(m));
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                None => {
                    if i == 0 {
                        return Ok(vec![]);
                    }
                }
            }
        }

        match first_set {
            Some(s) => Ok(s.into_iter().collect()),
            None => Ok(vec![]),
        }
    }

    /// 随机弹出 count 个成员并删除，返回弹出的成员列表
    /// count 为正数：不重复；count 为负数：可重复（但 pop 语义下 count 通常为正数）
    pub fn spop(&self, key: &str, count: i64) -> Result<Vec<Bytes>> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get_mut(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    return Ok(vec![]);
                }
                Self::check_set_type(&v.value)?;
                match &mut v.value {
                    StorageValue::Set(s) => {
                        if s.is_empty() {
                            map.remove(key);
                            return Ok(vec![]);
                        }
                        let abs_count = count.unsigned_abs() as usize;
                        let mut rng = rand::thread_rng();
                        let mut result = Vec::new();
                        if count > 0 {
                            // 不重复，最多 abs_count 个
                            let n = abs_count.min(s.len());
                            let members: Vec<Bytes> = s.iter().cloned().collect();
                            let selected = members.choose_multiple(&mut rng, n);
                            for item in selected {
                                result.push(item.clone());
                                s.remove(item);
                            }
                        } else {
                            // 可重复（虽然 Redis SPOP 通常只支持正数，但为了统一）
                            for _ in 0..abs_count {
                                let members: Vec<Bytes> = s.iter().cloned().collect();
                                let item = members.choose(&mut rng).unwrap().clone();
                                result.push(item.clone());
                                s.remove(&item);
                                if s.is_empty() {
                                    break;
                                }
                            }
                        }
                        if s.is_empty() {
                            map.remove(key);
                        }
self.bump_version(&mut map, key);
                        Ok(result)
                    }
                    _ => unreachable!(),
                }
            }
            None => Ok(vec![]),
        }
    }

    /// 随机返回 count 个成员（不删除）
    /// count > 0：不重复；count < 0：可重复
    pub fn srandmember(&self, key: &str, count: i64) -> Result<Vec<Bytes>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    return Ok(vec![]);
                }
                Self::check_set_type(&v.value)?;
                match &v.value {
                    StorageValue::Set(s) => {
                        if s.is_empty() {
                            return Ok(vec![]);
                        }
                        let abs_count = count.unsigned_abs() as usize;
                        let mut rng = rand::thread_rng();
                        let members: Vec<Bytes> = s.iter().cloned().collect();
                        if count > 0 {
                            let n = abs_count.min(s.len());
                            let selected: Vec<Bytes> =
                                members.choose_multiple(&mut rng, n).cloned().collect();
                            Ok(selected)
                        } else {
                            let mut result = Vec::with_capacity(abs_count);
                            for _ in 0..abs_count {
                                let item = members.choose(&mut rng).unwrap().clone();
                                result.push(item);
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

    /// 将成员从 source 移动到 destination，成功返回 true
    pub fn smove(&self, source: &str, destination: &str, member: Bytes) -> Result<bool> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut src_map = db
            .inner
            .get_shard(source)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        // 检查 source
        let removed = match src_map.get_mut(source) {
            Some(v) => {
                if v.is_expired() {
                    src_map.remove(source);
                    return Ok(false);
                }
                Self::check_set_type(&v.value)?;
                match &mut v.value {
                    StorageValue::Set(s) => {
                        let existed = s.remove(&member);
                        if s.is_empty() {
                            src_map.remove(source);
                        }
                        existed
                    }
                    _ => unreachable!(),
                }
            }
            None => return Ok(false),
        };

        if !removed {
            return Ok(false);
        }
        self.bump_version(&mut src_map, source);
        drop(src_map);

        let mut dst_map = db
            .inner
            .get_shard(destination)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        // 添加到 destination
        match dst_map.get_mut(destination) {
            Some(v) => {
                if v.is_expired() {
                    dst_map.remove(destination);
                    let mut s = HashSet::new();
                    s.insert(member);
                    dst_map.insert(destination.to_string(), Entry::new(StorageValue::Set(s)));
                } else {
                    Self::check_set_type(&v.value)?;
                    match &mut v.value {
                        StorageValue::Set(s) => {
                            s.insert(member);
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => {
                let mut s = HashSet::new();
                s.insert(member);
                dst_map.insert(destination.to_string(), Entry::new(StorageValue::Set(s)));
            }
        }

self.bump_version(&mut dst_map, destination);
        Ok(true)
    }

    /// 交集结果存入 destination，返回结果集大小
    pub fn sinterstore(&self, destination: &str, keys: &[String]) -> Result<usize> {
        self.evict_if_needed()?;
        let result = self.sinter(keys)?;
        let len = result.len();

        let db = self.db();
        let mut map = db
            .inner
            .get_shard(destination)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        if len == 0 {
            map.remove(destination);
        } else {
            let mut set = HashSet::new();
            for member in result {
                set.insert(member);
            }
            map.insert(destination.to_string(), Entry::new(StorageValue::Set(set)));
        }
self.bump_version(&mut map, destination);
        Ok(len)
    }

    /// 并集结果存入 destination，返回结果集大小
    pub fn sunionstore(&self, destination: &str, keys: &[String]) -> Result<usize> {
        self.evict_if_needed()?;
        let result = self.sunion(keys)?;
        let len = result.len();

        let db = self.db();
        let mut map = db
            .inner
            .get_shard(destination)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        if len == 0 {
            map.remove(destination);
        } else {
            let mut set = HashSet::new();
            for member in result {
                set.insert(member);
            }
            map.insert(destination.to_string(), Entry::new(StorageValue::Set(set)));
        }
self.bump_version(&mut map, destination);
        Ok(len)
    }

    /// 差集结果存入 destination，返回结果集大小
    pub fn sdiffstore(&self, destination: &str, keys: &[String]) -> Result<usize> {
        self.evict_if_needed()?;
        let result = self.sdiff(keys)?;
        let len = result.len();

        let db = self.db();
        let mut map = db
            .inner
            .get_shard(destination)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        if len == 0 {
            map.remove(destination);
        } else {
            let mut set = HashSet::new();
            for member in result {
                set.insert(member);
            }
            map.insert(destination.to_string(), Entry::new(StorageValue::Set(set)));
        }
self.bump_version(&mut map, destination);
        Ok(len)
    }

    /// 返回集合交集的基数（不实际创建结果集）
    /// limit > 0 时提前终止计数
    pub fn sintercard(&self, keys: &[String], limit: usize) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }

        let db = self.db();
        let mut first = true;
        let mut inter: HashSet<Bytes> = HashSet::new();

        for key in keys {
            let map = db
                .inner
                .get_shard(key)
                .read()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            let current: HashSet<Bytes> = if let Some(v) = map.get(key) {
                if v.is_expired() {
                    HashSet::new()
                } else if let StorageValue::Set(s) = &v.value {
                    s.clone()
                } else {
                    HashSet::new()
                }
            } else {
                HashSet::new()
            };

            if first {
                inter = current;
                first = false;
            } else {
                inter.retain(|m| current.contains(m));
            }

            if limit > 0 && inter.len() >= limit {
                return Ok(limit);
            }
        }

        Ok(inter.len())
    }

    /// 批量检查成员是否在集合中，返回 0/1 数组
    pub fn smismember(&self, key: &str, members: &[Bytes]) -> Result<Vec<i64>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(vec![0; members.len()])
                } else {
                    Self::check_set_type(&v.value)?;
                    match &v.value {
                        StorageValue::Set(s) => Ok(members
                            .iter()
                            .map(|m| if s.contains(m) { 1 } else { 0 })
                            .collect()),
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(vec![0; members.len()]),
        }
    }

    /// 增量迭代集合成员
    pub fn sscan(
        &self,
        key: &str,
        cursor: usize,
        pattern: &str,
        count: usize,
    ) -> Result<(usize, Vec<Bytes>)> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    return Ok((0, vec![]));
                }
                Self::check_set_type(&v.value)?;
                match &v.value {
                    StorageValue::Set(s) => {
                        let mut members: Vec<Bytes> = s.iter().cloned().collect();
                        members.sort();
                        let mut filtered = Vec::new();
                        for member in members {
                            if Self::glob_match(&String::from_utf8_lossy(&member), pattern) {
                                filtered.push(member);
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

    // ---------- ZSet 操作 ----------
}
