//! List 数据类型操作（对标 Redis List 命令族）
use super::*;

impl StorageEngine {
    /// 从列表左侧插入一个或多个值（对标 Redis LPUSH 命令）
    ///
    /// 将所有 value 依次插入到列表 key 的表头。
    /// 如果 key 不存在，则创建一个空列表再执行插入。
    /// 如果 key 存在但不是列表类型，返回 WRONGTYPE 错误。
    ///
    /// # 参数
    /// - `key` - 列表键名
    /// - `values` - 要插入的值列表（第一个值会成为新的表头）
    ///
    /// # 返回值
    /// - `Ok(usize)` - 插入后列表的长度
    /// - `Err(AppError::Storage)` - 类型错误或锁中毒
    ///
    /// # 行为差异
    /// 与 Redis 7 行为一致。
    pub fn lpush(&self, key: &str, values: Vec<Bytes>) -> Result<usize> {
        self.evict_if_needed()?;
        let db = self.db();
        let len = {
            let mut map = self.write_shard(key)?;

            self.check_and_remove_expired(&db, &mut map, key);
            match map.get_mut(key) {
                Some(v) => {
                                        Self::check_list_type(v)?;
                                        let list = Self::as_list_mut(v).unwrap();
                                        for value in values {
                                            list.push_front(value);
                                        }
                                        self.bump_version(key);
                                        list.len()
                }
                None => {
                                    let mut list: VecDeque<Bytes> = VecDeque::new();
                                    for value in values {
                                        list.push_front(value);
                                    }
                                    let len = list.len();
                                    map.insert(key.to_string(), StorageValue::List(list));
                                    self.bump_version(key);
                                    len
                }
            }
        };
        self.notify_blocking_waiters(key);
        Ok(len)
    }

    /// 从列表右侧插入一个或多个值（对标 Redis RPUSH 命令）
    ///
    /// 将所有 value 依次插入到列表 key 的表尾。
    /// 如果 key 不存在，则创建一个空列表再执行插入。
    /// 如果 key 存在但不是列表类型，返回 WRONGTYPE 错误。
    ///
    /// # 参数
    /// - `key` - 列表键名
    /// - `values` - 要插入的值列表（第一个值先插入，位于较前位置）
    ///
    /// # 返回值
    /// - `Ok(usize)` - 插入后列表的长度
    /// - `Err(AppError::Storage)` - 类型错误或锁中毒
    ///
    /// # 行为差异
    /// 与 Redis 7 行为一致。
    pub fn rpush(&self, key: &str, values: Vec<Bytes>) -> Result<usize> {
        self.evict_if_needed()?;
        let db = self.db();
        let len = {
            let mut map = self.write_shard(key)?;

            self.check_and_remove_expired(&db, &mut map, key);
            match map.get_mut(key) {
                Some(v) => {
                                        Self::check_list_type(v)?;
                                        let list = Self::as_list_mut(v).unwrap();
                                        for value in values {
                                            list.push_back(value);
                                        }
                                        self.bump_version(key);
                                        list.len()
                }
                None => {
                                    let mut list: VecDeque<Bytes> = VecDeque::new();
                                    for value in values {
                                        list.push_back(value);
                                    }
                                    let len = list.len();
                                    map.insert(key.to_string(), StorageValue::List(list));
                                    self.bump_version(key);
                                    len
                }
            }
        };
        self.notify_blocking_waiters(key);
        Ok(len)
    }

    /// 从列表左侧弹出一个值（对标 Redis LPOP 命令）
    ///
    /// 移除并返回列表 key 的表头元素。
    /// 键不存在返回 None；键存在但不是列表类型返回 WRONGTYPE 错误。
    /// 弹出后若列表为空，则自动删除该 key。
    ///
    /// # 参数
    /// - `key` - 列表键名
    ///
    /// # 返回值
    /// - `Ok(Some(Bytes))` - 弹出的表头元素
    /// - `Ok(None)` - 键不存在
    /// - `Err(AppError::Storage)` - 类型错误或锁中毒
    ///
    /// # 行为差异
    /// 与 Redis 7 行为一致。
    pub fn lpop(&self, key: &str) -> Result<Option<Bytes>> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        self.check_and_remove_expired(&db, &mut map, key);
        match map.get_mut(key) {
            Some(v) => {
                                Self::check_list_type(v)?;
                                let list = Self::as_list_mut(v).unwrap();
                                let result = list.pop_front();
                                if list.is_empty() {
                                    map.remove(key);
                                }
                                self.bump_version(key);
                                Ok(result)
            }
            None => Ok(None),
        }
    }

    /// 从列表右侧弹出一个值（对标 Redis RPOP 命令）
    ///
    /// 移除并返回列表 key 的表尾元素。
    /// 键不存在返回 None；键存在但不是列表类型返回 WRONGTYPE 错误。
    /// 弹出后若列表为空，则自动删除该 key。
    ///
    /// # 参数
    /// - `key` - 列表键名
    ///
    /// # 返回值
    /// - `Ok(Some(Bytes))` - 弹出的表尾元素
    /// - `Ok(None)` - 键不存在
    /// - `Err(AppError::Storage)` - 类型错误或锁中毒
    ///
    /// # 行为差异
    /// 与 Redis 7 行为一致。
    pub fn rpop(&self, key: &str) -> Result<Option<Bytes>> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        self.check_and_remove_expired(&db, &mut map, key);
        match map.get_mut(key) {
            Some(v) => {
                                Self::check_list_type(v)?;
                                let list = Self::as_list_mut(v).unwrap();
                                let result = list.pop_back();
                                if list.is_empty() {
                                    map.remove(key);
                                }
                                self.bump_version(key);
                                Ok(result)
            }
            None => Ok(None),
        }
    }

    /// 返回列表的长度（对标 Redis LLEN 命令）
    ///
    /// 获取列表 key 的长度。
    /// 如果 key 不存在，返回 0。
    /// 如果 key 存在但不是列表类型，返回 WRONGTYPE 错误。
    ///
    /// # 参数
    /// - `key` - 列表键名
    ///
    /// # 返回值
    /// - `Ok(usize)` - 列表的长度（不存在则为 0）
    /// - `Err(AppError::Storage)` - 类型错误或锁中毒
    ///
    /// # 行为差异
    /// 与 Redis 7 行为一致。
    pub fn llen(&self, key: &str) -> Result<usize> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        self.check_and_remove_expired(&db, &mut map, key);
        match map.get(key) {
            Some(v) => {
                                Self::check_list_type(v)?;
                                match v {
                                    StorageValue::List(list) => Ok(list.len()),
                                    _ => unreachable!(),
                                }
            }
            None => Ok(0),
        }
    }

    /// 返回列表指定范围内的元素（对标 Redis LRANGE 命令）
    ///
    /// 获取列表 key 中指定区间内的元素，区间包含 start 和 stop。
    /// 支持负数索引（-1 表示最后一个元素）。
    /// 如果 start 超过列表末尾或 start > stop，返回空列表。
    /// 如果 key 不存在，返回空列表。
    /// 如果 key 存在但不是列表类型，返回 WRONGTYPE 错误。
    ///
    /// # 参数
    /// - `key` - 列表键名
    /// - `start` - 起始索引（支持负数）
    /// - `stop` - 结束索引（支持负数）
    ///
    /// # 返回值
    /// - `Ok(Vec<Bytes>)` - 区间内的元素列表（可能为空）
    /// - `Err(AppError::Storage)` - 类型错误或锁中毒
    ///
    /// # 行为差异
    /// 与 Redis 7 行为一致。
    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Bytes>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        self.check_and_remove_expired(&db, &mut map, key);
        let list = match map.get(key) {
            Some(v) => {
                Self::check_list_type(v)?;
                match v {
                    StorageValue::List(l) => l,
                    _ => unreachable!(),
                }
            }
            None => return Ok(vec![]),
        };

        let len = list.len() as i64;
        let mut s = start;
        let mut e = stop;

        if s < 0 {
            s += len;
        }
        if e < 0 {
            e += len;
        }

        s = s.max(0);
        e = e.min(len - 1);

        if s > e || s >= len {
            return Ok(vec![]);
        }

        let start_idx = s as usize;
        let end_idx = e as usize;
        let result: Vec<Bytes> = list
            .iter()
            .skip(start_idx)
            .take(end_idx - start_idx + 1)
            .cloned()
            .collect();
        Ok(result)
    }

    /// 返回列表指定索引位置的元素（对标 Redis LINDEX 命令）
    ///
    /// 获取列表 key 中 index 位置的元素。
    /// 支持负数索引（-1 表示最后一个元素）。
    /// 如果 index 超出范围或 key 不存在，返回 None。
    /// 如果 key 存在但不是列表类型，返回 WRONGTYPE 错误。
    ///
    /// # 参数
    /// - `key` - 列表键名
    /// - `index` - 索引位置（支持负数）
    ///
    /// # 返回值
    /// - `Ok(Some(Bytes))` - 指定索引的元素
    /// - `Ok(None)` - 索引越界或键不存在
    /// - `Err(AppError::Storage)` - 类型错误或锁中毒
    ///
    /// # 行为差异
    /// 与 Redis 7 行为一致。
    pub fn lindex(&self, key: &str, index: i64) -> Result<Option<Bytes>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        self.check_and_remove_expired(&db, &mut map, key);
        let list = match map.get(key) {
            Some(v) => {
                Self::check_list_type(v)?;
                match v {
                    StorageValue::List(l) => l,
                    _ => unreachable!(),
                }
            }
            None => return Ok(None),
        };

        let len = list.len() as i64;
        let mut idx = index;
        if idx < 0 {
            idx += len;
        }

        if idx < 0 || idx >= len {
            Ok(None)
        } else {
            Ok(Some(list[idx as usize].clone()))
        }
    }

    /// 设置列表指定位置的值（对标 Redis LSET 命令）
    ///
    /// 将列表 key 中 index 位置的元素设置为 value。
    /// 支持负数索引（-1 表示最后一个元素）。
    /// 如果 index 超出范围返回 "ERR index out of range" 错误。
    /// 如果 key 不存在返回 "ERR no such key" 错误。
    /// 如果 key 存在但不是列表类型，返回 WRONGTYPE 错误。
    ///
    /// # 参数
    /// - `key` - 列表键名
    /// - `index` - 要设置的索引位置（支持负数）
    /// - `value` - 新值
    ///
    /// # 返回值
    /// - `Ok(())` - 设置成功
    /// - `Err(AppError::Storage)` - 索引越界、键不存在、类型错误或锁中毒
    ///
    /// # 行为差异
    /// 与 Redis 7 行为一致。
    pub fn lset(&self, key: &str, index: i64, value: Bytes) -> Result<()> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        self.check_and_remove_expired(&db, &mut map, key);
        match map.get_mut(key) {
            Some(v) => {
                            Self::check_list_type(v)?;
                            let list = Self::as_list_mut(v).unwrap();
                            let len = list.len() as i64;
                            let mut idx = index;
                            if idx < 0 {
                                idx += len;
                            }
                            if idx < 0 || idx >= len {
                                return Err(AppError::Storage(
                                    "ERR index out of range".to_string(),
                                ));
                            }
                            list[idx as usize] = value;
                            self.bump_version(key);
                            Ok(())
            }
            None => {
                Err(AppError::Storage(
                "ERR no such key".to_string()
                ))
            }
        }
    }

    /// 在列表中 pivot 元素的前或后插入值（对标 Redis LINSERT 命令）
    ///
    /// 在列表 key 中查找第一个与 pivot 相等的元素，
    /// 在其前面（Before）或后面（After）插入 value。
    /// 如果 key 不存在，返回 -1。
    /// 如果 pivot 不存在，返回 -1。
    /// 如果 key 存在但不是列表类型，返回 WRONGTYPE 错误。
    ///
    /// # 参数
    /// - `key` - 列表键名
    /// - `position` - 插入位置（Before 或 After）
    /// - `pivot` - 参照元素值
    /// - `value` - 要插入的新值
    ///
    /// # 返回值
    /// - `Ok(i64)` - 插入后列表的长度
    /// - `Ok(-1)` - 键不存在或 pivot 未找到
    /// - `Err(AppError::Storage)` - 类型错误或锁中毒
    ///
    /// # 行为差异
    /// 与 Redis 7 行为一致。
    pub fn linsert(&self, key: &str, position: LInsertPosition, pivot: Bytes, value: Bytes) -> Result<i64> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        self.check_and_remove_expired(&db, &mut map, key);
        match map.get_mut(key) {
            Some(v) => {
                            Self::check_list_type(v)?;
                            let list = Self::as_list_mut(v).unwrap();
                            let mut found = false;
                            for i in 0..list.len() {
                                if list[i] == pivot {
                                    let insert_idx = if position == LInsertPosition::Before {
                                        i
                                    } else {
                                        i + 1
                                    };
                                    // VecDeque 不支持直接按索引插入，先转为 Vec
                                    let mut vec: Vec<Bytes> = list.iter().cloned().collect();
                                    vec.insert(insert_idx, value.clone());
                                    *list = VecDeque::from(vec);
                                    found = true;
                                    break;
                                }
                            }
                            if !found {
                                return Ok(-1);
                            }
                            self.bump_version(key);
                            Ok(list.len() as i64)
            }
            None => Ok(-1),
        }
    }

    /// 从列表中删除 count 个等于 value 的元素（对标 Redis LREM 命令）
    ///
    /// 根据 count 的值，从列表 key 中移除与 value 相等的元素：
    /// - count > 0：从表头开始向表尾搜索，移除最多 count 个
    /// - count < 0：从表尾开始向表头搜索，移除最多 |count| 个
    /// - count = 0：移除所有与 value 相等的元素
    /// 如果 key 不存在，返回 0。
    /// 如果 key 存在但不是列表类型，返回 WRONGTYPE 错误。
    ///
    /// # 参数
    /// - `key` - 列表键名
    /// - `count` - 删除方向和数量（正数从头，负数从尾，0 删除全部）
    /// - `value` - 要删除的元素值
    ///
    /// # 返回值
    /// - `Ok(i64)` - 实际删除的元素数量
    /// - `Err(AppError::Storage)` - 类型错误或锁中毒
    ///
    /// # 行为差异
    /// 与 Redis 7 行为一致。
    pub fn lrem(&self, key: &str, count: i64, value: Bytes) -> Result<i64> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        self.check_and_remove_expired(&db, &mut map, key);
        match map.get_mut(key) {
            Some(v) => {
                            Self::check_list_type(v)?;
                            let list = Self::as_list_mut(v).unwrap();
                            let mut removed = 0i64;
                            if count == 0 {
                                // 删除全部匹配的元素
                                let _original_len = list.len();
                                list.retain(|item| {
                                    if *item == value {
                                        removed += 1;
                                        false
                                    } else {
                                        true
                                    }
                                });
                                if removed > 0 {
                                    self.bump_version(key);
                                }
                                return Ok(removed);
                            }
                            let mut indices_to_remove = Vec::new();
                            if count > 0 {
                                for (i, item) in list.iter().enumerate() {
                                    if *item == value {
                                        indices_to_remove.push(i);
                                        if indices_to_remove.len() >= count as usize {
                                            break;
                                        }
                                    }
                                }
                            } else {
                                let abs_count = (-count) as usize;
                                for (i, item) in list.iter().enumerate().rev() {
                                    if *item == value {
                                        indices_to_remove.push(i);
                                        if indices_to_remove.len() >= abs_count {
                                            break;
                                        }
                                    }
                                }
                            }
                            removed = indices_to_remove.len() as i64;
                            // 按索引降序删除，避免索引偏移问题
                            indices_to_remove.sort_unstable_by(|a, b| b.cmp(a));
                            let mut vec: Vec<Bytes> = list.iter().cloned().collect();
                            for idx in indices_to_remove {
                                vec.remove(idx);
                            }
                            *list = VecDeque::from(vec);
                            if removed > 0 {
                                self.bump_version(key);
                                if list.is_empty() {
                                    map.remove(key);
                                }
                            }
                            Ok(removed)
            }
            None => Ok(0),
        }
    }

    /// 修剪列表，只保留指定范围的元素（对标 Redis LTRIM 命令）
    ///
    /// 对列表 key 进行修剪，只保留 [start, stop] 区间内的元素，其余删除。
    /// 支持负数索引（-1 表示最后一个元素）。
    /// 如果 start 超过列表末尾或 start > stop，删除整个列表。
    /// 如果 key 不存在，直接返回成功。
    /// 如果 key 存在但不是列表类型，返回 WRONGTYPE 错误。
    ///
    /// # 参数
    /// - `key` - 列表键名
    /// - `start` - 保留区间的起始索引（支持负数）
    /// - `stop` - 保留区间的结束索引（支持负数）
    ///
    /// # 返回值
    /// - `Ok(())` - 修剪成功
    /// - `Err(AppError::Storage)` - 类型错误或锁中毒
    ///
    /// # 行为差异
    /// 与 Redis 7 行为一致。
    pub fn ltrim(&self, key: &str, start: i64, stop: i64) -> Result<()> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        self.check_and_remove_expired(&db, &mut map, key);
        match map.get_mut(key) {
            Some(v) => {
                            Self::check_list_type(v)?;
                            let list = Self::as_list_mut(v).unwrap();
                            let len = list.len() as i64;
                            let mut s = start;
                            let mut e = stop;
                            if s < 0 {
                                s += len;
                            }
                            if e < 0 {
                                e += len;
                            }
                            s = s.max(0);
                            e = e.min(len - 1);
                            if s > e || s >= len {
                                map.remove(key);
                                return Ok(());
                            }
                            let start_idx = s as usize;
                            let end_idx = e as usize;
                            let mut new_list = VecDeque::new();
                            for item in list.iter().take(end_idx + 1).skip(start_idx) {
                                new_list.push_back(item.clone());
                            }
                            if new_list.is_empty() {
                                map.remove(key);
                            } else {
                                *list = new_list;
                            }
                            self.bump_version(key);
                            Ok(())
            }
            None => Ok(()),
        }
    }

    /// 在列表中查找 value 的位置（对标 Redis LPOS 命令）
    ///
    /// 返回列表 key 中与 value 相等的元素的位置（0-based）。
    /// 支持通过 rank 控制从第几个匹配开始返回，
    /// 支持通过 count 限制返回数量（0 表示不限制），
    /// 支持通过 maxlen 限制最多检查的元素数（0 表示不限制）。
    /// 如果 rank 为 0，返回空列表。
    /// 如果 key 不存在，返回空列表。
    /// 如果 key 存在但不是列表类型，返回 WRONGTYPE 错误。
    ///
    /// # 参数
    /// - `key` - 列表键名
    /// - `value` - 要查找的元素值
    /// - `rank` - 从第 rank 个匹配开始计数（正数从头，负数从尾）
    /// - `count` - 最多返回的位置数量（0 表示不限制）
    /// - `maxlen` - 最多检查的元素数量（0 表示不限制）
    ///
    /// # 返回值
    /// - `Ok(Vec<i64>)` - 匹配位置列表（0-based），找不到则为空
    /// - `Err(AppError::Storage)` - 类型错误或锁中毒
    ///
    /// # 行为差异
    /// 与 Redis 7 行为一致。
    pub fn lpos(&self, key: &str, value: Bytes, rank: i64, count: i64, maxlen: i64) -> Result<Vec<i64>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        self.check_and_remove_expired(&db, &mut map, key);
        match map.get(key) {
            Some(v) => {
                            Self::check_list_type(v)?;
                            let list = match v {
                                StorageValue::List(l) => l,
                                _ => unreachable!(),
                            };
                            let mut result = Vec::new();
                            let len = list.len() as i64;
                            let check_limit = if maxlen > 0 { maxlen } else { len };
                            if rank == 0 {
                                return Ok(Vec::new());
                            }
                            let mut matched = 0i64;
                            let target_match = rank.abs();
                            let iter: Box<dyn Iterator<Item = (usize, &Bytes)>> = if rank > 0 {
                                Box::new(list.iter().enumerate())
                            } else {
                                Box::new(list.iter().enumerate().rev())
                            };
                            for (checked, (idx, item)) in iter.enumerate() {
                                if checked >= check_limit as usize {
                                    break;
                                }
                                if *item == value {
                                    matched += 1;
                                    if matched >= target_match {
                                        result.push(idx as i64);
                                        if count == 0 || result.len() >= count as usize {
                                            break;
                                        }
                                    }
                                }
                            }
                            Ok(result)
            }
            None => Ok(Vec::new()),
        }
    }

    /// 通知阻塞等待者有数据可消费
    ///
    /// 当列表 key 有新元素插入时，唤醒所有在该 key 上阻塞等待的客户端，
    /// 使它们能够重新尝试弹出操作。
    ///
    /// # 参数
    /// - `key` - 发生变化的列表键名
    pub(crate) fn notify_blocking_waiters(&self, key: &str) {
        let db = self.db();
        {
            let waiters_map = match db.blocking_waiters.read() {
                Ok(m) => m,
                Err(_) => return,
            };
            if waiters_map.is_empty() || !waiters_map.contains_key(key) {
                return;
            }
        }
        let waiters = {
            let mut waiters_map = match db.blocking_waiters.write() {
                Ok(m) => m,
                Err(_) => return,
            };
            waiters_map.remove(key).unwrap_or_default()
        };
        for notify in waiters {
            notify.notify_one();
        }
    }

    /// 从阻塞等待者列表中注销指定的通知对象
    ///
    /// 在阻塞操作完成或超时时，将指定 notify 从各个 key 的等待者列表中移除，
    /// 防止内存泄漏和重复通知。
    ///
    /// # 参数
    /// - `keys` - 之前注册等待的键名列表
    /// - `notify` - 要注销的通知对象
    fn unregister_blocking_waiter(&self, keys: &[String], notify: &Arc<tokio::sync::Notify>) {
        let db = self.db();
        let mut waiters_map = match db.blocking_waiters.write() {
            Ok(m) => m,
            Err(_) => return,
        };
        for key in keys {
            if let Some(list) = waiters_map.get_mut(key) {
                list.retain(|n| !Arc::ptr_eq(n, notify));
                if list.is_empty() {
                    waiters_map.remove(key);
                }
            }
        }
    }

    /// 阻塞式左弹出（对标 Redis BLPOP 命令）
    ///
    /// 按顺序检查多个列表键，对第一个非空列表执行 LPOP 操作。
    /// 如果所有列表都为空，则阻塞等待，直到有列表被 push 或超时。
    /// timeout_secs = 0 表示永久阻塞。
    ///
    /// # 参数
    /// - `keys` - 要检查的列表键名数组（按优先级顺序）
    /// - `timeout_secs` - 超时时间（秒），0 表示无限阻塞
    ///
    /// # 返回值
    /// - `Ok(Some((key, value)))` - 弹出的键名和值
    /// - `Ok(None)` - 超时且没有数据
    /// - `Err(AppError::Storage)` - 锁中毒
    ///
    /// # 行为差异
    /// 与 Redis 7 行为一致。
    pub async fn blpop(&self, keys: &[String], timeout_secs: f64) -> Result<Option<(String, Bytes)>> {
        let deadline = if timeout_secs > 0.0 {
            Some(tokio::time::Instant::now() + tokio::time::Duration::from_secs_f64(timeout_secs))
        } else {
            None
        };

        loop {
            // 先尝试立即弹出
            for key in keys {
                match self.lpop(key) {
                    Ok(Some(value)) => return Ok(Some((key.clone(), value))),
                    Ok(None) | Err(_) => continue,
                }
            }

            // 注册等待者
            let notify = Arc::new(tokio::sync::Notify::new());
            {
                let db = self.db();
                let mut waiters_map = db.blocking_waiters.write().map_err(|e| {
                    AppError::Storage(format!("阻塞等待者锁中毒: {}", e))
                })?;
                for key in keys {
                    waiters_map.entry(key.clone()).or_default().push(notify.clone());
                }
            }

            // 等待通知或超时
            let wait_result = if let Some(deadline) = deadline {
                let remaining = deadline - tokio::time::Instant::now();
                if remaining <= tokio::time::Duration::ZERO {
                    self.unregister_blocking_waiter(keys, &notify);
                    return Ok(None);
                }
                tokio::time::timeout(remaining, notify.notified()).await
            } else {
                let _: () = notify.notified().await;
                Ok(())
            };

            self.unregister_blocking_waiter(keys, &notify);

            match wait_result {
                Ok(()) => continue, // 被唤醒，重新尝试
                Err(_) => return Ok(None), // 超时
            }
        }
    }

    /// 阻塞式右弹出（对标 Redis BRPOP 命令）
    ///
    /// 按顺序检查多个列表键，对第一个非空列表执行 RPOP 操作。
    /// 如果所有列表都为空，则阻塞等待，直到有列表被 push 或超时。
    /// timeout_secs = 0 表示永久阻塞。
    ///
    /// # 参数
    /// - `keys` - 要检查的列表键名数组（按优先级顺序）
    /// - `timeout_secs` - 超时时间（秒），0 表示无限阻塞
    ///
    /// # 返回值
    /// - `Ok(Some((key, value)))` - 弹出的键名和值
    /// - `Ok(None)` - 超时且没有数据
    /// - `Err(AppError::Storage)` - 锁中毒
    ///
    /// # 行为差异
    /// 与 Redis 7 行为一致。
    pub async fn brpop(&self, keys: &[String], timeout_secs: f64) -> Result<Option<(String, Bytes)>> {
        let deadline = if timeout_secs > 0.0 {
            Some(tokio::time::Instant::now() + tokio::time::Duration::from_secs_f64(timeout_secs))
        } else {
            None
        };

        loop {
            // 先尝试立即弹出
            for key in keys {
                match self.rpop(key) {
                    Ok(Some(value)) => return Ok(Some((key.clone(), value))),
                    Ok(None) | Err(_) => continue,
                }
            }

            // 注册等待者
            let notify = Arc::new(tokio::sync::Notify::new());
            {
                let db = self.db();
                let mut waiters_map = db.blocking_waiters.write().map_err(|e| {
                    AppError::Storage(format!("阻塞等待者锁中毒: {}", e))
                })?;
                for key in keys {
                    waiters_map.entry(key.clone()).or_default().push(notify.clone());
                }
            }

            // 等待通知或超时
            let wait_result = if let Some(deadline) = deadline {
                let remaining = deadline - tokio::time::Instant::now();
                if remaining <= tokio::time::Duration::ZERO {
                    self.unregister_blocking_waiter(keys, &notify);
                    return Ok(None);
                }
                tokio::time::timeout(remaining, notify.notified()).await
            } else {
                let _: () = notify.notified().await;
                Ok(())
            };

            self.unregister_blocking_waiter(keys, &notify);

            match wait_result {
                Ok(()) => continue, // 被唤醒，重新尝试
                Err(_) => return Ok(None), // 超时
            }
        }
    }

    /// 原子性地从 source 列表弹出元素并推入 destination 列表（对标 Redis LMOVE 命令）
    ///
    /// 从 source 列表弹出元素，并立即将其推入 destination 列表。
    /// 可以分别指定从 source 的哪一侧弹出以及推入 destination 的哪一侧。
    /// 如果 source 不存在，返回 None。
    /// 如果 source 或 destination 存在但不是列表类型，返回 WRONGTYPE 错误。
    ///
    /// # 参数
    /// - `source` - 源列表键名
    /// - `destination` - 目标列表键名
    /// - `left_from` - true 表示从左侧弹出，false 表示从右侧弹出
    /// - `left_to` - true 表示推入左侧，false 表示推入右侧
    ///
    /// # 返回值
    /// - `Ok(Some(Bytes))` - 移动的元素
    /// - `Ok(None)` - source 不存在
    /// - `Err(AppError::Storage)` - 类型错误或锁中毒
    ///
    /// # 行为差异
    /// 与 Redis 7 行为一致。
    pub fn lmove(&self, source: &str, destination: &str, left_from: bool, left_to: bool) -> Result<Option<Bytes>> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut src_map = self.write_shard(source)?;

        // 从 source 弹出
        self.check_and_remove_expired(&db, &mut src_map, source);
        let popped = match src_map.get_mut(source) {
            Some(v) => {
                                Self::check_list_type(v)?;
                                let list = Self::as_list_mut(v).unwrap();
                                let result = if left_from { list.pop_front() } else { list.pop_back() };
                                if list.is_empty() {
                                    src_map.remove(source);
                                }
                                result
            }
            None => None,
        };
        drop(src_map);

        let value = match popped {
            Some(v) => v,
            None => return Ok(None),
        };

        let mut dst_map = self.write_shard(destination)?;

        // 推入 destination
        self.check_and_remove_expired(&db, &mut dst_map, destination);
        match dst_map.get_mut(destination) {
            Some(v) => {
                                Self::check_list_type(v)?;
                                let list = Self::as_list_mut(v).unwrap();
                                if left_to {
                                    list.push_front(value.clone());
                                } else {
                                    list.push_back(value.clone());
                                }
            }
            None => {
                            let mut list: VecDeque<Bytes> = VecDeque::new();
                            if left_to {
                                list.push_front(value.clone());
                            } else {
                                list.push_back(value.clone());
                            }
                            dst_map.insert(destination.to_string(), StorageValue::List(list));
            }
        };

        self.bump_version(source);
        self.bump_version(destination);
        drop(dst_map);
        self.notify_blocking_waiters(destination);
        Ok(Some(value))
    }

    /// 原子性地从 source 右侧弹出并推入 destination 左侧（对标 Redis RPOPLPUSH 命令）
    ///
    /// 等同于 LMOVE source destination RIGHT LEFT。
    /// 这是一个已废弃命令的兼容实现，建议使用 LMOVE 替代。
    ///
    /// # 参数
    /// - `source` - 源列表键名
    /// - `destination` - 目标列表键名
    ///
    /// # 返回值
    /// - `Ok(Some(Bytes))` - 移动的元素
    /// - `Ok(None)` - source 不存在
    /// - `Err(AppError::Storage)` - 类型错误或锁中毒
    ///
    /// # 行为差异
    /// 与 Redis 7 行为一致。
    pub fn rpoplpush(&self, source: &str, destination: &str) -> Result<Option<Bytes>> {
        self.lmove(source, destination, false, true)
    }

    /// 从多个列表中找第一个非空的，弹出 count 个元素（对标 Redis LMPOP 命令）
    ///
    /// 按顺序检查多个列表键，对第一个非空列表弹出最多 count 个元素。
    /// left 为 true 时从左侧弹出，false 时从右侧弹出。
    /// 如果所有列表都不存在或为空，返回 None。
    ///
    /// # 参数
    /// - `keys` - 要检查的列表键名数组（按优先级顺序）
    /// - `left` - true 表示从左侧弹出，false 表示从右侧弹出
    /// - `count` - 最多弹出的元素数量
    ///
    /// # 返回值
    /// - `Ok(Some((key, elements)))` - 弹出的键名和元素列表
    /// - `Ok(None)` - 所有列表都不存在或为空
    /// - `Err(AppError::Storage)` - 类型错误或锁中毒
    ///
    /// # 行为差异
    /// 与 Redis 7 行为一致。
    pub fn lmpop(&self, keys: &[String], left: bool, count: usize) -> Result<Option<(String, Vec<Bytes>)>> {
        self.evict_if_needed()?;
        let db = self.db();

        for key in keys {
            let mut map = self.write_shard(key)?;
            self.check_and_remove_expired(&db, &mut map, key);
            let popped = match map.get_mut(key) {
                Some(v) => {
                                    Self::check_list_type(v)?;
                                    let list = Self::as_list_mut(v).unwrap();
                                    let mut results = Vec::new();
                                    for _ in 0..count {
                                        match if left { list.pop_front() } else { list.pop_back() } {
                                            Some(val) => results.push(val),
                                            None => break,
                                        }
                                    }
                                    if list.is_empty() {
                                        map.remove(key);
                                    }
                                    if results.is_empty() {
                                        continue;
                                    }
                                    results
                }
                None => continue,
            };
            self.bump_version(key);
            drop(map);
            return Ok(Some((key.clone(), popped)));
        }

        Ok(None)
    }

    /// 阻塞式 LMOVE（对标 Redis BLMOVE 命令）
    ///
    /// 尝试原子性地从 source 列表弹出并推入 destination 列表。
    /// 如果 source 不存在，则阻塞等待直到 source 有数据或超时。
    /// timeout_secs = 0 表示永久阻塞。
    ///
    /// # 参数
    /// - `source` - 源列表键名
    /// - `destination` - 目标列表键名
    /// - `left_from` - true 表示从左侧弹出，false 表示从右侧弹出
    /// - `left_to` - true 表示推入左侧，false 表示推入右侧
    /// - `timeout_secs` - 超时时间（秒），0 表示无限阻塞
    ///
    /// # 返回值
    /// - `Ok(Some(Bytes))` - 移动的元素
    /// - `Ok(None)` - 超时且 source 仍为空
    /// - `Err(AppError::Storage)` - 类型错误或锁中毒
    ///
    /// # 行为差异
    /// 与 Redis 7 行为一致。
    pub async fn blmove(&self, source: &str, destination: &str, left_from: bool, left_to: bool, timeout_secs: f64) -> Result<Option<Bytes>> {
        let deadline = if timeout_secs > 0.0 {
            Some(tokio::time::Instant::now() + tokio::time::Duration::from_secs_f64(timeout_secs))
        } else {
            None
        };
        let keys = vec![source.to_string()];

        loop {
            if let Some(value) = self.lmove(source, destination, left_from, left_to)? { return Ok(Some(value)) }

            // 注册等待者
            let notify = Arc::new(tokio::sync::Notify::new());
            {
                let db = self.db();
                let mut waiters_map = db.blocking_waiters.write().map_err(|e| {
                    AppError::Storage(format!("阻塞等待者锁中毒: {}", e))
                })?;
                waiters_map.entry(source.to_string()).or_default().push(notify.clone());
            }

            // 等待通知或超时
            let wait_result = if let Some(deadline) = deadline {
                let remaining = deadline - tokio::time::Instant::now();
                if remaining <= tokio::time::Duration::ZERO {
                    self.unregister_blocking_waiter(&keys, &notify);
                    return Ok(None);
                }
                tokio::time::timeout(remaining, notify.notified()).await
            } else {
                let _: () = notify.notified().await;
                Ok(())
            };

            self.unregister_blocking_waiter(&keys, &notify);

            match wait_result {
                Ok(()) => continue,
                Err(_) => return Ok(None),
            }
        }
    }

    /// 阻塞式 LMPOP（对标 Redis BLMPOP 命令）
    ///
    /// 按顺序检查多个列表键，对第一个非空列表弹出最多 count 个元素。
    /// 如果所有列表都为空，则阻塞等待直到有列表被 push 或超时。
    /// left 为 true 时从左侧弹出，false 时从右侧弹出。
    /// timeout_secs = 0 表示永久阻塞。
    ///
    /// # 参数
    /// - `keys` - 要检查的列表键名数组（按优先级顺序）
    /// - `left` - true 表示从左侧弹出，false 表示从右侧弹出
    /// - `count` - 最多弹出的元素数量
    /// - `timeout_secs` - 超时时间（秒），0 表示无限阻塞
    ///
    /// # 返回值
    /// - `Ok(Some((key, elements)))` - 弹出的键名和元素列表
    /// - `Ok(None)` - 超时
    /// - `Err(AppError::Storage)` - 类型错误或锁中毒
    ///
    /// # 行为差异
    /// 与 Redis 7 行为一致。
    pub async fn blmpop(&self, keys: &[String], left: bool, count: usize, timeout_secs: f64) -> Result<Option<(String, Vec<Bytes>)>> {
        let deadline = if timeout_secs > 0.0 {
            Some(tokio::time::Instant::now() + tokio::time::Duration::from_secs_f64(timeout_secs))
        } else {
            None
        };

        loop {
            if let Some(result) = self.lmpop(keys, left, count)? { return Ok(Some(result)) }

            // 注册等待者
            let notify = Arc::new(tokio::sync::Notify::new());
            {
                let db = self.db();
                let mut waiters_map = db.blocking_waiters.write().map_err(|e| {
                    AppError::Storage(format!("阻塞等待者锁中毒: {}", e))
                })?;
                for key in keys {
                    waiters_map.entry(key.clone()).or_default().push(notify.clone());
                }
            }

            // 等待通知或超时
            let wait_result = if let Some(deadline) = deadline {
                let remaining = deadline - tokio::time::Instant::now();
                if remaining <= tokio::time::Duration::ZERO {
                    self.unregister_blocking_waiter(keys, &notify);
                    return Ok(None);
                }
                tokio::time::timeout(remaining, notify.notified()).await
            } else {
                let _: () = notify.notified().await;
                Ok(())
            };

            self.unregister_blocking_waiter(keys, &notify);

            match wait_result {
                Ok(()) => continue,
                Err(_) => return Ok(None),
            }
        }
    }

    /// 阻塞式 RPOPLPUSH（对标 Redis BRPOPLPUSH 命令）
    ///
    /// 等同于 BLMOVE source destination RIGHT LEFT。
    /// 这是一个已废弃命令的兼容实现，建议使用 BLMOVE 替代。
    ///
    /// # 参数
    /// - `source` - 源列表键名
    /// - `destination` - 目标列表键名
    /// - `timeout_secs` - 超时时间（秒），0 表示无限阻塞
    ///
    /// # 返回值
    /// - `Ok(Some(Bytes))` - 移动的元素
    /// - `Ok(None)` - 超时
    /// - `Err(AppError::Storage)` - 类型错误或锁中毒
    ///
    /// # 行为差异
    /// 与 Redis 7 行为一致。
    pub async fn brpoplpush(&self, source: &str, destination: &str, timeout_secs: f64) -> Result<Option<Bytes>> {
        self.blmove(source, destination, false, true, timeout_secs).await
    }
}
