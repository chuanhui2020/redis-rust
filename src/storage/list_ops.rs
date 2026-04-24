use super::*;

impl StorageEngine {
    pub fn lpush(&self, key: &str, values: Vec<Bytes>) -> Result<usize> {
        self.evict_if_needed()?;
        let len = {
            let mut map = self.write_shard(key)?;

            Self::check_and_remove_expired(&mut map, key);
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

    /// 从列表右侧插入一个或多个值
    pub fn rpush(&self, key: &str, values: Vec<Bytes>) -> Result<usize> {
        self.evict_if_needed()?;
        let len = {
            let mut map = self.write_shard(key)?;

            Self::check_and_remove_expired(&mut map, key);
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

    /// 从列表左侧弹出一个值，返回弹出的值
    /// 键不存在返回 None，键存在但不是列表类型返回 WRONGTYPE 错误
    pub fn lpop(&self, key: &str) -> Result<Option<Bytes>> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        Self::check_and_remove_expired(&mut map, key);
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

    /// 从列表右侧弹出一个值
    pub fn rpop(&self, key: &str) -> Result<Option<Bytes>> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        Self::check_and_remove_expired(&mut map, key);
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

    /// 返回列表的长度
    pub fn llen(&self, key: &str) -> Result<usize> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        Self::check_and_remove_expired(&mut map, key);
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

    /// 返回列表指定范围内的元素（支持负数索引）
    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Bytes>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        Self::check_and_remove_expired(&mut map, key);
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

    /// 返回列表指定索引位置的元素（支持负数索引）
    pub fn lindex(&self, key: &str, index: i64) -> Result<Option<Bytes>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        Self::check_and_remove_expired(&mut map, key);
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

    /// 设置列表指定位置的值（支持负数索引）
    /// 索引越界返回错误
    pub fn lset(&self, key: &str, index: i64, value: Bytes) -> Result<()> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        Self::check_and_remove_expired(&mut map, key);
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

    /// 在列表中 pivot 元素的前或后插入值
    /// 返回新长度，pivot 不存在返回 -1
    pub fn linsert(&self, key: &str, position: LInsertPosition, pivot: Bytes, value: Bytes) -> Result<i64> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        Self::check_and_remove_expired(&mut map, key);
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

    /// 从列表中删除 count 个等于 value 的元素
    /// count > 0 从头开始删除 count 个
    /// count < 0 从尾开始删除 |count| 个
    /// count = 0 删除全部
    /// 返回实际删除的数量
    pub fn lrem(&self, key: &str, count: i64, value: Bytes) -> Result<i64> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        Self::check_and_remove_expired(&mut map, key);
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

    /// 修剪列表，只保留指定范围的元素（支持负数索引）
    pub fn ltrim(&self, key: &str, start: i64, stop: i64) -> Result<()> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        Self::check_and_remove_expired(&mut map, key);
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

    /// 在列表中查找 value 的位置
    /// rank: 从第 rank 个匹配开始计数（正数从头，负数从尾）
    /// count: 最多返回 count 个位置（0 表示不限制）
    /// maxlen: 最多检查 maxlen 个元素（0 表示不限制）
    /// 返回位置列表（0-based），找不到返回空 Vec
    pub fn lpos(&self, key: &str, value: Bytes, rank: i64, count: i64, maxlen: i64) -> Result<Vec<i64>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        Self::check_and_remove_expired(&mut map, key);
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
    pub(crate) fn notify_blocking_waiters(&self, key: &str) {
        let db = self.db();
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

    /// 从阻塞等待者列表中注销指定 notify
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

    /// 阻塞式左弹出，从多个 key 中找到第一个非空 list 并弹出
    /// timeout_secs = 0 表示永久阻塞
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

    /// 阻塞式右弹出
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

    /// 原子性地从 source 列表弹出元素并推入 destination 列表
    /// left_from: true 表示从左侧弹出，false 表示从右侧弹出
    /// left_to: true 表示推入左侧，false 表示推入右侧
    /// 返回移动的元素
    pub fn lmove(&self, source: &str, destination: &str, left_from: bool, left_to: bool) -> Result<Option<Bytes>> {
        self.evict_if_needed()?;
        let mut src_map = self.write_shard(source)?;

        // 从 source 弹出
        Self::check_and_remove_expired(&mut src_map, source);
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
        Self::check_and_remove_expired(&mut dst_map, destination);
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

    /// RPOPLPUSH：等同于 LMOVE source destination RIGHT LEFT
    pub fn rpoplpush(&self, source: &str, destination: &str) -> Result<Option<Bytes>> {
        self.lmove(source, destination, false, true)
    }

    /// 从多个列表中找第一个非空的，弹出 count 个元素
    /// 返回 (key, elements)
    pub fn lmpop(&self, keys: &[String], left: bool, count: usize) -> Result<Option<(String, Vec<Bytes>)>> {
        self.evict_if_needed()?;

        for key in keys {
            let mut map = self.write_shard(key)?;
            Self::check_and_remove_expired(&mut map, key);
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

    /// 阻塞式 LMOVE
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

    /// 阻塞式 LMPOP
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

    /// 阻塞式 RPOPLPUSH
    pub async fn brpoplpush(&self, source: &str, destination: &str, timeout_secs: f64) -> Result<Option<Bytes>> {
        self.blmove(source, destination, false, true, timeout_secs).await
    }
}
