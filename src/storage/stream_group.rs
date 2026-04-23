use super::*;

impl StorageEngine {

    // ---------- Stream 消费者组操作 ----------

    /// 创建消费者组
    pub fn xgroup_create(&self, key: &str, group_name: &str, id: &str, mkstream: bool) -> Result<bool> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let _now = Self::now_millis();

        let stream = match map.get_mut(key) {
            Some(v) => {
                if Self::is_expired(v) {
                    map.remove(key);
                    if !mkstream {
                        return Err(AppError::Storage("XGROUP CREATE 键不存在".to_string()));
                    }
                    let s = StreamData::new();
                    map.insert(key.to_string(), StorageValue::Stream(s));
                    match map.get_mut(key) {
                        Some(StorageValue::Stream(s)) => s,
                        _ => unreachable!(),
                    }
                } else {
                    Self::check_stream_type(v)?;
                    match v {
                        StorageValue::Stream(s) => s,
                        _ => unreachable!(),
                    }
                }
            }
            None => {
                if !mkstream {
                    return Err(AppError::Storage("XGROUP CREATE 键不存在".to_string()));
                }
                let s = StreamData::new();
                map.insert(key.to_string(), StorageValue::Stream(s));
                match map.get_mut(key) {
                    Some(StorageValue::Stream(s)) => s,
                    _ => unreachable!(),
                }
            }
        };

        if stream.groups.contains_key(group_name) {
            return Err(AppError::Command(
                "XGROUP CREATE 消费者组已存在".to_string(),
            ));
        }

        let last_id = if id == "$" {
            stream.last_id
        } else {
            StreamId::parse(id)?
        };

        stream.groups.insert(
            group_name.to_string(),
            ConsumerGroup::new(group_name.to_string(), last_id),
        );
        self.bump_version(key);
        self.touch(key);
        Ok(true)
    }

    /// 删除消费者组
    pub fn xgroup_destroy(&self, key: &str, group_name: &str) -> Result<bool> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get_mut(key) {
            Some(v) => {
                if Self::is_expired(v) {
                    map.remove(key);
                    Ok(false)
                } else {
                    Self::check_stream_type(v)?;
                    match v {
                        StorageValue::Stream(s) => {
                            let removed = s.groups.remove(group_name).is_some();
                            if s.length == 0 && s.groups.is_empty() {
                                map.remove(key);
                            }
                            self.bump_version(key);
                            Ok(removed)
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(false),
        }
    }

    /// 设置消费者组的 last_delivered_id
    pub fn xgroup_setid(&self, key: &str, group_name: &str, id: &str) -> Result<bool> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get_mut(key) {
            Some(v) => {
                if Self::is_expired(v) {
                    map.remove(key);
                    return Err(AppError::Storage("XGROUP SETID 键不存在".to_string()));
                }
                Self::check_stream_type(v)?;
                match v {
                    StorageValue::Stream(s) => {
                        let group = s.groups.get_mut(group_name).ok_or_else(|| {
                            AppError::Storage("XGROUP SETID 消费者组不存在".to_string())
                        })?;
                        group.last_delivered_id = if id == "$" {
                            s.last_id
                        } else {
                            StreamId::parse(id)?
                        };
                        self.bump_version(key);
                        Ok(true)
                    }
                    _ => unreachable!(),
                }
            }
            None => Err(AppError::Storage("XGROUP SETID 键不存在".to_string())),
        }
    }

    /// 删除消费者组中的消费者，返回其 PEL 数量
    pub fn xgroup_delconsumer(&self, key: &str, group_name: &str, consumer_name: &str) -> Result<usize> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get_mut(key) {
            Some(v) => {
                if Self::is_expired(v) {
                    map.remove(key);
                    Ok(0)
                } else {
                    Self::check_stream_type(v)?;
                    match v {
                        StorageValue::Stream(s) => {
                            let group = s.groups.get_mut(group_name).ok_or_else(|| {
                                AppError::Storage("XGROUP DELCONSUMER 消费者组不存在".to_string())
                            })?;
                            let consumer = group.consumers.remove(consumer_name);
                            let count = consumer.map(|c| c.pel.len()).unwrap_or(0);
                            // 清理 PEL 中该消费者的消息
                            group.pel.retain(|_, entry| entry.consumer != consumer_name);
                            self.bump_version(key);
                            Ok(count)
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(0),
        }
    }

    /// 显式创建消费者
    pub fn xgroup_createconsumer(&self, key: &str, group_name: &str, consumer_name: &str) -> Result<bool> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get_mut(key) {
            Some(v) => {
                if Self::is_expired(v) {
                    map.remove(key);
                    Ok(false)
                } else {
                    Self::check_stream_type(v)?;
                    match v {
                        StorageValue::Stream(s) => {
                            let group = s.groups.get_mut(group_name).ok_or_else(|| {
                                AppError::Storage("XGROUP CREATECONSUMER 消费者组不存在".to_string())
                            })?;
                            if group.consumers.contains_key(consumer_name) {
                                Ok(false)
                            } else {
                                group.consumers.insert(
                                    consumer_name.to_string(),
                                    Consumer::new(consumer_name.to_string()),
                                );
                                self.bump_version(key);
                                Ok(true)
                            }
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(false),
        }
    }

    /// 消费者组读取消息
    /// id 为 ">" 时读取新消息，其他 ID 读取该消费者 PEL 中的消息
    pub fn xreadgroup(
        &self,
        group_name: &str,
        consumer_name: &str,
        keys: &[String],
        ids: &[String],
        count: Option<usize>,
        noack: bool,
    ) -> Result<Vec<(String, Vec<(StreamId, Vec<(String, String)>)>)>> {
        let db = self.db();

        let now = Self::now_millis();
        let mut result = Vec::new();

        for (idx, key) in keys.iter().enumerate() {
            let mut map = db
                .inner
                .get_shard(key)
                .write()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            let read_new = ids[idx] == ">";

            let stream = match map.get_mut(key) {
                Some(v) => {
                    if Self::is_expired(v) {
                        continue;
                    }
                    match v {
                        StorageValue::Stream(s) => s,
                        _ => continue,
                    }
                }
                None => continue,
            };

            let group = match stream.groups.get_mut(group_name) {
                Some(g) => g,
                None => continue,
            };

            // 自动创建消费者
            let consumer = group
                .consumers
                .entry(consumer_name.to_string())
                .or_insert_with(|| Consumer::new(consumer_name.to_string()));
            consumer.seen_time = now;

            let mut stream_result = Vec::new();

            if read_new {
                // 读取新消息（从 last_delivered_id 之后）
                let start_id = group.last_delivered_id;
                for (id, fields) in stream.entries.range((std::ops::Bound::Excluded(start_id), std::ops::Bound::Unbounded)) {
                    stream_result.push((*id, fields.clone()));
                    if let Some(c) = count
                        && stream_result.len() >= c {
                            break;
                        }
                }
                // 更新 last_delivered_id
                if let Some((last_id, _)) = stream_result.last() {
                    group.last_delivered_id = *last_id;
                }
            } else {
                // 读取该消费者 PEL 中的消息（从指定 ID 开始）
                let start_id = if ids[idx] == "$" {
                    stream.last_id
                } else {
                    StreamId::parse(&ids[idx]).unwrap_or_else(|_| StreamId::new(0, 0))
                };
                for id in &consumer.pel {
                    if *id > start_id
                        && let Some(fields) = stream.entries.get(id) {
                            stream_result.push((*id, fields.clone()));
                            if let Some(c) = count
                                && stream_result.len() >= c {
                                    break;
                                }
                        }
                }
            }

            // 加入 PEL（除非 noack）
            if !noack {
                for (id, _) in &stream_result {
                    consumer.pel.insert(*id);
                    group.pel.entry(*id).or_insert(PendingEntry {
                        consumer: consumer_name.to_string(),
                        delivery_time: now,
                        delivery_count: 1,
                    });
                }
            }

            if !stream_result.is_empty() {
                result.push((key.clone(), stream_result));
            }
        }

        Ok(result)
    }

    /// 确认消息，从 PEL 中移除
    pub fn xack(&self, key: &str, group_name: &str, ids: &[StreamId]) -> Result<usize> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get_mut(key) {
            Some(v) => {
                if Self::is_expired(v) {
                    map.remove(key);
                    Ok(0)
                } else {
                    Self::check_stream_type(v)?;
                    match v {
                        StorageValue::Stream(s) => {
                            let group = match s.groups.get_mut(group_name) {
                                Some(g) => g,
                                None => return Ok(0),
                            };
                            let mut acked = 0usize;
                            for id in ids {
                                if let Some(entry) = group.pel.remove(id) {
                                    if let Some(consumer) = group.consumers.get_mut(&entry.consumer) {
                                        consumer.pel.remove(id);
                                    }
                                    acked += 1;
                                }
                            }
                            self.bump_version(key);
                            Ok(acked)
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(0),
        }
    }

    /// 转移消息所有权
    pub fn xclaim(
        &self,
        key: &str,
        group_name: &str,
        consumer_name: &str,
        min_idle_time: u64,
        ids: &[StreamId],
        justid: bool,
    ) -> Result<Vec<(StreamId, Option<Vec<(String, String)>>)>> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let now = Self::now_millis();

        match map.get_mut(key) {
            Some(v) => {
                if Self::is_expired(v) {
                    map.remove(key);
                    Ok(vec![])
                } else {
                    Self::check_stream_type(v)?;
                    match v {
                        StorageValue::Stream(s) => {
                            let group = s.groups.get_mut(group_name).ok_or_else(|| {
                                AppError::Storage("XCLAIM 消费者组不存在".to_string())
                            })?;
                            let mut result = Vec::new();
                            for id in ids {
                                if let Some(entry) = group.pel.get_mut(id) {
                                    let idle = now - entry.delivery_time;
                                    if idle >= min_idle_time {
                                        let old_consumer_name = entry.consumer.clone();
                                        // 转移所有权
                                        if old_consumer_name != consumer_name
                                            && let Some(old_consumer) = group.consumers.get_mut(&old_consumer_name) {
                                                old_consumer.pel.remove(id);
                                            }
                                        entry.consumer = consumer_name.to_string();
                                        entry.delivery_time = now;
                                        entry.delivery_count += 1;

                                        let consumer = group
                                            .consumers
                                            .entry(consumer_name.to_string())
                                            .or_insert_with(|| Consumer::new(consumer_name.to_string()));
                                        consumer.seen_time = now;
                                        consumer.pel.insert(*id);

                                        if justid {
                                            result.push((*id, None));
                                        } else if let Some(fields) = s.entries.get(id) {
                                            result.push((*id, Some(fields.clone())));
                                        }
                                    }
                                }
                            }
                            self.bump_version(key);
                            Ok(result)
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(vec![]),
        }
    }

    /// 自动转移空闲消息
    pub fn xautoclaim(
        &self,
        key: &str,
        group_name: &str,
        consumer_name: &str,
        min_idle_time: u64,
        start: StreamId,
        count: usize,
        justid: bool,
    ) -> Result<(StreamId, Vec<(StreamId, Option<Vec<(String, String)>>)>)> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let now = Self::now_millis();

        match map.get_mut(key) {
            Some(v) => {
                if Self::is_expired(v) {
                    map.remove(key);
                    Ok((start, vec![]))
                } else {
                    Self::check_stream_type(v)?;
                    match v {
                        StorageValue::Stream(s) => {
                            let group = s.groups.get_mut(group_name).ok_or_else(|| {
                                AppError::Storage("XAUTOCLAIM 消费者组不存在".to_string())
                            })?;
                            let mut result = Vec::new();
                            let mut next_id = start;

                            // 遍历 PEL 中从 start 开始的消息
                            let pel_ids: Vec<StreamId> = group.pel
                                .keys()
                                .filter(|id| **id >= start)
                                .copied()
                                .collect();

                            for (processed, id) in pel_ids.into_iter().enumerate() {
                                if processed >= count {
                                    next_id = id;
                                    break;
                                }
                                if let Some(entry) = group.pel.get_mut(&id) {
                                    let idle = now - entry.delivery_time;
                                    if idle >= min_idle_time {
                                        let old_consumer_name = entry.consumer.clone();
                                        if old_consumer_name != consumer_name
                                            && let Some(old_consumer) = group.consumers.get_mut(&old_consumer_name) {
                                                old_consumer.pel.remove(&id);
                                            }
                                        entry.consumer = consumer_name.to_string();
                                        entry.delivery_time = now;
                                        entry.delivery_count += 1;

                                        let consumer = group
                                            .consumers
                                            .entry(consumer_name.to_string())
                                            .or_insert_with(|| Consumer::new(consumer_name.to_string()));
                                        consumer.seen_time = now;
                                        consumer.pel.insert(id);

                                        if justid {
                                            result.push((id, None));
                                        } else if let Some(fields) = s.entries.get(&id) {
                                            result.push((id, Some(fields.clone())));
                                        }
                                    }
                                }
                                next_id = StreamId::new(id.ms_time, id.seq + 1);
                            }

                            self.bump_version(key);
                            Ok((next_id, result))
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok((start, vec![])),
        }
    }

    /// 查询待确认消息
    /// 无参数时返回摘要，有参数时返回详细列表
    pub fn xpending(
        &self,
        key: &str,
        group_name: &str,
        start: Option<StreamId>,
        end: Option<StreamId>,
        count: Option<usize>,
        consumer: Option<&str>,
    ) -> Result<(usize, Option<StreamId>, Option<StreamId>, Vec<(String, usize)>, Vec<(StreamId, String, u64, u32)>)> {
        let db = self.db();
        let map = db
            .inner
            .get_shard(key)
            .read()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if Self::is_expired(v) {
                    return Ok((0, None, None, vec![], vec![]));
                }
                match v {
                    StorageValue::Stream(s) => {
                        let group = match s.groups.get(group_name) {
                            Some(g) => g,
                            None => return Ok((0, None, None, vec![], vec![])),
                        };

                        let total = group.pel.len();
                        let min_id = group.pel.keys().next().copied();
                        let max_id = group.pel.keys().next_back().copied();

                        // 消费者统计
                        let mut consumer_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
                        for entry in group.pel.values() {
                            *consumer_counts.entry(entry.consumer.clone()).or_insert(0) += 1;
                        }
                        let consumers: Vec<(String, usize)> = consumer_counts.into_iter().collect();

                        // 详细列表（如果提供了范围参数）
                        let mut details = Vec::new();
                        if let (Some(s), Some(e), Some(c)) = (start, end, count) {
                            for (id, entry) in group.pel.range(s..=e) {
                                if let Some(cname) = consumer
                                    && entry.consumer != cname {
                                        continue;
                                    }
                                let now = Self::now_millis();
                                let idle = now - entry.delivery_time;
                                details.push((*id, entry.consumer.clone(), idle, entry.delivery_count));
                                if details.len() >= c {
                                    break;
                                }
                            }
                        }

                        Ok((total, min_id, max_id, consumers, details))
                    }
                    _ => Ok((0, None, None, vec![], vec![])),
                }
            }
            None => Ok((0, None, None, vec![], vec![])),
        }
    }

    /// 返回 Stream 信息
    pub fn xinfo_stream(&self, key: &str, _full: bool) -> Result<Option<(usize, usize, StreamId, StreamId, Vec<String>)>> {
        let db = self.db();
        let map = db
            .inner
            .get_shard(key)
            .read()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if Self::is_expired(v) {
                    Ok(None)
                } else {
                    match v {
                        StorageValue::Stream(s) => {
                            let first_id = s.entries.keys().next().copied();
                            let group_names: Vec<String> = s.groups.keys().cloned().collect();
                            Ok(Some((
                                s.length,
                                s.groups.len(),
                                s.last_id,
                                first_id.unwrap_or(StreamId::new(0, 0)),
                                group_names,
                            )))
                        }
                        _ => Ok(None),
                    }
                }
            }
            None => Ok(None),
        }
    }

    /// 返回所有消费者组信息
    pub fn xinfo_groups(&self, key: &str) -> Result<Vec<(String, usize, usize, StreamId, usize)>> {
        let db = self.db();
        let map = db
            .inner
            .get_shard(key)
            .read()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if Self::is_expired(v) {
                    Ok(vec![])
                } else {
                    match v {
                        StorageValue::Stream(s) => {
                            let mut result = Vec::new();
                            for (name, group) in &s.groups {
                                result.push((
                                    name.clone(),
                                    group.consumers.len(),
                                    group.pel.len(),
                                    group.last_delivered_id,
                                    0usize, // entries-read 暂不支持
                                ));
                            }
                            Ok(result)
                        }
                        _ => Ok(vec![]),
                    }
                }
            }
            None => Ok(vec![]),
        }
    }

    /// 返回组内消费者信息
    pub fn xinfo_consumers(&self, key: &str, group_name: &str) -> Result<Vec<(String, usize, u64, u64)>> {
        let db = self.db();
        let map = db
            .inner
            .get_shard(key)
            .read()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if Self::is_expired(v) {
                    Ok(vec![])
                } else {
                    match v {
                        StorageValue::Stream(s) => {
                            let group = match s.groups.get(group_name) {
                                Some(g) => g,
                                None => return Ok(vec![]),
                            };
                            let mut result = Vec::new();
                            for (name, consumer) in &group.consumers {
                                result.push((
                                    name.clone(),
                                    consumer.pel.len(),
                                    consumer.seen_time,
                                    consumer.seen_time,
                                ));
                            }
                            Ok(result)
                        }
                        _ => Ok(vec![]),
                    }
                }
            }
            None => Ok(vec![]),
        }
    }

}
