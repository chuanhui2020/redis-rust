use super::*;

// ---------- Stream 数据结构 ----------

/// Stream 消息 ID（毫秒时间戳-序号）
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId {
    /// 毫秒时间戳
    pub ms_time: u64,
    /// 序号
    pub seq: u64,
}

impl StreamId {
    /// 创建新的 StreamId
    pub fn new(ms_time: u64, seq: u64) -> Self {
        Self { ms_time, seq }
    }

    /// 解析 ID 字符串，如 "1234567890-1" 或 "1234567890-*"
    pub fn parse(s: &str) -> Result<Self> {
        if s == "*" {
            return Err(AppError::Command("StreamId::parse 不支持 * 通配符".to_string()));
        }
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() == 1 {
            let ms_time: u64 = parts[0].parse().map_err(|_| {
                AppError::Command(format!("Stream ID 格式错误: {}", s))
            })?;
            return Ok(Self::new(ms_time, 0));
        }
        if parts.len() != 2 {
            return Err(AppError::Command(format!("Stream ID 格式错误: {}", s)));
        }
        let ms_time: u64 = parts[0].parse().map_err(|_| {
            AppError::Command(format!("Stream ID 毫秒时间戳必须是整数: {}", parts[0]))
        })?;
        let seq: u64 = parts[1].parse().map_err(|_| {
            AppError::Command(format!("Stream ID 序号必须是整数: {}", parts[1]))
        })?;
        Ok(Self::new(ms_time, seq))
    }

    /// 解析部分 ID，如 "1234567890-*"
    pub fn parse_partial(s: &str) -> Result<(u64, Option<u64>)> {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() != 2 {
            return Err(AppError::Command(format!("Stream ID 格式错误: {}", s)));
        }
        let ms_time: u64 = parts[0].parse().map_err(|_| {
            AppError::Command(format!("Stream ID 毫秒时间戳必须是整数: {}", parts[0]))
        })?;
        let seq = if parts[1] == "*" {
            None
        } else {
            Some(parts[1].parse().map_err(|_| {
                AppError::Command(format!("Stream ID 序号必须是整数: {}", parts[1]))
            })?)
        };
        Ok((ms_time, seq))
    }

    /// 解析范围边界 ID
    /// "-" 返回 (0, 0)，"+" 返回 (u64::MAX, u64::MAX)
    pub fn parse_range(s: &str) -> Result<Self> {
        if s == "-" {
            return Ok(Self::new(0, 0));
        }
        if s == "+" {
            return Ok(Self::new(u64::MAX, u64::MAX));
        }
        Self::parse(s)
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms_time, self.seq)
    }
}

/// Stream 待确认消息条目
#[derive(Debug, Clone)]
pub struct PendingEntry {
    /// 消费者名称
    pub consumer: String,
    /// 最后投递时间（毫秒时间戳）
    pub delivery_time: u64,
    /// 投递次数
    pub delivery_count: u32,
}

/// Stream 消费者
#[derive(Debug, Clone)]
pub struct Consumer {
    /// 消费者名称
    pub name: String,
    /// 该消费者的待确认消息 ID 集合
    pub pel: std::collections::HashSet<StreamId>,
    /// 最后活跃时间（毫秒时间戳）
    pub seen_time: u64,
}

impl Consumer {
    pub fn new(name: String) -> Self {
        Self {
            name,
            pel: std::collections::HashSet::new(),
            seen_time: 0,
        }
    }
}

/// Stream 消费者组
#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    /// 组名
    pub name: String,
    /// 最后投递的消息 ID
    pub last_delivered_id: StreamId,
    /// 待确认消息列表：StreamId -> PendingEntry
    pub pel: std::collections::BTreeMap<StreamId, PendingEntry>,
    /// 消费者列表：consumer_name -> Consumer
    pub consumers: std::collections::HashMap<String, Consumer>,
}

impl ConsumerGroup {
    pub fn new(name: String, last_id: StreamId) -> Self {
        Self {
            name,
            last_delivered_id: last_id,
            pel: std::collections::BTreeMap::new(),
            consumers: std::collections::HashMap::new(),
        }
    }
}

/// Stream 内部数据结构
#[derive(Debug, Clone)]
pub struct StreamData {
    /// 有序消息存储：StreamId -> [(field, value)]
    pub entries: std::collections::BTreeMap<StreamId, Vec<(String, String)>>,
    /// 最后生成的 ID
    pub last_id: StreamId,
    /// 消息数量
    pub length: usize,
    /// 消费者组列表：group_name -> ConsumerGroup
    pub groups: std::collections::HashMap<String, ConsumerGroup>,
}

impl StreamData {
    /// 创建空的 Stream
    pub fn new() -> Self {
        Self {
            entries: std::collections::BTreeMap::new(),
            last_id: StreamId::new(0, 0),
            length: 0,
            groups: std::collections::HashMap::new(),
        }
    }
}

impl Default for StreamData {
    fn default() -> Self {
        Self::new()
    }
}


impl StorageEngine {
    pub fn xadd(&self, key: &str, id: &str, fields: Vec<(String, String)>, nomkstream: bool, max_len: Option<usize>, min_id: Option<StreamId>) -> Result<Option<String>> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let now = Self::now_millis();
        let mut create_new = false;

        let stream = match map.get_mut(key) {
            Some(v) => {
                if Self::is_expired(v) {
                    map.remove(key);
                    if nomkstream {
                        return Ok(None);
                    }
                    create_new = true;
                    None
                } else {
                    Self::check_stream_type(v)?;
                    match v {
                        StorageValue::Stream(s) => Some(s),
                        _ => unreachable!(),
                    }
                }
            }
            None => {
                if nomkstream {
                    return Ok(None);
                }
                create_new = true;
                None
            }
        };

        let stream = if create_new {
            let s = StreamData::new();
            map.insert(key.to_string(), StorageValue::Stream(s));
            match map.get_mut(key) {
                Some(StorageValue::Stream(s)) => s,
                _ => unreachable!(),
            }
        } else {
            stream.unwrap()
        };

        // 生成 ID
        let new_id = if id == "*" {
            // 自动生成：毫秒时间戳-序号
            let ms = now.max(stream.last_id.ms_time);
            let seq = if ms == stream.last_id.ms_time {
                stream.last_id.seq + 1
            } else {
                0
            };
            StreamId::new(ms, seq)
        } else if id.ends_with("-*") {
            // "ms-*" 格式
            let ms_str = &id[..id.len() - 2];
            let ms: u64 = ms_str.parse().map_err(|_| {
                AppError::Command("XADD ID 毫秒时间戳必须是整数".to_string())
            })?;
            let seq = if ms == stream.last_id.ms_time {
                stream.last_id.seq + 1
            } else {
                0
            };
            StreamId::new(ms, seq)
        } else {
            StreamId::parse(id)?
        };

        // ID 必须大于 0-0（除了显式指定 0-0 的情况）
        if new_id.ms_time == 0 && new_id.seq == 0 {
            return Err(AppError::Command("XADD ID 必须大于 0-0".to_string()));
        }

        // ID 必须大于等于 last_id（对于自动生成的总是满足）
        if new_id <= stream.last_id && id != "*" && !id.ends_with("-*") {
            return Err(AppError::Command(format!(
                "XADD ID 必须大于 {}，但收到 {}",
                stream.last_id, new_id
            )));
        }

        stream.entries.insert(new_id, fields);
        stream.last_id = new_id;
        stream.length += 1;

        // 应用 MAXLEN/MINID 裁剪
        if let Some(max) = max_len {
            while stream.length > max {
                if let Some(first_id) = stream.entries.keys().next().copied() {
                    stream.entries.remove(&first_id);
                    stream.length -= 1;
                } else {
                    break;
                }
            }
        }
        if let Some(min) = min_id {
            let to_remove: Vec<StreamId> = stream.entries
                .keys()
                .take_while(|id| **id < min)
                .copied()
                .collect();
            for id in to_remove {
                stream.entries.remove(&id);
                stream.length -= 1;
            }
        }

        self.bump_version(key);
        self.touch(key);
        Ok(Some(new_id.to_string()))
    }

    /// 返回 Stream 的消息数量
    pub fn xlen(&self, key: &str) -> Result<usize> {
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
                    Self::check_stream_type(v)?;
                    match v {
                        StorageValue::Stream(s) => Ok(s.length),
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(0),
        }
    }

    /// 按 ID 范围查询 Stream 消息
    /// start/end 支持 "-"、"+"、具体 ID
    pub fn xrange(&self, key: &str, start: &str, end: &str, count: Option<usize>) -> Result<Vec<(StreamId, Vec<(String, String)>)>> {
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
                    Ok(vec![])
                } else {
                    Self::check_stream_type(v)?;
                    match v {
                        StorageValue::Stream(s) => {
                            let start_id = StreamId::parse_range(start)?;
                            let end_id = StreamId::parse_range(end)?;
                            let mut result = Vec::new();
                            for (id, fields) in s.entries.range(start_id..=end_id) {
                                result.push((*id, fields.clone()));
                                if let Some(c) = count {
                                    if result.len() >= c {
                                        break;
                                    }
                                }
                            }
                            Ok(result)
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(vec![]),
        }
    }

    /// 反向按 ID 范围查询 Stream 消息
    pub fn xrevrange(&self, key: &str, end: &str, start: &str, count: Option<usize>) -> Result<Vec<(StreamId, Vec<(String, String)>)>> {
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
                    Ok(vec![])
                } else {
                    Self::check_stream_type(v)?;
                    match v {
                        StorageValue::Stream(s) => {
                            let start_id = StreamId::parse_range(start)?;
                            let end_id = StreamId::parse_range(end)?;
                            let mut result = Vec::new();
                            for (id, fields) in s.entries.range(start_id..=end_id).rev() {
                                result.push((*id, fields.clone()));
                                if let Some(c) = count {
                                    if result.len() >= c {
                                        break;
                                    }
                                }
                            }
                            Ok(result)
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(vec![]),
        }
    }

    /// 裁剪 Stream
    /// strategy: "MAXLEN" 或 "MINID"
    pub fn xtrim(&self, key: &str, strategy: &str, threshold: &str, _approx: bool) -> Result<usize> {
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
                            let before = s.length;
                            let strategy_upper = strategy.to_ascii_uppercase();
                            if strategy_upper == "MAXLEN" {
                                let max: usize = threshold.parse().map_err(|_| {
                                    AppError::Command("XTRIM MAXLEN threshold 必须是整数".to_string())
                                })?;
                                while s.length > max {
                                    if let Some(first_id) = s.entries.keys().next().copied() {
                                        s.entries.remove(&first_id);
                                        s.length -= 1;
                                    } else {
                                        break;
                                    }
                                }
                            } else if strategy_upper == "MINID" {
                                let min_id = StreamId::parse(threshold)?;
                                let to_remove: Vec<StreamId> = s.entries
                                    .keys()
                                    .take_while(|id| **id < min_id)
                                    .copied()
                                    .collect();
                                for id in to_remove {
                                    s.entries.remove(&id);
                                    s.length -= 1;
                                }
                            } else {
                                return Err(AppError::Command(format!("XTRIM 不支持策略: {}", strategy)));
                            }
                            let removed = before - s.length;
                            if removed > 0 {
                                self.bump_version(key);
                            }
                            Ok(removed)
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(0),
        }
    }

    /// 删除 Stream 中的指定消息
    pub fn xdel(&self, key: &str, ids: &[StreamId]) -> Result<usize> {
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
                            let mut removed = 0usize;
                            for id in ids {
                                if s.entries.remove(id).is_some() {
                                    removed += 1;
                                    s.length -= 1;
                                }
                            }
                            if s.length == 0 {
                                map.remove(key);
                            } else if removed > 0 {
                                self.bump_version(key);
                            }
                            Ok(removed)
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(0),
        }
    }

    /// 从多个 Stream 读取消息
    /// keys: 要读取的 key 列表
    /// ids: 每个 key 的起始 ID（"$" 表示从最后一条之后开始）
    /// count: 每个 stream 最多返回的消息数
    pub fn xread(&self, keys: &[String], ids: &[String], count: Option<usize>) -> Result<Vec<(String, Vec<(StreamId, Vec<(String, String)>)>)>> {
        let db = self.db();

        let mut result = Vec::new();
        for (idx, key) in keys.iter().enumerate() {
            let map = db
                .inner
                .get_shard(key)
                .read()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            let start_id = if ids[idx] == "$" {
                // 找到该 stream 的 last_id
                if let Some(v) = map.get(key) {
                    if let StorageValue::Stream(s) = v {
                        StreamId::new(s.last_id.ms_time, s.last_id.seq)
                    } else {
                        continue;
                    }
                } else {
                    continue;
                }
            } else {
                StreamId::parse(&ids[idx])?
            };

            if let Some(v) = map.get(key) {
                if Self::is_expired(v) {
                    continue;
                }
                if let StorageValue::Stream(s) = v {
                    let mut stream_result = Vec::new();
                    // 找到 start_id 之后的所有消息（不包括 start_id）
                    for (id, fields) in s.entries.range((std::ops::Bound::Excluded(start_id), std::ops::Bound::Unbounded)) {
                        stream_result.push((*id, fields.clone()));
                        if let Some(c) = count {
                            if stream_result.len() >= c {
                                break;
                            }
                        }
                    }
                    if !stream_result.is_empty() {
                        result.push((key.clone(), stream_result));
                    }
                }
            }
        }
        Ok(result)
    }

    /// 设置 Stream 的 last_id
    pub fn xsetid(&self, key: &str, id: StreamId) -> Result<bool> {
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
                            if id <= s.last_id {
                                return Err(AppError::Command(format!(
                                    "XSETID ID 必须大于当前 last_id {}，但收到 {}",
                                    s.last_id, id
                                )));
                            }
                            s.last_id = id;
                            self.bump_version(key);
                            Ok(true)
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(false),
        }
    }

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

        let now = Self::now_millis();

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
                    if let Some(c) = count {
                        if stream_result.len() >= c {
                            break;
                        }
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
                    if *id > start_id {
                        if let Some(fields) = stream.entries.get(id) {
                            stream_result.push((*id, fields.clone()));
                            if let Some(c) = count {
                                if stream_result.len() >= c {
                                    break;
                                }
                            }
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
                                        if old_consumer_name != consumer_name {
                                            if let Some(old_consumer) = group.consumers.get_mut(&old_consumer_name) {
                                                old_consumer.pel.remove(id);
                                            }
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
                            let mut processed = 0usize;

                            // 遍历 PEL 中从 start 开始的消息
                            let pel_ids: Vec<StreamId> = group.pel
                                .keys()
                                .filter(|id| **id >= start)
                                .copied()
                                .collect();

                            for id in pel_ids {
                                if processed >= count {
                                    next_id = id;
                                    break;
                                }
                                if let Some(entry) = group.pel.get_mut(&id) {
                                    let idle = now - entry.delivery_time;
                                    if idle >= min_idle_time {
                                        let old_consumer_name = entry.consumer.clone();
                                        if old_consumer_name != consumer_name {
                                            if let Some(old_consumer) = group.consumers.get_mut(&old_consumer_name) {
                                                old_consumer.pel.remove(&id);
                                            }
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
                                processed += 1;
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
                                if let Some(cname) = consumer {
                                    if entry.consumer != cname {
                                        continue;
                                    }
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

    // ---------- Set 操作 ----------

}

