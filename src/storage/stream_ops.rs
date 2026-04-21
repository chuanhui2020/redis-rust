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
}

