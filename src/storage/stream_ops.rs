//! Stream 数据类型操作（对标 Redis Stream 命令族）
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
    /// 创建新的 StreamId（对标 Redis Stream 消息 ID 格式）
    ///
    /// 由毫秒时间戳和序号组成，格式为 `ms_time-seq`。
    ///
    /// # 参数
    /// - `ms_time` - 毫秒时间戳
    /// - `seq` - 同一毫秒内的递增序号
    ///
    /// # 返回值
    /// 返回构造的 `StreamId`
    pub fn new(ms_time: u64, seq: u64) -> Self {
        Self { ms_time, seq }
    }

    /// 解析完整 Stream ID 字符串（对标 Redis XADD ID 格式）
    ///
    /// 支持格式：`"ms_time-seq"`、`"ms_time"`（seq 默认为 0）。
    /// 不支持 `"*"` 通配符，通配符由上层处理。
    ///
    /// # 参数
    /// - `s` - ID 字符串
    ///
    /// # 返回值
    /// - `Ok(StreamId)` - 解析成功
    /// - `Err(AppError::Command)` - 格式错误或包含 `*`
    ///
    /// # Redis 兼容性
    /// 对应 Redis Stream ID 的解析规则，与 Redis 7 行为一致。
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

    /// 解析部分 Stream ID 字符串（如 `"1234567890-*"`）
    ///
    /// 用于 XADD 等命令中 `"ms_time-*"` 的语义：固定毫秒时间戳，
    /// 序号部分若为 `"*"` 则返回 `None`，由上层自动生成。
    ///
    /// # 参数
    /// - `s` - 部分 ID 字符串，必须包含 `-`
    ///
    /// # 返回值
    /// - `Ok((ms_time, Some(seq)))` - 完整指定
    /// - `Ok((ms_time, None))` - 序号部分为 `*`
    /// - `Err(AppError::Command)` - 格式错误
    ///
    /// # Redis 兼容性
    /// 对应 Redis XADD 中 `"<ms>-*"` 的 ID 自动补全行为。
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

    /// 解析范围边界 ID（用于 XRANGE / XREVRANGE 的 start/end 参数）
    ///
    /// 支持特殊符号：
    /// - `"-"` 表示最小 ID `(0, 0)`
    /// - `"+"` 表示最大 ID `(u64::MAX, u64::MAX)`
    /// - 其他按 `parse` 规则解析
    ///
    /// # 参数
    /// - `s` - 范围边界字符串
    ///
    /// # 返回值
    /// - `Ok(StreamId)` - 解析成功
    /// - `Err(AppError::Command)` - 格式错误
    ///
    /// # Redis 兼容性
    /// 对标 Redis XRANGE 的 `-` 和 `+` 语义。
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
    /// 创建新的消费者
    ///
    /// # 参数
    /// - `name` - 消费者名称
    ///
    /// # 返回值
    /// 返回 `Consumer`，初始时 PEL 为空，`seen_time` 为 0
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
    /// 创建新的消费者组
    ///
    /// # 参数
    /// - `name` - 组名
    /// - `last_id` - 初始的 `last_delivered_id`
    ///
    /// # 返回值
    /// 返回空的 `ConsumerGroup`，无消费者和 PEL 条目
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
    /// 创建空的 Stream 数据结构
    ///
    /// # 返回值
    /// 返回 `StreamData`，`last_id` 初始化为 `0-0`，无消息和消费者组
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
    /// 向 Stream 添加消息（对标 Redis XADD 命令）
    ///
    /// 将一条包含多个 field-value 对的消息追加到 Stream 中。
    /// 自动生成或验证消息 ID，ID 必须严格递增。
    /// 支持 MAXLEN / MINID 裁剪策略。
    ///
    /// # 参数
    /// - `key` - Stream 键名
    /// - `id` - 消息 ID，`"*"` 表示自动生成，`"ms-*"` 表示固定毫秒自动生成序号
    /// - `fields` - 消息包含的 field-value 列表
    /// - `nomkstream` - 为 `true` 时，如果 key 不存在则不创建 Stream
    /// - `max_len` - 可选的最大长度限制（MAXLEN）
    /// - `min_id` - 可选的最小 ID 限制（MINID）
    ///
    /// # 返回值
    /// - `Ok(Some(String))` - 成功，返回生成的消息 ID
    /// - `Ok(None)` - `nomkstream=true` 且 key 不存在
    /// - `Err(AppError::Command)` - ID 格式错误或 ID 不大于 `last_id`
    ///
    /// # Redis 兼容性
    /// 对标 Redis 7 `XADD` 命令，支持 `NOMKSTREAM`、`MAXLEN`、`MINID` 选项。
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
                if Self::is_key_expired(&db, key) {
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
        } else if let Some(ms_str) = id.strip_suffix("-*") {
            // "ms-*" 格式
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

    /// 返回 Stream 的消息数量（对标 Redis XLEN 命令）
    ///
    /// # 参数
    /// - `key` - Stream 键名
    ///
    /// # 返回值
    /// - `Ok(usize)` - 消息数量；键不存在或已过期返回 0
    ///
    /// # Redis 兼容性
    /// 对标 Redis 7 `XLEN` 命令。
    pub fn xlen(&self, key: &str) -> Result<usize> {
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

    /// 按 ID 范围正向查询 Stream 消息（对标 Redis XRANGE 命令）
    ///
    /// `start` 和 `end` 支持 `"-"`（最小）、`"+"`（最大）或具体 ID。
    /// 返回按 ID 升序排列的消息列表。
    ///
    /// # 参数
    /// - `key` - Stream 键名
    /// - `start` - 范围起始 ID（包含）
    /// - `end` - 范围结束 ID（包含）
    /// - `count` - 可选的最大返回条数
    ///
    /// # 返回值
    /// - `Ok(Vec)` - 查询结果，键不存在或已过期返回空 Vec
    /// - `Err(AppError::Command)` - ID 格式错误
    ///
    /// # Redis 兼容性
    /// 对标 Redis 7 `XRANGE` 命令，支持 `COUNT` 选项。
    pub fn xrange(&self, key: &str, start: &str, end: &str, count: Option<usize>) -> Result<Vec<(StreamId, Vec<(String, String)>)>> {
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
                                if let Some(c) = count
                                    && result.len() >= c {
                                        break;
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

    /// 按 ID 范围反向查询 Stream 消息（对标 Redis XREVRANGE 命令）
    ///
    /// 参数语义与 `xrange` 相同，但返回按 ID 降序排列。
    ///
    /// # 参数
    /// - `key` - Stream 键名
    /// - `end` - 范围结束 ID（包含）
    /// - `start` - 范围起始 ID（包含）
    /// - `count` - 可选的最大返回条数
    ///
    /// # 返回值
    /// - `Ok(Vec)` - 查询结果，键不存在或已过期返回空 Vec
    /// - `Err(AppError::Command)` - ID 格式错误
    ///
    /// # Redis 兼容性
    /// 对标 Redis 7 `XREVRANGE` 命令。
    pub fn xrevrange(&self, key: &str, end: &str, start: &str, count: Option<usize>) -> Result<Vec<(StreamId, Vec<(String, String)>)>> {
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
                                if let Some(c) = count
                                    && result.len() >= c {
                                        break;
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

    /// 裁剪 Stream 长度（对标 Redis XTRIM 命令）
    ///
    /// 按指定策略删除旧消息，支持 `MAXLEN` 和 `MINID`。
    ///
    /// # 参数
    /// - `key` - Stream 键名
    /// - `strategy` - 裁剪策略，`"MAXLEN"` 或 `"MINID"`
    /// - `threshold` - 阈值：MAXLEN 时为最大长度，MINID 时为最小 ID
    /// - `_approx` - 近似裁剪标志（当前实现为精确裁剪）
    ///
    /// # 返回值
    /// - `Ok(usize)` - 实际删除的消息数量
    /// - `Err(AppError::Command)` - 策略不支持或阈值格式错误
    ///
    /// # Redis 兼容性
    /// 对标 Redis 7 `XTRIM` 命令，支持 `MAXLEN` 和 `MINID` 策略。
    /// `~` 近似标志当前未实现，按精确裁剪处理。
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
                if Self::is_key_expired(&db, key) {
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

    /// 删除 Stream 中的指定消息（对标 Redis XDEL 命令）
    ///
    /// 删除给定 ID 对应的消息，不存在的 ID 被忽略。
    /// 若删除后 Stream 为空，则自动删除该键。
    ///
    /// # 参数
    /// - `key` - Stream 键名
    /// - `ids` - 要删除的消息 ID 列表
    ///
    /// # 返回值
    /// - `Ok(usize)` - 实际删除的消息数量
    ///
    /// # Redis 兼容性
    /// 对标 Redis 7 `XDEL` 命令。
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
                if Self::is_key_expired(&db, key) {
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

    /// 从多个 Stream 读取消息（对标 Redis XREAD 命令）
    ///
    /// 同时读取多个 Stream 中指定 ID 之后的消息。
    /// `"$"` 表示从该 Stream 当前最后一条消息之后开始。
    ///
    /// # 参数
    /// - `keys` - 要读取的 Stream key 列表
    /// - `ids` - 每个 key 对应的起始 ID，`"$"` 表示从最后一条之后开始
    /// - `count` - 每个 Stream 最多返回的消息数
    ///
    /// # 返回值
    /// - `Ok(Vec<(key, messages)>)` - 各 Stream 的读取结果，仅返回非空 Stream
    /// - `Err(AppError::Command)` - ID 格式错误
    ///
    /// # Redis 兼容性
    /// 对标 Redis 7 `XREAD` 命令，支持 `COUNT` 和多 key 读取。
    /// 阻塞读取（BLOCK）在上层命令处理中实现。
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
                if let Some(StorageValue::Stream(s)) = map.get(key) {
                    StreamId::new(s.last_id.ms_time, s.last_id.seq)
                } else {
                    continue;
                }
            } else {
                StreamId::parse(&ids[idx])?
            };

            if let Some(v) = map.get(key) {
                if Self::is_key_expired(&db, key) {
                    continue;
                }
                if let StorageValue::Stream(s) = v {
                    let mut stream_result = Vec::new();
                    // 找到 start_id 之后的所有消息（不包括 start_id）
                    for (id, fields) in s.entries.range((std::ops::Bound::Excluded(start_id), std::ops::Bound::Unbounded)) {
                        stream_result.push((*id, fields.clone()));
                        if let Some(c) = count
                            && stream_result.len() >= c {
                                break;
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

    /// 设置 Stream 的 last_id（对标 Redis XSETID 命令）
    ///
    /// 用于主从复制或恢复场景，强制设置 Stream 的最后生成 ID。
    /// 新 ID 必须大于当前 `last_id`。
    ///
    /// # 参数
    /// - `key` - Stream 键名
    /// - `id` - 要设置的新的 last_id
    ///
    /// # 返回值
    /// - `Ok(true)` - 设置成功
    /// - `Ok(false)` - 键不存在或已过期
    /// - `Err(AppError::Command)` - 新 ID 不大于当前 `last_id`
    ///
    /// # Redis 兼容性
    /// 对标 Redis 7 `XSETID` 命令。
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
                if Self::is_key_expired(&db, key) {
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

