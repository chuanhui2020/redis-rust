use super::*;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use crate::aof::AofWriter;
use crate::error::{AppError, Result};
use crate::keyspace::KeyspaceNotifier;
use crate::protocol::RespValue;
use crate::scripting::ScriptEngine;
use crate::slowlog::SlowLog;
use crate::storage::{EvictionPolicy, StorageEngine};

pub struct CommandExecutor {
    /// 存储引擎引用
    pub(crate) storage: StorageEngine,
    /// AOF 写入器（可选），用于持久化写操作
    aof: Option<Arc<Mutex<AofWriter>>>,
    /// Lua 脚本引擎（可选）
    script_engine: Option<ScriptEngine>,
    /// 慢查询日志（可选）
    slowlog: Option<SlowLog>,
    /// ACL 管理器（可选）
    acl: Option<crate::acl::AclManager>,
    /// 延迟追踪器（可选）
    latency: Option<crate::latency::LatencyTracker>,
    /// Keyspace 通知器（可选）
    keyspace_notifier: Option<Arc<KeyspaceNotifier>>,
    /// 是否使用 AOF RDB preamble（混合持久化）
    aof_use_rdb_preamble: Arc<AtomicBool>,
}

impl CommandExecutor {
    /// 创建不带 AOF 的命令执行器（用于 AOF 重放）
    pub fn new(storage: StorageEngine) -> Self {
        Self {
            storage,
            aof: None,
            script_engine: None,
            slowlog: None,
            acl: None,
            latency: None,
            keyspace_notifier: None,
            aof_use_rdb_preamble: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 创建带 AOF 的命令执行器（用于正常服务）
    pub fn new_with_aof(
        storage: StorageEngine,
        aof: Arc<Mutex<AofWriter>>,
    ) -> Self {
        Self {
            storage,
            aof: Some(aof),
            script_engine: None,
            slowlog: None,
            acl: None,
            latency: None,
            keyspace_notifier: None,
            aof_use_rdb_preamble: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 设置 Lua 脚本引擎
    pub fn set_script_engine(&mut self, engine: ScriptEngine) {
        self.script_engine = Some(engine);
    }

    /// 设置慢查询日志
    pub fn set_slowlog(&mut self, slowlog: SlowLog) {
        self.slowlog = Some(slowlog);
    }

    /// 设置 ACL 管理器
    pub fn set_acl(&mut self, acl: crate::acl::AclManager) {
        self.acl = Some(acl);
    }

    /// 设置延迟追踪器
    pub fn set_latency(&mut self, latency: crate::latency::LatencyTracker) {
        self.latency = Some(latency);
    }

    /// 设置 Keyspace 通知器
    pub fn set_keyspace_notifier(&mut self, notifier: Arc<KeyspaceNotifier>) {
        self.keyspace_notifier = Some(notifier);
    }

    /// 设置是否使用 AOF RDB preamble
    pub fn set_aof_use_rdb_preamble(&self, enabled: bool) {
        self.aof_use_rdb_preamble.store(enabled, Ordering::Relaxed);
    }

    /// 获取是否使用 AOF RDB preamble
    pub fn aof_use_rdb_preamble(&self) -> bool {
        self.aof_use_rdb_preamble.load(Ordering::Relaxed)
    }

    /// 获取存储引擎的克隆引用
    pub fn storage(&self) -> StorageEngine {
        self.storage.clone()
    }

    /// 获取 ACL 管理器
    pub fn acl(&self) -> Option<crate::acl::AclManager> {
        self.acl.clone()
    }

    /// 切换当前数据库
    pub fn select_db(&self, index: usize) -> Result<()> {
        self.storage.select(index)
    }

    /// 构建 DEBUG OBJECT key 的返回信息
    fn build_debug_object_info(&self, key: &str) -> Result<String> {
        let encoding = self.storage.object_encoding(key)?
            .unwrap_or_else(|| "none".to_string());
        let refcount = self.storage.object_refcount(key)?
            .unwrap_or(0);
        let idletime = self.storage.object_idletime(key)?
            .unwrap_or(0);
        let serialized_len = match self.storage.dump(key)? {
            Some(data) => data.len(),
            None => 0,
        };

        Ok(format!(
            "Value at:0x{:p} refcount:{} encoding:{} serializedlength:{} lru_seconds_idle:{}",
            key.as_ptr(), refcount, encoding, serialized_len, idletime
        ))
    }

    /// 将写操作追加到 AOF 文件
    fn append_to_aof(&self, cmd: &Command) {
        if !cmd.is_write_command() {
            return;
        }
        if let Some(ref aof) = self.aof {
            match aof.lock() {
                Ok(mut writer) => {
                    if let Err(e) = writer.append(cmd) {
                        log::error!("AOF 写入失败: {}", e);
                    }
                }
                Err(e) => {
                    log::error!("AOF writer 锁中毒: {}", e);
                }
            }
        }
    }

    /// 执行命令并返回 RESP 结果（自动记录慢查询）
    pub fn execute(&self, cmd: Command) -> Result<RespValue> {
        let start = std::time::Instant::now();
        let (cmd_name, args) = extract_cmd_info(&cmd);

        // 写操作先记录到 AOF，再执行
        self.append_to_aof(&cmd);

        // 保留命令引用用于 keyspace 通知
        let cmd_for_notify = cmd.clone();
        let result = self.do_execute(cmd);

        // 命令执行成功后发送 Keyspace 通知
        if result.is_ok() {
            if let Some(ref notifier) = self.keyspace_notifier {
                let db = self.storage.current_db();
                notifier.notify_command(&cmd_for_notify, db);
            }
        }

        let duration_us = start.elapsed().as_micros() as u64;
        if let Some(ref slowlog) = self.slowlog {
            slowlog.record(&cmd_name, args, duration_us);
        }
        if let Some(ref latency) = self.latency {
            // LATENCY 命令自身不记录延迟，避免 RESET 后立即被重新填充
            if !cmd_name.starts_with("LATENCY ") {
                let _ = latency.record("command", duration_us / 1000);
            }
        }

        result
    }

    /// 内部执行命令（不包含计时）
    fn do_execute(&self, cmd: Command) -> Result<RespValue> {
        match cmd {
            Command::Ping(message) => {
                // PING 命令：有参数返回参数，否则返回 PONG
                let reply = message.unwrap_or_else(|| "PONG".to_string());
                Ok(RespValue::SimpleString(reply))
            }
            Command::Get(key) => {
                match self.storage.get(&key)? {
                    Some(value) => {
                        Ok(RespValue::BulkString(Some(value)))
                    }
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::Set(key, value, options) => {
                let (ok, old_value) = self.storage.set_with_options(key, value, &options)?;
                if options.get {
                    Ok(RespValue::BulkString(old_value))
                } else if ok {
                    Ok(RespValue::SimpleString("OK".to_string()))
                } else {
                    Ok(RespValue::BulkString(None))
                }
            }
            Command::SetEx(key, value, ttl_ms) => {
                self.storage.set_with_ttl(key, value, ttl_ms)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::Del(keys) => {
                let mut count = 0i64;
                for key in keys {
                    if self.storage.del(&key)? {
                        count += 1;
                    }
                }
                Ok(RespValue::Integer(count))
            }
            Command::Exists(keys) => {
                let mut count = 0i64;
                for key in keys {
                    if self.storage.exists(&key)? {
                        count += 1;
                    }
                }
                Ok(RespValue::Integer(count))
            }
            Command::FlushAll => {
                self.storage.flush()?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::Expire(key, seconds) => {
                let result = if self.storage.expire(&key, seconds)? {
                    1i64
                } else {
                    0i64
                };
                Ok(RespValue::Integer(result))
            }
            Command::Ttl(key) => {
                let ttl_ms = self.storage.ttl(&key)?;
                // Redis TTL 返回秒数，-1 和 -2 保持不变
                let ttl_sec = if ttl_ms >= 0 {
                    ttl_ms / 1000
                } else {
                    ttl_ms
                };
                Ok(RespValue::Integer(ttl_sec))
            }
            // redis-benchmark 兼容：CONFIG GET 返回空数组
            // 对 maxmemory 返回实际值
            Command::ConfigGet(key) => {
                let key_lower = key.to_ascii_lowercase();
                if key_lower == "maxmemory" {
                    let maxmem = self.storage.get_maxmemory();
                    Ok(RespValue::Array(vec![
                        RespValue::BulkString(Some(bytes::Bytes::from(key))),
                        RespValue::BulkString(Some(bytes::Bytes::from(maxmem.to_string()))),
                    ]))
                } else if key_lower == "maxmemory-policy" {
                    let policy = self.storage.get_eviction_policy();
                    let policy_str = format!("{:?}", policy).to_ascii_lowercase();
                    Ok(RespValue::Array(vec![
                        RespValue::BulkString(Some(bytes::Bytes::from(key))),
                        RespValue::BulkString(Some(bytes::Bytes::from(policy_str))),
                    ]))
                } else if key_lower == "notify-keyspace-events" {
                    let raw = self.keyspace_notifier.as_ref()
                        .map(|n| n.config().raw().to_string())
                        .unwrap_or_default();
                    Ok(RespValue::Array(vec![
                        RespValue::BulkString(Some(bytes::Bytes::from(key))),
                        RespValue::BulkString(Some(bytes::Bytes::from(raw))),
                    ]))
                } else if key_lower == "aof-use-rdb-preamble" {
                    let val = if self.aof_use_rdb_preamble.load(Ordering::Relaxed) {
                        "yes"
                    } else {
                        "no"
                    };
                    Ok(RespValue::Array(vec![
                        RespValue::BulkString(Some(bytes::Bytes::from(key))),
                        RespValue::BulkString(Some(bytes::Bytes::from(val))),
                    ]))
                } else {
                    Ok(RespValue::Array(vec![]))
                }
            }
            Command::ConfigSet(key, value) => {
                let key_lower = key.to_ascii_lowercase();
                if key_lower == "maxmemory" {
                    match value.parse::<u64>() {
                        Ok(bytes) => {
                            self.storage.set_maxmemory(bytes);
                            Ok(RespValue::SimpleString("OK".to_string()))
                        }
                        Err(_) => {
                            Ok(RespValue::Error("ERR value is not an integer or out of range".to_string()))
                        }
                    }
                } else if key_lower == "maxmemory-policy" {
                    let policy = match value.to_ascii_lowercase().as_str() {
                        "noeviction" => EvictionPolicy::NoEviction,
                        "allkeys-lru" => EvictionPolicy::AllKeysLru,
                        "allkeys-random" => EvictionPolicy::AllKeysRandom,
                        "allkeys-lfu" => EvictionPolicy::AllKeysLfu,
                        "volatile-lru" => EvictionPolicy::VolatileLru,
                        "volatile-ttl" => EvictionPolicy::VolatileTtl,
                        "volatile-random" => EvictionPolicy::VolatileRandom,
                        "volatile-lfu" => EvictionPolicy::VolatileLfu,
                        _ => {
                            return Ok(RespValue::Error(format!(
                                "ERR Invalid maxmemory-policy: {}", value
                            )));
                        }
                    };
                    self.storage.set_eviction_policy(policy);
                    Ok(RespValue::SimpleString("OK".to_string()))
                } else if key_lower == "notify-keyspace-events" {
                    if let Some(ref notifier) = self.keyspace_notifier {
                        notifier.set_config(crate::keyspace::NotifyKeyspaceEvents::from_str(&value));
                        Ok(RespValue::SimpleString("OK".to_string()))
                    } else {
                        Ok(RespValue::Error("ERR Keyspace notifier not initialized".to_string()))
                    }
                } else if key_lower == "aof-use-rdb-preamble" {
                    let enabled = match value.to_ascii_lowercase().as_str() {
                        "yes" => true,
                        "no" => false,
                        _ => {
                            return Ok(RespValue::Error(
                                "ERR Invalid argument 'yes' or 'no' expected".to_string()
                            ));
                        }
                    };
                    self.aof_use_rdb_preamble.store(enabled, Ordering::Relaxed);
                    Ok(RespValue::SimpleString("OK".to_string()))
                } else {
                    Ok(RespValue::Error(format!("ERR Unsupported CONFIG parameter: {}", key)))
                }
            }
            // redis-benchmark 兼容：COMMAND 返回空数组
            Command::ConfigRewrite => {
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::ConfigResetStat => {
                self.slowlog.as_ref().map(|s| s.reset());
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::MemoryUsage(key, _samples) => {
                match self.storage.memory_key_usage(&key, _samples)? {
                    Some(size) => Ok(RespValue::Integer(size as i64)),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::MemoryDoctor => {
                let info = self.storage.memory_doctor()?;
                Ok(RespValue::BulkString(Some(bytes::Bytes::from(info))))
            }
            Command::LatencyLatest => {
                let tracker = self.latency.as_ref().ok_or_else(|| {
                    AppError::Command("延迟追踪器未初始化".to_string())
                })?;
                let latest = tracker.latest()?;
                let parts: Vec<RespValue> = latest.into_iter()
                    .map(|(name, lat, ts, max)| {
                        RespValue::Array(vec![
                            RespValue::BulkString(Some(bytes::Bytes::from(name))),
                            RespValue::Integer(ts as i64),
                            RespValue::Integer(lat as i64),
                            RespValue::Integer(max as i64),
                        ])
                    })
                    .collect();
                Ok(RespValue::Array(parts))
            }
            Command::LatencyHistory(event) => {
                let tracker = self.latency.as_ref().ok_or_else(|| {
                    AppError::Command("延迟追踪器未初始化".to_string())
                })?;
                let history = tracker.history(&event)?;
                let parts: Vec<RespValue> = history.into_iter()
                    .map(|(ts, lat)| {
                        RespValue::Array(vec![
                            RespValue::Integer(ts as i64),
                            RespValue::Integer(lat as i64),
                        ])
                    })
                    .collect();
                Ok(RespValue::Array(parts))
            }
            Command::LatencyReset(events) => {
                let tracker = self.latency.as_ref().ok_or_else(|| {
                    AppError::Command("延迟追踪器未初始化".to_string())
                })?;
                let count = if events.is_empty() {
                    tracker.reset_all()?
                } else {
                    let refs: Vec<&str> = events.iter().map(|s| s.as_str()).collect();
                    tracker.reset(&refs)?
                };
                Ok(RespValue::Integer(count as i64))
            }
            Command::Reset => {
                Err(AppError::Command("RESET 应在连接层处理".to_string()))
            }
            Command::Hello(_protover, _auth, _setname) => {
                Err(AppError::Command("HELLO 应在连接层处理".to_string()))
            }
            Command::Monitor => {
                Err(AppError::Command("MONITOR 应在连接层处理".to_string()))
            }
            Command::CommandInfo => {
                Ok(RespValue::Array(vec![]))
            }
            Command::BgRewriteAof => {
                // BGREWRITEAOF 在 server.rs 中处理，需要 AOF writer
                Err(AppError::Command("BGREWRITEAOF 应在连接层处理".to_string()))
            }
            Command::SetBit(key, offset, value) => {
                let old_val = self.storage.setbit(&key, offset, value)?;
                Ok(RespValue::Integer(old_val))
            }
            Command::GetBit(key, offset) => {
                let val = self.storage.getbit(&key, offset)?;
                Ok(RespValue::Integer(val))
            }
            Command::BitCount(key, start, end, is_byte) => {
                let count = self.storage.bitcount(&key, start, end, is_byte)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::BitOp(op, destkey, keys) => {
                let len = self.storage.bitop(&op, &destkey, &keys)?;
                Ok(RespValue::Integer(len as i64))
            }
            Command::BitPos(key, bit, start, end, is_byte) => {
                let pos = self.storage.bitpos(&key, bit, start, end, is_byte)?;
                Ok(RespValue::Integer(pos))
            }
            Command::BitField(key, ops) => {
                let results = self.storage.bitfield(&key, &ops)?;
                let resp_values: Vec<RespValue> = results
                    .into_iter()
                    .map(|r| match r {
                        crate::storage::BitFieldResult::Value(v) => RespValue::Integer(v),
                        crate::storage::BitFieldResult::Nil => RespValue::BulkString(None),
                    })
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::BitFieldRo(key, ops) => {
                let results = self.storage.bitfield_ro(&key, &ops)?;
                let resp_values: Vec<RespValue> = results
                    .into_iter()
                    .map(|r| match r {
                        crate::storage::BitFieldResult::Value(v) => RespValue::Integer(v),
                        crate::storage::BitFieldResult::Nil => RespValue::BulkString(None),
                    })
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::XAdd(key, id, fields, nomkstream, max_len, min_id) => {
                let min_id_parsed = match min_id {
                    Some(s) => Some(crate::storage::StreamId::parse(s.as_str())?),
                    None => None,
                };
                match self.storage.xadd(&key, &id, fields, nomkstream, max_len, min_id_parsed)? {
                    Some(new_id) => Ok(RespValue::BulkString(Some(Bytes::from(new_id)))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::XLen(key) => {
                let len = self.storage.xlen(&key)?;
                Ok(RespValue::Integer(len as i64))
            }
            Command::XRange(key, start, end, count) => {
                let entries = self.storage.xrange(&key, &start, &end, count)?;
                let mut resp_values = Vec::new();
                for (id, fields) in entries {
                    let mut field_values = Vec::new();
                    for (f, v) in fields {
                        field_values.push(RespValue::BulkString(Some(Bytes::from(f))));
                        field_values.push(RespValue::BulkString(Some(Bytes::from(v))));
                    }
                    resp_values.push(RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from(id.to_string()))),
                        RespValue::Array(field_values),
                    ]));
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::XRevRange(key, end, start, count) => {
                let entries = self.storage.xrevrange(&key, &end, &start, count)?;
                let mut resp_values = Vec::new();
                for (id, fields) in entries {
                    let mut field_values = Vec::new();
                    for (f, v) in fields {
                        field_values.push(RespValue::BulkString(Some(Bytes::from(f))));
                        field_values.push(RespValue::BulkString(Some(Bytes::from(v))));
                    }
                    resp_values.push(RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from(id.to_string()))),
                        RespValue::Array(field_values),
                    ]));
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::XTrim(key, strategy, threshold) => {
                let removed = self.storage.xtrim(&key, &strategy, &threshold, false)?;
                Ok(RespValue::Integer(removed as i64))
            }
            Command::XDel(key, ids) => {
                let id_vec: Vec<crate::storage::StreamId> = ids
                    .iter()
                    .map(|s| crate::storage::StreamId::parse(s))
                    .collect::<Result<Vec<_>>>()?;
                let removed = self.storage.xdel(&key, &id_vec)?;
                Ok(RespValue::Integer(removed as i64))
            }
            Command::XRead(keys, ids, count) => {
                let result = self.storage.xread(&keys, &ids, count)?;
                let mut resp_values = Vec::new();
                for (key, entries) in result {
                    let mut stream_entries = Vec::new();
                    for (id, fields) in entries {
                        let mut field_values = Vec::new();
                        for (f, v) in fields {
                            field_values.push(RespValue::BulkString(Some(Bytes::from(f))));
                            field_values.push(RespValue::BulkString(Some(Bytes::from(v))));
                        }
                        stream_entries.push(RespValue::Array(vec![
                            RespValue::BulkString(Some(Bytes::from(id.to_string()))),
                            RespValue::Array(field_values),
                        ]));
                    }
                    resp_values.push(RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from(key))),
                        RespValue::Array(stream_entries),
                    ]));
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::XSetId(key, id) => {
                let sid = crate::storage::StreamId::parse(&id)?;
                let ok = self.storage.xsetid(&key, sid)?;
                if ok {
                    Ok(RespValue::SimpleString("OK".to_string()))
                } else {
                    Ok(RespValue::Error("ERR 键不存在".to_string()))
                }
            }
            Command::XGroupCreate(key, group, id, mkstream) => {
                self.storage.xgroup_create(&key, &group, &id, mkstream)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::XGroupDestroy(key, group) => {
                let removed = self.storage.xgroup_destroy(&key, &group)?;
                Ok(RespValue::Integer(if removed { 1 } else { 0 }))
            }
            Command::XGroupSetId(key, group, id) => {
                self.storage.xgroup_setid(&key, &group, &id)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::XGroupDelConsumer(key, group, consumer) => {
                let pending = self.storage.xgroup_delconsumer(&key, &group, &consumer)?;
                Ok(RespValue::Integer(pending as i64))
            }
            Command::XGroupCreateConsumer(key, group, consumer) => {
                let created = self.storage.xgroup_createconsumer(&key, &group, &consumer)?;
                Ok(RespValue::Integer(if created { 1 } else { 0 }))
            }
            Command::XReadGroup(group, consumer, keys, ids, count, noack) => {
                let result = self.storage.xreadgroup(&group, &consumer, &keys, &ids, count, noack)?;
                let mut resp_values = Vec::new();
                for (key, entries) in result {
                    let mut stream_entries = Vec::new();
                    for (id, fields) in entries {
                        let mut field_values = Vec::new();
                        for (f, v) in fields {
                            field_values.push(RespValue::BulkString(Some(Bytes::from(f))));
                            field_values.push(RespValue::BulkString(Some(Bytes::from(v))));
                        }
                        stream_entries.push(RespValue::Array(vec![
                            RespValue::BulkString(Some(Bytes::from(id.to_string()))),
                            RespValue::Array(field_values),
                        ]));
                    }
                    resp_values.push(RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from(key))),
                        RespValue::Array(stream_entries),
                    ]));
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::XAck(key, group, ids) => {
                let id_vec: Vec<crate::storage::StreamId> = ids
                    .iter()
                    .map(|s| crate::storage::StreamId::parse(s))
                    .collect::<Result<Vec<_>>>()?;
                let acked = self.storage.xack(&key, &group, &id_vec)?;
                Ok(RespValue::Integer(acked as i64))
            }
            Command::XClaim(key, group, consumer, min_idle, ids, justid) => {
                let id_vec: Vec<crate::storage::StreamId> = ids
                    .iter()
                    .map(|s| crate::storage::StreamId::parse(s))
                    .collect::<Result<Vec<_>>>()?;
                let claimed = self.storage.xclaim(&key, &group, &consumer, min_idle, &id_vec, justid)?;
                let mut resp = Vec::new();
                for (id, fields) in claimed {
                    if justid {
                        resp.push(RespValue::BulkString(Some(Bytes::from(id.to_string()))));
                    } else if let Some(flds) = fields {
                        let mut fv = Vec::new();
                        for (f, v) in flds {
                            fv.push(RespValue::BulkString(Some(Bytes::from(f))));
                            fv.push(RespValue::BulkString(Some(Bytes::from(v))));
                        }
                        resp.push(RespValue::Array(vec![
                            RespValue::BulkString(Some(Bytes::from(id.to_string()))),
                            RespValue::Array(fv),
                        ]));
                    }
                }
                Ok(RespValue::Array(resp))
            }
            Command::XAutoClaim(key, group, consumer, min_idle, start, count, justid) => {
                let start_id = crate::storage::StreamId::parse(&start)?;
                let (next_id, claimed) = self.storage.xautoclaim(&key, &group, &consumer, min_idle, start_id, count, justid)?;
                let mut entries = Vec::new();
                for (id, fields) in claimed {
                    if justid {
                        entries.push(RespValue::BulkString(Some(Bytes::from(id.to_string()))));
                    } else if let Some(flds) = fields {
                        let mut fv = Vec::new();
                        for (f, v) in flds {
                            fv.push(RespValue::BulkString(Some(Bytes::from(f))));
                            fv.push(RespValue::BulkString(Some(Bytes::from(v))));
                        }
                        entries.push(RespValue::Array(vec![
                            RespValue::BulkString(Some(Bytes::from(id.to_string()))),
                            RespValue::Array(fv),
                        ]));
                    }
                }
                Ok(RespValue::Array(vec![
                    RespValue::BulkString(Some(Bytes::from(next_id.to_string()))),
                    RespValue::Array(entries),
                    RespValue::Array(vec![]),
                ]))
            }
            Command::XPending(key, group, start, end, count, consumer) => {
                let start_id = match &start {
                    Some(s) => {
                        if s == "-" { Some(crate::storage::StreamId::new(0, 0)) }
                        else { Some(crate::storage::StreamId::parse(s)?) }
                    }
                    None => None,
                };
                let end_id = match &end {
                    Some(s) => {
                        if s == "+" { Some(crate::storage::StreamId::new(u64::MAX, u64::MAX)) }
                        else { Some(crate::storage::StreamId::parse(s)?) }
                    }
                    None => None,
                };
                let (total, min_id, max_id, consumers, details) = self.storage.xpending(
                    &key, &group, start_id, end_id, count, consumer.as_deref(),
                )?;
                if start.is_none() {
                    let mut resp = vec![RespValue::Integer(total as i64)];
                    match min_id {
                        Some(id) => resp.push(RespValue::BulkString(Some(Bytes::from(id.to_string())))),
                        None => resp.push(RespValue::BulkString(None)),
                    }
                    match max_id {
                        Some(id) => resp.push(RespValue::BulkString(Some(Bytes::from(id.to_string())))),
                        None => resp.push(RespValue::BulkString(None)),
                    }
                    if consumers.is_empty() {
                        resp.push(RespValue::BulkString(None));
                    } else {
                        let mut consumer_list = Vec::new();
                        for (name, cnt) in consumers {
                            consumer_list.push(RespValue::Array(vec![
                                RespValue::BulkString(Some(Bytes::from(name))),
                                RespValue::BulkString(Some(Bytes::from(cnt.to_string()))),
                            ]));
                        }
                        resp.push(RespValue::Array(consumer_list));
                    }
                    Ok(RespValue::Array(resp))
                } else {
                    let mut resp = Vec::new();
                    for (id, consumer_name, idle, delivery_count) in details {
                        resp.push(RespValue::Array(vec![
                            RespValue::BulkString(Some(Bytes::from(id.to_string()))),
                            RespValue::BulkString(Some(Bytes::from(consumer_name))),
                            RespValue::Integer(idle as i64),
                            RespValue::Integer(delivery_count as i64),
                        ]));
                    }
                    Ok(RespValue::Array(resp))
                }
            }
            Command::XInfoStream(key, full) => {
                match self.storage.xinfo_stream(&key, full)? {
                    Some((length, groups, last_id, first_id, group_names)) => {
                        let resp = vec![
                            RespValue::BulkString(Some(Bytes::from("length"))),
                            RespValue::Integer(length as i64),
                            RespValue::BulkString(Some(Bytes::from("groups"))),
                            RespValue::Integer(groups as i64),
                            RespValue::BulkString(Some(Bytes::from("last-generated-id"))),
                            RespValue::BulkString(Some(Bytes::from(last_id.to_string()))),
                            RespValue::BulkString(Some(Bytes::from("first-entry"))),
                            RespValue::BulkString(Some(Bytes::from(first_id.to_string()))),
                        ];
                        Ok(RespValue::Array(resp))
                    }
                    None => Ok(RespValue::Error("ERR no such key".to_string())),
                }
            }
            Command::XInfoGroups(key) => {
                let groups = self.storage.xinfo_groups(&key)?;
                let mut resp = Vec::new();
                for (name, consumers, pending, last_id, entries_read) in groups {
                    resp.push(RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from("name"))),
                        RespValue::BulkString(Some(Bytes::from(name))),
                        RespValue::BulkString(Some(Bytes::from("consumers"))),
                        RespValue::Integer(consumers as i64),
                        RespValue::BulkString(Some(Bytes::from("pending"))),
                        RespValue::Integer(pending as i64),
                        RespValue::BulkString(Some(Bytes::from("last-delivered-id"))),
                        RespValue::BulkString(Some(Bytes::from(last_id.to_string()))),
                        RespValue::BulkString(Some(Bytes::from("entries-read"))),
                        RespValue::Integer(entries_read as i64),
                    ]));
                }
                Ok(RespValue::Array(resp))
            }
            Command::XInfoConsumers(key, group) => {
                let consumers = self.storage.xinfo_consumers(&key, &group)?;
                let mut resp = Vec::new();
                for (name, pending, idle, inactive) in consumers {
                    resp.push(RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from("name"))),
                        RespValue::BulkString(Some(Bytes::from(name))),
                        RespValue::BulkString(Some(Bytes::from("pending"))),
                        RespValue::Integer(pending as i64),
                        RespValue::BulkString(Some(Bytes::from("idle"))),
                        RespValue::Integer(idle as i64),
                        RespValue::BulkString(Some(Bytes::from("inactive"))),
                        RespValue::Integer(inactive as i64),
                    ]));
                }
                Ok(RespValue::Array(resp))
            }
            Command::PfAdd(key, elements) => {
                let updated = self.storage.pfadd(&key, &elements)?;
                Ok(RespValue::Integer(updated))
            }
            Command::PfCount(keys) => {
                let count = self.storage.pfcount(&keys)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::PfMerge(destkey, sourcekeys) => {
                self.storage.pfmerge(&destkey, &sourcekeys)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::GeoAdd(key, items) => {
                let count = self.storage.geoadd(&key, items)?;
                Ok(RespValue::Integer(count))
            }
            Command::GeoDist(key, member1, member2, unit) => {
                match self.storage.geodist(&key, &member1, &member2, &unit)? {
                    Some(dist) => {
                        let trimmed = format!("{:.17}", dist).trim_end_matches('0').trim_end_matches('.').to_string();
                        Ok(RespValue::BulkString(Some(Bytes::from(trimmed))))
                    }
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::GeoHash(key, members) => {
                let hashes = self.storage.geohash(&key, &members)?;
                let resp_values: Vec<RespValue> = hashes
                    .into_iter()
                    .map(|h| RespValue::BulkString(h.map(|s| Bytes::from(s))))
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::GeoPos(key, members) => {
                let positions = self.storage.geopos(&key, &members)?;
                let resp_values: Vec<RespValue> = positions
                    .into_iter()
                    .map(|opt| {
                        match opt {
                            Some((lon, lat)) => {
                                let parts = vec![
                                    RespValue::BulkString(Some(Bytes::from(
                                        format!("{:.17}", lon).trim_end_matches('0').trim_end_matches('.').to_string()
                                    ))),
                                    RespValue::BulkString(Some(Bytes::from(
                                        format!("{:.17}", lat).trim_end_matches('0').trim_end_matches('.').to_string()
                                    ))),
                                ];
                                RespValue::Array(parts)
                            }
                            None => RespValue::BulkString(None),
                        }
                    })
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::GeoSearch(key, center_lon, center_lat, by_radius, by_box, order, count, withcoord, withdist, withhash) => {
                let results = self.storage.geosearch(
                    &key, center_lon, center_lat, by_radius, by_box,
                    order.as_deref(), count,
                )?;
                let mut resp_values = Vec::new();
                for (member, dist, lon, lat, hash) in results {
                    let mut item_parts = Vec::new();
                    // 成员名
                    item_parts.push(RespValue::BulkString(Some(Bytes::from(member))));
                    // 距离
                    if withdist {
                        let dist_s = format!("{:.17}", dist / 1000.0);
                        let dist_str = dist_s.trim_end_matches('0').trim_end_matches('.').to_string();
                        item_parts.push(RespValue::BulkString(Some(Bytes::from(dist_str))));
                    }
                    // hash
                    if withhash {
                        item_parts.push(RespValue::Integer(hash as i64));
                    }
                    // 坐标
                    if withcoord {
                        let coord_parts = vec![
                            RespValue::BulkString(Some(Bytes::from(
                                format!("{:.17}", lon).trim_end_matches('0').trim_end_matches('.').to_string()
                            ))),
                            RespValue::BulkString(Some(Bytes::from(
                                format!("{:.17}", lat).trim_end_matches('0').trim_end_matches('.').to_string()
                            ))),
                        ];
                        item_parts.push(RespValue::Array(coord_parts));
                    }
                    if item_parts.len() == 1 {
                        resp_values.push(item_parts.into_iter().next().unwrap());
                    } else {
                        resp_values.push(RespValue::Array(item_parts));
                    }
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::GeoSearchStore(destination, source, center_lon, center_lat, by_radius, by_box, order, count, storedist) => {
                let count_result = self.storage.geosearchstore(
                    &destination, &source, center_lon, center_lat, by_radius, by_box,
                    order.as_deref(), count, storedist,
                )?;
                Ok(RespValue::Integer(count_result as i64))
            }
            Command::Select(index) => {
                self.storage.select(index)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::Auth(_, _) => {
                Err(AppError::Command("AUTH 应在连接层处理".to_string()))
            }
            Command::AclSetUser(username, rules) => {
                let acl = self.acl.as_ref().ok_or_else(|| {
                    AppError::Command("ACL 未启用".to_string())
                })?;
                let rules_ref: Vec<&str> = rules.iter().map(|s| s.as_str()).collect();
                acl.setuser(&username, &rules_ref)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::AclGetUser(username) => {
                let acl = self.acl.as_ref().ok_or_else(|| {
                    AppError::Command("ACL 未启用".to_string())
                })?;
                match acl.getuser(&username)? {
                    Some(user) => {
                        let rules = user.to_rules();
                        let parts: Vec<RespValue> = rules.into_iter()
                            .map(|r| RespValue::BulkString(Some(Bytes::from(r))))
                            .collect();
                        Ok(RespValue::Array(parts))
                    }
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::AclDelUser(names) => {
                let acl = self.acl.as_ref().ok_or_else(|| {
                    AppError::Command("ACL 未启用".to_string())
                })?;
                let names_ref: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
                let count = acl.deluser(&names_ref)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::AclList => {
                let acl = self.acl.as_ref().ok_or_else(|| {
                    AppError::Command("ACL 未启用".to_string())
                })?;
                let list = acl.list()?;
                let parts: Vec<RespValue> = list.into_iter()
                    .map(|s| RespValue::BulkString(Some(Bytes::from(s))))
                    .collect();
                Ok(RespValue::Array(parts))
            }
            Command::AclCat(category) => {
                let acl = self.acl.as_ref().ok_or_else(|| {
                    AppError::Command("ACL 未启用".to_string())
                })?;
                let cmds = acl.cat(category.as_deref())?;
                let parts: Vec<RespValue> = cmds.into_iter()
                    .map(|s| RespValue::BulkString(Some(Bytes::from(s))))
                    .collect();
                Ok(RespValue::Array(parts))
            }
            Command::AclWhoAmI => {
                Ok(RespValue::SimpleString("default".to_string()))
            }
            Command::AclLog(arg) => {
                let acl = self.acl.as_ref().ok_or_else(|| {
                    AppError::Command("ACL 未启用".to_string())
                })?;
                if let Some(arg) = arg {
                    if arg.to_ascii_uppercase() == "RESET" {
                        acl.log_reset()?;
                        Ok(RespValue::SimpleString("OK".to_string()))
                    } else {
                        let count = arg.parse::<usize>().map_err(|_| {
                            AppError::Command("ACL LOG count 必须是整数".to_string())
                        })?;
                        let logs = acl.log(Some(count))?;
                        let parts: Vec<RespValue> = logs.into_iter()
                            .map(|entry| {
                                RespValue::Array(vec![
                                    RespValue::BulkString(Some(Bytes::from("reason"))),
                                    RespValue::BulkString(Some(Bytes::from(entry.reason))),
                                    RespValue::BulkString(Some(Bytes::from("context"))),
                                    RespValue::BulkString(Some(Bytes::from(entry.context))),
                                    RespValue::BulkString(Some(Bytes::from("object"))),
                                    RespValue::BulkString(Some(Bytes::from(entry.object))),
                                    RespValue::BulkString(Some(Bytes::from("username"))),
                                    RespValue::BulkString(Some(Bytes::from(entry.username))),
                                ])
                            })
                            .collect();
                        Ok(RespValue::Array(parts))
                    }
                } else {
                    let logs = acl.log(None)?;
                    let parts: Vec<RespValue> = logs.into_iter()
                        .map(|entry| {
                            RespValue::Array(vec![
                                RespValue::BulkString(Some(Bytes::from("reason"))),
                                RespValue::BulkString(Some(Bytes::from(entry.reason))),
                                RespValue::BulkString(Some(Bytes::from("context"))),
                                RespValue::BulkString(Some(Bytes::from(entry.context))),
                                RespValue::BulkString(Some(Bytes::from("object"))),
                                RespValue::BulkString(Some(Bytes::from(entry.object))),
                                RespValue::BulkString(Some(Bytes::from("username"))),
                                RespValue::BulkString(Some(Bytes::from(entry.username))),
                            ])
                        })
                        .collect();
                    Ok(RespValue::Array(parts))
                }
            }
            Command::AclGenPass(bits) => {
                let acl = self.acl.as_ref().ok_or_else(|| {
                    AppError::Command("ACL 未启用".to_string())
                })?;
                let pass = acl.genpass(bits)?;
                Ok(RespValue::BulkString(Some(Bytes::from(pass))))
            }
            Command::ClientSetName(_) | Command::ClientGetName | Command::ClientList | Command::ClientId
            | Command::ClientInfo | Command::ClientKill { .. } | Command::ClientPause(_, _)
            | Command::ClientUnpause | Command::ClientNoEvict(_) | Command::ClientNoTouch(_)
            | Command::ClientReply(_) | Command::ClientUnblock(_, _) => {
                Err(AppError::Command("CLIENT 应在连接层处理".to_string()))
            }
            Command::Quit => {
                Err(AppError::Command("QUIT 应在连接层处理".to_string()))
            }
            Command::Eval(script, keys, args) => {
                match &self.script_engine {
                    Some(engine) => engine.eval(&script, keys, args, self.storage.clone()),
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::EvalSha(sha1, keys, args) => {
                match &self.script_engine {
                    Some(engine) => engine.evalsha(&sha1, keys, args, self.storage.clone()),
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::ScriptLoad(script) => {
                match &self.script_engine {
                    Some(engine) => {
                        let sha1 = engine.script_load(&script)?;
                        Ok(RespValue::BulkString(Some(Bytes::from(sha1))))
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::ScriptExists(sha1s) => {
                match &self.script_engine {
                    Some(engine) => {
                        let exists = engine.script_exists(&sha1s)?;
                        let arr: Vec<RespValue> = exists.into_iter().map(|b| RespValue::Integer(if b { 1 } else { 0 })).collect();
                        Ok(RespValue::Array(arr))
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::ScriptFlush => {
                match &self.script_engine {
                    Some(engine) => {
                        engine.script_flush()?;
                        Ok(RespValue::SimpleString("OK".to_string()))
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::FunctionLoad(code, replace) => {
                match &self.script_engine {
                    Some(engine) => {
                        let name = engine.function_load(&code, replace)?;
                        Ok(RespValue::SimpleString(name))
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::FunctionDelete(lib) => {
                match &self.script_engine {
                    Some(engine) => {
                        let ok = engine.function_delete(&lib)?;
                        if ok {
                            Ok(RespValue::Integer(1))
                        } else {
                            Ok(RespValue::Integer(0))
                        }
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::FunctionList(pattern, withcode) => {
                match &self.script_engine {
                    Some(engine) => {
                        let list = engine.function_list(pattern.as_deref(), withcode)?;
                        let mut parts = Vec::new();
                        for (name, engine_name, code, funcs) in list {
                            let mut lib_parts = Vec::new();
                            lib_parts.push(RespValue::BulkString(Some(Bytes::from("library_name"))));
                            lib_parts.push(RespValue::BulkString(Some(Bytes::from(name))));
                            lib_parts.push(RespValue::BulkString(Some(Bytes::from("engine"))));
                            lib_parts.push(RespValue::BulkString(Some(Bytes::from(engine_name))));
                            if withcode {
                                lib_parts.push(RespValue::BulkString(Some(Bytes::from("library_code"))));
                                lib_parts.push(RespValue::BulkString(Some(Bytes::from(code))));
                            }
                            lib_parts.push(RespValue::BulkString(Some(Bytes::from("functions"))));
                            let mut func_parts = Vec::new();
                            for (fname, flags) in funcs {
                                let mut f = Vec::new();
                                f.push(RespValue::BulkString(Some(Bytes::from("name"))));
                                f.push(RespValue::BulkString(Some(Bytes::from(fname))));
                                f.push(RespValue::BulkString(Some(Bytes::from("flags"))));
                                let flag_parts: Vec<RespValue> = flags.into_iter()
                                    .map(|s| RespValue::BulkString(Some(Bytes::from(s))))
                                    .collect();
                                f.push(RespValue::Array(flag_parts));
                                func_parts.push(RespValue::Array(f));
                            }
                            lib_parts.push(RespValue::Array(func_parts));
                            parts.push(RespValue::Array(lib_parts));
                        }
                        Ok(RespValue::Array(parts))
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::FunctionDump => {
                match &self.script_engine {
                    Some(engine) => {
                        let dump = engine.function_dump()?;
                        Ok(RespValue::BulkString(Some(Bytes::from(dump))))
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::FunctionRestore(data, policy) => {
                match &self.script_engine {
                    Some(engine) => {
                        engine.function_restore(&data, &policy)?;
                        Ok(RespValue::SimpleString("OK".to_string()))
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::FunctionStats => {
                match &self.script_engine {
                    Some(engine) => {
                        let (libs, funcs) = engine.function_stats()?;
                        let mut parts = Vec::new();
                        parts.push(RespValue::BulkString(Some(Bytes::from("libraries_count"))));
                        parts.push(RespValue::Integer(libs as i64));
                        parts.push(RespValue::BulkString(Some(Bytes::from("functions_count"))));
                        parts.push(RespValue::Integer(funcs as i64));
                        Ok(RespValue::Array(parts))
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::FunctionFlush(async_mode) => {
                match &self.script_engine {
                    Some(engine) => {
                        engine.function_flush(async_mode)?;
                        Ok(RespValue::SimpleString("OK".to_string()))
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::FCall(name, keys, args) => {
                match &self.script_engine {
                    Some(engine) => {
                        let resp = engine.fcall(&name, keys.clone(), args.clone(), self.storage.clone())?;
                        Ok(resp)
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::FCallRO(name, keys, args) => {
                match &self.script_engine {
                    Some(engine) => {
                        let resp = engine.fcall_ro(&name, keys.clone(), args.clone(), self.storage.clone())?;
                        Ok(resp)
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::EvalRO(script, keys, args) => {
                match &self.script_engine {
                    Some(engine) => {
                        let resp = engine.eval(&script, keys.clone(), args.clone(), self.storage.clone())?;
                        Ok(resp)
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::EvalShaRO(sha1, keys, args) => {
                match &self.script_engine {
                    Some(engine) => {
                        let resp = engine.evalsha(&sha1, keys.clone(), args.clone(), self.storage.clone())?;
                        Ok(resp)
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::Save => {
                Err(AppError::Command("SAVE 应在连接层处理".to_string()))
            }
            Command::BgSave => {
                Err(AppError::Command("BGSAVE 应在连接层处理".to_string()))
            }
            Command::SlowLogGet(count) => {
                match &self.slowlog {
                    Some(log) => {
                        let entries = log.get(count);
                        let arr: Vec<RespValue> = entries.iter()
                            .map(|e| SlowLog::entry_to_resp(e))
                            .collect();
                        Ok(RespValue::Array(arr))
                    }
                    None => Ok(RespValue::Array(vec![])),
                }
            }
            Command::SlowLogLen => {
                match &self.slowlog {
                    Some(log) => Ok(RespValue::Integer(log.len() as i64)),
                    None => Ok(RespValue::Integer(0)),
                }
            }
            Command::SlowLogReset => {
                match &self.slowlog {
                    Some(log) => {
                        log.reset();
                        Ok(RespValue::SimpleString("OK".to_string()))
                    }
                    None => Ok(RespValue::SimpleString("OK".to_string())),
                }
            }
            Command::ObjectEncoding(key) => {
                match self.storage.object_encoding(&key)? {
                    Some(enc) => Ok(RespValue::BulkString(Some(Bytes::from(enc)))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::ObjectRefCount(key) => {
                match self.storage.object_refcount(&key)? {
                    Some(count) => Ok(RespValue::Integer(count)),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::ObjectIdleTime(key) => {
                match self.storage.object_idletime(&key)? {
                    Some(secs) => Ok(RespValue::Integer(secs as i64)),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::ObjectHelp => {
                let help = crate::storage::StorageEngine::object_help();
                let arr: Vec<RespValue> = help
                    .into_iter()
                    .map(|s| RespValue::BulkString(Some(Bytes::from(s))))
                    .collect();
                Ok(RespValue::Array(arr))
            }
            Command::DebugSetActiveExpire(enabled) => {
                self.storage.set_active_expire(enabled);
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::DebugSleep(seconds) => {
                std::thread::sleep(std::time::Duration::from_secs_f64(seconds));
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::DebugObject(key) => {
                let info = self.build_debug_object_info(&key)?;
                Ok(RespValue::SimpleString(info))
            }
            Command::Echo(msg) => {
                Ok(RespValue::BulkString(Some(Bytes::from(msg))))
            }
            Command::Time => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                let secs = now.as_secs() as i64;
                let micros = (now.as_micros() % 1_000_000) as i64;
                Ok(RespValue::Array(vec![
                    RespValue::Integer(secs),
                    RespValue::Integer(micros),
                ]))
            }
            Command::RandomKey => {
                match self.storage.randomkey()? {
                    Some(key) => Ok(RespValue::BulkString(Some(Bytes::from(key)))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::Touch(keys) => {
                let count = self.storage.touch_keys(&keys)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::ExpireAt(key, timestamp) => {
                let ok = self.storage.expire_at(&key, timestamp)?;
                Ok(RespValue::Integer(if ok { 1 } else { 0 }))
            }
            Command::PExpireAt(key, timestamp) => {
                let ok = self.storage.pexpire_at(&key, timestamp)?;
                Ok(RespValue::Integer(if ok { 1 } else { 0 }))
            }
            Command::ExpireTime(key) => {
                Ok(RespValue::Integer(self.storage.expire_time(&key)?))
            }
            Command::PExpireTime(key) => {
                Ok(RespValue::Integer(self.storage.pexpire_time(&key)?))
            }
            Command::RenameNx(key, newkey) => {
                let ok = self.storage.renamenx(&key, &newkey)?;
                Ok(RespValue::Integer(if ok { 1 } else { 0 }))
            }
            Command::SwapDb(idx1, idx2) => {
                self.storage.swap_db(idx1, idx2)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::FlushDb => {
                self.storage.flush_db(self.storage.current_db())?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::Shutdown(_) => {
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::LastSave => {
                Ok(RespValue::Integer(self.storage.get_last_save_time() as i64))
            }
            Command::SubStr(key, start, end) => {
                // SUBSTR 是 GETRANGE 的别名
                match self.storage.getrange(&key, start, end)? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::Lcs(key1, key2, len, idx, _minmatchlen, withmatchlen) => {
                let lcs_str = self.storage.lcs(&key1, &key2)?;
                if len {
                    // LEN 模式：只返回长度
                    let length = lcs_str.as_ref().map(|s| s.len()).unwrap_or(0);
                    Ok(RespValue::Integer(length as i64))
                } else if idx {
                    // IDX 模式：返回匹配位置
                    // 简化实现：返回整个匹配的起点和终点
                    let mut arr = Vec::new();
                    if let Some(s) = lcs_str {
                        let v1 = match self.storage.get(&key1)? {
                            Some(b) => String::from_utf8_lossy(&b).to_string(),
                            None => String::new(),
                        };
                        let v2 = match self.storage.get(&key2)? {
                            Some(b) => String::from_utf8_lossy(&b).to_string(),
                            None => String::new(),
                        };
                        // 找到 LCS 在 v1 和 v2 中的位置
                        if !s.is_empty() && !v1.is_empty() && !v2.is_empty() {
                            let pos1 = v1.find(&s).unwrap_or(0) as i64;
                            let pos2 = v2.find(&s).unwrap_or(0) as i64;
                            let match_len = s.len() as i64;
                            let mut match_arr = Vec::new();
                            let mut m1 = Vec::new();
                            m1.push(RespValue::Integer(pos1));
                            m1.push(RespValue::Integer(pos1 + match_len - 1));
                            let mut m2 = Vec::new();
                            m2.push(RespValue::Integer(pos2));
                            m2.push(RespValue::Integer(pos2 + match_len - 1));
                            match_arr.push(RespValue::Array(m1));
                            match_arr.push(RespValue::Array(m2));
                            if withmatchlen {
                                match_arr.push(RespValue::Integer(match_len));
                            }
                            arr.push(RespValue::Array(match_arr));
                        }
                        arr.push(RespValue::Integer(v1.len() as i64));
                        arr.push(RespValue::Integer(v2.len() as i64));
                    } else {
                        arr.push(RespValue::Array(vec![]));
                        arr.push(RespValue::Integer(0));
                        arr.push(RespValue::Integer(0));
                    }
                    Ok(RespValue::Array(arr))
                } else {
                    // 默认模式：返回最长公共子串
                    match lcs_str {
                        Some(s) => Ok(RespValue::BulkString(Some(Bytes::from(s)))),
                        None => Ok(RespValue::BulkString(None)),
                    }
                }
            }
            Command::Lmove(source, dest, left_from, left_to) => {
                match self.storage.lmove(&source, &dest, left_from, left_to)? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::Rpoplpush(source, dest) => {
                match self.storage.rpoplpush(&source, &dest)? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::Lmpop(keys, left, count) => {
                match self.storage.lmpop(&keys, left, count)? {
                    Some((key, values)) => {
                        let mut arr = Vec::new();
                        arr.push(RespValue::BulkString(Some(Bytes::from(key))));
                        let vals: Vec<RespValue> = values.into_iter()
                            .map(|v| RespValue::BulkString(Some(v)))
                            .collect();
                        arr.push(RespValue::Array(vals));
                        Ok(RespValue::Array(arr))
                    }
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::Sort(key, by_pattern, get_patterns, limit_offset, limit_count, asc, alpha, store_key) => {
                let result = self.storage.sort(
                    &key, by_pattern, get_patterns, limit_offset, limit_count, asc, alpha, store_key.clone(),
                )?;
                if let Some(dest) = store_key {
                    // 有 STORE 时返回存入的元素数量
                    let count = self.storage.llen(&dest)?;
                    Ok(RespValue::Integer(count as i64))
                } else {
                    let resp_values: Vec<RespValue> = result
                        .into_iter()
                        .map(|s| RespValue::BulkString(Some(Bytes::from(s))))
                        .collect();
                    Ok(RespValue::Array(resp_values))
                }
            }
            Command::Unlink(keys) => {
                let count = self.storage.unlink(&keys)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::Copy(source, destination, replace) => {
                let ok = self.storage.copy(&source, &destination, replace)?;
                Ok(RespValue::Integer(if ok { 1 } else { 0 }))
            }
            Command::Dump(key) => {
                match self.storage.dump(&key)? {
                    Some(data) => Ok(RespValue::BulkString(Some(Bytes::from(data)))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::Restore(key, ttl_ms, serialized, replace) => {
                self.storage.restore(&key, ttl_ms, &serialized, replace)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::MGet(keys) => {
                let values = self.storage.mget(&keys)?;
                let resp_values: Vec<RespValue> = values
                    .into_iter()
                    .map(|v| RespValue::BulkString(v))
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::MSet(pairs) => {
                self.storage.mset(&pairs)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::Incr(key) => {
                let new_val = self.storage.incr(&key)?;
                Ok(RespValue::Integer(new_val))
            }
            Command::Decr(key) => {
                let new_val = self.storage.decr(&key)?;
                Ok(RespValue::Integer(new_val))
            }
            Command::IncrBy(key, delta) => {
                let new_val = self.storage.incrby(&key, delta)?;
                Ok(RespValue::Integer(new_val))
            }
            Command::DecrBy(key, delta) => {
                let new_val = self.storage.decrby(&key, delta)?;
                Ok(RespValue::Integer(new_val))
            }
            Command::Append(key, value) => {
                let new_len = self.storage.append(&key, value)?;
                Ok(RespValue::Integer(new_len as i64))
            }
            Command::SetNx(key, value) => {
                let result = if self.storage.setnx(key, value)? {
                    1i64
                } else {
                    0i64
                };
                Ok(RespValue::Integer(result))
            }
            Command::SetExCmd(key, value, seconds) => {
                self.storage.setex(key, seconds, value)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::PSetEx(key, value, ms) => {
                self.storage.psetex(key, ms, value)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::GetSet(key, value) => {
                match self.storage.getset(&key, value)? {
                    Some(data) => Ok(RespValue::BulkString(Some(data))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::GetDel(key) => {
                match self.storage.getdel(&key)? {
                    Some(data) => Ok(RespValue::BulkString(Some(data))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::GetEx(key, opt) => {
                match self.storage.getex(&key, opt)? {
                    Some(data) => Ok(RespValue::BulkString(Some(data))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::MSetNx(pairs) => {
                let result = self.storage.msetnx(&pairs)?;
                Ok(RespValue::Integer(result))
            }
            Command::IncrByFloat(key, delta) => {
                let new_val = self.storage.incrbyfloat(&key, delta)?;
                Ok(RespValue::BulkString(Some(Bytes::from(new_val))))
            }
            Command::SetRange(key, offset, value) => {
                let new_len = self.storage.setrange(&key, offset, value)?;
                Ok(RespValue::Integer(new_len as i64))
            }
            Command::GetRange(key, start, end) => {
                match self.storage.getrange(&key, start, end)? {
                    Some(data) => Ok(RespValue::BulkString(Some(data))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::StrLen(key) => {
                let len = self.storage.strlen(&key)?;
                Ok(RespValue::Integer(len as i64))
            }
            Command::LPush(key, values) => {
                let len = self.storage.lpush(&key, values)?;
                Ok(RespValue::Integer(len as i64))
            }
            Command::RPush(key, values) => {
                let len = self.storage.rpush(&key, values)?;
                Ok(RespValue::Integer(len as i64))
            }
            Command::LPop(key) => {
                match self.storage.lpop(&key)? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::RPop(key) => {
                match self.storage.rpop(&key)? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::LLen(key) => {
                let len = self.storage.llen(&key)?;
                Ok(RespValue::Integer(len as i64))
            }
            Command::LRange(key, start, stop) => {
                let values = self.storage.lrange(&key, start, stop)?;
                let resp_values: Vec<RespValue> = values
                    .into_iter()
                    .map(|v| RespValue::BulkString(Some(v)))
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::LIndex(key, index) => {
                match self.storage.lindex(&key, index)? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::LSet(key, index, value) => {
                self.storage.lset(&key, index, value)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::LInsert(key, pos, pivot, value) => {
                let result = self.storage.linsert(&key, pos, pivot, value)?;
                Ok(RespValue::Integer(result))
            }
            Command::LRem(key, count, value) => {
                let removed = self.storage.lrem(&key, count, value)?;
                Ok(RespValue::Integer(removed))
            }
            Command::LTrim(key, start, stop) => {
                self.storage.ltrim(&key, start, stop)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::LPos(key, value, rank, count, maxlen) => {
                let positions = self.storage.lpos(&key, value, rank, count, maxlen)?;
                if count == 0 {
                    // 不指定 COUNT，返回第一个匹配位置或 nil
                    if let Some(&pos) = positions.first() {
                        Ok(RespValue::Integer(pos))
                    } else {
                        Ok(RespValue::BulkString(None))
                    }
                } else {
                    // 指定了 COUNT，返回位置数组
                    let arr: Vec<RespValue> = positions.into_iter().map(|p| RespValue::Integer(p)).collect();
                    Ok(RespValue::Array(arr))
                }
            }
            Command::BLPop(keys, _timeout) => {
                // 非阻塞版本：直接尝试弹出（用于事务和 AOF 重放）
                for key in &keys {
                    if let Ok(Some(value)) = self.storage.lpop(key) {
                        // 记录等效 LPOP 到 AOF
                        let lpop_cmd = Command::LPop(key.clone());
                        self.append_to_aof(&lpop_cmd);
                        return Ok(RespValue::Array(vec![
                            RespValue::BulkString(Some(bytes::Bytes::from(key.clone()))),
                            RespValue::BulkString(Some(value)),
                        ]));
                    }
                }
                Ok(RespValue::BulkString(None))
            }
            Command::BRPop(keys, _timeout) => {
                // 非阻塞版本：直接尝试弹出（用于事务和 AOF 重放）
                for key in &keys {
                    if let Ok(Some(value)) = self.storage.rpop(key) {
                        // 记录等效 RPOP 到 AOF
                        let rpop_cmd = Command::RPop(key.clone());
                        self.append_to_aof(&rpop_cmd);
                        return Ok(RespValue::Array(vec![
                            RespValue::BulkString(Some(bytes::Bytes::from(key.clone()))),
                            RespValue::BulkString(Some(value)),
                        ]));
                    }
                }
                Ok(RespValue::BulkString(None))
            }
            Command::BLmove(source, dest, left_from, left_to, _timeout) => {
                // 非阻塞版本：直接尝试移动（用于事务和 AOF 重放）
                if let Ok(Some(value)) = self.storage.lmove(&source, &dest, left_from, left_to) {
                    self.append_to_aof(&Command::Lmove(source.clone(), dest.clone(), left_from, left_to));
                    Ok(RespValue::BulkString(Some(value)))
                } else {
                    Ok(RespValue::BulkString(None))
                }
            }
            Command::BLmpop(keys, left, count, _timeout) => {
                // 非阻塞版本：直接尝试弹出（用于事务和 AOF 重放）
                if let Ok(Some((key, values))) = self.storage.lmpop(&keys, left, count) {
                    self.append_to_aof(&Command::Lmpop(keys.clone(), left, count));
                    let mut arr = Vec::new();
                    arr.push(RespValue::BulkString(Some(bytes::Bytes::from(key))));
                    let vals: Vec<RespValue> = values.into_iter()
                        .map(|v| RespValue::BulkString(Some(v)))
                        .collect();
                    arr.push(RespValue::Array(vals));
                    Ok(RespValue::Array(arr))
                } else {
                    Ok(RespValue::BulkString(None))
                }
            }
            Command::BRpoplpush(source, dest, _timeout) => {
                // 非阻塞版本：直接尝试移动（用于事务和 AOF 重放）
                if let Ok(Some(value)) = self.storage.rpoplpush(&source, &dest) {
                    self.append_to_aof(&Command::Rpoplpush(source.clone(), dest.clone()));
                    Ok(RespValue::BulkString(Some(value)))
                } else {
                    Ok(RespValue::BulkString(None))
                }
            }
            Command::HSet(key, pairs) => {
                let mut count = 0i64;
                for (field, value) in pairs {
                    count += self.storage.hset(&key, field, value)?;
                }
                Ok(RespValue::Integer(count))
            }
            Command::HGet(key, field) => {
                match self.storage.hget(&key, &field)? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::HDel(key, fields) => {
                let count = self.storage.hdel(&key, &fields)?;
                Ok(RespValue::Integer(count))
            }
            Command::HExists(key, field) => {
                let result = if self.storage.hexists(&key, &field)? {
                    1i64
                } else {
                    0i64
                };
                Ok(RespValue::Integer(result))
            }
            Command::HGetAll(key) => {
                let pairs = self.storage.hgetall(&key)?;
                let mut resp_values = Vec::new();
                for (field, value) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(field))));
                    resp_values.push(RespValue::BulkString(Some(value)));
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::HLen(key) => {
                let len = self.storage.hlen(&key)?;
                Ok(RespValue::Integer(len as i64))
            }
            Command::HMSet(key, pairs) => {
                self.storage.hmset(&key, &pairs)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::HMGet(key, fields) => {
                let values = self.storage.hmget(&key, &fields)?;
                let resp_values: Vec<RespValue> = values
                    .into_iter()
                    .map(|v| RespValue::BulkString(v))
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::HIncrBy(key, field, delta) => {
                let new_val = self.storage.hincrby(&key, field, delta)?;
                Ok(RespValue::Integer(new_val))
            }
            Command::HIncrByFloat(key, field, delta) => {
                let new_val = self.storage.hincrbyfloat(&key, field, delta)?;
                Ok(RespValue::BulkString(Some(Bytes::from(new_val))))
            }
            Command::HKeys(key) => {
                let keys = self.storage.hkeys(&key)?;
                let resp_values: Vec<RespValue> = keys.into_iter().map(|k| RespValue::BulkString(Some(Bytes::from(k)))).collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::HVals(key) => {
                let vals = self.storage.hvals(&key)?;
                let resp_values: Vec<RespValue> = vals.into_iter().map(|v| RespValue::BulkString(Some(v))).collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::HSetNx(key, field, value) => {
                let result = self.storage.hsetnx(&key, field, value)?;
                Ok(RespValue::Integer(result))
            }
            Command::HRandField(key, count, with_values) => {
                let result = self.storage.hrandfield(&key, count, with_values)?;
                if count == 1 && !with_values {
                    // 单字段不带值，返回单个 BulkString
                    if let Some((field, _)) = result.first() {
                        Ok(RespValue::BulkString(Some(Bytes::from(field.clone()))))
                    } else {
                        Ok(RespValue::BulkString(None))
                    }
                } else {
                    let mut parts = Vec::new();
                    for (field, value) in result {
                        parts.push(RespValue::BulkString(Some(Bytes::from(field))));
                        if let Some(v) = value {
                            parts.push(RespValue::BulkString(Some(v)));
                        }
                    }
                    Ok(RespValue::Array(parts))
                }
            }
            Command::HScan(key, cursor, pattern, count) => {
                let (new_cursor, fields) = self.storage.hscan(&key, cursor, &pattern, count)?;
                let mut parts: Vec<RespValue> = vec![
                    RespValue::BulkString(Some(Bytes::from(new_cursor.to_string()))),
                ];
                let field_values: Vec<RespValue> = fields
                    .into_iter()
                    .flat_map(|(f, v)| {
                        vec![
                            RespValue::BulkString(Some(Bytes::from(f))),
                            RespValue::BulkString(Some(v)),
                        ]
                    })
                    .collect();
                parts.push(RespValue::Array(field_values));
                Ok(RespValue::Array(parts))
            }
            Command::HExpire(key, fields, seconds) => {
                let result = self.storage.hexpire(&key, &fields, seconds)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(|v| RespValue::Integer(v))
                    .collect();
                Ok(RespValue::Array(arr))
            }
            Command::HPExpire(key, fields, ms) => {
                let result = self.storage.hpexpire(&key, &fields, ms)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(|v| RespValue::Integer(v))
                    .collect();
                Ok(RespValue::Array(arr))
            }
            Command::HExpireAt(key, fields, ts) => {
                let result = self.storage.hexpireat(&key, &fields, ts)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(|v| RespValue::Integer(v))
                    .collect();
                Ok(RespValue::Array(arr))
            }
            Command::HPExpireAt(key, fields, ts) => {
                let result = self.storage.hpexpireat(&key, &fields, ts)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(|v| RespValue::Integer(v))
                    .collect();
                Ok(RespValue::Array(arr))
            }
            Command::HTtl(key, fields) => {
                let result = self.storage.httl(&key, &fields)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(|v| RespValue::Integer(v))
                    .collect();
                Ok(RespValue::Array(arr))
            }
            Command::HPTtl(key, fields) => {
                let result = self.storage.hpttl(&key, &fields)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(|v| RespValue::Integer(v))
                    .collect();
                Ok(RespValue::Array(arr))
            }
            Command::HExpireTime(key, fields) => {
                let result = self.storage.hexpiretime(&key, &fields)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(|v| RespValue::Integer(v))
                    .collect();
                Ok(RespValue::Array(arr))
            }
            Command::HPExpireTime(key, fields) => {
                let result = self.storage.hpexpiretime(&key, &fields)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(|v| RespValue::Integer(v))
                    .collect();
                Ok(RespValue::Array(arr))
            }
            Command::HPersist(key, fields) => {
                let result = self.storage.hpersist(&key, &fields)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(|v| RespValue::Integer(v))
                    .collect();
                Ok(RespValue::Array(arr))
            }
            Command::SAdd(key, members) => {
                let count = self.storage.sadd(&key, members)?;
                Ok(RespValue::Integer(count))
            }
            Command::SRem(key, members) => {
                let count = self.storage.srem(&key, &members)?;
                Ok(RespValue::Integer(count))
            }
            Command::SMembers(key) => {
                let members = self.storage.smembers(&key)?;
                let resp_values: Vec<RespValue> = members
                    .into_iter()
                    .map(|m| RespValue::BulkString(Some(m)))
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::SIsMember(key, member) => {
                let result = if self.storage.sismember(&key, &member)? {
                    1i64
                } else {
                    0i64
                };
                Ok(RespValue::Integer(result))
            }
            Command::SCard(key) => {
                let len = self.storage.scard(&key)?;
                Ok(RespValue::Integer(len as i64))
            }
            Command::SInter(keys) => {
                let members = self.storage.sinter(&keys)?;
                let resp_values: Vec<RespValue> = members
                    .into_iter()
                    .map(|m| RespValue::BulkString(Some(m)))
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::SUnion(keys) => {
                let members = self.storage.sunion(&keys)?;
                let resp_values: Vec<RespValue> = members
                    .into_iter()
                    .map(|m| RespValue::BulkString(Some(m)))
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::SDiff(keys) => {
                let members = self.storage.sdiff(&keys)?;
                let resp_values: Vec<RespValue> = members
                    .into_iter()
                    .map(|m| RespValue::BulkString(Some(m)))
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::SPop(key, count) => {
                let members = self.storage.spop(&key, count)?;
                if count == 1 {
                    if let Some(m) = members.into_iter().next() {
                        Ok(RespValue::BulkString(Some(m)))
                    } else {
                        Ok(RespValue::BulkString(None))
                    }
                } else {
                    let resp_values: Vec<RespValue> = members
                        .into_iter()
                        .map(|m| RespValue::BulkString(Some(m)))
                        .collect();
                    Ok(RespValue::Array(resp_values))
                }
            }
            Command::SRandMember(key, count) => {
                let members = self.storage.srandmember(&key, count)?;
                if count == 1 {
                    if let Some(m) = members.into_iter().next() {
                        Ok(RespValue::BulkString(Some(m)))
                    } else {
                        Ok(RespValue::BulkString(None))
                    }
                } else {
                    let resp_values: Vec<RespValue> = members
                        .into_iter()
                        .map(|m| RespValue::BulkString(Some(m)))
                        .collect();
                    Ok(RespValue::Array(resp_values))
                }
            }
            Command::SMove(source, destination, member) => {
                let result = if self.storage.smove(&source, &destination, member)? {
                    1i64
                } else {
                    0i64
                };
                Ok(RespValue::Integer(result))
            }
            Command::SInterStore(destination, keys) => {
                let count = self.storage.sinterstore(&destination, &keys)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::SUnionStore(destination, keys) => {
                let count = self.storage.sunionstore(&destination, &keys)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::SDiffStore(destination, keys) => {
                let count = self.storage.sdiffstore(&destination, &keys)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::SScan(key, cursor, pattern, count) => {
                let (new_cursor, members) = self.storage.sscan(&key, cursor, &pattern, count)?;
                let mut parts: Vec<RespValue> = vec![
                    RespValue::BulkString(Some(Bytes::from(new_cursor.to_string()))),
                ];
                let member_values: Vec<RespValue> = members
                    .into_iter()
                    .map(|m| RespValue::BulkString(Some(m)))
                    .collect();
                parts.push(RespValue::Array(member_values));
                Ok(RespValue::Array(parts))
            }
            Command::ZAdd(key, pairs) => {
                let count = self.storage.zadd(&key, pairs)?;
                Ok(RespValue::Integer(count))
            }
            Command::ZRem(key, members) => {
                let count = self.storage.zrem(&key, &members)?;
                Ok(RespValue::Integer(count))
            }
            Command::ZScore(key, member) => {
                match self.storage.zscore(&key, &member)? {
                    Some(score) => {
                        Ok(RespValue::BulkString(Some(Bytes::from(
                            format!("{}", score),
                        ))))
                    }
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::ZRank(key, member) => {
                match self.storage.zrank(&key, &member)? {
                    Some(rank) => Ok(RespValue::Integer(rank as i64)),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::ZRange(key, start, stop, with_scores) => {
                let pairs = self.storage.zrange(&key, start, stop, with_scores)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(
                            format!("{}", score),
                        ))));
                    }
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::ZRangeByScore(key, min, max, with_scores) => {
                let pairs = self.storage.zrangebyscore(&key, min, max, with_scores)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(
                            format!("{}", score),
                        ))));
                    }
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::ZCard(key) => {
                let len = self.storage.zcard(&key)?;
                Ok(RespValue::Integer(len as i64))
            }
            Command::ZRevRange(key, start, stop, with_scores) => {
                let pairs = self.storage.zrevrange(&key, start, stop, with_scores)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(
                            format!("{}", score),
                        ))));
                    }
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::ZRevRank(key, member) => {
                match self.storage.zrevrank(&key, &member)? {
                    Some(rank) => Ok(RespValue::Integer(rank as i64)),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::ZIncrBy(key, increment, member) => {
                let new_score = self.storage.zincrby(&key, increment, member)?;
                Ok(RespValue::BulkString(Some(Bytes::from(new_score))))
            }
            Command::ZCount(key, min, max) => {
                let count = self.storage.zcount(&key, min, max)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::ZPopMin(key, count) => {
                let pairs = self.storage.zpopmin(&key, count)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(
                        format!("{}", score),
                    ))));
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::ZPopMax(key, count) => {
                let pairs = self.storage.zpopmax(&key, count)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(
                        format!("{}", score),
                    ))));
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::ZUnionStore(destination, keys, weights, aggregate) => {
                let weights_slice = if weights.is_empty() { None } else { Some(weights.as_slice()) };
                let count = self.storage.zunionstore(&destination, &keys, weights_slice, &aggregate)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::ZInterStore(destination, keys, weights, aggregate) => {
                let weights_slice = if weights.is_empty() { None } else { Some(weights.as_slice()) };
                let count = self.storage.zinterstore(&destination, &keys, weights_slice, &aggregate)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::ZScan(key, cursor, pattern, count) => {
                let (next_cursor, items) = self.storage.zscan(&key, cursor, &pattern, count)?;
                let mut resp_values = Vec::new();
                resp_values.push(RespValue::BulkString(Some(Bytes::from(
                    next_cursor.to_string(),
                ))));
                let mut item_values = Vec::new();
                for (member, score) in items {
                    item_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    item_values.push(RespValue::BulkString(Some(Bytes::from(
                        format!("{}", score),
                    ))));
                }
                resp_values.push(RespValue::Array(item_values));
                Ok(RespValue::Array(resp_values))
            }
            Command::ZRangeByLex(key, min, max) => {
                let members = self.storage.zrangebylex(&key, &min, &max)?;
                let resp_values: Vec<RespValue> = members
                    .into_iter()
                    .map(|m| RespValue::BulkString(Some(Bytes::from(m))))
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::SInterCard(keys, limit) => {
                let count = self.storage.sintercard(&keys, limit)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::SMisMember(key, members) => {
                let result = self.storage.smismember(&key, &members)?;
                let arr: Vec<RespValue> = result.into_iter().map(|v| RespValue::Integer(v)).collect();
                Ok(RespValue::Array(arr))
            }
            Command::ZRandMember(key, count, with_scores) => {
                let pairs = self.storage.zrandmember(&key, count, with_scores)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(format!("{}", score)))));
                    }
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::ZDiff(keys, with_scores) => {
                let pairs = self.storage.zdiff(&keys, with_scores)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(format!("{}", score)))));
                    }
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::ZDiffStore(destination, keys) => {
                let count = self.storage.zdiffstore(&destination, &keys)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::ZInter(keys, weights, aggregate, with_scores) => {
                let weights_slice = if weights.is_empty() { None } else { Some(weights.as_slice()) };
                let pairs = self.storage.zinter(&keys, weights_slice, &aggregate, with_scores)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(format!("{}", score)))));
                    }
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::ZUnion(keys, weights, aggregate, with_scores) => {
                let weights_slice = if weights.is_empty() { None } else { Some(weights.as_slice()) };
                let pairs = self.storage.zunion(&keys, weights_slice, &aggregate, with_scores)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(format!("{}", score)))));
                    }
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::ZRangeStore(dst, src, min, max, by_score, by_lex, rev, limit_offset, limit_count) => {
                let count = self.storage.zrangestore(&dst, &src, &min, &max, by_score, by_lex, rev, limit_offset, limit_count)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::ZMpop(keys, min_or_max, count) => {
                match self.storage.zmpop(&keys, min_or_max, count)? {
                    Some((key, pairs)) => {
                        let mut pair_values = Vec::new();
                        for (member, score) in pairs {
                            pair_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                            pair_values.push(RespValue::BulkString(Some(Bytes::from(format!("{}", score)))));
                        }
                        let resp = RespValue::Array(vec![
                            RespValue::BulkString(Some(Bytes::from(key))),
                            RespValue::Array(pair_values),
                        ]);
                        Ok(resp)
                    }
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::ZRevRangeByScore(key, max, min, with_scores, limit_offset, limit_count) => {
                let pairs = self.storage.zrevrangebyscore(&key, max, min, with_scores, limit_offset, limit_count)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(format!("{}", score)))));
                    }
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::ZRevRangeByLex(key, max, min, limit_offset, limit_count) => {
                let members = self.storage.zrevrangebylex(&key, &max, &min, limit_offset, limit_count)?;
                let resp_values: Vec<RespValue> = members
                    .into_iter()
                    .map(|m| RespValue::BulkString(Some(Bytes::from(m))))
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::ZMScore(key, members) => {
                let scores = self.storage.zmscore(&key, &members)?;
                let resp_values: Vec<RespValue> = scores
                    .into_iter()
                    .map(|s| match s {
                        Some(score) => RespValue::BulkString(Some(Bytes::from(format!("{}", score)))),
                        None => RespValue::BulkString(None),
                    })
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::ZLexCount(key, min, max) => {
                let count = self.storage.zlexcount(&key, &min, &max)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::ZRangeUnified(key, min, max, by_score, by_lex, rev, with_scores, limit_offset, limit_count) => {
                let pairs = self.storage.zrange_unified(&key, &min, &max, by_score, by_lex, rev, with_scores, limit_offset, limit_count)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(format!("{}", score)))));
                    }
                }
                Ok(RespValue::Array(resp_values))
            }
            // 阻塞命令在非阻塞上下文中由 server.rs 处理
            Command::BZMpop(_, _, _, _) | Command::BZPopMin(_, _) | Command::BZPopMax(_, _) => {
                Ok(RespValue::BulkString(None))
            }
            Command::Keys(pattern) => {
                let keys = self.storage.keys(&pattern)?;
                let resp_values: Vec<RespValue> = keys
                    .into_iter()
                    .map(|k| RespValue::BulkString(Some(Bytes::from(k))))
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::Scan(cursor, pattern, count) => {
                let (next_cursor, keys) = self.storage.scan(cursor, &pattern, count)?;
                let mut resp_values = Vec::new();
                resp_values.push(RespValue::BulkString(Some(Bytes::from(
                    next_cursor.to_string(),
                ))));
                let key_values: Vec<RespValue> = keys
                    .into_iter()
                    .map(|k| RespValue::BulkString(Some(Bytes::from(k))))
                    .collect();
                resp_values.push(RespValue::Array(key_values));
                Ok(RespValue::Array(resp_values))
            }
            Command::Rename(key, newkey) => {
                self.storage.rename(&key, &newkey)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::Type(key) => {
                let key_type = self.storage.key_type(&key)?;
                Ok(RespValue::SimpleString(key_type))
            }
            Command::Persist(key) => {
                let removed = self.storage.persist(&key)?;
                Ok(RespValue::Integer(if removed { 1 } else { 0 }))
            }
            Command::PExpire(key, ms) => {
                let success = self.storage.pexpire(&key, ms)?;
                Ok(RespValue::Integer(if success { 1 } else { 0 }))
            }
            Command::PTtl(key) => {
                let ttl_ms = self.storage.pttl(&key)?;
                Ok(RespValue::Integer(ttl_ms))
            }
            Command::DbSize => {
                let size = self.storage.dbsize()?;
                Ok(RespValue::Integer(size as i64))
            }
            Command::Info(section) => {
                let info = self.storage.info(section.as_deref())?;
                Ok(RespValue::BulkString(Some(Bytes::from(info))))
            }
            Command::Subscribe(_) | Command::Unsubscribe(_) | Command::PSubscribe(_) | Command::PUnsubscribe(_) => {
                // Pub/Sub 命令在 server.rs 中直接处理，不应到达此处
                Err(AppError::Command("pub/sub 命令应在连接层处理".to_string()))
            }
            Command::Publish(channel, message) => {
                // PUBLISH 需要 PubSubManager，但当前执行器未持有它。
                // 为保持兼容，返回提示性错误（实际在 server.rs 中处理）
                Err(AppError::Command(format!(
                    "PUBLISH 命令应在连接层处理 (channel={}, message={})",
                    channel,
                    String::from_utf8_lossy(&message),
                )))
            }
            Command::Multi | Command::Exec | Command::Discard | Command::Watch(_) => {
                // 事务命令在 server.rs 中直接处理，不应到达此处
                Err(AppError::Command("事务命令应在连接层处理".to_string()))
            }
            Command::Unknown(cmd_name) => {
                Ok(RespValue::Error(format!(
                    "ERR unknown command '{}'",
                    cmd_name,
                )))
            }
        }
    }
}

impl std::fmt::Debug for CommandExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandExecutor")
            .field("storage", &self.storage)
            .field("aof_enabled", &self.aof.is_some())
            .finish()
    }
}

impl Clone for CommandExecutor {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            aof: self.aof.clone(),
            script_engine: self.script_engine.clone(),
            slowlog: self.slowlog.clone(),
            acl: self.acl.clone(),
            latency: self.latency.clone(),
            keyspace_notifier: self.keyspace_notifier.clone(),
            aof_use_rdb_preamble: self.aof_use_rdb_preamble.clone(),
        }
    }
}
