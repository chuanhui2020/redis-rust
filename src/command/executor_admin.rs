//! Server/Admin 命令执行器
use super::*;

use super::executor::CommandExecutor;
use crate::error::Result;
use crate::protocol::RespValue;

/// 执行 PING 命令
///
/// Redis 语法: PING [message]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_ping(
    _executor: &CommandExecutor,
    message: Option<String>,
) -> Result<RespValue> {
    // PING 命令：有参数返回参数，否则返回 PONG
    let reply = message.unwrap_or_else(|| "PONG".to_string());
    Ok(RespValue::SimpleString(reply.into()))
}

/// 执行 CONFIG_GET 命令
///
/// Redis 语法: CONFIG GET key
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_config_get(executor: &CommandExecutor, key: String) -> Result<RespValue> {
    let key_lower = key.to_ascii_lowercase();
    if key_lower == "maxmemory" {
        let maxmem = executor.storage.get_maxmemory();
        Ok(RespValue::Array(vec![
            RespValue::BulkString(Some(bytes::Bytes::from(key))),
            RespValue::BulkString(Some(bytes::Bytes::from(maxmem.to_string()))),
        ]))
    } else if key_lower == "maxmemory-policy" {
        let policy = executor.storage.get_eviction_policy();
        let policy_str = format!("{:?}", policy).to_ascii_lowercase();
        Ok(RespValue::Array(vec![
            RespValue::BulkString(Some(bytes::Bytes::from(key))),
            RespValue::BulkString(Some(bytes::Bytes::from(policy_str))),
        ]))
    } else if key_lower == "notify-keyspace-events" {
        let raw = executor
            .keyspace_notifier
            .as_ref()
            .map(|n| n.config().raw().to_string())
            .unwrap_or_default();
        Ok(RespValue::Array(vec![
            RespValue::BulkString(Some(bytes::Bytes::from(key))),
            RespValue::BulkString(Some(bytes::Bytes::from(raw))),
        ]))
    } else if key_lower == "aof-use-rdb-preamble" {
        let val = if executor.aof_use_rdb_preamble.load(Ordering::Relaxed) {
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

/// 执行 CONFIG_SET 命令
///
/// Redis 语法: CONFIG SET key value
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_config_set(
    executor: &CommandExecutor,
    key: String,
    value: String,
) -> Result<RespValue> {
    let key_lower = key.to_ascii_lowercase();
    if key_lower == "maxmemory" {
        match value.parse::<u64>() {
            Ok(bytes) => {
                executor.storage.set_maxmemory(bytes);
                Ok(RespValue::SimpleString(bytes::Bytes::from_static(b"OK")))
            }
            Err(_) => Ok(RespValue::Error(
                bytes::Bytes::from("ERR value is not an integer or out of range"),
            )),
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
                return Ok(RespValue::Error(bytes::Bytes::from(format!(
                    "ERR Invalid maxmemory-policy: {}",
                    value
                ))));
            }
        };
        executor.storage.set_eviction_policy(policy);
        Ok(RespValue::SimpleString(bytes::Bytes::from_static(b"OK")))
    } else if key_lower == "notify-keyspace-events" {
        if let Some(ref notifier) = executor.keyspace_notifier {
            notifier.set_config(crate::keyspace::NotifyKeyspaceEvents::from_str(&value));
            Ok(RespValue::SimpleString(bytes::Bytes::from_static(b"OK")))
        } else {
            Ok(RespValue::Error(
                bytes::Bytes::from("ERR Keyspace notifier not initialized"),
            ))
        }
    } else if key_lower == "aof-use-rdb-preamble" {
        let enabled = match value.to_ascii_lowercase().as_str() {
            "yes" => true,
            "no" => false,
            _ => {
                return Ok(RespValue::Error(
                    bytes::Bytes::from("ERR Invalid argument 'yes' or 'no' expected"),
                ));
            }
        };
        executor
            .aof_use_rdb_preamble
            .store(enabled, Ordering::Relaxed);
        Ok(RespValue::SimpleString(bytes::Bytes::from_static(b"OK")))
    } else {
        Ok(RespValue::Error(bytes::Bytes::from(format!(
            "ERR Unsupported CONFIG parameter: {}",
            key
        ))))
    }
}

/// 执行 CONFIG_REWRITE 命令
///
/// Redis 语法:
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_config_rewrite(_executor: &CommandExecutor) -> Result<RespValue> {
    Ok(RespValue::SimpleString(bytes::Bytes::from_static(b"OK")))
}

/// 执行 CONFIG_RESET_STAT 命令
///
/// Redis 语法:
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_config_reset_stat(executor: &CommandExecutor) -> Result<RespValue> {
    if let Some(s) = executor.slowlog.as_ref() {
        s.reset()
    }
    Ok(RespValue::SimpleString(bytes::Bytes::from_static(b"OK")))
}

/// 执行 MEMORY_USAGE 命令
///
/// Redis 语法: MEMORY USAGE key [SAMPLES count]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_memory_usage(
    executor: &CommandExecutor,
    key: String,
    _samples: Option<usize>,
) -> Result<RespValue> {
    match executor.storage.memory_key_usage(&key, _samples)? {
        Some(size) => Ok(RespValue::Integer(size as i64)),
        None => Ok(RespValue::BulkString(None)),
    }
}

/// 执行 MEMORY_DOCTOR 命令
///
/// Redis 语法:
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_memory_doctor(executor: &CommandExecutor) -> Result<RespValue> {
    let info = executor.storage.memory_doctor()?;
    Ok(RespValue::BulkString(Some(bytes::Bytes::from(info))))
}

/// 执行 LATENCY_LATEST 命令
///
/// Redis 语法:
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_latency_latest(executor: &CommandExecutor) -> Result<RespValue> {
    let tracker = executor
        .latency
        .as_ref()
        .ok_or_else(|| AppError::Command("延迟追踪器未初始化".to_string()))?;
    let latest = tracker.latest()?;
    let parts: Vec<RespValue> = latest
        .into_iter()
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

/// 执行 LATENCY_HISTORY 命令
///
/// Redis 语法: LATENCY HISTORY event-name
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_latency_history(
    executor: &CommandExecutor,
    event: String,
) -> Result<RespValue> {
    let tracker = executor
        .latency
        .as_ref()
        .ok_or_else(|| AppError::Command("延迟追踪器未初始化".to_string()))?;
    let history = tracker.history(&event)?;
    let parts: Vec<RespValue> = history
        .into_iter()
        .map(|(ts, lat)| {
            RespValue::Array(vec![
                RespValue::Integer(ts as i64),
                RespValue::Integer(lat as i64),
            ])
        })
        .collect();
    Ok(RespValue::Array(parts))
}

/// 执行 LATENCY_RESET 命令
///
/// Redis 语法: LATENCY RESET [event-name ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_latency_reset(
    executor: &CommandExecutor,
    events: Vec<String>,
) -> Result<RespValue> {
    let tracker = executor
        .latency
        .as_ref()
        .ok_or_else(|| AppError::Command("延迟追踪器未初始化".to_string()))?;
    let count = if events.is_empty() {
        tracker.reset_all()?
    } else {
        let refs: Vec<&str> = events.iter().map(|s| s.as_str()).collect();
        tracker.reset(&refs)?
    };
    Ok(RespValue::Integer(count as i64))
}

/// 执行 RESET 命令
///
/// Redis 语法:
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_reset(_executor: &CommandExecutor) -> Result<RespValue> {
    Err(AppError::Command("RESET 应在连接层处理".to_string()))
}

/// 执行 HELLO 命令
///
/// Redis 语法: HELLO protover [AUTH username password] [SETNAME clientname]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_hello(
    _executor: &CommandExecutor,
    _protover: u8,
    _auth: Option<(String, String)>,
    _setname: Option<String>,
) -> Result<RespValue> {
    Err(AppError::Command("HELLO 应在连接层处理".to_string()))
}

/// 执行 MONITOR 命令
///
/// Redis 语法:
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_monitor(_executor: &CommandExecutor) -> Result<RespValue> {
    Err(AppError::Command("MONITOR 应在连接层处理".to_string()))
}

/// 执行 COMMAND_INFO 命令
///
/// Redis 语法:
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_command_info(_executor: &CommandExecutor) -> Result<RespValue> {
    Ok(RespValue::Array(vec![]))
}

/// 执行 COMMAND_COUNT 命令
///
/// Redis 语法:
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_command_count(_executor: &CommandExecutor) -> Result<RespValue> {
    Ok(RespValue::Integer(220))
}

/// 执行 COMMAND_LIST 命令
///
/// Redis 语法: COMMAND LIST [FILTERBY MODULE name | ACLCAT cat | PATTERN pattern]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_command_list(
    _executor: &CommandExecutor,
    _filter: Option<String>,
) -> Result<RespValue> {
    let commands = vec![
        "get",
        "set",
        "del",
        "exists",
        "expire",
        "ttl",
        "keys",
        "scan",
        "mget",
        "mset",
        "incr",
        "decr",
        "append",
        "strlen",
        "lpush",
        "rpush",
        "lpop",
        "rpop",
        "llen",
        "lrange",
        "lindex",
        "hset",
        "hget",
        "hdel",
        "hgetall",
        "hkeys",
        "hvals",
        "hlen",
        "sadd",
        "srem",
        "smembers",
        "scard",
        "sinter",
        "sunion",
        "sdiff",
        "zadd",
        "zrem",
        "zrange",
        "zrank",
        "zscore",
        "zcard",
        "ping",
        "echo",
        "info",
        "dbsize",
        "flushall",
        "flushdb",
        "select",
        "auth",
        "multi",
        "exec",
        "discard",
        "watch",
        "unwatch",
        "subscribe",
        "unsubscribe",
        "publish",
        "psubscribe",
        "punsubscribe",
        "ssubscribe",
        "sunsubscribe",
        "spublish",
        "eval",
        "evalsha",
        "script",
        "role",
        "replicaof",
        "slaveof",
        "replconf",
        "psync",
        "acl",
        "client",
        "config",
        "command",
        "type",
        "rename",
        "persist",
        "pexpire",
        "pttl",
        "expireat",
        "pexpireat",
        "sort",
        "object",
        "debug",
        "save",
        "bgsave",
        "bgrewriteaof",
        "shutdown",
        "lastsave",
        "time",
        "randomkey",
        "copy",
        "dump",
        "restore",
        "touch",
        "unlink",
        "wait",
        "setnx",
        "setex",
        "psetex",
        "getset",
        "getdel",
        "getex",
        "msetnx",
        "incrby",
        "decrby",
        "incrbyfloat",
        "setrange",
        "getrange",
        "lset",
        "linsert",
        "lrem",
        "ltrim",
        "lpos",
        "blpop",
        "brpop",
        "hexists",
        "hmset",
        "hmget",
        "hincrby",
        "hincrbyfloat",
        "hsetnx",
        "hrandfield",
        "hscan",
        "hexpire",
        "hpexpire",
        "hexpireat",
        "hpexpireat",
        "httl",
        "hpttl",
        "hexpiretime",
        "hpexpiretime",
        "hpersist",
        "sismember",
        "spop",
        "srandmember",
        "smove",
        "sinterstore",
        "sunionstore",
        "sdiffstore",
        "sscan",
        "zcount",
        "zrangebyscore",
        "zrevrange",
        "zrevrank",
        "zincrby",
        "zpopmin",
        "zpopmax",
        "zunionstore",
        "zinterstore",
        "zscan",
        "zrangebylex",
        "zlexcount",
        "zmscore",
        "zdiff",
        "zdiffstore",
        "zinter",
        "zunion",
        "zrangestore",
        "zmpop",
        "zrevrangebyscore",
        "zrevrangebylex",
        "sintercard",
        "smismember",
        "zrandmember",
        "bzpopmin",
        "bzpopmax",
        "bzmpop",
        "setbit",
        "getbit",
        "bitcount",
        "bitop",
        "bitpos",
        "bitfield",
        "bitfield_ro",
        "xadd",
        "xlen",
        "xrange",
        "xrevrange",
        "xtrim",
        "xdel",
        "xread",
        "xsetid",
        "xgroup",
        "xreadgroup",
        "xack",
        "xclaim",
        "xautoclaim",
        "xpending",
        "xinfo",
        "pfadd",
        "pfcount",
        "pfmerge",
        "geoadd",
        "geodist",
        "geohash",
        "geopos",
        "geosearch",
        "geosearchstore",
        "function",
        "fcall",
        "fcall_ro",
        "eval_ro",
        "evalsha_ro",
        "slowlog",
        "memory",
        "latency",
        "hello",
        "monitor",
        "reset",
        "quit",
        "replconf",
        "role",
        "swapdb",
        "renamenx",
        "substr",
        "lcs",
        "lmove",
        "rpoplpush",
        "lmpop",
        "blmove",
        "blmpop",
        "brpoplpush",
    ];
    let arr: Vec<RespValue> = commands
        .iter()
        .map(|c| RespValue::BulkString(Some(Bytes::from(*c))))
        .collect();
    Ok(RespValue::Array(arr))
}

/// 执行 COMMAND_DOCS 命令
///
/// Redis 语法: COMMAND DOCS [command-name ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_command_docs(
    _executor: &CommandExecutor,
    _names: Vec<String>,
) -> Result<RespValue> {
    Ok(RespValue::Array(vec![]))
}

/// 执行 COMMAND_GET_KEYS 命令
///
/// Redis 语法: COMMAND GETKEYS command [arg ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_command_get_keys(
    _executor: &CommandExecutor,
    args: Vec<String>,
) -> Result<RespValue> {
    if args.is_empty() {
        return Err(AppError::Command(
            "COMMAND GETKEYS 需要命令参数".to_string(),
        ));
    }
    // 简化实现：返回命令后的所有参数作为候选 key
    let keys: Vec<RespValue> = args[1..]
        .iter()
        .map(|k| RespValue::BulkString(Some(Bytes::from(k.clone()))))
        .collect();
    Ok(RespValue::Array(keys))
}

/// 执行 BG_REWRITE_AOF 命令
///
/// Redis 语法:
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_bg_rewrite_aof(_executor: &CommandExecutor) -> Result<RespValue> {
    // BGREWRITEAOF 在 server.rs 中处理，需要 AOF writer
    Err(AppError::Command("BGREWRITEAOF 应在连接层处理".to_string()))
}

/// 执行 SELECT 命令
///
/// Redis 语法: SELECT index
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_select(executor: &CommandExecutor, index: usize) -> Result<RespValue> {
    executor.storage.select(index)?;
    Ok(RespValue::SimpleString(bytes::Bytes::from_static(b"OK")))
}

/// 执行 EVAL 命令
///
/// Redis 语法: EVAL script numkeys key [key ...] arg [arg ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_eval(
    executor: &CommandExecutor,
    script: String,
    keys: Vec<String>,
    args: Vec<String>,
) -> Result<RespValue> {
    match &executor.script_engine {
        Some(engine) => engine.eval(&script, keys, args, executor.storage.clone()),
        None => Err(AppError::Command("脚本引擎未初始化".to_string())),
    }
}

/// 执行 EVAL_SHA 命令
///
/// Redis 语法: EVALSHA sha1 numkeys key [key ...] arg [arg ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_eval_sha(
    executor: &CommandExecutor,
    sha1: String,
    keys: Vec<String>,
    args: Vec<String>,
) -> Result<RespValue> {
    match &executor.script_engine {
        Some(engine) => engine.evalsha(&sha1, keys, args, executor.storage.clone()),
        None => Err(AppError::Command("脚本引擎未初始化".to_string())),
    }
}

/// 执行 SCRIPT_LOAD 命令
///
/// Redis 语法: SCRIPT LOAD script
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_script_load(executor: &CommandExecutor, script: String) -> Result<RespValue> {
    match &executor.script_engine {
        Some(engine) => {
            let sha1 = engine.script_load(&script)?;
            Ok(RespValue::BulkString(Some(Bytes::from(sha1))))
        }
        None => Err(AppError::Command("脚本引擎未初始化".to_string())),
    }
}

/// 执行 SCRIPT_EXISTS 命令
///
/// Redis 语法: SCRIPT EXISTS sha1 [sha1 ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_script_exists(
    executor: &CommandExecutor,
    sha1s: Vec<String>,
) -> Result<RespValue> {
    match &executor.script_engine {
        Some(engine) => {
            let exists = engine.script_exists(&sha1s)?;
            let arr: Vec<RespValue> = exists
                .into_iter()
                .map(|b| RespValue::Integer(if b { 1 } else { 0 }))
                .collect();
            Ok(RespValue::Array(arr))
        }
        None => Err(AppError::Command("脚本引擎未初始化".to_string())),
    }
}

/// 执行 SCRIPT_FLUSH 命令
///
/// Redis 语法:
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_script_flush(executor: &CommandExecutor) -> Result<RespValue> {
    match &executor.script_engine {
        Some(engine) => {
            engine.script_flush()?;
            Ok(RespValue::SimpleString(bytes::Bytes::from_static(b"OK")))
        }
        None => Err(AppError::Command("脚本引擎未初始化".to_string())),
    }
}

/// 执行 FUNCTION_LOAD 命令
///
/// Redis 语法: FUNCTION LOAD [REPLACE] function-code
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_function_load(
    executor: &CommandExecutor,
    code: String,
    replace: bool,
) -> Result<RespValue> {
    match &executor.script_engine {
        Some(engine) => {
            let name = engine.function_load(&code, replace)?;
            Ok(RespValue::SimpleString(name.into()))
        }
        None => Err(AppError::Command("脚本引擎未初始化".to_string())),
    }
}

/// 执行 FUNCTION_DELETE 命令
///
/// Redis 语法: FUNCTION DELETE library-name
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_function_delete(
    executor: &CommandExecutor,
    lib: String,
) -> Result<RespValue> {
    match &executor.script_engine {
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

/// 执行 FUNCTION_LIST 命令
///
/// Redis 语法: FUNCTION LIST [LIBRARYNAME pattern] [WITHCODE]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_function_list(
    executor: &CommandExecutor,
    pattern: Option<String>,
    withcode: bool,
) -> Result<RespValue> {
    match &executor.script_engine {
        Some(engine) => {
            let list = engine.function_list(pattern.as_deref(), withcode)?;
            let mut parts = Vec::new();
            for (name, engine_name, code, funcs) in list {
                let mut lib_parts = vec![
                    RespValue::BulkString(Some(Bytes::from("library_name"))),
                    RespValue::BulkString(Some(Bytes::from(name))),
                    RespValue::BulkString(Some(Bytes::from("engine"))),
                    RespValue::BulkString(Some(Bytes::from(engine_name))),
                ];
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
                    let flag_parts: Vec<RespValue> = flags
                        .into_iter()
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

/// 执行 FUNCTION_DUMP 命令
///
/// Redis 语法:
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_function_dump(executor: &CommandExecutor) -> Result<RespValue> {
    match &executor.script_engine {
        Some(engine) => {
            let dump = engine.function_dump()?;
            Ok(RespValue::BulkString(Some(Bytes::from(dump))))
        }
        None => Err(AppError::Command("脚本引擎未初始化".to_string())),
    }
}

/// 执行 FUNCTION_RESTORE 命令
///
/// Redis 语法: FUNCTION RESTORE serialized-value policy
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_function_restore(
    executor: &CommandExecutor,
    data: String,
    policy: String,
) -> Result<RespValue> {
    match &executor.script_engine {
        Some(engine) => {
            engine.function_restore(&data, &policy)?;
            Ok(RespValue::SimpleString(bytes::Bytes::from_static(b"OK")))
        }
        None => Err(AppError::Command("脚本引擎未初始化".to_string())),
    }
}

/// 执行 FUNCTION_STATS 命令
///
/// Redis 语法:
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_function_stats(executor: &CommandExecutor) -> Result<RespValue> {
    match &executor.script_engine {
        Some(engine) => {
            let (libs, funcs) = engine.function_stats()?;
            let parts = vec![
                RespValue::BulkString(Some(Bytes::from("libraries_count"))),
                RespValue::Integer(libs as i64),
                RespValue::BulkString(Some(Bytes::from("functions_count"))),
                RespValue::Integer(funcs as i64),
            ];
            Ok(RespValue::Array(parts))
        }
        None => Err(AppError::Command("脚本引擎未初始化".to_string())),
    }
}

/// 执行 FUNCTION_FLUSH 命令
///
/// Redis 语法: FUNCTION FLUSH [ASYNC|SYNC]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_function_flush(
    executor: &CommandExecutor,
    async_mode: bool,
) -> Result<RespValue> {
    match &executor.script_engine {
        Some(engine) => {
            engine.function_flush(async_mode)?;
            Ok(RespValue::SimpleString(bytes::Bytes::from_static(b"OK")))
        }
        None => Err(AppError::Command("脚本引擎未初始化".to_string())),
    }
}

/// 执行 F_CALL 命令
///
/// Redis 语法: FCALL function numkeys key [key ...] arg [arg ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_f_call(
    executor: &CommandExecutor,
    name: String,
    keys: Vec<String>,
    args: Vec<String>,
) -> Result<RespValue> {
    match &executor.script_engine {
        Some(engine) => {
            let resp = engine.fcall(&name, keys.clone(), args.clone(), executor.storage.clone())?;
            Ok(resp)
        }
        None => Err(AppError::Command("脚本引擎未初始化".to_string())),
    }
}

/// 执行 F_CALL_RO 命令
///
/// Redis 语法: FCALL_RO function numkeys key [key ...] arg [arg ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_f_call_r_o(
    executor: &CommandExecutor,
    name: String,
    keys: Vec<String>,
    args: Vec<String>,
) -> Result<RespValue> {
    match &executor.script_engine {
        Some(engine) => {
            let resp =
                engine.fcall_ro(&name, keys.clone(), args.clone(), executor.storage.clone())?;
            Ok(resp)
        }
        None => Err(AppError::Command("脚本引擎未初始化".to_string())),
    }
}

/// 执行 EVAL_RO 命令
///
/// Redis 语法: EVAL_RO script numkeys key [key ...] arg [arg ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_eval_r_o(
    executor: &CommandExecutor,
    script: String,
    keys: Vec<String>,
    args: Vec<String>,
) -> Result<RespValue> {
    match &executor.script_engine {
        Some(engine) => {
            let resp = engine.eval(
                &script,
                keys.clone(),
                args.clone(),
                executor.storage.clone(),
            )?;
            Ok(resp)
        }
        None => Err(AppError::Command("脚本引擎未初始化".to_string())),
    }
}

/// 执行 EVAL_SHA_RO 命令
///
/// Redis 语法: EVALSHA_RO sha1 numkeys key [key ...] arg [arg ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_eval_sha_r_o(
    executor: &CommandExecutor,
    sha1: String,
    keys: Vec<String>,
    args: Vec<String>,
) -> Result<RespValue> {
    match &executor.script_engine {
        Some(engine) => {
            let resp =
                engine.evalsha(&sha1, keys.clone(), args.clone(), executor.storage.clone())?;
            Ok(resp)
        }
        None => Err(AppError::Command("脚本引擎未初始化".to_string())),
    }
}

/// 执行 SLOW_LOG_GET 命令
///
/// Redis 语法: SLOWLOG GET [count]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_slow_log_get(executor: &CommandExecutor, count: usize) -> Result<RespValue> {
    match &executor.slowlog {
        Some(log) => {
            let entries = log.get(count);
            let arr: Vec<RespValue> = entries.iter().map(SlowLog::entry_to_resp).collect();
            Ok(RespValue::Array(arr))
        }
        None => Ok(RespValue::Array(vec![])),
    }
}

/// 执行 SLOW_LOG_LEN 命令
///
/// Redis 语法:
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_slow_log_len(executor: &CommandExecutor) -> Result<RespValue> {
    match &executor.slowlog {
        Some(log) => Ok(RespValue::Integer(log.len() as i64)),
        None => Ok(RespValue::Integer(0)),
    }
}

/// 执行 SLOW_LOG_RESET 命令
///
/// Redis 语法:
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_slow_log_reset(executor: &CommandExecutor) -> Result<RespValue> {
    match &executor.slowlog {
        Some(log) => {
            log.reset();
            Ok(RespValue::SimpleString(bytes::Bytes::from_static(b"OK")))
        }
        None => Ok(RespValue::SimpleString(bytes::Bytes::from_static(b"OK"))),
    }
}

/// 执行 DEBUG_OBJECT 命令
///
/// Redis 语法: DEBUG OBJECT key
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_debug_object(executor: &CommandExecutor, key: String) -> Result<RespValue> {
    let info = executor.build_debug_object_info(&key)?;
    Ok(RespValue::SimpleString(info.into()))
}

/// 执行 TOUCH 命令
///
/// Redis 语法: TOUCH key [key ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_touch(executor: &CommandExecutor, keys: Vec<String>) -> Result<RespValue> {
    let count = executor.storage.touch_keys(&keys)?;
    Ok(RespValue::Integer(count as i64))
}

/// 执行 EXPIRE_AT 命令
///
/// Redis 语法: EXPIREAT key timestamp
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_expire_at(
    executor: &CommandExecutor,
    key: String,
    timestamp: u64,
) -> Result<RespValue> {
    let ok = executor.storage.expire_at(&key, timestamp)?;
    Ok(RespValue::Integer(if ok { 1 } else { 0 }))
}

/// 执行 P_EXPIRE_AT 命令
///
/// Redis 语法: PEXPIREAT key ms-timestamp
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_p_expire_at(
    executor: &CommandExecutor,
    key: String,
    timestamp: u64,
) -> Result<RespValue> {
    let ok = executor.storage.pexpire_at(&key, timestamp)?;
    Ok(RespValue::Integer(if ok { 1 } else { 0 }))
}

/// 执行 EXPIRE_TIME 命令
///
/// Redis 语法: EXPIRETIME key
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_expire_time(executor: &CommandExecutor, key: String) -> Result<RespValue> {
    Ok(RespValue::Integer(executor.storage.expire_time(&key)?))
}

/// 执行 P_EXPIRE_TIME 命令
///
/// Redis 语法: PEXPIRETIME key
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_p_expire_time(executor: &CommandExecutor, key: String) -> Result<RespValue> {
    Ok(RespValue::Integer(executor.storage.pexpire_time(&key)?))
}

/// 执行 RENAME_NX 命令
///
/// Redis 语法: RENAMENX key newkey
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_rename_nx(
    executor: &CommandExecutor,
    key: String,
    newkey: String,
) -> Result<RespValue> {
    let ok = executor.storage.renamenx(&key, &newkey)?;
    Ok(RespValue::Integer(if ok { 1 } else { 0 }))
}

/// 执行 SWAP_DB 命令
///
/// Redis 语法: SWAPDB index1 index2
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_swap_db(
    executor: &CommandExecutor,
    idx1: usize,
    idx2: usize,
) -> Result<RespValue> {
    executor.storage.swap_db(idx1, idx2)?;
    Ok(RespValue::SimpleString(bytes::Bytes::from_static(b"OK")))
}

/// 执行 FLUSH_DB 命令
///
/// Redis 语法:
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_flush_db(executor: &CommandExecutor) -> Result<RespValue> {
    executor.storage.flush_db(executor.storage.current_db())?;
    Ok(RespValue::SimpleString(bytes::Bytes::from_static(b"OK")))
}

/// 执行 SORT 命令
///
/// Redis 语法: SORT key [BY pattern] [LIMIT offset count] [GET pattern ...] [ASC|DESC] [ALPHA] [STORE destination]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_sort(
    executor: &CommandExecutor,
    key: String,
    by_pattern: Option<String>,
    get_patterns: Vec<String>,
    limit_offset: Option<isize>,
    limit_count: Option<isize>,
    asc: bool,
    alpha: bool,
    store_key: Option<String>,
) -> Result<RespValue> {
    let result = executor.storage.sort(
        &key,
        by_pattern,
        get_patterns,
        limit_offset,
        limit_count,
        asc,
        alpha,
        store_key.clone(),
    )?;
    if let Some(dest) = store_key {
        // 有 STORE 时返回存入的元素数量
        let count = executor.storage.llen(&dest)?;
        Ok(RespValue::Integer(count as i64))
    } else {
        let resp_values: Vec<RespValue> = result
            .into_iter()
            .map(|s| RespValue::BulkString(Some(Bytes::from(s))))
            .collect();
        Ok(RespValue::Array(resp_values))
    }
}

/// 执行 SORT_RO 命令
///
/// Redis 语法: SORT_RO key [BY pattern] [LIMIT offset count] [GET pattern ...] [ASC|DESC] [ALPHA]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_sort_ro(
    executor: &CommandExecutor,
    key: String,
    by_pattern: Option<String>,
    get_patterns: Vec<String>,
    limit_offset: Option<isize>,
    limit_count: Option<isize>,
    asc: bool,
    alpha: bool,
) -> Result<RespValue> {
    // SORT_RO 是只读 SORT，不允许 STORE
    let result = executor.storage.sort(
        &key,
        by_pattern,
        get_patterns,
        limit_offset,
        limit_count,
        asc,
        alpha,
        None,
    )?;
    let resp_values: Vec<RespValue> = result
        .into_iter()
        .map(|s| RespValue::BulkString(Some(Bytes::from(s))))
        .collect();
    Ok(RespValue::Array(resp_values))
}

/// 执行 MOVE 命令
///
/// Redis 语法: MOVE key db
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_move(
    executor: &CommandExecutor,
    key: String,
    db: usize,
) -> Result<RespValue> {
    let ok = executor.storage.move_key(&key, db)?;
    Ok(RespValue::Integer(if ok { 1 } else { 0 }))
}

/// 执行 UNLINK 命令
///
/// Redis 语法: UNLINK key [key ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_unlink(executor: &CommandExecutor, keys: Vec<String>) -> Result<RespValue> {
    let count = executor.storage.unlink(&keys)?;
    Ok(RespValue::Integer(count as i64))
}

/// 执行 COPY 命令
///
/// Redis 语法: COPY source destination [REPLACE]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_copy(
    executor: &CommandExecutor,
    source: String,
    destination: String,
    replace: bool,
) -> Result<RespValue> {
    let ok = executor.storage.copy(&source, &destination, replace)?;
    Ok(RespValue::Integer(if ok { 1 } else { 0 }))
}

/// 执行 DUMP 命令
///
/// Redis 语法: DUMP key
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_dump(executor: &CommandExecutor, key: String) -> Result<RespValue> {
    match executor.storage.dump(&key)? {
        Some(data) => Ok(RespValue::BulkString(Some(Bytes::from(data)))),
        None => Ok(RespValue::BulkString(None)),
    }
}

/// 执行 RESTORE 命令
///
/// Redis 语法: RESTORE key ttl serialized-value [REPLACE]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_restore(
    executor: &CommandExecutor,
    key: String,
    ttl_ms: u64,
    serialized: Vec<u8>,
    replace: bool,
) -> Result<RespValue> {
    executor
        .storage
        .restore(&key, ttl_ms, &serialized, replace)?;
    Ok(RespValue::SimpleString(bytes::Bytes::from_static(b"OK")))
}

/// 执行 KEYS 命令
///
/// Redis 语法: KEYS pattern
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_keys(executor: &CommandExecutor, pattern: String) -> Result<RespValue> {
    let keys = executor.storage.keys(&pattern)?;
    let resp_values: Vec<RespValue> = keys
        .into_iter()
        .map(|k| RespValue::BulkString(Some(Bytes::from(k))))
        .collect();
    Ok(RespValue::Array(resp_values))
}

/// 执行 SCAN 命令
///
/// Redis 语法: SCAN cursor [MATCH pattern] [COUNT count]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_scan(
    executor: &CommandExecutor,
    cursor: usize,
    pattern: String,
    count: usize,
) -> Result<RespValue> {
    let (next_cursor, keys) = executor.storage.scan(cursor, &pattern, count)?;
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

/// 执行 RENAME 命令
///
/// Redis 语法: RENAME key newkey
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_rename(
    executor: &CommandExecutor,
    key: String,
    newkey: String,
) -> Result<RespValue> {
    executor.storage.rename(&key, &newkey)?;
    Ok(RespValue::SimpleString(bytes::Bytes::from_static(b"OK")))
}

/// 执行 TYPE 命令
///
/// Redis 语法: TYPE key
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_type(executor: &CommandExecutor, key: String) -> Result<RespValue> {
    let key_type = executor.storage.key_type(&key)?;
    Ok(RespValue::SimpleString(key_type.into()))
}

/// 执行 PERSIST 命令
///
/// Redis 语法: PERSIST key
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_persist(executor: &CommandExecutor, key: String) -> Result<RespValue> {
    let removed = executor.storage.persist(&key)?;
    Ok(RespValue::Integer(if removed { 1 } else { 0 }))
}

/// 执行 P_EXPIRE 命令
///
/// Redis 语法: PEXPIRE key milliseconds
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_p_expire(
    executor: &CommandExecutor,
    key: String,
    ms: u64,
) -> Result<RespValue> {
    let success = executor.storage.pexpire(&key, ms)?;
    Ok(RespValue::Integer(if success { 1 } else { 0 }))
}

/// 执行 P_TTL 命令
///
/// Redis 语法: PTTL key
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_p_ttl(executor: &CommandExecutor, key: String) -> Result<RespValue> {
    let ttl_ms = executor.storage.pttl(&key)?;
    Ok(RespValue::Integer(ttl_ms))
}

/// 执行 INFO 命令
///
/// Redis 语法: INFO [section]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_info(
    executor: &CommandExecutor,
    section: Option<String>,
) -> Result<RespValue> {
    let mut info = executor.storage.info(section.as_deref())?;
    let sec = section.as_deref().map(|s| s.to_ascii_lowercase());
    let include_repl = sec.is_none() || sec.as_deref() == Some("replication");
    if include_repl && let Some(ref repl) = executor.replication() {
        info.push_str("\r\n# Replication\r\n");
        info.push_str(&repl.get_info_string());
    }
    Ok(RespValue::BulkString(Some(Bytes::from(info))))
}
