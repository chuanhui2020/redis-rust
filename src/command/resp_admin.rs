use super::*;

use crate::protocol::RespValue;

/// 将 Command::Ping 序列化为 RESP 数组
///
/// 对应 Redis 命令: PING [message]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Ping 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Ping 变体，将触发 unreachable!()
pub(crate) fn to_resp_ping(cmd: &Command) -> RespValue {
    match cmd {
        Command::Ping(msg) => {
                let mut parts = vec![bulk("PING")];
                if let Some(m) = msg {
                    parts.push(bulk(m));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ConfigGet 序列化为 RESP 数组
///
/// 对应 Redis 命令: CONFIG GET key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ConfigGet 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ConfigGet 变体，将触发 unreachable!()
pub(crate) fn to_resp_config_get(cmd: &Command) -> RespValue {
    match cmd {
        Command::ConfigGet(key) => {
                RespValue::Array(vec![bulk("CONFIG"), bulk("GET"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ConfigSet 序列化为 RESP 数组
///
/// 对应 Redis 命令: CONFIG SET key value
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ConfigSet 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ConfigSet 变体，将触发 unreachable!()
pub(crate) fn to_resp_config_set(cmd: &Command) -> RespValue {
    match cmd {
        Command::ConfigSet(key, value) => {
                RespValue::Array(vec![bulk("CONFIG"), bulk("SET"), bulk(key), bulk(value)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ConfigRewrite 序列化为 RESP 数组
///
/// 对应 Redis 命令: 
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ConfigRewrite 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ConfigRewrite 变体，将触发 unreachable!()
pub(crate) fn to_resp_config_rewrite(cmd: &Command) -> RespValue {
    match cmd {
        Command::ConfigRewrite => {
                RespValue::Array(vec![bulk("CONFIG"), bulk("REWRITE")])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ConfigResetStat 序列化为 RESP 数组
///
/// 对应 Redis 命令: 
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ConfigResetStat 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ConfigResetStat 变体，将触发 unreachable!()
pub(crate) fn to_resp_config_reset_stat(cmd: &Command) -> RespValue {
    match cmd {
        Command::ConfigResetStat => {
                RespValue::Array(vec![bulk("CONFIG"), bulk("RESETSTAT")])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::MemoryUsage 序列化为 RESP 数组
///
/// 对应 Redis 命令: MEMORY USAGE key [SAMPLES count]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::MemoryUsage 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::MemoryUsage 变体，将触发 unreachable!()
pub(crate) fn to_resp_memory_usage(cmd: &Command) -> RespValue {
    match cmd {
        Command::MemoryUsage(key, samples) => {
                let mut parts = vec![bulk("MEMORY"), bulk("USAGE"), bulk(key)];
                if let Some(s) = samples {
                    parts.push(bulk("SAMPLES"));
                    parts.push(bulk(&s.to_string()));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::MemoryDoctor 序列化为 RESP 数组
///
/// 对应 Redis 命令: 
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::MemoryDoctor 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::MemoryDoctor 变体，将触发 unreachable!()
pub(crate) fn to_resp_memory_doctor(cmd: &Command) -> RespValue {
    match cmd {
        Command::MemoryDoctor => {
                RespValue::Array(vec![bulk("MEMORY"), bulk("DOCTOR")])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::LatencyLatest 序列化为 RESP 数组
///
/// 对应 Redis 命令: 
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::LatencyLatest 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::LatencyLatest 变体，将触发 unreachable!()
pub(crate) fn to_resp_latency_latest(cmd: &Command) -> RespValue {
    match cmd {
        Command::LatencyLatest => {
                RespValue::Array(vec![bulk("LATENCY"), bulk("LATEST")])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::LatencyHistory 序列化为 RESP 数组
///
/// 对应 Redis 命令: LATENCY HISTORY event-name
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::LatencyHistory 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::LatencyHistory 变体，将触发 unreachable!()
pub(crate) fn to_resp_latency_history(cmd: &Command) -> RespValue {
    match cmd {
        Command::LatencyHistory(event) => {
                RespValue::Array(vec![bulk("LATENCY"), bulk("HISTORY"), bulk(event)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::LatencyReset 序列化为 RESP 数组
///
/// 对应 Redis 命令: LATENCY RESET [event-name ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::LatencyReset 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::LatencyReset 变体，将触发 unreachable!()
pub(crate) fn to_resp_latency_reset(cmd: &Command) -> RespValue {
    match cmd {
        Command::LatencyReset(events) => {
                let mut parts = vec![bulk("LATENCY"), bulk("RESET")];
                for e in events {
                    parts.push(bulk(e));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::Reset 序列化为 RESP 数组
///
/// 对应 Redis 命令: 
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Reset 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Reset 变体，将触发 unreachable!()
pub(crate) fn to_resp_reset(cmd: &Command) -> RespValue {
    match cmd {
        Command::Reset => {
                RespValue::Array(vec![bulk("RESET")])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::Hello 序列化为 RESP 数组
///
/// 对应 Redis 命令: HELLO protover [AUTH username password] [SETNAME clientname]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Hello 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Hello 变体，将触发 unreachable!()
pub(crate) fn to_resp_hello(cmd: &Command) -> RespValue {
    match cmd {
        Command::Hello(protover, auth, setname) => {
                let mut parts = vec![bulk("HELLO"), bulk(&protover.to_string())];
                if let Some((user, pass)) = auth {
                    parts.push(bulk("AUTH"));
                    parts.push(bulk(user));
                    parts.push(bulk(pass));
                }
                if let Some(name) = setname {
                    parts.push(bulk("SETNAME"));
                    parts.push(bulk(name));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::Monitor 序列化为 RESP 数组
///
/// 对应 Redis 命令: 
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Monitor 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Monitor 变体，将触发 unreachable!()
pub(crate) fn to_resp_monitor(cmd: &Command) -> RespValue {
    match cmd {
        Command::Monitor => {
                RespValue::Array(vec![bulk("MONITOR")])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::CommandInfo 序列化为 RESP 数组
///
/// 对应 Redis 命令: 
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::CommandInfo 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::CommandInfo 变体，将触发 unreachable!()
pub(crate) fn to_resp_command_info(cmd: &Command) -> RespValue {
    match cmd {
        Command::CommandInfo => {
                RespValue::Array(vec![bulk("COMMAND")])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::CommandCount 序列化为 RESP 数组
///
/// 对应 Redis 命令: 
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::CommandCount 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::CommandCount 变体，将触发 unreachable!()
pub(crate) fn to_resp_command_count(cmd: &Command) -> RespValue {
    match cmd {
        Command::CommandCount => {
                RespValue::Array(vec![bulk("COMMAND"), bulk("COUNT")])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::CommandList 序列化为 RESP 数组
///
/// 对应 Redis 命令: COMMAND LIST [FILTERBY MODULE name | ACLCAT cat | PATTERN pattern]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::CommandList 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::CommandList 变体，将触发 unreachable!()
pub(crate) fn to_resp_command_list(cmd: &Command) -> RespValue {
    match cmd {
        Command::CommandList(filter) => {
                let mut parts = vec![bulk("COMMAND"), bulk("LIST")];
                if let Some(f) = filter {
                    parts.push(bulk(f));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::CommandDocs 序列化为 RESP 数组
///
/// 对应 Redis 命令: COMMAND DOCS [command-name ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::CommandDocs 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::CommandDocs 变体，将触发 unreachable!()
pub(crate) fn to_resp_command_docs(cmd: &Command) -> RespValue {
    match cmd {
        Command::CommandDocs(names) => {
                let mut parts = vec![bulk("COMMAND"), bulk("DOCS")];
                parts.extend(names.iter().map(|n| bulk(n)));
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::CommandGetKeys 序列化为 RESP 数组
///
/// 对应 Redis 命令: COMMAND GETKEYS command [arg ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::CommandGetKeys 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::CommandGetKeys 变体，将触发 unreachable!()
pub(crate) fn to_resp_command_get_keys(cmd: &Command) -> RespValue {
    match cmd {
        Command::CommandGetKeys(args) => {
                let mut parts = vec![bulk("COMMAND"), bulk("GETKEYS")];
                parts.extend(args.iter().map(|a| bulk(a)));
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::Keys 序列化为 RESP 数组
///
/// 对应 Redis 命令: KEYS pattern
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Keys 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Keys 变体，将触发 unreachable!()
pub(crate) fn to_resp_keys(cmd: &Command) -> RespValue {
    match cmd {
        Command::Keys(pattern) => {
                RespValue::Array(vec![bulk("KEYS"), bulk(pattern)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::Scan 序列化为 RESP 数组
///
/// 对应 Redis 命令: SCAN cursor [MATCH pattern] [COUNT count]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Scan 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Scan 变体，将触发 unreachable!()
pub(crate) fn to_resp_scan(cmd: &Command) -> RespValue {
    match cmd {
        Command::Scan(cursor, pattern, count) => {
                let mut parts = vec![bulk("SCAN"), bulk(&cursor.to_string())];
                if !pattern.is_empty() {
                    parts.push(bulk("MATCH"));
                    parts.push(bulk(pattern));
                }
                if *count > 0 {
                    parts.push(bulk("COUNT"));
                    parts.push(bulk(&count.to_string()));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::Rename 序列化为 RESP 数组
///
/// 对应 Redis 命令: RENAME key newkey
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Rename 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Rename 变体，将触发 unreachable!()
pub(crate) fn to_resp_rename(cmd: &Command) -> RespValue {
    match cmd {
        Command::Rename(key, newkey) => {
                RespValue::Array(vec![bulk("RENAME"), bulk(key), bulk(newkey)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::Type 序列化为 RESP 数组
///
/// 对应 Redis 命令: TYPE key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Type 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Type 变体，将触发 unreachable!()
pub(crate) fn to_resp_type(cmd: &Command) -> RespValue {
    match cmd {
        Command::Type(key) => {
                RespValue::Array(vec![bulk("TYPE"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::Persist 序列化为 RESP 数组
///
/// 对应 Redis 命令: PERSIST key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Persist 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Persist 变体，将触发 unreachable!()
pub(crate) fn to_resp_persist(cmd: &Command) -> RespValue {
    match cmd {
        Command::Persist(key) => {
                RespValue::Array(vec![bulk("PERSIST"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::PExpire 序列化为 RESP 数组
///
/// 对应 Redis 命令: PEXPIRE key milliseconds
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::PExpire 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::PExpire 变体，将触发 unreachable!()
pub(crate) fn to_resp_p_expire(cmd: &Command) -> RespValue {
    match cmd {
        Command::PExpire(key, ms) => {
                RespValue::Array(vec![bulk("PEXPIRE"), bulk(key), bulk(&ms.to_string())])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::PTtl 序列化为 RESP 数组
///
/// 对应 Redis 命令: PTTL key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::PTtl 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::PTtl 变体，将触发 unreachable!()
pub(crate) fn to_resp_p_ttl(cmd: &Command) -> RespValue {
    match cmd {
        Command::PTtl(key) => {
                RespValue::Array(vec![bulk("PTTL"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::Info 序列化为 RESP 数组
///
/// 对应 Redis 命令: INFO [section]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Info 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Info 变体，将触发 unreachable!()
pub(crate) fn to_resp_info(cmd: &Command) -> RespValue {
    match cmd {
        Command::Info(section) => {
                match section {
                    Some(s) => RespValue::Array(vec![bulk("INFO"), bulk(s)]),
                    None => RespValue::Array(vec![bulk("INFO")]),
                }
        }
        _ => unreachable!(),
    }
}

/// 将 Command::BgRewriteAof 序列化为 RESP 数组
///
/// 对应 Redis 命令: 
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::BgRewriteAof 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::BgRewriteAof 变体，将触发 unreachable!()
pub(crate) fn to_resp_bg_rewrite_aof(cmd: &Command) -> RespValue {
    match cmd {
        Command::BgRewriteAof => {
                RespValue::Array(vec![bulk("BGREWRITEAOF")])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::Select 序列化为 RESP 数组
///
/// 对应 Redis 命令: SELECT index
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Select 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Select 变体，将触发 unreachable!()
pub(crate) fn to_resp_select(cmd: &Command) -> RespValue {
    match cmd {
        Command::Select(index) => {
                RespValue::Array(vec![bulk("SELECT"), bulk(&index.to_string())])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::Auth 序列化为 RESP 数组
///
/// 对应 Redis 命令: AUTH [username] password
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Auth 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Auth 变体，将触发 unreachable!()
pub(crate) fn to_resp_auth(cmd: &Command) -> RespValue {
    match cmd {
        Command::Auth(username, password) => {
                if username == "default" {
                    RespValue::Array(vec![bulk("AUTH"), bulk(password)])
                } else {
                    RespValue::Array(vec![bulk("AUTH"), bulk(username), bulk(password)])
                }
        }
        _ => unreachable!(),
    }
}

/// 将 Command::Unlink 序列化为 RESP 数组
///
/// 对应 Redis 命令: UNLINK key [key ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Unlink 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Unlink 变体，将触发 unreachable!()
pub(crate) fn to_resp_unlink(cmd: &Command) -> RespValue {
    match cmd {
        Command::Unlink(keys) => {
                let mut parts = vec![bulk("UNLINK")];
                for k in keys {
                    parts.push(bulk(k));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::Dump 序列化为 RESP 数组
///
/// 对应 Redis 命令: DUMP key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Dump 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Dump 变体，将触发 unreachable!()
pub(crate) fn to_resp_dump(cmd: &Command) -> RespValue {
    match cmd {
        Command::Dump(key) => {
                RespValue::Array(vec![bulk("DUMP"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::Eval 序列化为 RESP 数组
///
/// 对应 Redis 命令: EVAL script numkeys key [key ...] arg [arg ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Eval 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Eval 变体，将触发 unreachable!()
pub(crate) fn to_resp_eval(cmd: &Command) -> RespValue {
    match cmd {
        Command::Eval(script, keys, args) => {
                let mut parts = vec![bulk("EVAL"), bulk(script)];
                parts.push(bulk(&keys.len().to_string()));
                for k in keys {
                    parts.push(bulk(k));
                }
                for a in args {
                    parts.push(bulk(a));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::EvalSha 序列化为 RESP 数组
///
/// 对应 Redis 命令: EVALSHA sha1 numkeys key [key ...] arg [arg ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::EvalSha 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::EvalSha 变体，将触发 unreachable!()
pub(crate) fn to_resp_eval_sha(cmd: &Command) -> RespValue {
    match cmd {
        Command::EvalSha(sha1, keys, args) => {
                let mut parts = vec![bulk("EVALSHA"), bulk(sha1)];
                parts.push(bulk(&keys.len().to_string()));
                for k in keys {
                    parts.push(bulk(k));
                }
                for a in args {
                    parts.push(bulk(a));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ScriptLoad 序列化为 RESP 数组
///
/// 对应 Redis 命令: SCRIPT LOAD script
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ScriptLoad 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ScriptLoad 变体，将触发 unreachable!()
pub(crate) fn to_resp_script_load(cmd: &Command) -> RespValue {
    match cmd {
        Command::ScriptLoad(script) => {
                RespValue::Array(vec![bulk("SCRIPT"), bulk("LOAD"), bulk(script)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ScriptExists 序列化为 RESP 数组
///
/// 对应 Redis 命令: SCRIPT EXISTS sha1 [sha1 ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ScriptExists 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ScriptExists 变体，将触发 unreachable!()
pub(crate) fn to_resp_script_exists(cmd: &Command) -> RespValue {
    match cmd {
        Command::ScriptExists(sha1s) => {
                let mut parts = vec![bulk("SCRIPT"), bulk("EXISTS")];
                for s in sha1s {
                    parts.push(bulk(s));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ScriptFlush 序列化为 RESP 数组
///
/// 对应 Redis 命令: 
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ScriptFlush 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ScriptFlush 变体，将触发 unreachable!()
pub(crate) fn to_resp_script_flush(cmd: &Command) -> RespValue {
    match cmd {
        Command::ScriptFlush => {
                RespValue::Array(vec![bulk("SCRIPT"), bulk("FLUSH")])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::FunctionLoad 序列化为 RESP 数组
///
/// 对应 Redis 命令: FUNCTION LOAD [REPLACE] function-code
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::FunctionLoad 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::FunctionLoad 变体，将触发 unreachable!()
pub(crate) fn to_resp_function_load(cmd: &Command) -> RespValue {
    match cmd {
        Command::FunctionLoad(code, replace) => {
                if *replace {
                    RespValue::Array(vec![bulk("FUNCTION"), bulk("LOAD"), bulk("REPLACE"), bulk(code)])
                } else {
                    RespValue::Array(vec![bulk("FUNCTION"), bulk("LOAD"), bulk(code)])
                }
        }
        _ => unreachable!(),
    }
}

/// 将 Command::FunctionDelete 序列化为 RESP 数组
///
/// 对应 Redis 命令: FUNCTION DELETE library-name
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::FunctionDelete 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::FunctionDelete 变体，将触发 unreachable!()
pub(crate) fn to_resp_function_delete(cmd: &Command) -> RespValue {
    match cmd {
        Command::FunctionDelete(lib) => {
                RespValue::Array(vec![bulk("FUNCTION"), bulk("DELETE"), bulk(lib)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::FunctionList 序列化为 RESP 数组
///
/// 对应 Redis 命令: FUNCTION LIST [LIBRARYNAME pattern] [WITHCODE]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::FunctionList 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::FunctionList 变体，将触发 unreachable!()
pub(crate) fn to_resp_function_list(cmd: &Command) -> RespValue {
    match cmd {
        Command::FunctionList(pattern, withcode) => {
                let mut parts = vec![bulk("FUNCTION"), bulk("LIST")];
                if let Some(p) = pattern {
                    parts.push(bulk("LIBRARYNAME"));
                    parts.push(bulk(p));
                }
                if *withcode {
                    parts.push(bulk("WITHCODE"));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::FunctionDump 序列化为 RESP 数组
///
/// 对应 Redis 命令: 
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::FunctionDump 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::FunctionDump 变体，将触发 unreachable!()
pub(crate) fn to_resp_function_dump(cmd: &Command) -> RespValue {
    match cmd {
        Command::FunctionDump => {
                RespValue::Array(vec![bulk("FUNCTION"), bulk("DUMP")])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::FunctionRestore 序列化为 RESP 数组
///
/// 对应 Redis 命令: FUNCTION RESTORE serialized-value policy
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::FunctionRestore 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::FunctionRestore 变体，将触发 unreachable!()
pub(crate) fn to_resp_function_restore(cmd: &Command) -> RespValue {
    match cmd {
        Command::FunctionRestore(data, policy) => {
                RespValue::Array(vec![
                    bulk("FUNCTION"), bulk("RESTORE"), bulk(data), bulk(policy),
                ])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::FunctionStats 序列化为 RESP 数组
///
/// 对应 Redis 命令: 
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::FunctionStats 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::FunctionStats 变体，将触发 unreachable!()
pub(crate) fn to_resp_function_stats(cmd: &Command) -> RespValue {
    match cmd {
        Command::FunctionStats => {
                RespValue::Array(vec![bulk("FUNCTION"), bulk("STATS")])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::FunctionFlush 序列化为 RESP 数组
///
/// 对应 Redis 命令: FUNCTION FLUSH [ASYNC|SYNC]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::FunctionFlush 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::FunctionFlush 变体，将触发 unreachable!()
pub(crate) fn to_resp_function_flush(cmd: &Command) -> RespValue {
    match cmd {
        Command::FunctionFlush(async_mode) => {
                if *async_mode {
                    RespValue::Array(vec![bulk("FUNCTION"), bulk("FLUSH"), bulk("ASYNC")])
                } else {
                    RespValue::Array(vec![bulk("FUNCTION"), bulk("FLUSH")])
                }
        }
        _ => unreachable!(),
    }
}

/// 将 Command::FCall 序列化为 RESP 数组
///
/// 对应 Redis 命令: FCALL function numkeys key [key ...] arg [arg ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::FCall 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::FCall 变体，将触发 unreachable!()
pub(crate) fn to_resp_f_call(cmd: &Command) -> RespValue {
    match cmd {
        Command::FCall(name, keys, args) => {
                let mut parts = vec![bulk("FCALL"), bulk(name), bulk(&keys.len().to_string())];
                for k in keys {
                    parts.push(bulk(k));
                }
                for a in args {
                    parts.push(bulk(a));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::FCallRO 序列化为 RESP 数组
///
/// 对应 Redis 命令: FCALL_RO function numkeys key [key ...] arg [arg ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::FCallRO 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::FCallRO 变体，将触发 unreachable!()
pub(crate) fn to_resp_f_call_r_o(cmd: &Command) -> RespValue {
    match cmd {
        Command::FCallRO(name, keys, args) => {
                let mut parts = vec![bulk("FCALL_RO"), bulk(name), bulk(&keys.len().to_string())];
                for k in keys {
                    parts.push(bulk(k));
                }
                for a in args {
                    parts.push(bulk(a));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::EvalRO 序列化为 RESP 数组
///
/// 对应 Redis 命令: EVAL_RO script numkeys key [key ...] arg [arg ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::EvalRO 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::EvalRO 变体，将触发 unreachable!()
pub(crate) fn to_resp_eval_r_o(cmd: &Command) -> RespValue {
    match cmd {
        Command::EvalRO(script, keys, args) => {
                let mut parts = vec![bulk("EVAL_RO"), bulk(script)];
                parts.push(bulk(&keys.len().to_string()));
                for k in keys {
                    parts.push(bulk(k));
                }
                for a in args {
                    parts.push(bulk(a));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::EvalShaRO 序列化为 RESP 数组
///
/// 对应 Redis 命令: EVALSHA_RO sha1 numkeys key [key ...] arg [arg ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::EvalShaRO 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::EvalShaRO 变体，将触发 unreachable!()
pub(crate) fn to_resp_eval_sha_r_o(cmd: &Command) -> RespValue {
    match cmd {
        Command::EvalShaRO(sha1, keys, args) => {
                let mut parts = vec![bulk("EVALSHA_RO"), bulk(sha1)];
                parts.push(bulk(&keys.len().to_string()));
                for k in keys {
                    parts.push(bulk(k));
                }
                for a in args {
                    parts.push(bulk(a));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::SlowLogGet 序列化为 RESP 数组
///
/// 对应 Redis 命令: SLOWLOG GET [count]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::SlowLogGet 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::SlowLogGet 变体，将触发 unreachable!()
pub(crate) fn to_resp_slow_log_get(cmd: &Command) -> RespValue {
    match cmd {
        Command::SlowLogGet(count) => {
                RespValue::Array(vec![bulk("SLOWLOG"), bulk("GET"), bulk(&count.to_string())])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::SlowLogLen 序列化为 RESP 数组
///
/// 对应 Redis 命令: 
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::SlowLogLen 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::SlowLogLen 变体，将触发 unreachable!()
pub(crate) fn to_resp_slow_log_len(cmd: &Command) -> RespValue {
    match cmd {
        Command::SlowLogLen => {
                RespValue::Array(vec![bulk("SLOWLOG"), bulk("LEN")])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::SlowLogReset 序列化为 RESP 数组
///
/// 对应 Redis 命令: 
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::SlowLogReset 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::SlowLogReset 变体，将触发 unreachable!()
pub(crate) fn to_resp_slow_log_reset(cmd: &Command) -> RespValue {
    match cmd {
        Command::SlowLogReset => {
                RespValue::Array(vec![bulk("SLOWLOG"), bulk("RESET")])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::DebugObject 序列化为 RESP 数组
///
/// 对应 Redis 命令: DEBUG OBJECT key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::DebugObject 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::DebugObject 变体，将触发 unreachable!()
pub(crate) fn to_resp_debug_object(cmd: &Command) -> RespValue {
    match cmd {
        Command::DebugObject(key) => {
                RespValue::Array(vec![bulk("DEBUG"), bulk("OBJECT"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::Touch 序列化为 RESP 数组
///
/// 对应 Redis 命令: TOUCH key [key ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Touch 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Touch 变体，将触发 unreachable!()
pub(crate) fn to_resp_touch(cmd: &Command) -> RespValue {
    match cmd {
        Command::Touch(keys) => {
                let mut parts = vec![bulk("TOUCH")];
                for k in keys {
                    parts.push(bulk(k));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ExpireAt 序列化为 RESP 数组
///
/// 对应 Redis 命令: EXPIREAT key timestamp
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ExpireAt 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ExpireAt 变体，将触发 unreachable!()
pub(crate) fn to_resp_expire_at(cmd: &Command) -> RespValue {
    match cmd {
        Command::ExpireAt(key, ts) => {
                RespValue::Array(vec![bulk("EXPIREAT"), bulk(key), bulk(&ts.to_string())])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::PExpireAt 序列化为 RESP 数组
///
/// 对应 Redis 命令: PEXPIREAT key ms-timestamp
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::PExpireAt 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::PExpireAt 变体，将触发 unreachable!()
pub(crate) fn to_resp_p_expire_at(cmd: &Command) -> RespValue {
    match cmd {
        Command::PExpireAt(key, ts) => {
                RespValue::Array(vec![bulk("PEXPIREAT"), bulk(key), bulk(&ts.to_string())])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ExpireTime 序列化为 RESP 数组
///
/// 对应 Redis 命令: EXPIRETIME key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ExpireTime 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ExpireTime 变体，将触发 unreachable!()
pub(crate) fn to_resp_expire_time(cmd: &Command) -> RespValue {
    match cmd {
        Command::ExpireTime(key) => {
                RespValue::Array(vec![bulk("EXPIRETIME"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::PExpireTime 序列化为 RESP 数组
///
/// 对应 Redis 命令: PEXPIRETIME key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::PExpireTime 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::PExpireTime 变体，将触发 unreachable!()
pub(crate) fn to_resp_p_expire_time(cmd: &Command) -> RespValue {
    match cmd {
        Command::PExpireTime(key) => {
                RespValue::Array(vec![bulk("PEXPIRETIME"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::RenameNx 序列化为 RESP 数组
///
/// 对应 Redis 命令: RENAMENX key newkey
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::RenameNx 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::RenameNx 变体，将触发 unreachable!()
pub(crate) fn to_resp_rename_nx(cmd: &Command) -> RespValue {
    match cmd {
        Command::RenameNx(key, newkey) => {
                RespValue::Array(vec![bulk("RENAMENX"), bulk(key), bulk(newkey)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::SwapDb 序列化为 RESP 数组
///
/// 对应 Redis 命令: SWAPDB index1 index2
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::SwapDb 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::SwapDb 变体，将触发 unreachable!()
pub(crate) fn to_resp_swap_db(cmd: &Command) -> RespValue {
    match cmd {
        Command::SwapDb(idx1, idx2) => {
                RespValue::Array(vec![bulk("SWAPDB"), bulk(&idx1.to_string()), bulk(&idx2.to_string())])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::FlushDb 序列化为 RESP 数组
///
/// 对应 Redis 命令: 
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::FlushDb 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::FlushDb 变体，将触发 unreachable!()
pub(crate) fn to_resp_flush_db(cmd: &Command) -> RespValue {
    match cmd {
        Command::FlushDb => {
                RespValue::Array(vec![bulk("FLUSHDB")])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::Shutdown 序列化为 RESP 数组
///
/// 对应 Redis 命令: SHUTDOWN [NOSAVE|SAVE]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Shutdown 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Shutdown 变体，将触发 unreachable!()
pub(crate) fn to_resp_shutdown(cmd: &Command) -> RespValue {
    match cmd {
        Command::Shutdown(opt) => {
                let mut parts = vec![bulk("SHUTDOWN")];
                if let Some(s) = opt {
                    parts.push(bulk(s));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

