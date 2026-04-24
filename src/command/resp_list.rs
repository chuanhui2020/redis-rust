use super::*;

use crate::protocol::RespValue;

/// 将 Command::LPush 序列化为 RESP 数组
///
/// 对应 Redis 命令: LPUSH key value [value ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::LPush 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::LPush 变体，将触发 unreachable!()
pub(crate) fn to_resp_l_push(cmd: &Command) -> RespValue {
    match cmd {
        Command::LPush(key, values) => {
                let mut parts = vec![bulk("LPUSH"), bulk(key)];
                for v in values {
                    parts.push(bulk_bytes(v));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::RPush 序列化为 RESP 数组
///
/// 对应 Redis 命令: RPUSH key value [value ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::RPush 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::RPush 变体，将触发 unreachable!()
pub(crate) fn to_resp_r_push(cmd: &Command) -> RespValue {
    match cmd {
        Command::RPush(key, values) => {
                let mut parts = vec![bulk("RPUSH"), bulk(key)];
                for v in values {
                    parts.push(bulk_bytes(v));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::LPushX 序列化为 RESP 数组
///
/// 对应 Redis 命令: LPUSHX key value [value ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::LPushX 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::LPushX 变体，将触发 unreachable!()
pub(crate) fn to_resp_l_push_x(cmd: &Command) -> RespValue {
    match cmd {
        Command::LPushX(key, values) => {
                let mut parts = vec![bulk("LPUSHX"), bulk(key)];
                for v in values {
                    parts.push(bulk_bytes(v));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::RPushX 序列化为 RESP 数组
///
/// 对应 Redis 命令: RPUSHX key value [value ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::RPushX 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::RPushX 变体，将触发 unreachable!()
pub(crate) fn to_resp_r_push_x(cmd: &Command) -> RespValue {
    match cmd {
        Command::RPushX(key, values) => {
                let mut parts = vec![bulk("RPUSHX"), bulk(key)];
                for v in values {
                    parts.push(bulk_bytes(v));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::LPop 序列化为 RESP 数组
///
/// 对应 Redis 命令: LPOP key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::LPop 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::LPop 变体，将触发 unreachable!()
pub(crate) fn to_resp_l_pop(cmd: &Command) -> RespValue {
    match cmd {
        Command::LPop(key) => {
                RespValue::Array(vec![bulk("LPOP"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::RPop 序列化为 RESP 数组
///
/// 对应 Redis 命令: RPOP key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::RPop 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::RPop 变体，将触发 unreachable!()
pub(crate) fn to_resp_r_pop(cmd: &Command) -> RespValue {
    match cmd {
        Command::RPop(key) => {
                RespValue::Array(vec![bulk("RPOP"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::LLen 序列化为 RESP 数组
///
/// 对应 Redis 命令: LLEN key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::LLen 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::LLen 变体，将触发 unreachable!()
pub(crate) fn to_resp_l_len(cmd: &Command) -> RespValue {
    match cmd {
        Command::LLen(key) => {
                RespValue::Array(vec![bulk("LLEN"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::LRange 序列化为 RESP 数组
///
/// 对应 Redis 命令: LRANGE key start stop
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::LRange 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::LRange 变体，将触发 unreachable!()
pub(crate) fn to_resp_l_range(cmd: &Command) -> RespValue {
    match cmd {
        Command::LRange(key, start, stop) => {
                RespValue::Array(vec![
                    bulk("LRANGE"),
                    bulk(key),
                    bulk(&start.to_string()),
                    bulk(&stop.to_string()),
                ])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::LIndex 序列化为 RESP 数组
///
/// 对应 Redis 命令: LINDEX key index
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::LIndex 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::LIndex 变体，将触发 unreachable!()
pub(crate) fn to_resp_l_index(cmd: &Command) -> RespValue {
    match cmd {
        Command::LIndex(key, index) => {
                RespValue::Array(vec![
                    bulk("LINDEX"),
                    bulk(key),
                    bulk(&index.to_string()),
                ])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::LSet 序列化为 RESP 数组
///
/// 对应 Redis 命令: LSET key index value
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::LSet 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::LSet 变体，将触发 unreachable!()
pub(crate) fn to_resp_l_set(cmd: &Command) -> RespValue {
    match cmd {
        Command::LSet(key, index, value) => {
                RespValue::Array(vec![
                    bulk("LSET"),
                    bulk(key),
                    bulk(&index.to_string()),
                    bulk_bytes(value),
                ])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::LInsert 序列化为 RESP 数组
///
/// 对应 Redis 命令: LINSERT key BEFORE|AFTER pivot value
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::LInsert 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::LInsert 变体，将触发 unreachable!()
pub(crate) fn to_resp_l_insert(cmd: &Command) -> RespValue {
    match cmd {
        Command::LInsert(key, pos, pivot, value) => {
                let pos_str = match pos {
                    crate::storage::LInsertPosition::Before => "BEFORE",
                    crate::storage::LInsertPosition::After => "AFTER",
                };
                RespValue::Array(vec![
                    bulk("LINSERT"),
                    bulk(key),
                    bulk(pos_str),
                    bulk_bytes(pivot),
                    bulk_bytes(value),
                ])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::LRem 序列化为 RESP 数组
///
/// 对应 Redis 命令: LREM key count value
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::LRem 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::LRem 变体，将触发 unreachable!()
pub(crate) fn to_resp_l_rem(cmd: &Command) -> RespValue {
    match cmd {
        Command::LRem(key, count, value) => {
                RespValue::Array(vec![
                    bulk("LREM"),
                    bulk(key),
                    bulk(&count.to_string()),
                    bulk_bytes(value),
                ])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::LTrim 序列化为 RESP 数组
///
/// 对应 Redis 命令: LTRIM key start stop
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::LTrim 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::LTrim 变体，将触发 unreachable!()
pub(crate) fn to_resp_l_trim(cmd: &Command) -> RespValue {
    match cmd {
        Command::LTrim(key, start, stop) => {
                RespValue::Array(vec![
                    bulk("LTRIM"),
                    bulk(key),
                    bulk(&start.to_string()),
                    bulk(&stop.to_string()),
                ])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::LPos 序列化为 RESP 数组
///
/// 对应 Redis 命令: LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::LPos 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::LPos 变体，将触发 unreachable!()
pub(crate) fn to_resp_l_pos(cmd: &Command) -> RespValue {
    match cmd {
        Command::LPos(key, value, rank, count, maxlen) => {
                let mut parts = vec![
                    bulk("LPOS"),
                    bulk(key),
                    bulk_bytes(value),
                ];
                if *rank != 1 {
                    parts.push(bulk("RANK"));
                    parts.push(bulk(&rank.to_string()));
                }
                if *count != 0 {
                    parts.push(bulk("COUNT"));
                    parts.push(bulk(&count.to_string()));
                }
                if *maxlen != 0 {
                    parts.push(bulk("MAXLEN"));
                    parts.push(bulk(&maxlen.to_string()));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::BLPop 序列化为 RESP 数组
///
/// 对应 Redis 命令: BLPOP key [key ...] timeout
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::BLPop 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::BLPop 变体，将触发 unreachable!()
pub(crate) fn to_resp_b_l_pop(cmd: &Command) -> RespValue {
    match cmd {
        Command::BLPop(keys, timeout) => {
                let mut parts = vec![bulk("BLPOP")];
                for key in keys {
                    parts.push(bulk(key));
                }
                parts.push(bulk(&timeout.to_string()));
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::BRPop 序列化为 RESP 数组
///
/// 对应 Redis 命令: BRPOP key [key ...] timeout
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::BRPop 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::BRPop 变体，将触发 unreachable!()
pub(crate) fn to_resp_b_r_pop(cmd: &Command) -> RespValue {
    match cmd {
        Command::BRPop(keys, timeout) => {
                let mut parts = vec![bulk("BRPOP")];
                for key in keys {
                    parts.push(bulk(key));
                }
                parts.push(bulk(&timeout.to_string()));
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::Lmpop 序列化为 RESP 数组
///
/// 对应 Redis 命令: LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Lmpop 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Lmpop 变体，将触发 unreachable!()
pub(crate) fn to_resp_lmpop(cmd: &Command) -> RespValue {
    match cmd {
        Command::Lmpop(keys, left, count) => {
                let mut parts = vec![bulk("LMPOP"), bulk(&keys.len().to_string())];
                for k in keys {
                    parts.push(bulk(k));
                }
                parts.push(bulk(if *left { "LEFT" } else { "RIGHT" }));
                if *count > 1 {
                    parts.push(bulk("COUNT"));
                    parts.push(bulk(&count.to_string()));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::BLmpop 序列化为 RESP 数组
///
/// 对应 Redis 命令: BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::BLmpop 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::BLmpop 变体，将触发 unreachable!()
pub(crate) fn to_resp_b_lmpop(cmd: &Command) -> RespValue {
    match cmd {
        Command::BLmpop(keys, left, count, timeout) => {
                let mut parts = vec![bulk("BLMPOP"), bulk(&timeout.to_string()), bulk(&keys.len().to_string())];
                for k in keys {
                    parts.push(bulk(k));
                }
                parts.push(bulk(if *left { "LEFT" } else { "RIGHT" }));
                if *count > 1 {
                    parts.push(bulk("COUNT"));
                    parts.push(bulk(&count.to_string()));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

