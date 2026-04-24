use super::*;

use crate::protocol::RespValue;

/// 将 Command::HSet 序列化为 RESP 数组
///
/// 对应 Redis 命令: HSET key field value [field value ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HSet 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HSet 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_set(cmd: &Command) -> RespValue {
    match cmd {
        Command::HSet(key, pairs) => {
                let mut parts = vec![bulk("HSET"), bulk(key)];
                for (field, value) in pairs {
                    parts.push(bulk(field));
                    parts.push(bulk_bytes(value));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HGet 序列化为 RESP 数组
///
/// 对应 Redis 命令: HGET key field
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HGet 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HGet 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_get(cmd: &Command) -> RespValue {
    match cmd {
        Command::HGet(key, field) => {
                RespValue::Array(vec![bulk("HGET"), bulk(key), bulk(field)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HStrLen 序列化为 RESP 数组
///
/// 对应 Redis 命令: HSTRLEN key field
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HStrLen 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HStrLen 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_str_len(cmd: &Command) -> RespValue {
    match cmd {
        Command::HStrLen(key, field) => {
                RespValue::Array(vec![bulk("HSTRLEN"), bulk(key), bulk(field)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HDel 序列化为 RESP 数组
///
/// 对应 Redis 命令: HDEL key field [field ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HDel 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HDel 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_del(cmd: &Command) -> RespValue {
    match cmd {
        Command::HDel(key, fields) => {
                let mut parts = vec![bulk("HDEL"), bulk(key)];
                for field in fields {
                    parts.push(bulk(field));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HExists 序列化为 RESP 数组
///
/// 对应 Redis 命令: HEXISTS key field
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HExists 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HExists 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_exists(cmd: &Command) -> RespValue {
    match cmd {
        Command::HExists(key, field) => {
                RespValue::Array(vec![bulk("HEXISTS"), bulk(key), bulk(field)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HGetAll 序列化为 RESP 数组
///
/// 对应 Redis 命令: HGETALL key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HGetAll 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HGetAll 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_get_all(cmd: &Command) -> RespValue {
    match cmd {
        Command::HGetAll(key) => {
                RespValue::Array(vec![bulk("HGETALL"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HLen 序列化为 RESP 数组
///
/// 对应 Redis 命令: HLEN key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HLen 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HLen 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_len(cmd: &Command) -> RespValue {
    match cmd {
        Command::HLen(key) => {
                RespValue::Array(vec![bulk("HLEN"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HMSet 序列化为 RESP 数组
///
/// 对应 Redis 命令: HMSET key field value [field value ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HMSet 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HMSet 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_m_set(cmd: &Command) -> RespValue {
    match cmd {
        Command::HMSet(key, pairs) => {
                let mut parts = vec![bulk("HMSET"), bulk(key)];
                for (field, value) in pairs {
                    parts.push(bulk(field));
                    parts.push(bulk_bytes(value));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HMGet 序列化为 RESP 数组
///
/// 对应 Redis 命令: HMGET key field [field ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HMGet 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HMGet 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_m_get(cmd: &Command) -> RespValue {
    match cmd {
        Command::HMGet(key, fields) => {
                let mut parts = vec![bulk("HMGET"), bulk(key)];
                for field in fields {
                    parts.push(bulk(field));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HIncrBy 序列化为 RESP 数组
///
/// 对应 Redis 命令: HINCRBY key field increment
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HIncrBy 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HIncrBy 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_incr_by(cmd: &Command) -> RespValue {
    match cmd {
        Command::HIncrBy(key, field, delta) => {
                RespValue::Array(vec![
                    bulk("HINCRBY"),
                    bulk(key),
                    bulk(field),
                    bulk(&delta.to_string()),
                ])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HIncrByFloat 序列化为 RESP 数组
///
/// 对应 Redis 命令: HINCRBYFLOAT key field increment
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HIncrByFloat 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HIncrByFloat 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_incr_by_float(cmd: &Command) -> RespValue {
    match cmd {
        Command::HIncrByFloat(key, field, delta) => {
                RespValue::Array(vec![
                    bulk("HINCRBYFLOAT"),
                    bulk(key),
                    bulk(field),
                    bulk(&format!("{}", delta)),
                ])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HKeys 序列化为 RESP 数组
///
/// 对应 Redis 命令: HKEYS key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HKeys 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HKeys 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_keys(cmd: &Command) -> RespValue {
    match cmd {
        Command::HKeys(key) => {
                RespValue::Array(vec![bulk("HKEYS"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HVals 序列化为 RESP 数组
///
/// 对应 Redis 命令: HVALS key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HVals 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HVals 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_vals(cmd: &Command) -> RespValue {
    match cmd {
        Command::HVals(key) => {
                RespValue::Array(vec![bulk("HVALS"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HSetNx 序列化为 RESP 数组
///
/// 对应 Redis 命令: HSETNX key field value
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HSetNx 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HSetNx 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_set_nx(cmd: &Command) -> RespValue {
    match cmd {
        Command::HSetNx(key, field, value) => {
                RespValue::Array(vec![
                    bulk("HSETNX"),
                    bulk(key),
                    bulk(field),
                    bulk_bytes(value),
                ])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HRandField 序列化为 RESP 数组
///
/// 对应 Redis 命令: HRANDFIELD key [count [WITHVALUES]]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HRandField 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HRandField 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_rand_field(cmd: &Command) -> RespValue {
    match cmd {
        Command::HRandField(key, count, with_values) => {
                let mut parts = vec![bulk("HRANDFIELD"), bulk(key)];
                if *count != 1 || *with_values {
                    parts.push(bulk(&count.to_string()));
                    if *with_values {
                        parts.push(bulk("WITHVALUES"));
                    }
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HScan 序列化为 RESP 数组
///
/// 对应 Redis 命令: HSCAN key cursor [MATCH pattern] [COUNT count]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HScan 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HScan 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_scan(cmd: &Command) -> RespValue {
    match cmd {
        Command::HScan(key, cursor, pattern, count) => {
                let mut parts = vec![
                    bulk("HSCAN"),
                    bulk(key),
                    bulk(&cursor.to_string()),
                ];
                if !pattern.is_empty() && pattern != "*" {
                    parts.push(bulk("MATCH"));
                    parts.push(bulk(pattern));
                }
                if *count != 0 {
                    parts.push(bulk("COUNT"));
                    parts.push(bulk(&count.to_string()));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HExpire 序列化为 RESP 数组
///
/// 对应 Redis 命令: HEXPIRE key field [field ...] seconds
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HExpire 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HExpire 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_expire(cmd: &Command) -> RespValue {
    match cmd {
        Command::HExpire(key, fields, seconds) => {
                let mut parts = vec![bulk("HEXPIRE"), bulk(key)];
                for f in fields {
                    parts.push(bulk(f));
                }
                parts.push(bulk(&seconds.to_string()));
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HPExpire 序列化为 RESP 数组
///
/// 对应 Redis 命令: HPEXPIRE key field [field ...] milliseconds
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HPExpire 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HPExpire 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_p_expire(cmd: &Command) -> RespValue {
    match cmd {
        Command::HPExpire(key, fields, ms) => {
                let mut parts = vec![bulk("HPEXPIRE"), bulk(key)];
                for f in fields {
                    parts.push(bulk(f));
                }
                parts.push(bulk(&ms.to_string()));
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HExpireAt 序列化为 RESP 数组
///
/// 对应 Redis 命令: HEXPIREAT key field [field ...] timestamp-seconds
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HExpireAt 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HExpireAt 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_expire_at(cmd: &Command) -> RespValue {
    match cmd {
        Command::HExpireAt(key, fields, ts) => {
                let mut parts = vec![bulk("HEXPIREAT"), bulk(key)];
                for f in fields {
                    parts.push(bulk(f));
                }
                parts.push(bulk(&ts.to_string()));
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HPExpireAt 序列化为 RESP 数组
///
/// 对应 Redis 命令: HPEXPIREAT key field [field ...] timestamp-ms
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HPExpireAt 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HPExpireAt 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_p_expire_at(cmd: &Command) -> RespValue {
    match cmd {
        Command::HPExpireAt(key, fields, ts) => {
                let mut parts = vec![bulk("HPEXPIREAT"), bulk(key)];
                for f in fields {
                    parts.push(bulk(f));
                }
                parts.push(bulk(&ts.to_string()));
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HTtl 序列化为 RESP 数组
///
/// 对应 Redis 命令: HTTL key field [field ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HTtl 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HTtl 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_ttl(cmd: &Command) -> RespValue {
    match cmd {
        Command::HTtl(key, fields) => {
                let mut parts = vec![bulk("HTTL"), bulk(key)];
                for f in fields {
                    parts.push(bulk(f));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HPTtl 序列化为 RESP 数组
///
/// 对应 Redis 命令: HPTTL key field [field ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HPTtl 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HPTtl 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_p_ttl(cmd: &Command) -> RespValue {
    match cmd {
        Command::HPTtl(key, fields) => {
                let mut parts = vec![bulk("HPTTL"), bulk(key)];
                for f in fields {
                    parts.push(bulk(f));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HExpireTime 序列化为 RESP 数组
///
/// 对应 Redis 命令: HEXPIRETIME key field [field ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HExpireTime 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HExpireTime 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_expire_time(cmd: &Command) -> RespValue {
    match cmd {
        Command::HExpireTime(key, fields) => {
                let mut parts = vec![bulk("HEXPIRETIME"), bulk(key)];
                for f in fields {
                    parts.push(bulk(f));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HPExpireTime 序列化为 RESP 数组
///
/// 对应 Redis 命令: HPEXPIRETIME key field [field ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HPExpireTime 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HPExpireTime 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_p_expire_time(cmd: &Command) -> RespValue {
    match cmd {
        Command::HPExpireTime(key, fields) => {
                let mut parts = vec![bulk("HPEXPIRETIME"), bulk(key)];
                for f in fields {
                    parts.push(bulk(f));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HPersist 序列化为 RESP 数组
///
/// 对应 Redis 命令: HPERSIST key field [field ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HPersist 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HPersist 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_persist(cmd: &Command) -> RespValue {
    match cmd {
        Command::HPersist(key, fields) => {
                let mut parts = vec![bulk("HPERSIST"), bulk(key)];
                for f in fields {
                    parts.push(bulk(f));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HGetDel 序列化为 RESP 数组
///
/// 对应 Redis 命令: HGETDEL key field [field ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HGetDel 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HGetDel 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_get_del(cmd: &Command) -> RespValue {
    match cmd {
        Command::HGetDel(key, fields) => {
                let mut parts = vec![bulk("HGETDEL"), bulk(key)];
                for f in fields {
                    parts.push(bulk(f));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HGetEx 序列化为 RESP 数组
///
/// 对应 Redis 命令: HGETEX key [EX seconds|PX milliseconds|EXAT timestamp|PXAT ms-timestamp|PERSIST] field [field ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HGetEx 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HGetEx 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_get_ex(cmd: &Command) -> RespValue {
    match cmd {
        Command::HGetEx(key, opt, fields) => {
                let mut parts = vec![bulk("HGETEX"), bulk(key)];
                match opt {
                    GetExOption::Persist => {
                        parts.push(bulk("PERSIST"));
                    }
                    GetExOption::Ex(s) => {
                        parts.push(bulk("EX"));
                        parts.push(bulk(&s.to_string()));
                    }
                    GetExOption::Px(ms) => {
                        parts.push(bulk("PX"));
                        parts.push(bulk(&ms.to_string()));
                    }
                    GetExOption::ExAt(ts) => {
                        parts.push(bulk("EXAT"));
                        parts.push(bulk(&ts.to_string()));
                    }
                    GetExOption::PxAt(ts) => {
                        parts.push(bulk("PXAT"));
                        parts.push(bulk(&ts.to_string()));
                    }
                }
                for f in fields {
                    parts.push(bulk(f));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::HSetEx 序列化为 RESP 数组
///
/// 对应 Redis 命令: HSETEX key seconds field value [field value ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::HSetEx 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::HSetEx 变体，将触发 unreachable!()
pub(crate) fn to_resp_h_set_ex(cmd: &Command) -> RespValue {
    match cmd {
        Command::HSetEx(key, seconds, pairs) => {
                let mut parts = vec![bulk("HSETEX"), bulk(key), bulk(&seconds.to_string())];
                for (field, value) in pairs {
                    parts.push(bulk(field));
                    parts.push(bulk_bytes(value));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

