//! String 命令 RESP 序列化
use super::*;

use crate::protocol::RespValue;

/// 将 Command::Get 序列化为 RESP 数组
///
/// 对应 Redis 命令: GET key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Get 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Get 变体，将触发 unreachable!()
pub(crate) fn to_resp_get(cmd: &Command) -> RespValue {
    match cmd {
        Command::Get(key) => RespValue::Array(vec![bulk("GET"), bulk(key)]),
        _ => unreachable!(),
    }
}

/// 将 Command::Set 序列化为 RESP 数组
///
/// 对应 Redis 命令: SET key value [NX|XX] [GET] [EX seconds|PX milliseconds|EXAT timestamp|PXAT ms-timestamp|KEEPTTL]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Set 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Set 变体，将触发 unreachable!()
pub(crate) fn to_resp_set(cmd: &Command) -> RespValue {
    match cmd {
        Command::Set(key, value, options) => {
            let mut parts = vec![bulk("SET"), bulk(key), bulk_bytes(value)];
            if options.nx {
                parts.push(bulk("NX"));
            }
            if options.xx {
                parts.push(bulk("XX"));
            }
            if options.get {
                parts.push(bulk("GET"));
            }
            if options.keepttl {
                parts.push(bulk("KEEPTTL"));
            }
            match &options.expire {
                Some(crate::storage::SetExpireOption::Ex(s)) => {
                    parts.push(bulk("EX"));
                    parts.push(bulk(&s.to_string()));
                }
                Some(crate::storage::SetExpireOption::Px(ms)) => {
                    parts.push(bulk("PX"));
                    parts.push(bulk(&ms.to_string()));
                }
                Some(crate::storage::SetExpireOption::ExAt(ts)) => {
                    parts.push(bulk("EXAT"));
                    parts.push(bulk(&ts.to_string()));
                }
                Some(crate::storage::SetExpireOption::PxAt(ts)) => {
                    parts.push(bulk("PXAT"));
                    parts.push(bulk(&ts.to_string()));
                }
                None => {}
            }
            RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::SetEx 序列化为 RESP 数组
///
/// 对应 Redis 命令: SET key value EX seconds
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::SetEx 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::SetEx 变体，将触发 unreachable!()
pub(crate) fn to_resp_set_ex(cmd: &Command) -> RespValue {
    match cmd {
        Command::SetEx(key, value, ttl_ms) => RespValue::Array(vec![
            bulk("SET"),
            bulk(key),
            bulk_bytes(value),
            bulk("PX"),
            bulk(&ttl_ms.to_string()),
        ]),
        _ => unreachable!(),
    }
}

/// 将 Command::Del 序列化为 RESP 数组
///
/// 对应 Redis 命令: DEL key [key ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Del 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Del 变体，将触发 unreachable!()
pub(crate) fn to_resp_del(cmd: &Command) -> RespValue {
    match cmd {
        Command::Del(keys) => {
            let mut parts = vec![bulk("DEL")];
            for key in keys {
                parts.push(bulk(key));
            }
            RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::Exists 序列化为 RESP 数组
///
/// 对应 Redis 命令: EXISTS key [key ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Exists 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Exists 变体，将触发 unreachable!()
pub(crate) fn to_resp_exists(cmd: &Command) -> RespValue {
    match cmd {
        Command::Exists(keys) => {
            let mut parts = vec![bulk("EXISTS")];
            for key in keys {
                parts.push(bulk(key));
            }
            RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::FlushAll 序列化为 RESP 数组
///
/// 对应 Redis 命令:
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::FlushAll 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::FlushAll 变体，将触发 unreachable!()
pub(crate) fn to_resp_flush_all(cmd: &Command) -> RespValue {
    match cmd {
        Command::FlushAll => RespValue::Array(vec![bulk("FLUSHALL")]),
        _ => unreachable!(),
    }
}

/// 将 Command::MGet 序列化为 RESP 数组
///
/// 对应 Redis 命令: MGET key [key ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::MGet 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::MGet 变体，将触发 unreachable!()
pub(crate) fn to_resp_m_get(cmd: &Command) -> RespValue {
    match cmd {
        Command::MGet(keys) => {
            let mut parts = vec![bulk("MGET")];
            for key in keys {
                parts.push(bulk(key));
            }
            RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::MSet 序列化为 RESP 数组
///
/// 对应 Redis 命令: MSET key value [key value ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::MSet 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::MSet 变体，将触发 unreachable!()
pub(crate) fn to_resp_m_set(cmd: &Command) -> RespValue {
    match cmd {
        Command::MSet(pairs) => {
            let mut parts = vec![bulk("MSET")];
            for (key, value) in pairs {
                parts.push(bulk(key));
                parts.push(bulk_bytes(value));
            }
            RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::Incr 序列化为 RESP 数组
///
/// 对应 Redis 命令: INCR key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Incr 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Incr 变体，将触发 unreachable!()
pub(crate) fn to_resp_incr(cmd: &Command) -> RespValue {
    match cmd {
        Command::Incr(key) => RespValue::Array(vec![bulk("INCR"), bulk(key)]),
        _ => unreachable!(),
    }
}

/// 将 Command::Decr 序列化为 RESP 数组
///
/// 对应 Redis 命令: DECR key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Decr 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Decr 变体，将触发 unreachable!()
pub(crate) fn to_resp_decr(cmd: &Command) -> RespValue {
    match cmd {
        Command::Decr(key) => RespValue::Array(vec![bulk("DECR"), bulk(key)]),
        _ => unreachable!(),
    }
}

/// 将 Command::IncrBy 序列化为 RESP 数组
///
/// 对应 Redis 命令: INCRBY key delta
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::IncrBy 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::IncrBy 变体，将触发 unreachable!()
pub(crate) fn to_resp_incr_by(cmd: &Command) -> RespValue {
    match cmd {
        Command::IncrBy(key, delta) => {
            RespValue::Array(vec![bulk("INCRBY"), bulk(key), bulk(&delta.to_string())])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::DecrBy 序列化为 RESP 数组
///
/// 对应 Redis 命令: DECRBY key delta
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::DecrBy 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::DecrBy 变体，将触发 unreachable!()
pub(crate) fn to_resp_decr_by(cmd: &Command) -> RespValue {
    match cmd {
        Command::DecrBy(key, delta) => {
            RespValue::Array(vec![bulk("DECRBY"), bulk(key), bulk(&delta.to_string())])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::Append 序列化为 RESP 数组
///
/// 对应 Redis 命令: APPEND key value
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Append 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Append 变体，将触发 unreachable!()
pub(crate) fn to_resp_append(cmd: &Command) -> RespValue {
    match cmd {
        Command::Append(key, value) => {
            RespValue::Array(vec![bulk("APPEND"), bulk(key), bulk_bytes(value)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::SetNx 序列化为 RESP 数组
///
/// 对应 Redis 命令: SETNX key value
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::SetNx 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::SetNx 变体，将触发 unreachable!()
pub(crate) fn to_resp_set_nx(cmd: &Command) -> RespValue {
    match cmd {
        Command::SetNx(key, value) => {
            RespValue::Array(vec![bulk("SETNX"), bulk(key), bulk_bytes(value)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::SetExCmd 序列化为 RESP 数组
///
/// 对应 Redis 命令: SETEX key seconds value
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::SetExCmd 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::SetExCmd 变体，将触发 unreachable!()
pub(crate) fn to_resp_set_ex_cmd(cmd: &Command) -> RespValue {
    match cmd {
        Command::SetExCmd(key, value, seconds) => RespValue::Array(vec![
            bulk("SETEX"),
            bulk(key),
            bulk(&seconds.to_string()),
            bulk_bytes(value),
        ]),
        _ => unreachable!(),
    }
}

/// 将 Command::PSetEx 序列化为 RESP 数组
///
/// 对应 Redis 命令: PSETEX key milliseconds value
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::PSetEx 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::PSetEx 变体，将触发 unreachable!()
pub(crate) fn to_resp_p_set_ex(cmd: &Command) -> RespValue {
    match cmd {
        Command::PSetEx(key, value, ms) => RespValue::Array(vec![
            bulk("PSETEX"),
            bulk(key),
            bulk(&ms.to_string()),
            bulk_bytes(value),
        ]),
        _ => unreachable!(),
    }
}

/// 将 Command::GetSet 序列化为 RESP 数组
///
/// 对应 Redis 命令: GETSET key value
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::GetSet 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::GetSet 变体，将触发 unreachable!()
pub(crate) fn to_resp_get_set(cmd: &Command) -> RespValue {
    match cmd {
        Command::GetSet(key, value) => {
            RespValue::Array(vec![bulk("GETSET"), bulk(key), bulk_bytes(value)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::GetDel 序列化为 RESP 数组
///
/// 对应 Redis 命令: GETDEL key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::GetDel 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::GetDel 变体，将触发 unreachable!()
pub(crate) fn to_resp_get_del(cmd: &Command) -> RespValue {
    match cmd {
        Command::GetDel(key) => RespValue::Array(vec![bulk("GETDEL"), bulk(key)]),
        _ => unreachable!(),
    }
}

/// 将 Command::GetEx 序列化为 RESP 数组
///
/// 对应 Redis 命令: GETEX key [EX seconds|PX milliseconds|EXAT timestamp|PXAT ms-timestamp|PERSIST]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::GetEx 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::GetEx 变体，将触发 unreachable!()
pub(crate) fn to_resp_get_ex(cmd: &Command) -> RespValue {
    match cmd {
        Command::GetEx(key, opt) => {
            let mut parts = vec![bulk("GETEX"), bulk(key)];
            match opt {
                GetExOption::Persist => parts.push(bulk("PERSIST")),
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
            RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::MSetNx 序列化为 RESP 数组
///
/// 对应 Redis 命令: MSETNX key value [key value ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::MSetNx 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::MSetNx 变体，将触发 unreachable!()
pub(crate) fn to_resp_m_set_nx(cmd: &Command) -> RespValue {
    match cmd {
        Command::MSetNx(pairs) => {
            let mut parts = vec![bulk("MSETNX")];
            for (key, value) in pairs {
                parts.push(bulk(key));
                parts.push(bulk_bytes(value));
            }
            RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::IncrByFloat 序列化为 RESP 数组
///
/// 对应 Redis 命令: INCRBYFLOAT key increment
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::IncrByFloat 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::IncrByFloat 变体，将触发 unreachable!()
pub(crate) fn to_resp_incr_by_float(cmd: &Command) -> RespValue {
    match cmd {
        Command::IncrByFloat(key, delta) => RespValue::Array(vec![
            bulk("INCRBYFLOAT"),
            bulk(key),
            bulk(&format!("{}", delta)),
        ]),
        _ => unreachable!(),
    }
}

/// 将 Command::SetRange 序列化为 RESP 数组
///
/// 对应 Redis 命令: SETRANGE key offset value
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::SetRange 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::SetRange 变体，将触发 unreachable!()
pub(crate) fn to_resp_set_range(cmd: &Command) -> RespValue {
    match cmd {
        Command::SetRange(key, offset, value) => RespValue::Array(vec![
            bulk("SETRANGE"),
            bulk(key),
            bulk(&offset.to_string()),
            bulk_bytes(value),
        ]),
        _ => unreachable!(),
    }
}

/// 将 Command::GetRange 序列化为 RESP 数组
///
/// 对应 Redis 命令: GETRANGE key start end
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::GetRange 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::GetRange 变体，将触发 unreachable!()
pub(crate) fn to_resp_get_range(cmd: &Command) -> RespValue {
    match cmd {
        Command::GetRange(key, start, end) => RespValue::Array(vec![
            bulk("GETRANGE"),
            bulk(key),
            bulk(&start.to_string()),
            bulk(&end.to_string()),
        ]),
        _ => unreachable!(),
    }
}

/// 将 Command::StrLen 序列化为 RESP 数组
///
/// 对应 Redis 命令: STRLEN key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::StrLen 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::StrLen 变体，将触发 unreachable!()
pub(crate) fn to_resp_str_len(cmd: &Command) -> RespValue {
    match cmd {
        Command::StrLen(key) => RespValue::Array(vec![bulk("STRLEN"), bulk(key)]),
        _ => unreachable!(),
    }
}

/// 将 Command::Lcs 序列化为 RESP 数组
///
/// 对应 Redis 命令: LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::Lcs 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::Lcs 变体，将触发 unreachable!()
pub(crate) fn to_resp_lcs(cmd: &Command) -> RespValue {
    match cmd {
        Command::Lcs(key1, key2, len, idx, minmatchlen, withmatchlen) => {
            let mut parts = vec![bulk("LCS"), bulk(key1), bulk(key2)];
            if *len {
                parts.push(bulk("LEN"));
            }
            if *idx {
                parts.push(bulk("IDX"));
            }
            if *minmatchlen > 0 {
                parts.push(bulk("MINMATCHLEN"));
                parts.push(bulk(&minmatchlen.to_string()));
            }
            if *withmatchlen {
                parts.push(bulk("WITHMATCHLEN"));
            }
            RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}
