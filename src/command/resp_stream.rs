use super::*;

use crate::protocol::RespValue;

/// 将 Command::XAdd 序列化为 RESP 数组
///
/// 对应 Redis 命令: XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold] *|id field value [field value ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::XAdd 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::XAdd 变体，将触发 unreachable!()
pub(crate) fn to_resp_x_add(cmd: &Command) -> RespValue {
    match cmd {
        Command::XAdd(key, id, fields, nomkstream, max_len, min_id) => {
                let mut parts = vec![bulk("XADD"), bulk(key)];
                if *nomkstream {
                    parts.push(bulk("NOMKSTREAM"));
                }
                if let Some(max) = max_len {
                    parts.push(bulk("MAXLEN"));
                    parts.push(bulk(&max.to_string()));
                }
                if let Some(min) = min_id {
                    parts.push(bulk("MINID"));
                    parts.push(bulk(min));
                }
                parts.push(bulk(id));
                for (f, v) in fields {
                    parts.push(bulk(f));
                    parts.push(bulk(v));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::XLen 序列化为 RESP 数组
///
/// 对应 Redis 命令: XLEN key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::XLen 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::XLen 变体，将触发 unreachable!()
pub(crate) fn to_resp_x_len(cmd: &Command) -> RespValue {
    match cmd {
        Command::XLen(key) => {
                RespValue::Array(vec![bulk("XLEN"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::XRange 序列化为 RESP 数组
///
/// 对应 Redis 命令: XRANGE key start end [COUNT count]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::XRange 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::XRange 变体，将触发 unreachable!()
pub(crate) fn to_resp_x_range(cmd: &Command) -> RespValue {
    match cmd {
        Command::XRange(key, start, end, count) => {
                let mut parts = vec![bulk("XRANGE"), bulk(key), bulk(start), bulk(end)];
                if let Some(c) = count {
                    parts.push(bulk("COUNT"));
                    parts.push(bulk(&c.to_string()));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::XRevRange 序列化为 RESP 数组
///
/// 对应 Redis 命令: XREVRANGE key end start [COUNT count]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::XRevRange 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::XRevRange 变体，将触发 unreachable!()
pub(crate) fn to_resp_x_rev_range(cmd: &Command) -> RespValue {
    match cmd {
        Command::XRevRange(key, end, start, count) => {
                let mut parts = vec![bulk("XREVRANGE"), bulk(key), bulk(end), bulk(start)];
                if let Some(c) = count {
                    parts.push(bulk("COUNT"));
                    parts.push(bulk(&c.to_string()));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::XTrim 序列化为 RESP 数组
///
/// 对应 Redis 命令: XTRIM key MAXLEN|MINID [=|~] threshold
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::XTrim 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::XTrim 变体，将触发 unreachable!()
pub(crate) fn to_resp_x_trim(cmd: &Command) -> RespValue {
    match cmd {
        Command::XTrim(key, strategy, threshold) => {
                RespValue::Array(vec![
                    bulk("XTRIM"),
                    bulk(key),
                    bulk(strategy),
                    bulk(threshold),
                ])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::XDel 序列化为 RESP 数组
///
/// 对应 Redis 命令: XDEL key id [id ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::XDel 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::XDel 变体，将触发 unreachable!()
pub(crate) fn to_resp_x_del(cmd: &Command) -> RespValue {
    match cmd {
        Command::XDel(key, ids) => {
                let mut parts = vec![bulk("XDEL"), bulk(key)];
                for id in ids {
                    parts.push(bulk(id));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::XRead 序列化为 RESP 数组
///
/// 对应 Redis 命令: XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::XRead 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::XRead 变体，将触发 unreachable!()
pub(crate) fn to_resp_x_read(cmd: &Command) -> RespValue {
    match cmd {
        Command::XRead(keys, ids, count) => {
                let mut parts = vec![bulk("XREAD")];
                if let Some(c) = count {
                    parts.push(bulk("COUNT"));
                    parts.push(bulk(&c.to_string()));
                }
                parts.push(bulk("STREAMS"));
                for key in keys {
                    parts.push(bulk(key));
                }
                for id in ids {
                    parts.push(bulk(id));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::XSetId 序列化为 RESP 数组
///
/// 对应 Redis 命令: XSETID key id
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::XSetId 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::XSetId 变体，将触发 unreachable!()
pub(crate) fn to_resp_x_set_id(cmd: &Command) -> RespValue {
    match cmd {
        Command::XSetId(key, id) => {
                RespValue::Array(vec![bulk("XSETID"), bulk(key), bulk(id)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::XGroupCreate 序列化为 RESP 数组
///
/// 对应 Redis 命令: XGROUP CREATE key groupname id [MKSTREAM]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::XGroupCreate 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::XGroupCreate 变体，将触发 unreachable!()
pub(crate) fn to_resp_x_group_create(cmd: &Command) -> RespValue {
    match cmd {
        Command::XGroupCreate(key, group, id, mkstream) => {
                let mut parts = vec![bulk("XGROUP"), bulk("CREATE"), bulk(key), bulk(group), bulk(id)];
                if *mkstream { parts.push(bulk("MKSTREAM")); }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::XGroupDestroy 序列化为 RESP 数组
///
/// 对应 Redis 命令: XGROUP DESTROY key groupname
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::XGroupDestroy 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::XGroupDestroy 变体，将触发 unreachable!()
pub(crate) fn to_resp_x_group_destroy(cmd: &Command) -> RespValue {
    match cmd {
        Command::XGroupDestroy(key, group) => {
                RespValue::Array(vec![bulk("XGROUP"), bulk("DESTROY"), bulk(key), bulk(group)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::XGroupSetId 序列化为 RESP 数组
///
/// 对应 Redis 命令: XGROUP SETID key groupname id
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::XGroupSetId 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::XGroupSetId 变体，将触发 unreachable!()
pub(crate) fn to_resp_x_group_set_id(cmd: &Command) -> RespValue {
    match cmd {
        Command::XGroupSetId(key, group, id) => {
                RespValue::Array(vec![bulk("XGROUP"), bulk("SETID"), bulk(key), bulk(group), bulk(id)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::XGroupDelConsumer 序列化为 RESP 数组
///
/// 对应 Redis 命令: XGROUP DELCONSUMER key groupname consumername
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::XGroupDelConsumer 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::XGroupDelConsumer 变体，将触发 unreachable!()
pub(crate) fn to_resp_x_group_del_consumer(cmd: &Command) -> RespValue {
    match cmd {
        Command::XGroupDelConsumer(key, group, consumer) => {
                RespValue::Array(vec![bulk("XGROUP"), bulk("DELCONSUMER"), bulk(key), bulk(group), bulk(consumer)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::XGroupCreateConsumer 序列化为 RESP 数组
///
/// 对应 Redis 命令: XGROUP CREATECONSUMER key groupname consumername
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::XGroupCreateConsumer 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::XGroupCreateConsumer 变体，将触发 unreachable!()
pub(crate) fn to_resp_x_group_create_consumer(cmd: &Command) -> RespValue {
    match cmd {
        Command::XGroupCreateConsumer(key, group, consumer) => {
                RespValue::Array(vec![bulk("XGROUP"), bulk("CREATECONSUMER"), bulk(key), bulk(group), bulk(consumer)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::XReadGroup 序列化为 RESP 数组
///
/// 对应 Redis 命令: XREADGROUP GROUP group consumer [COUNT count] [NOACK] STREAMS key [key ...] id [id ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::XReadGroup 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::XReadGroup 变体，将触发 unreachable!()
pub(crate) fn to_resp_x_read_group(cmd: &Command) -> RespValue {
    match cmd {
        Command::XReadGroup(group, consumer, keys, ids, count, noack) => {
                let mut parts = vec![bulk("XREADGROUP"), bulk("GROUP"), bulk(group), bulk(consumer)];
                if let Some(c) = count { parts.push(bulk("COUNT")); parts.push(bulk(&c.to_string())); }
                if *noack { parts.push(bulk("NOACK")); }
                parts.push(bulk("STREAMS"));
                for k in keys { parts.push(bulk(k)); }
                for id in ids { parts.push(bulk(id)); }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::XAck 序列化为 RESP 数组
///
/// 对应 Redis 命令: XACK key group id [id ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::XAck 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::XAck 变体，将触发 unreachable!()
pub(crate) fn to_resp_x_ack(cmd: &Command) -> RespValue {
    match cmd {
        Command::XAck(key, group, ids) => {
                let mut parts = vec![bulk("XACK"), bulk(key), bulk(group)];
                for id in ids { parts.push(bulk(id)); }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::XClaim 序列化为 RESP 数组
///
/// 对应 Redis 命令: XCLAIM key group consumer min-idle-time id [id ...] [JUSTID]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::XClaim 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::XClaim 变体，将触发 unreachable!()
pub(crate) fn to_resp_x_claim(cmd: &Command) -> RespValue {
    match cmd {
        Command::XClaim(key, group, consumer, min_idle, ids, justid) => {
                let mut parts = vec![bulk("XCLAIM"), bulk(key), bulk(group), bulk(consumer), bulk(&min_idle.to_string())];
                for id in ids { parts.push(bulk(id)); }
                if *justid { parts.push(bulk("JUSTID")); }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::XAutoClaim 序列化为 RESP 数组
///
/// 对应 Redis 命令: XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::XAutoClaim 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::XAutoClaim 变体，将触发 unreachable!()
pub(crate) fn to_resp_x_auto_claim(cmd: &Command) -> RespValue {
    match cmd {
        Command::XAutoClaim(key, group, consumer, min_idle, start, count, justid) => {
                let mut parts = vec![bulk("XAUTOCLAIM"), bulk(key), bulk(group), bulk(consumer), bulk(&min_idle.to_string()), bulk(start)];
                parts.push(bulk("COUNT")); parts.push(bulk(&count.to_string()));
                if *justid { parts.push(bulk("JUSTID")); }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::XPending 序列化为 RESP 数组
///
/// 对应 Redis 命令: XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::XPending 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::XPending 变体，将触发 unreachable!()
pub(crate) fn to_resp_x_pending(cmd: &Command) -> RespValue {
    match cmd {
        Command::XPending(key, group, start, end, count, consumer) => {
                let mut parts = vec![bulk("XPENDING"), bulk(key), bulk(group)];
                if let Some(s) = start { parts.push(bulk(s)); }
                if let Some(e) = end { parts.push(bulk(e)); }
                if let Some(c) = count { parts.push(bulk(&c.to_string())); }
                if let Some(cn) = consumer { parts.push(bulk(cn)); }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::XInfoStream 序列化为 RESP 数组
///
/// 对应 Redis 命令: XINFO STREAM key [FULL]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::XInfoStream 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::XInfoStream 变体，将触发 unreachable!()
pub(crate) fn to_resp_x_info_stream(cmd: &Command) -> RespValue {
    match cmd {
        Command::XInfoStream(key, full) => {
                let mut parts = vec![bulk("XINFO"), bulk("STREAM"), bulk(key)];
                if *full { parts.push(bulk("FULL")); }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::XInfoGroups 序列化为 RESP 数组
///
/// 对应 Redis 命令: XINFO GROUPS key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::XInfoGroups 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::XInfoGroups 变体，将触发 unreachable!()
pub(crate) fn to_resp_x_info_groups(cmd: &Command) -> RespValue {
    match cmd {
        Command::XInfoGroups(key) => {
                RespValue::Array(vec![bulk("XINFO"), bulk("GROUPS"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::XInfoConsumers 序列化为 RESP 数组
///
/// 对应 Redis 命令: XINFO CONSUMERS key groupname
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::XInfoConsumers 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::XInfoConsumers 变体，将触发 unreachable!()
pub(crate) fn to_resp_x_info_consumers(cmd: &Command) -> RespValue {
    match cmd {
        Command::XInfoConsumers(key, group) => {
                RespValue::Array(vec![bulk("XINFO"), bulk("CONSUMERS"), bulk(key), bulk(group)])
        }
        _ => unreachable!(),
    }
}

