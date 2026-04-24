use super::*;

use crate::protocol::RespValue;

/// 将 Command::SAdd 序列化为 RESP 数组
///
/// 对应 Redis 命令: SADD key member [member ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::SAdd 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::SAdd 变体，将触发 unreachable!()
pub(crate) fn to_resp_s_add(cmd: &Command) -> RespValue {
    match cmd {
        Command::SAdd(key, members) => {
                let mut parts = vec![bulk("SADD"), bulk(key)];
                for m in members {
                    parts.push(bulk_bytes(m));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::SRem 序列化为 RESP 数组
///
/// 对应 Redis 命令: SREM key member [member ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::SRem 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::SRem 变体，将触发 unreachable!()
pub(crate) fn to_resp_s_rem(cmd: &Command) -> RespValue {
    match cmd {
        Command::SRem(key, members) => {
                let mut parts = vec![bulk("SREM"), bulk(key)];
                for m in members {
                    parts.push(bulk_bytes(m));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::SMembers 序列化为 RESP 数组
///
/// 对应 Redis 命令: SMEMBERS key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::SMembers 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::SMembers 变体，将触发 unreachable!()
pub(crate) fn to_resp_s_members(cmd: &Command) -> RespValue {
    match cmd {
        Command::SMembers(key) => {
                RespValue::Array(vec![bulk("SMEMBERS"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::SIsMember 序列化为 RESP 数组
///
/// 对应 Redis 命令: SISMEMBER key member
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::SIsMember 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::SIsMember 变体，将触发 unreachable!()
pub(crate) fn to_resp_s_is_member(cmd: &Command) -> RespValue {
    match cmd {
        Command::SIsMember(key, member) => {
                RespValue::Array(vec![
                    bulk("SISMEMBER"),
                    bulk(key),
                    bulk_bytes(member),
                ])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::SCard 序列化为 RESP 数组
///
/// 对应 Redis 命令: SCARD key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::SCard 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::SCard 变体，将触发 unreachable!()
pub(crate) fn to_resp_s_card(cmd: &Command) -> RespValue {
    match cmd {
        Command::SCard(key) => {
                RespValue::Array(vec![bulk("SCARD"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::SInter 序列化为 RESP 数组
///
/// 对应 Redis 命令: SINTER key [key ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::SInter 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::SInter 变体，将触发 unreachable!()
pub(crate) fn to_resp_s_inter(cmd: &Command) -> RespValue {
    match cmd {
        Command::SInter(keys) => {
                let mut parts = vec![bulk("SINTER")];
                for key in keys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::SUnion 序列化为 RESP 数组
///
/// 对应 Redis 命令: SUNION key [key ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::SUnion 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::SUnion 变体，将触发 unreachable!()
pub(crate) fn to_resp_s_union(cmd: &Command) -> RespValue {
    match cmd {
        Command::SUnion(keys) => {
                let mut parts = vec![bulk("SUNION")];
                for key in keys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::SDiff 序列化为 RESP 数组
///
/// 对应 Redis 命令: SDIFF key [key ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::SDiff 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::SDiff 变体，将触发 unreachable!()
pub(crate) fn to_resp_s_diff(cmd: &Command) -> RespValue {
    match cmd {
        Command::SDiff(keys) => {
                let mut parts = vec![bulk("SDIFF")];
                for key in keys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::SPop 序列化为 RESP 数组
///
/// 对应 Redis 命令: SPOP key [count]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::SPop 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::SPop 变体，将触发 unreachable!()
pub(crate) fn to_resp_s_pop(cmd: &Command) -> RespValue {
    match cmd {
        Command::SPop(key, count) => {
                let mut parts = vec![bulk("SPOP"), bulk(key)];
                if *count != 1 {
                    parts.push(bulk(&count.to_string()));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::SRandMember 序列化为 RESP 数组
///
/// 对应 Redis 命令: SRANDMEMBER key [count]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::SRandMember 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::SRandMember 变体，将触发 unreachable!()
pub(crate) fn to_resp_s_rand_member(cmd: &Command) -> RespValue {
    match cmd {
        Command::SRandMember(key, count) => {
                let mut parts = vec![bulk("SRANDMEMBER"), bulk(key)];
                if *count != 1 {
                    parts.push(bulk(&count.to_string()));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::SMove 序列化为 RESP 数组
///
/// 对应 Redis 命令: SMOVE source destination member
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::SMove 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::SMove 变体，将触发 unreachable!()
pub(crate) fn to_resp_s_move(cmd: &Command) -> RespValue {
    match cmd {
        Command::SMove(source, destination, member) => {
                RespValue::Array(vec![
                    bulk("SMOVE"),
                    bulk(source),
                    bulk(destination),
                    bulk_bytes(member),
                ])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::SInterStore 序列化为 RESP 数组
///
/// 对应 Redis 命令: SINTERSTORE destination key [key ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::SInterStore 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::SInterStore 变体，将触发 unreachable!()
pub(crate) fn to_resp_s_inter_store(cmd: &Command) -> RespValue {
    match cmd {
        Command::SInterStore(destination, keys) => {
                let mut parts = vec![bulk("SINTERSTORE"), bulk(destination)];
                for key in keys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::SUnionStore 序列化为 RESP 数组
///
/// 对应 Redis 命令: SUNIONSTORE destination key [key ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::SUnionStore 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::SUnionStore 变体，将触发 unreachable!()
pub(crate) fn to_resp_s_union_store(cmd: &Command) -> RespValue {
    match cmd {
        Command::SUnionStore(destination, keys) => {
                let mut parts = vec![bulk("SUNIONSTORE"), bulk(destination)];
                for key in keys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::SDiffStore 序列化为 RESP 数组
///
/// 对应 Redis 命令: SDIFFSTORE destination key [key ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::SDiffStore 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::SDiffStore 变体，将触发 unreachable!()
pub(crate) fn to_resp_s_diff_store(cmd: &Command) -> RespValue {
    match cmd {
        Command::SDiffStore(destination, keys) => {
                let mut parts = vec![bulk("SDIFFSTORE"), bulk(destination)];
                for key in keys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::SScan 序列化为 RESP 数组
///
/// 对应 Redis 命令: SSCAN key cursor [MATCH pattern] [COUNT count]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::SScan 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::SScan 变体，将触发 unreachable!()
pub(crate) fn to_resp_s_scan(cmd: &Command) -> RespValue {
    match cmd {
        Command::SScan(key, cursor, pattern, count) => {
                let mut parts = vec![
                    bulk("SSCAN"),
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

