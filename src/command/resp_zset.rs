//! Sorted Set 命令 RESP 序列化
use super::*;

use crate::protocol::RespValue;

/// 将 Command::ZAdd 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZADD key score member [score member ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZAdd 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZAdd 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_add(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZAdd(key, pairs) => {
                let mut parts = vec![bulk("ZADD"), bulk(key)];
                for (score, member) in pairs {
                    parts.push(bulk(&score.to_string()));
                    parts.push(bulk(member));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZRem 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZREM key member [member ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZRem 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZRem 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_rem(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZRem(key, members) => {
                let mut parts = vec![bulk("ZREM"), bulk(key)];
                for member in members {
                    parts.push(bulk(member));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZScore 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZSCORE key member
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZScore 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZScore 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_score(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZScore(key, member) => {
                RespValue::Array(vec![bulk("ZSCORE"), bulk(key), bulk(member)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZRank 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZRANK key member
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZRank 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZRank 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_rank(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZRank(key, member) => {
                RespValue::Array(vec![bulk("ZRANK"), bulk(key), bulk(member)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZRange 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZRANGE key start stop [WITHSCORES]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZRange 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZRange 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_range(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZRange(key, start, stop, with_scores) => {
                let mut parts = vec![
                    bulk("ZRANGE"),
                    bulk(key),
                    bulk(&start.to_string()),
                    bulk(&stop.to_string()),
                ];
                if *with_scores {
                    parts.push(bulk("WITHSCORES"));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZRangeByScore 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZRANGEBYSCORE key min max [WITHSCORES]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZRangeByScore 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZRangeByScore 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_range_by_score(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZRangeByScore(key, min, max, with_scores) => {
                let mut parts = vec![
                    bulk("ZRANGEBYSCORE"),
                    bulk(key),
                    bulk(&min.to_string()),
                    bulk(&max.to_string()),
                ];
                if *with_scores {
                    parts.push(bulk("WITHSCORES"));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZCard 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZCARD key
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZCard 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZCard 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_card(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZCard(key) => {
                RespValue::Array(vec![bulk("ZCARD"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZRevRange 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZREVRANGE key start stop [WITHSCORES]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZRevRange 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZRevRange 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_rev_range(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZRevRange(key, start, stop, with_scores) => {
                let mut parts = vec![
                    bulk("ZREVRANGE"),
                    bulk(key),
                    bulk(&start.to_string()),
                    bulk(&stop.to_string()),
                ];
                if *with_scores {
                    parts.push(bulk("WITHSCORES"));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZRevRank 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZREVRANK key member
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZRevRank 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZRevRank 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_rev_rank(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZRevRank(key, member) => {
                RespValue::Array(vec![bulk("ZREVRANK"), bulk(key), bulk(member)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZIncrBy 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZINCRBY key increment member
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZIncrBy 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZIncrBy 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_incr_by(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZIncrBy(key, increment, member) => {
                RespValue::Array(vec![
                    bulk("ZINCRBY"),
                    bulk(key),
                    bulk(&increment.to_string()),
                    bulk(member),
                ])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZCount 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZCOUNT key min max
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZCount 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZCount 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_count(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZCount(key, min, max) => {
                RespValue::Array(vec![
                    bulk("ZCOUNT"),
                    bulk(key),
                    bulk(&min.to_string()),
                    bulk(&max.to_string()),
                ])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZPopMin 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZPOPMIN key [count]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZPopMin 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZPopMin 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_pop_min(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZPopMin(key, count) => {
                let mut parts = vec![bulk("ZPOPMIN"), bulk(key)];
                if *count > 1 {
                    parts.push(bulk(&count.to_string()));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZPopMax 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZPOPMAX key [count]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZPopMax 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZPopMax 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_pop_max(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZPopMax(key, count) => {
                let mut parts = vec![bulk("ZPOPMAX"), bulk(key)];
                if *count > 1 {
                    parts.push(bulk(&count.to_string()));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZUnionStore 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZUnionStore 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZUnionStore 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_union_store(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZUnionStore(destination, keys, weights, aggregate) => {
                let mut parts = vec![
                    bulk("ZUNIONSTORE"),
                    bulk(destination),
                    bulk(&keys.len().to_string()),
                ];
                for key in keys {
                    parts.push(bulk(key));
                }
                if !weights.is_empty() {
                    parts.push(bulk("WEIGHTS"));
                    for w in weights {
                        parts.push(bulk(&w.to_string()));
                    }
                }
                if aggregate != "SUM" {
                    parts.push(bulk("AGGREGATE"));
                    parts.push(bulk(aggregate));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZInterStore 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZInterStore 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZInterStore 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_inter_store(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZInterStore(destination, keys, weights, aggregate) => {
                let mut parts = vec![
                    bulk("ZINTERSTORE"),
                    bulk(destination),
                    bulk(&keys.len().to_string()),
                ];
                for key in keys {
                    parts.push(bulk(key));
                }
                if !weights.is_empty() {
                    parts.push(bulk("WEIGHTS"));
                    for w in weights {
                        parts.push(bulk(&w.to_string()));
                    }
                }
                if aggregate != "SUM" {
                    parts.push(bulk("AGGREGATE"));
                    parts.push(bulk(aggregate));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZScan 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZSCAN key cursor [MATCH pattern] [COUNT count]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZScan 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZScan 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_scan(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZScan(key, cursor, pattern, count) => {
                let mut parts = vec![
                    bulk("ZSCAN"),
                    bulk(key),
                    bulk(&cursor.to_string()),
                ];
                if !pattern.is_empty() && pattern != "*" {
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

/// 将 Command::ZRangeByLex 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZRANGEBYLEX key min max [LIMIT offset count]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZRangeByLex 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZRangeByLex 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_range_by_lex(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZRangeByLex(key, min, max) => {
                RespValue::Array(vec![bulk("ZRANGEBYLEX"), bulk(key), bulk(min), bulk(max)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZRemRangeByLex 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZREMRANGEBYLEX key min max
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZRemRangeByLex 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZRemRangeByLex 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_rem_range_by_lex(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZRemRangeByLex(key, min, max) => {
                RespValue::Array(vec![bulk("ZREMRANGEBYLEX"), bulk(key), bulk(min), bulk(max)])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZRemRangeByRank 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZREMRANGEBYRANK key start stop
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZRemRangeByRank 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZRemRangeByRank 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_rem_range_by_rank(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZRemRangeByRank(key, start, stop) => {
                RespValue::Array(vec![
                    bulk("ZREMRANGEBYRANK"),
                    bulk(key),
                    bulk(&start.to_string()),
                    bulk(&stop.to_string()),
                ])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZRemRangeByScore 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZREMRANGEBYSCORE key min max
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZRemRangeByScore 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZRemRangeByScore 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_rem_range_by_score(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZRemRangeByScore(key, min, max) => {
                RespValue::Array(vec![
                    bulk("ZREMRANGEBYSCORE"),
                    bulk(key),
                    bulk(&min.to_string()),
                    bulk(&max.to_string()),
                ])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZRandMember 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZRANDMEMBER key [count [WITHSCORES]]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZRandMember 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZRandMember 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_rand_member(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZRandMember(key, count, with_scores) => {
                let mut parts = vec![bulk("ZRANDMEMBER"), bulk(key)];
                if *count != 1 || *with_scores {
                    parts.push(bulk(&count.to_string()));
                }
                if *with_scores {
                    parts.push(bulk("WITHSCORES"));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZDiff 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZDIFF key [key ...] [WITHSCORES]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZDiff 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZDiff 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_diff(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZDiff(keys, with_scores) => {
                let mut parts = vec![bulk("ZDIFF"), bulk(&keys.len().to_string())];
                for key in keys {
                    parts.push(bulk(key));
                }
                if *with_scores {
                    parts.push(bulk("WITHSCORES"));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZDiffStore 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZDIFFSTORE destination numkeys key [key ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZDiffStore 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZDiffStore 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_diff_store(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZDiffStore(destination, keys) => {
                let mut parts = vec![bulk("ZDIFFSTORE"), bulk(destination), bulk(&keys.len().to_string())];
                for key in keys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZInter 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZINTER numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZInter 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZInter 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_inter(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZInter(keys, weights, aggregate, with_scores) => {
                let mut parts = vec![bulk("ZINTER"), bulk(&keys.len().to_string())];
                for key in keys {
                    parts.push(bulk(key));
                }
                if !weights.is_empty() {
                    parts.push(bulk("WEIGHTS"));
                    for w in weights {
                        parts.push(bulk(&w.to_string()));
                    }
                }
                if aggregate != "SUM" {
                    parts.push(bulk("AGGREGATE"));
                    parts.push(bulk(aggregate));
                }
                if *with_scores {
                    parts.push(bulk("WITHSCORES"));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZUnion 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZUNION numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZUnion 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZUnion 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_union(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZUnion(keys, weights, aggregate, with_scores) => {
                let mut parts = vec![bulk("ZUNION"), bulk(&keys.len().to_string())];
                for key in keys {
                    parts.push(bulk(key));
                }
                if !weights.is_empty() {
                    parts.push(bulk("WEIGHTS"));
                    for w in weights {
                        parts.push(bulk(&w.to_string()));
                    }
                }
                if aggregate != "SUM" {
                    parts.push(bulk("AGGREGATE"));
                    parts.push(bulk(aggregate));
                }
                if *with_scores {
                    parts.push(bulk("WITHSCORES"));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZRangeStore 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZRangeStore 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZRangeStore 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_range_store(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZRangeStore(dst, src, min, max, by_score, by_lex, rev, limit_offset, limit_count) => {
                let mut parts = vec![bulk("ZRANGESTORE"), bulk(dst), bulk(src), bulk(min), bulk(max)];
                if *by_score {
                    parts.push(bulk("BYSCORE"));
                }
                if *by_lex {
                    parts.push(bulk("BYLEX"));
                }
                if *rev {
                    parts.push(bulk("REV"));
                }
                if *limit_count > 0 {
                    parts.push(bulk("LIMIT"));
                    parts.push(bulk(&limit_offset.to_string()));
                    parts.push(bulk(&limit_count.to_string()));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZMpop 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZMpop 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZMpop 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_mpop(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZMpop(keys, min_or_max, count) => {
                let mut parts = vec![bulk("ZMPOP"), bulk(&keys.len().to_string())];
                for key in keys {
                    parts.push(bulk(key));
                }
                parts.push(bulk(if *min_or_max { "MIN" } else { "MAX" }));
                if *count > 1 {
                    parts.push(bulk("COUNT"));
                    parts.push(bulk(&count.to_string()));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::BZMpop 序列化为 RESP 数组
///
/// 对应 Redis 命令: BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::BZMpop 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::BZMpop 变体，将触发 unreachable!()
pub(crate) fn to_resp_b_z_mpop(cmd: &Command) -> RespValue {
    match cmd {
        Command::BZMpop(timeout, keys, min_or_max, count) => {
                let mut parts = vec![bulk("BZMPOP"), bulk(&timeout.to_string()), bulk(&keys.len().to_string())];
                for key in keys {
                    parts.push(bulk(key));
                }
                parts.push(bulk(if *min_or_max { "MIN" } else { "MAX" }));
                if *count > 1 {
                    parts.push(bulk("COUNT"));
                    parts.push(bulk(&count.to_string()));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::BZPopMin 序列化为 RESP 数组
///
/// 对应 Redis 命令: BZPOPMIN key [key ...] timeout
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::BZPopMin 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::BZPopMin 变体，将触发 unreachable!()
pub(crate) fn to_resp_b_z_pop_min(cmd: &Command) -> RespValue {
    match cmd {
        Command::BZPopMin(keys, timeout) => {
                let mut parts = vec![bulk("BZPOPMIN")];
                for key in keys {
                    parts.push(bulk(key));
                }
                parts.push(bulk(&timeout.to_string()));
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::BZPopMax 序列化为 RESP 数组
///
/// 对应 Redis 命令: BZPOPMAX key [key ...] timeout
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::BZPopMax 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::BZPopMax 变体，将触发 unreachable!()
pub(crate) fn to_resp_b_z_pop_max(cmd: &Command) -> RespValue {
    match cmd {
        Command::BZPopMax(keys, timeout) => {
                let mut parts = vec![bulk("BZPOPMAX")];
                for key in keys {
                    parts.push(bulk(key));
                }
                parts.push(bulk(&timeout.to_string()));
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZRevRangeByScore 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZRevRangeByScore 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZRevRangeByScore 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_rev_range_by_score(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZRevRangeByScore(key, max, min, with_scores, limit_offset, limit_count) => {
                let mut parts = vec![bulk("ZREVRANGEBYSCORE"), bulk(key), bulk(&max.to_string()), bulk(&min.to_string())];
                if *with_scores {
                    parts.push(bulk("WITHSCORES"));
                }
                if *limit_count > 0 {
                    parts.push(bulk("LIMIT"));
                    parts.push(bulk(&limit_offset.to_string()));
                    parts.push(bulk(&limit_count.to_string()));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZRevRangeByLex 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZREVRANGEBYLEX key max min [LIMIT offset count]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZRevRangeByLex 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZRevRangeByLex 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_rev_range_by_lex(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZRevRangeByLex(key, max, min, limit_offset, limit_count) => {
                let mut parts = vec![bulk("ZREVRANGEBYLEX"), bulk(key), bulk(max), bulk(min)];
                if *limit_count > 0 {
                    parts.push(bulk("LIMIT"));
                    parts.push(bulk(&limit_offset.to_string()));
                    parts.push(bulk(&limit_count.to_string()));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZMScore 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZMSCORE key member [member ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZMScore 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZMScore 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_m_score(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZMScore(key, members) => {
                let mut parts = vec![bulk("ZMSCORE"), bulk(key)];
                for m in members {
                    parts.push(bulk(m));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::ZLexCount 序列化为 RESP 数组
///
/// 对应 Redis 命令: ZLEXCOUNT key min max
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::ZLexCount 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::ZLexCount 变体，将触发 unreachable!()
pub(crate) fn to_resp_z_lex_count(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZLexCount(key, min, max) => {
                RespValue::Array(vec![bulk("ZLEXCOUNT"), bulk(key), bulk(min), bulk(max)])
        }
        _ => unreachable!(),
    }
}

