use super::*;

use crate::protocol::RespValue;

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

pub(crate) fn to_resp_z_score(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZScore(key, member) => {
                RespValue::Array(vec![bulk("ZSCORE"), bulk(key), bulk(member)])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_z_rank(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZRank(key, member) => {
                RespValue::Array(vec![bulk("ZRANK"), bulk(key), bulk(member)])
        }
        _ => unreachable!(),
    }
}

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

pub(crate) fn to_resp_z_card(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZCard(key) => {
                RespValue::Array(vec![bulk("ZCARD"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

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

pub(crate) fn to_resp_z_rev_rank(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZRevRank(key, member) => {
                RespValue::Array(vec![bulk("ZREVRANK"), bulk(key), bulk(member)])
        }
        _ => unreachable!(),
    }
}

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

pub(crate) fn to_resp_z_range_by_lex(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZRangeByLex(key, min, max) => {
                RespValue::Array(vec![bulk("ZRANGEBYLEX"), bulk(key), bulk(min), bulk(max)])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_z_rem_range_by_lex(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZRemRangeByLex(key, min, max) => {
                RespValue::Array(vec![bulk("ZREMRANGEBYLEX"), bulk(key), bulk(min), bulk(max)])
        }
        _ => unreachable!(),
    }
}

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

pub(crate) fn to_resp_z_lex_count(cmd: &Command) -> RespValue {
    match cmd {
        Command::ZLexCount(key, min, max) => {
                RespValue::Array(vec![bulk("ZLEXCOUNT"), bulk(key), bulk(min), bulk(max)])
        }
        _ => unreachable!(),
    }
}

