use super::*;

use bytes::Bytes;
use crate::protocol::RespValue;

impl Command {
    /// 将命令转换为 RESP 值，用于 AOF 持久化
    pub fn to_resp_value(&self) -> RespValue {
        match self {
            Command::Ping(msg) => {
                let mut parts = vec![bulk("PING")];
                if let Some(m) = msg {
                    parts.push(bulk(m));
                }
                RespValue::Array(parts)
            }
            Command::Get(key) => {
                RespValue::Array(vec![bulk("GET"), bulk(key)])
            }
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
            Command::SetEx(key, value, ttl_ms) => {
                RespValue::Array(vec![
                    bulk("SET"),
                    bulk(key),
                    bulk_bytes(value),
                    bulk("PX"),
                    bulk(&ttl_ms.to_string()),
                ])
            }
            Command::Del(keys) => {
                let mut parts = vec![bulk("DEL")];
                for key in keys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
            }
            Command::Exists(keys) => {
                let mut parts = vec![bulk("EXISTS")];
                for key in keys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
            }
            Command::FlushAll => {
                RespValue::Array(vec![bulk("FLUSHALL")])
            }
            Command::Expire(key, seconds) => {
                RespValue::Array(vec![
                    bulk("EXPIRE"),
                    bulk(key),
                    bulk(&seconds.to_string()),
                ])
            }
            Command::Ttl(key) => {
                RespValue::Array(vec![bulk("TTL"), bulk(key)])
            }
            Command::ConfigGet(key) => {
                RespValue::Array(vec![bulk("CONFIG"), bulk("GET"), bulk(key)])
            }
            Command::ConfigSet(key, value) => {
                RespValue::Array(vec![bulk("CONFIG"), bulk("SET"), bulk(key), bulk(value)])
            }
            Command::ConfigRewrite => {
                RespValue::Array(vec![bulk("CONFIG"), bulk("REWRITE")])
            }
            Command::ConfigResetStat => {
                RespValue::Array(vec![bulk("CONFIG"), bulk("RESETSTAT")])
            }
            Command::MemoryUsage(key, samples) => {
                let mut parts = vec![bulk("MEMORY"), bulk("USAGE"), bulk(key)];
                if let Some(s) = samples {
                    parts.push(bulk("SAMPLES"));
                    parts.push(bulk(&s.to_string()));
                }
                RespValue::Array(parts)
            }
            Command::MemoryDoctor => {
                RespValue::Array(vec![bulk("MEMORY"), bulk("DOCTOR")])
            }
            Command::LatencyLatest => {
                RespValue::Array(vec![bulk("LATENCY"), bulk("LATEST")])
            }
            Command::LatencyHistory(event) => {
                RespValue::Array(vec![bulk("LATENCY"), bulk("HISTORY"), bulk(event)])
            }
            Command::LatencyReset(events) => {
                let mut parts = vec![bulk("LATENCY"), bulk("RESET")];
                for e in events {
                    parts.push(bulk(e));
                }
                RespValue::Array(parts)
            }
            Command::Reset => {
                RespValue::Array(vec![bulk("RESET")])
            }
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
            Command::Monitor => {
                RespValue::Array(vec![bulk("MONITOR")])
            }
            Command::CommandInfo => {
                RespValue::Array(vec![bulk("COMMAND")])
            }
            Command::MGet(keys) => {
                let mut parts = vec![bulk("MGET")];
                for key in keys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
            }
            Command::MSet(pairs) => {
                let mut parts = vec![bulk("MSET")];
                for (key, value) in pairs {
                    parts.push(bulk(key));
                    parts.push(bulk_bytes(value));
                }
                RespValue::Array(parts)
            }
            Command::Incr(key) => {
                RespValue::Array(vec![bulk("INCR"), bulk(key)])
            }
            Command::Decr(key) => {
                RespValue::Array(vec![bulk("DECR"), bulk(key)])
            }
            Command::IncrBy(key, delta) => {
                RespValue::Array(vec![
                    bulk("INCRBY"),
                    bulk(key),
                    bulk(&delta.to_string()),
                ])
            }
            Command::DecrBy(key, delta) => {
                RespValue::Array(vec![
                    bulk("DECRBY"),
                    bulk(key),
                    bulk(&delta.to_string()),
                ])
            }
            Command::Append(key, value) => {
                RespValue::Array(vec![
                    bulk("APPEND"),
                    bulk(key),
                    bulk_bytes(value),
                ])
            }
            Command::SetNx(key, value) => {
                RespValue::Array(vec![
                    bulk("SETNX"),
                    bulk(key),
                    bulk_bytes(value),
                ])
            }
            Command::SetExCmd(key, value, seconds) => {
                RespValue::Array(vec![
                    bulk("SETEX"),
                    bulk(key),
                    bulk(&seconds.to_string()),
                    bulk_bytes(value),
                ])
            }
            Command::PSetEx(key, value, ms) => {
                RespValue::Array(vec![
                    bulk("PSETEX"),
                    bulk(key),
                    bulk(&ms.to_string()),
                    bulk_bytes(value),
                ])
            }
            Command::GetSet(key, value) => {
                RespValue::Array(vec![
                    bulk("GETSET"),
                    bulk(key),
                    bulk_bytes(value),
                ])
            }
            Command::GetDel(key) => {
                RespValue::Array(vec![bulk("GETDEL"), bulk(key)])
            }
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
            Command::MSetNx(pairs) => {
                let mut parts = vec![bulk("MSETNX")];
                for (key, value) in pairs {
                    parts.push(bulk(key));
                    parts.push(bulk_bytes(value));
                }
                RespValue::Array(parts)
            }
            Command::IncrByFloat(key, delta) => {
                RespValue::Array(vec![
                    bulk("INCRBYFLOAT"),
                    bulk(key),
                    bulk(&format!("{}", delta)),
                ])
            }
            Command::SetRange(key, offset, value) => {
                RespValue::Array(vec![
                    bulk("SETRANGE"),
                    bulk(key),
                    bulk(&offset.to_string()),
                    bulk_bytes(value),
                ])
            }
            Command::GetRange(key, start, end) => {
                RespValue::Array(vec![
                    bulk("GETRANGE"),
                    bulk(key),
                    bulk(&start.to_string()),
                    bulk(&end.to_string()),
                ])
            }
            Command::StrLen(key) => {
                RespValue::Array(vec![bulk("STRLEN"), bulk(key)])
            }
            Command::LPush(key, values) => {
                let mut parts = vec![bulk("LPUSH"), bulk(key)];
                for v in values {
                    parts.push(bulk_bytes(&v));
                }
                RespValue::Array(parts)
            }
            Command::RPush(key, values) => {
                let mut parts = vec![bulk("RPUSH"), bulk(key)];
                for v in values {
                    parts.push(bulk_bytes(&v));
                }
                RespValue::Array(parts)
            }
            Command::LPop(key) => {
                RespValue::Array(vec![bulk("LPOP"), bulk(key)])
            }
            Command::RPop(key) => {
                RespValue::Array(vec![bulk("RPOP"), bulk(key)])
            }
            Command::LLen(key) => {
                RespValue::Array(vec![bulk("LLEN"), bulk(key)])
            }
            Command::LRange(key, start, stop) => {
                RespValue::Array(vec![
                    bulk("LRANGE"),
                    bulk(key),
                    bulk(&start.to_string()),
                    bulk(&stop.to_string()),
                ])
            }
            Command::LIndex(key, index) => {
                RespValue::Array(vec![
                    bulk("LINDEX"),
                    bulk(key),
                    bulk(&index.to_string()),
                ])
            }
            Command::LSet(key, index, value) => {
                RespValue::Array(vec![
                    bulk("LSET"),
                    bulk(key),
                    bulk(&index.to_string()),
                    bulk_bytes(value),
                ])
            }
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
            Command::LRem(key, count, value) => {
                RespValue::Array(vec![
                    bulk("LREM"),
                    bulk(key),
                    bulk(&count.to_string()),
                    bulk_bytes(value),
                ])
            }
            Command::LTrim(key, start, stop) => {
                RespValue::Array(vec![
                    bulk("LTRIM"),
                    bulk(key),
                    bulk(&start.to_string()),
                    bulk(&stop.to_string()),
                ])
            }
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
            Command::BLPop(keys, timeout) => {
                let mut parts = vec![bulk("BLPOP")];
                for key in keys {
                    parts.push(bulk(key));
                }
                parts.push(bulk(&timeout.to_string()));
                RespValue::Array(parts)
            }
            Command::BRPop(keys, timeout) => {
                let mut parts = vec![bulk("BRPOP")];
                for key in keys {
                    parts.push(bulk(key));
                }
                parts.push(bulk(&timeout.to_string()));
                RespValue::Array(parts)
            }
            Command::HSet(key, pairs) => {
                let mut parts = vec![bulk("HSET"), bulk(key)];
                for (field, value) in pairs {
                    parts.push(bulk(field));
                    parts.push(bulk_bytes(value));
                }
                RespValue::Array(parts)
            }
            Command::HGet(key, field) => {
                RespValue::Array(vec![bulk("HGET"), bulk(key), bulk(field)])
            }
            Command::HDel(key, fields) => {
                let mut parts = vec![bulk("HDEL"), bulk(key)];
                for field in fields {
                    parts.push(bulk(field));
                }
                RespValue::Array(parts)
            }
            Command::HExists(key, field) => {
                RespValue::Array(vec![bulk("HEXISTS"), bulk(key), bulk(field)])
            }
            Command::HGetAll(key) => {
                RespValue::Array(vec![bulk("HGETALL"), bulk(key)])
            }
            Command::HLen(key) => {
                RespValue::Array(vec![bulk("HLEN"), bulk(key)])
            }
            Command::HMSet(key, pairs) => {
                let mut parts = vec![bulk("HMSET"), bulk(key)];
                for (field, value) in pairs {
                    parts.push(bulk(field));
                    parts.push(bulk_bytes(value));
                }
                RespValue::Array(parts)
            }
            Command::HMGet(key, fields) => {
                let mut parts = vec![bulk("HMGET"), bulk(key)];
                for field in fields {
                    parts.push(bulk(field));
                }
                RespValue::Array(parts)
            }
            Command::HIncrBy(key, field, delta) => {
                RespValue::Array(vec![
                    bulk("HINCRBY"),
                    bulk(key),
                    bulk(field),
                    bulk(&delta.to_string()),
                ])
            }
            Command::HIncrByFloat(key, field, delta) => {
                RespValue::Array(vec![
                    bulk("HINCRBYFLOAT"),
                    bulk(key),
                    bulk(field),
                    bulk(&format!("{}", delta)),
                ])
            }
            Command::HKeys(key) => {
                RespValue::Array(vec![bulk("HKEYS"), bulk(key)])
            }
            Command::HVals(key) => {
                RespValue::Array(vec![bulk("HVALS"), bulk(key)])
            }
            Command::HSetNx(key, field, value) => {
                RespValue::Array(vec![
                    bulk("HSETNX"),
                    bulk(key),
                    bulk(field),
                    bulk_bytes(value),
                ])
            }
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
            Command::HExpire(key, fields, seconds) => {
                let mut parts = vec![bulk("HEXPIRE"), bulk(key)];
                for f in fields {
                    parts.push(bulk(f));
                }
                parts.push(bulk(&seconds.to_string()));
                RespValue::Array(parts)
            }
            Command::HPExpire(key, fields, ms) => {
                let mut parts = vec![bulk("HPEXPIRE"), bulk(key)];
                for f in fields {
                    parts.push(bulk(f));
                }
                parts.push(bulk(&ms.to_string()));
                RespValue::Array(parts)
            }
            Command::HExpireAt(key, fields, ts) => {
                let mut parts = vec![bulk("HEXPIREAT"), bulk(key)];
                for f in fields {
                    parts.push(bulk(f));
                }
                parts.push(bulk(&ts.to_string()));
                RespValue::Array(parts)
            }
            Command::HPExpireAt(key, fields, ts) => {
                let mut parts = vec![bulk("HPEXPIREAT"), bulk(key)];
                for f in fields {
                    parts.push(bulk(f));
                }
                parts.push(bulk(&ts.to_string()));
                RespValue::Array(parts)
            }
            Command::HTtl(key, fields) => {
                let mut parts = vec![bulk("HTTL"), bulk(key)];
                for f in fields {
                    parts.push(bulk(f));
                }
                RespValue::Array(parts)
            }
            Command::HPTtl(key, fields) => {
                let mut parts = vec![bulk("HPTTL"), bulk(key)];
                for f in fields {
                    parts.push(bulk(f));
                }
                RespValue::Array(parts)
            }
            Command::HExpireTime(key, fields) => {
                let mut parts = vec![bulk("HEXPIRETIME"), bulk(key)];
                for f in fields {
                    parts.push(bulk(f));
                }
                RespValue::Array(parts)
            }
            Command::HPExpireTime(key, fields) => {
                let mut parts = vec![bulk("HPEXPIRETIME"), bulk(key)];
                for f in fields {
                    parts.push(bulk(f));
                }
                RespValue::Array(parts)
            }
            Command::HPersist(key, fields) => {
                let mut parts = vec![bulk("HPERSIST"), bulk(key)];
                for f in fields {
                    parts.push(bulk(f));
                }
                RespValue::Array(parts)
            }
            Command::SAdd(key, members) => {
                let mut parts = vec![bulk("SADD"), bulk(key)];
                for m in members {
                    parts.push(bulk_bytes(&m));
                }
                RespValue::Array(parts)
            }
            Command::SRem(key, members) => {
                let mut parts = vec![bulk("SREM"), bulk(key)];
                for m in members {
                    parts.push(bulk_bytes(&m));
                }
                RespValue::Array(parts)
            }
            Command::SMembers(key) => {
                RespValue::Array(vec![bulk("SMEMBERS"), bulk(key)])
            }
            Command::SIsMember(key, member) => {
                RespValue::Array(vec![
                    bulk("SISMEMBER"),
                    bulk(key),
                    bulk_bytes(member),
                ])
            }
            Command::SCard(key) => {
                RespValue::Array(vec![bulk("SCARD"), bulk(key)])
            }
            Command::SInter(keys) => {
                let mut parts = vec![bulk("SINTER")];
                for key in keys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
            }
            Command::SUnion(keys) => {
                let mut parts = vec![bulk("SUNION")];
                for key in keys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
            }
            Command::SDiff(keys) => {
                let mut parts = vec![bulk("SDIFF")];
                for key in keys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
            }
            Command::SPop(key, count) => {
                let mut parts = vec![bulk("SPOP"), bulk(key)];
                if *count != 1 {
                    parts.push(bulk(&count.to_string()));
                }
                RespValue::Array(parts)
            }
            Command::SRandMember(key, count) => {
                let mut parts = vec![bulk("SRANDMEMBER"), bulk(key)];
                if *count != 1 {
                    parts.push(bulk(&count.to_string()));
                }
                RespValue::Array(parts)
            }
            Command::SMove(source, destination, member) => {
                RespValue::Array(vec![
                    bulk("SMOVE"),
                    bulk(source),
                    bulk(destination),
                    bulk_bytes(member),
                ])
            }
            Command::SInterStore(destination, keys) => {
                let mut parts = vec![bulk("SINTERSTORE"), bulk(destination)];
                for key in keys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
            }
            Command::SUnionStore(destination, keys) => {
                let mut parts = vec![bulk("SUNIONSTORE"), bulk(destination)];
                for key in keys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
            }
            Command::SDiffStore(destination, keys) => {
                let mut parts = vec![bulk("SDIFFSTORE"), bulk(destination)];
                for key in keys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
            }
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
            Command::ZAdd(key, pairs) => {
                let mut parts = vec![bulk("ZADD"), bulk(key)];
                for (score, member) in pairs {
                    parts.push(bulk(&score.to_string()));
                    parts.push(bulk(member));
                }
                RespValue::Array(parts)
            }
            Command::ZRem(key, members) => {
                let mut parts = vec![bulk("ZREM"), bulk(key)];
                for member in members {
                    parts.push(bulk(member));
                }
                RespValue::Array(parts)
            }
            Command::ZScore(key, member) => {
                RespValue::Array(vec![bulk("ZSCORE"), bulk(key), bulk(member)])
            }
            Command::ZRank(key, member) => {
                RespValue::Array(vec![bulk("ZRANK"), bulk(key), bulk(member)])
            }
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
            Command::ZCard(key) => {
                RespValue::Array(vec![bulk("ZCARD"), bulk(key)])
            }
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
            Command::ZRevRank(key, member) => {
                RespValue::Array(vec![bulk("ZREVRANK"), bulk(key), bulk(member)])
            }
            Command::ZIncrBy(key, increment, member) => {
                RespValue::Array(vec![
                    bulk("ZINCRBY"),
                    bulk(key),
                    bulk(&increment.to_string()),
                    bulk(member),
                ])
            }
            Command::ZCount(key, min, max) => {
                RespValue::Array(vec![
                    bulk("ZCOUNT"),
                    bulk(key),
                    bulk(&min.to_string()),
                    bulk(&max.to_string()),
                ])
            }
            Command::ZPopMin(key, count) => {
                let mut parts = vec![bulk("ZPOPMIN"), bulk(key)];
                if *count > 1 {
                    parts.push(bulk(&count.to_string()));
                }
                RespValue::Array(parts)
            }
            Command::ZPopMax(key, count) => {
                let mut parts = vec![bulk("ZPOPMAX"), bulk(key)];
                if *count > 1 {
                    parts.push(bulk(&count.to_string()));
                }
                RespValue::Array(parts)
            }
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
            Command::ZRangeByLex(key, min, max) => {
                RespValue::Array(vec![bulk("ZRANGEBYLEX"), bulk(key), bulk(min), bulk(max)])
            }
            Command::SInterCard(keys, limit) => {
                let mut parts = vec![bulk("SINTERCARD"), bulk(&keys.len().to_string())];
                for key in keys {
                    parts.push(bulk(key));
                }
                if *limit > 0 {
                    parts.push(bulk("LIMIT"));
                    parts.push(bulk(&limit.to_string()));
                }
                RespValue::Array(parts)
            }
            Command::SMisMember(key, members) => {
                let mut parts = vec![bulk("SMISMEMBER"), bulk(key)];
                for m in members {
                    parts.push(bulk_bytes(m));
                }
                RespValue::Array(parts)
            }
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
            Command::ZDiffStore(destination, keys) => {
                let mut parts = vec![bulk("ZDIFFSTORE"), bulk(destination), bulk(&keys.len().to_string())];
                for key in keys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
            }
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
            Command::BZPopMin(keys, timeout) => {
                let mut parts = vec![bulk("BZPOPMIN")];
                for key in keys {
                    parts.push(bulk(key));
                }
                parts.push(bulk(&timeout.to_string()));
                RespValue::Array(parts)
            }
            Command::BZPopMax(keys, timeout) => {
                let mut parts = vec![bulk("BZPOPMAX")];
                for key in keys {
                    parts.push(bulk(key));
                }
                parts.push(bulk(&timeout.to_string()));
                RespValue::Array(parts)
            }
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
            Command::ZRevRangeByLex(key, max, min, limit_offset, limit_count) => {
                let mut parts = vec![bulk("ZREVRANGEBYLEX"), bulk(key), bulk(max), bulk(min)];
                if *limit_count > 0 {
                    parts.push(bulk("LIMIT"));
                    parts.push(bulk(&limit_offset.to_string()));
                    parts.push(bulk(&limit_count.to_string()));
                }
                RespValue::Array(parts)
            }
            Command::ZMScore(key, members) => {
                let mut parts = vec![bulk("ZMSCORE"), bulk(key)];
                for m in members {
                    parts.push(bulk(m));
                }
                RespValue::Array(parts)
            }
            Command::ZLexCount(key, min, max) => {
                RespValue::Array(vec![bulk("ZLEXCOUNT"), bulk(key), bulk(min), bulk(max)])
            }
            Command::ZRangeUnified(key, min, max, by_score, by_lex, rev, with_scores, limit_offset, limit_count) => {
                let mut parts = vec![bulk("ZRANGE"), bulk(key), bulk(min), bulk(max)];
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
                if *with_scores {
                    parts.push(bulk("WITHSCORES"));
                }
                RespValue::Array(parts)
            }
            Command::Keys(pattern) => {
                RespValue::Array(vec![bulk("KEYS"), bulk(pattern)])
            }
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
            Command::Rename(key, newkey) => {
                RespValue::Array(vec![bulk("RENAME"), bulk(key), bulk(newkey)])
            }
            Command::Type(key) => {
                RespValue::Array(vec![bulk("TYPE"), bulk(key)])
            }
            Command::Persist(key) => {
                RespValue::Array(vec![bulk("PERSIST"), bulk(key)])
            }
            Command::PExpire(key, ms) => {
                RespValue::Array(vec![bulk("PEXPIRE"), bulk(key), bulk(&ms.to_string())])
            }
            Command::PTtl(key) => {
                RespValue::Array(vec![bulk("PTTL"), bulk(key)])
            }
            Command::DbSize => {
                RespValue::Array(vec![bulk("DBSIZE")])
            }
            Command::Info(section) => {
                match section {
                    Some(s) => RespValue::Array(vec![bulk("INFO"), bulk(s)]),
                    None => RespValue::Array(vec![bulk("INFO")]),
                }
            }
            Command::Subscribe(channels) => {
                let mut parts = vec![bulk("SUBSCRIBE")];
                for ch in channels {
                    parts.push(bulk(ch));
                }
                RespValue::Array(parts)
            }
            Command::Unsubscribe(channels) => {
                let mut parts = vec![bulk("UNSUBSCRIBE")];
                for ch in channels {
                    parts.push(bulk(ch));
                }
                RespValue::Array(parts)
            }
            Command::Publish(channel, message) => {
                RespValue::Array(vec![
                    bulk("PUBLISH"),
                    bulk(channel),
                    bulk_bytes(message),
                ])
            }
            Command::PSubscribe(patterns) => {
                let mut parts = vec![bulk("PSUBSCRIBE")];
                for pat in patterns {
                    parts.push(bulk(pat));
                }
                RespValue::Array(parts)
            }
            Command::PUnsubscribe(patterns) => {
                let mut parts = vec![bulk("PUNSUBSCRIBE")];
                for pat in patterns {
                    parts.push(bulk(pat));
                }
                RespValue::Array(parts)
            }
            Command::Multi => {
                RespValue::Array(vec![bulk("MULTI")])
            }
            Command::Exec => {
                RespValue::Array(vec![bulk("EXEC")])
            }
            Command::Discard => {
                RespValue::Array(vec![bulk("DISCARD")])
            }
            Command::Watch(keys) => {
                let mut parts = vec![bulk("WATCH")];
                for key in keys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
            }
            Command::BgRewriteAof => {
                RespValue::Array(vec![bulk("BGREWRITEAOF")])
            }
            Command::SetBit(key, offset, value) => {
                RespValue::Array(vec![
                    bulk("SETBIT"),
                    bulk(key),
                    bulk(&offset.to_string()),
                    bulk(if *value { "1" } else { "0" }),
                ])
            }
            Command::GetBit(key, offset) => {
                RespValue::Array(vec![
                    bulk("GETBIT"),
                    bulk(key),
                    bulk(&offset.to_string()),
                ])
            }
            Command::BitCount(key, start, end, is_byte) => {
                let mut parts = vec![bulk("BITCOUNT"), bulk(key)];
                parts.push(bulk(&start.to_string()));
                parts.push(bulk(&end.to_string()));
                parts.push(bulk(if *is_byte { "BYTE" } else { "BIT" }));
                RespValue::Array(parts)
            }
            Command::BitOp(op, destkey, keys) => {
                let mut parts = vec![bulk("BITOP"), bulk(op), bulk(destkey)];
                for key in keys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
            }
            Command::BitPos(key, bit, start, end, is_byte) => {
                let mut parts = vec![
                    bulk("BITPOS"),
                    bulk(key),
                    bulk(&bit.to_string()),
                    bulk(&start.to_string()),
                    bulk(&end.to_string()),
                ];
                parts.push(bulk(if *is_byte { "BYTE" } else { "BIT" }));
                RespValue::Array(parts)
            }
            Command::BitField(key, ops) => {
                let mut parts = vec![bulk("BITFIELD"), bulk(key)];
                for op in ops {
                    match op {
                        crate::storage::BitFieldOp::Get(enc, off) => {
                            parts.push(bulk("GET"));
                            let type_str = format!("{}{}", if enc.signed { "i" } else { "u" }, enc.bits);
                            parts.push(bulk(&type_str));
                            let off_str = match off {
                                crate::storage::BitFieldOffset::Num(n) => n.to_string(),
                                crate::storage::BitFieldOffset::Hash(n) => format!("#{}", n),
                            };
                            parts.push(bulk(&off_str));
                        }
                        crate::storage::BitFieldOp::Set(enc, off, value) => {
                            parts.push(bulk("SET"));
                            let type_str = format!("{}{}", if enc.signed { "i" } else { "u" }, enc.bits);
                            parts.push(bulk(&type_str));
                            let off_str = match off {
                                crate::storage::BitFieldOffset::Num(n) => n.to_string(),
                                crate::storage::BitFieldOffset::Hash(n) => format!("#{}", n),
                            };
                            parts.push(bulk(&off_str));
                            parts.push(bulk(&value.to_string()));
                        }
                        crate::storage::BitFieldOp::IncrBy(enc, off, inc) => {
                            parts.push(bulk("INCRBY"));
                            let type_str = format!("{}{}", if enc.signed { "i" } else { "u" }, enc.bits);
                            parts.push(bulk(&type_str));
                            let off_str = match off {
                                crate::storage::BitFieldOffset::Num(n) => n.to_string(),
                                crate::storage::BitFieldOffset::Hash(n) => format!("#{}", n),
                            };
                            parts.push(bulk(&off_str));
                            parts.push(bulk(&inc.to_string()));
                        }
                        crate::storage::BitFieldOp::Overflow(o) => {
                            parts.push(bulk("OVERFLOW"));
                            let strategy = match o {
                                crate::storage::BitFieldOverflow::Wrap => "WRAP",
                                crate::storage::BitFieldOverflow::Sat => "SAT",
                                crate::storage::BitFieldOverflow::Fail => "FAIL",
                            };
                            parts.push(bulk(strategy));
                        }
                    }
                }
                RespValue::Array(parts)
            }
            Command::BitFieldRo(key, ops) => {
                let mut parts = vec![bulk("BITFIELD_RO"), bulk(key)];
                for op in ops {
                    if let crate::storage::BitFieldOp::Get(enc, off) = op {
                        parts.push(bulk("GET"));
                        let type_str = format!("{}{}", if enc.signed { "i" } else { "u" }, enc.bits);
                        parts.push(bulk(&type_str));
                        let off_str = match off {
                            crate::storage::BitFieldOffset::Num(n) => n.to_string(),
                            crate::storage::BitFieldOffset::Hash(n) => format!("#{}", n),
                        };
                        parts.push(bulk(&off_str));
                    }
                }
                RespValue::Array(parts)
            }
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
            Command::XLen(key) => {
                RespValue::Array(vec![bulk("XLEN"), bulk(key)])
            }
            Command::XRange(key, start, end, count) => {
                let mut parts = vec![bulk("XRANGE"), bulk(key), bulk(start), bulk(end)];
                if let Some(c) = count {
                    parts.push(bulk("COUNT"));
                    parts.push(bulk(&c.to_string()));
                }
                RespValue::Array(parts)
            }
            Command::XRevRange(key, end, start, count) => {
                let mut parts = vec![bulk("XREVRANGE"), bulk(key), bulk(end), bulk(start)];
                if let Some(c) = count {
                    parts.push(bulk("COUNT"));
                    parts.push(bulk(&c.to_string()));
                }
                RespValue::Array(parts)
            }
            Command::XTrim(key, strategy, threshold) => {
                RespValue::Array(vec![
                    bulk("XTRIM"),
                    bulk(key),
                    bulk(strategy),
                    bulk(threshold),
                ])
            }
            Command::XDel(key, ids) => {
                let mut parts = vec![bulk("XDEL"), bulk(key)];
                for id in ids {
                    parts.push(bulk(id));
                }
                RespValue::Array(parts)
            }
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
            Command::XSetId(key, id) => {
                RespValue::Array(vec![bulk("XSETID"), bulk(key), bulk(id)])
            }
            Command::XGroupCreate(key, group, id, mkstream) => {
                let mut parts = vec![bulk("XGROUP"), bulk("CREATE"), bulk(key), bulk(group), bulk(id)];
                if *mkstream { parts.push(bulk("MKSTREAM")); }
                RespValue::Array(parts)
            }
            Command::XGroupDestroy(key, group) => {
                RespValue::Array(vec![bulk("XGROUP"), bulk("DESTROY"), bulk(key), bulk(group)])
            }
            Command::XGroupSetId(key, group, id) => {
                RespValue::Array(vec![bulk("XGROUP"), bulk("SETID"), bulk(key), bulk(group), bulk(id)])
            }
            Command::XGroupDelConsumer(key, group, consumer) => {
                RespValue::Array(vec![bulk("XGROUP"), bulk("DELCONSUMER"), bulk(key), bulk(group), bulk(consumer)])
            }
            Command::XGroupCreateConsumer(key, group, consumer) => {
                RespValue::Array(vec![bulk("XGROUP"), bulk("CREATECONSUMER"), bulk(key), bulk(group), bulk(consumer)])
            }
            Command::XReadGroup(group, consumer, keys, ids, count, noack) => {
                let mut parts = vec![bulk("XREADGROUP"), bulk("GROUP"), bulk(group), bulk(consumer)];
                if let Some(c) = count { parts.push(bulk("COUNT")); parts.push(bulk(&c.to_string())); }
                if *noack { parts.push(bulk("NOACK")); }
                parts.push(bulk("STREAMS"));
                for k in keys { parts.push(bulk(k)); }
                for id in ids { parts.push(bulk(id)); }
                RespValue::Array(parts)
            }
            Command::XAck(key, group, ids) => {
                let mut parts = vec![bulk("XACK"), bulk(key), bulk(group)];
                for id in ids { parts.push(bulk(id)); }
                RespValue::Array(parts)
            }
            Command::XClaim(key, group, consumer, min_idle, ids, justid) => {
                let mut parts = vec![bulk("XCLAIM"), bulk(key), bulk(group), bulk(consumer), bulk(&min_idle.to_string())];
                for id in ids { parts.push(bulk(id)); }
                if *justid { parts.push(bulk("JUSTID")); }
                RespValue::Array(parts)
            }
            Command::XAutoClaim(key, group, consumer, min_idle, start, count, justid) => {
                let mut parts = vec![bulk("XAUTOCLAIM"), bulk(key), bulk(group), bulk(consumer), bulk(&min_idle.to_string()), bulk(start)];
                parts.push(bulk("COUNT")); parts.push(bulk(&count.to_string()));
                if *justid { parts.push(bulk("JUSTID")); }
                RespValue::Array(parts)
            }
            Command::XPending(key, group, start, end, count, consumer) => {
                let mut parts = vec![bulk("XPENDING"), bulk(key), bulk(group)];
                if let Some(s) = start { parts.push(bulk(s)); }
                if let Some(e) = end { parts.push(bulk(e)); }
                if let Some(c) = count { parts.push(bulk(&c.to_string())); }
                if let Some(cn) = consumer { parts.push(bulk(cn)); }
                RespValue::Array(parts)
            }
            Command::XInfoStream(key, full) => {
                let mut parts = vec![bulk("XINFO"), bulk("STREAM"), bulk(key)];
                if *full { parts.push(bulk("FULL")); }
                RespValue::Array(parts)
            }
            Command::XInfoGroups(key) => {
                RespValue::Array(vec![bulk("XINFO"), bulk("GROUPS"), bulk(key)])
            }
            Command::XInfoConsumers(key, group) => {
                RespValue::Array(vec![bulk("XINFO"), bulk("CONSUMERS"), bulk(key), bulk(group)])
            }
            Command::PfAdd(key, elements) => {
                let mut parts = vec![bulk("PFADD"), bulk(key)];
                for element in elements {
                    parts.push(bulk(element));
                }
                RespValue::Array(parts)
            }
            Command::PfCount(keys) => {
                let mut parts = vec![bulk("PFCOUNT")];
                for key in keys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
            }
            Command::PfMerge(destkey, sourcekeys) => {
                let mut parts = vec![bulk("PFMERGE"), bulk(destkey)];
                for key in sourcekeys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
            }
            Command::GeoAdd(key, items) => {
                let mut parts = vec![bulk("GEOADD"), bulk(key)];
                for (lon, lat, member) in items {
                    parts.push(bulk(&lon.to_string()));
                    parts.push(bulk(&lat.to_string()));
                    parts.push(bulk(member));
                }
                RespValue::Array(parts)
            }
            Command::GeoDist(key, m1, m2, unit) => {
                let mut parts = vec![
                    bulk("GEODIST"),
                    bulk(key),
                    bulk(m1),
                    bulk(m2),
                ];
                if !unit.is_empty() && unit != "m" {
                    parts.push(bulk(unit));
                }
                RespValue::Array(parts)
            }
            Command::GeoHash(key, members) => {
                let mut parts = vec![bulk("GEOHASH"), bulk(key)];
                for member in members {
                    parts.push(bulk(member));
                }
                RespValue::Array(parts)
            }
            Command::GeoPos(key, members) => {
                let mut parts = vec![bulk("GEOPOS"), bulk(key)];
                for member in members {
                    parts.push(bulk(member));
                }
                RespValue::Array(parts)
            }
            Command::GeoSearch(_, _, _, _, _, _, _, _, _, _) => {
                // GEOSEARCH 命令较复杂，简化序列化
                RespValue::Array(vec![bulk("GEOSEARCH")])
            }
            Command::GeoSearchStore(_, _, _, _, _, _, _, _, _) => {
                RespValue::Array(vec![bulk("GEOSEARCHSTORE")])
            }
            Command::Select(index) => {
                RespValue::Array(vec![bulk("SELECT"), bulk(&index.to_string())])
            }
            Command::Auth(username, password) => {
                if username == "default" {
                    RespValue::Array(vec![bulk("AUTH"), bulk(password)])
                } else {
                    RespValue::Array(vec![bulk("AUTH"), bulk(username), bulk(password)])
                }
            }
            Command::ClientSetName(name) => {
                RespValue::Array(vec![bulk("CLIENT"), bulk("SETNAME"), bulk(name)])
            }
            Command::ClientGetName => {
                RespValue::Array(vec![bulk("CLIENT"), bulk("GETNAME")])
            }
            Command::ClientList => {
                RespValue::Array(vec![bulk("CLIENT"), bulk("LIST")])
            }
            Command::ClientId => {
                RespValue::Array(vec![bulk("CLIENT"), bulk("ID")])
            }
            Command::ClientInfo => {
                RespValue::Array(vec![bulk("CLIENT"), bulk("INFO")])
            }
            Command::ClientKill { id, addr, user, skipme } => {
                let mut parts = vec![bulk("CLIENT"), bulk("KILL")];
                if let Some(i) = id {
                    parts.push(bulk("ID"));
                    parts.push(bulk(&i.to_string()));
                }
                if let Some(a) = addr {
                    parts.push(bulk("ADDR"));
                    parts.push(bulk(a));
                }
                if let Some(u) = user {
                    parts.push(bulk("USER"));
                    parts.push(bulk(u));
                }
                if !skipme {
                    parts.push(bulk("SKIPME"));
                    parts.push(bulk("no"));
                }
                RespValue::Array(parts)
            }
            Command::ClientPause(timeout, mode) => {
                RespValue::Array(vec![
                    bulk("CLIENT"), bulk("PAUSE"), bulk(&timeout.to_string()), bulk(mode),
                ])
            }
            Command::ClientUnpause => {
                RespValue::Array(vec![bulk("CLIENT"), bulk("UNPAUSE")])
            }
            Command::ClientNoEvict(flag) => {
                RespValue::Array(vec![
                    bulk("CLIENT"), bulk("NO-EVICT"), bulk(if *flag { "ON" } else { "OFF" }),
                ])
            }
            Command::ClientNoTouch(flag) => {
                RespValue::Array(vec![
                    bulk("CLIENT"), bulk("NO-TOUCH"), bulk(if *flag { "ON" } else { "OFF" }),
                ])
            }
            Command::ClientReply(mode) => {
                let mode_str = match mode {
                    crate::server::ReplyMode::On => "ON",
                    crate::server::ReplyMode::Off => "OFF",
                    crate::server::ReplyMode::Skip => "SKIP",
                };
                RespValue::Array(vec![bulk("CLIENT"), bulk("REPLY"), bulk(mode_str)])
            }
            Command::ClientUnblock(id, reason) => {
                RespValue::Array(vec![
                    bulk("CLIENT"), bulk("UNBLOCK"), bulk(&id.to_string()), bulk(reason),
                ])
            }
            Command::AclSetUser(username, rules) => {
                let mut parts = vec![bulk("ACL"), bulk("SETUSER"), bulk(username)];
                for r in rules {
                    parts.push(bulk(r));
                }
                RespValue::Array(parts)
            }
            Command::AclGetUser(username) => {
                RespValue::Array(vec![bulk("ACL"), bulk("GETUSER"), bulk(username)])
            }
            Command::AclDelUser(names) => {
                let mut parts = vec![bulk("ACL"), bulk("DELUSER")];
                for n in names {
                    parts.push(bulk(n));
                }
                RespValue::Array(parts)
            }
            Command::AclList => {
                RespValue::Array(vec![bulk("ACL"), bulk("LIST")])
            }
            Command::AclCat(category) => {
                let mut parts = vec![bulk("ACL"), bulk("CAT")];
                if let Some(cat) = category {
                    parts.push(bulk(cat));
                }
                RespValue::Array(parts)
            }
            Command::AclWhoAmI => {
                RespValue::Array(vec![bulk("ACL"), bulk("WHOAMI")])
            }
            Command::AclLog(arg) => {
                let mut parts = vec![bulk("ACL"), bulk("LOG")];
                if let Some(a) = arg {
                    parts.push(bulk(a));
                }
                RespValue::Array(parts)
            }
            Command::AclGenPass(bits) => {
                let mut parts = vec![bulk("ACL"), bulk("GENPASS")];
                if let Some(b) = bits {
                    parts.push(bulk(&b.to_string()));
                }
                RespValue::Array(parts)
            }
            Command::Sort(key, _, _, _, _, _, _, _) => {
                let mut parts = vec![bulk("SORT"), bulk(key)];
                parts.push(bulk("ASC"));
                RespValue::Array(parts)
            }
            Command::Unlink(keys) => {
                let mut parts = vec![bulk("UNLINK")];
                for k in keys {
                    parts.push(bulk(k));
                }
                RespValue::Array(parts)
            }
            Command::Copy(source, destination, _) => {
                RespValue::Array(vec![bulk("COPY"), bulk(source), bulk(destination)])
            }
            Command::Dump(key) => {
                RespValue::Array(vec![bulk("DUMP"), bulk(key)])
            }
            Command::Restore(key, ttl, _, _) => {
                RespValue::Array(vec![bulk("RESTORE"), bulk(key), bulk(&ttl.to_string())])
            }
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
            Command::ScriptLoad(script) => {
                RespValue::Array(vec![bulk("SCRIPT"), bulk("LOAD"), bulk(script)])
            }
            Command::ScriptExists(sha1s) => {
                let mut parts = vec![bulk("SCRIPT"), bulk("EXISTS")];
                for s in sha1s {
                    parts.push(bulk(s));
                }
                RespValue::Array(parts)
            }
            Command::ScriptFlush => {
                RespValue::Array(vec![bulk("SCRIPT"), bulk("FLUSH")])
            }
            Command::FunctionLoad(code, replace) => {
                if *replace {
                    RespValue::Array(vec![bulk("FUNCTION"), bulk("LOAD"), bulk("REPLACE"), bulk(code)])
                } else {
                    RespValue::Array(vec![bulk("FUNCTION"), bulk("LOAD"), bulk(code)])
                }
            }
            Command::FunctionDelete(lib) => {
                RespValue::Array(vec![bulk("FUNCTION"), bulk("DELETE"), bulk(lib)])
            }
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
            Command::FunctionDump => {
                RespValue::Array(vec![bulk("FUNCTION"), bulk("DUMP")])
            }
            Command::FunctionRestore(data, policy) => {
                RespValue::Array(vec![
                    bulk("FUNCTION"), bulk("RESTORE"), bulk(data), bulk(policy),
                ])
            }
            Command::FunctionStats => {
                RespValue::Array(vec![bulk("FUNCTION"), bulk("STATS")])
            }
            Command::FunctionFlush(async_mode) => {
                if *async_mode {
                    RespValue::Array(vec![bulk("FUNCTION"), bulk("FLUSH"), bulk("ASYNC")])
                } else {
                    RespValue::Array(vec![bulk("FUNCTION"), bulk("FLUSH")])
                }
            }
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
            Command::Save => {
                RespValue::Array(vec![bulk("SAVE")])
            }
            Command::BgSave => {
                RespValue::Array(vec![bulk("BGSAVE")])
            }
            Command::SlowLogGet(count) => {
                RespValue::Array(vec![bulk("SLOWLOG"), bulk("GET"), bulk(&count.to_string())])
            }
            Command::SlowLogLen => {
                RespValue::Array(vec![bulk("SLOWLOG"), bulk("LEN")])
            }
            Command::SlowLogReset => {
                RespValue::Array(vec![bulk("SLOWLOG"), bulk("RESET")])
            }
            Command::ObjectEncoding(key) => {
                RespValue::Array(vec![bulk("OBJECT"), bulk("ENCODING"), bulk(key)])
            }
            Command::ObjectRefCount(key) => {
                RespValue::Array(vec![bulk("OBJECT"), bulk("REFCOUNT"), bulk(key)])
            }
            Command::ObjectIdleTime(key) => {
                RespValue::Array(vec![bulk("OBJECT"), bulk("IDLETIME"), bulk(key)])
            }
            Command::ObjectHelp => {
                RespValue::Array(vec![bulk("OBJECT"), bulk("HELP")])
            }
            Command::DebugSetActiveExpire(flag) => {
                RespValue::Array(vec![bulk("DEBUG"), bulk("SET-ACTIVE-EXPIRE"), bulk(if *flag { "1" } else { "0" })])
            }
            Command::DebugSleep(seconds) => {
                RespValue::Array(vec![bulk("DEBUG"), bulk("SLEEP"), bulk(&seconds.to_string())])
            }
            Command::DebugObject(key) => {
                RespValue::Array(vec![bulk("DEBUG"), bulk("OBJECT"), bulk(key)])
            }
            Command::Echo(msg) => {
                RespValue::Array(vec![bulk("ECHO"), bulk(msg)])
            }
            Command::Time => {
                RespValue::Array(vec![bulk("TIME")])
            }
            Command::RandomKey => {
                RespValue::Array(vec![bulk("RANDOMKEY")])
            }
            Command::Touch(keys) => {
                let mut parts = vec![bulk("TOUCH")];
                for k in keys {
                    parts.push(bulk(k));
                }
                RespValue::Array(parts)
            }
            Command::ExpireAt(key, ts) => {
                RespValue::Array(vec![bulk("EXPIREAT"), bulk(key), bulk(&ts.to_string())])
            }
            Command::PExpireAt(key, ts) => {
                RespValue::Array(vec![bulk("PEXPIREAT"), bulk(key), bulk(&ts.to_string())])
            }
            Command::ExpireTime(key) => {
                RespValue::Array(vec![bulk("EXPIRETIME"), bulk(key)])
            }
            Command::PExpireTime(key) => {
                RespValue::Array(vec![bulk("PEXPIRETIME"), bulk(key)])
            }
            Command::RenameNx(key, newkey) => {
                RespValue::Array(vec![bulk("RENAMENX"), bulk(key), bulk(newkey)])
            }
            Command::SwapDb(idx1, idx2) => {
                RespValue::Array(vec![bulk("SWAPDB"), bulk(&idx1.to_string()), bulk(&idx2.to_string())])
            }
            Command::FlushDb => {
                RespValue::Array(vec![bulk("FLUSHDB")])
            }
            Command::Shutdown(opt) => {
                let mut parts = vec![bulk("SHUTDOWN")];
                if let Some(s) = opt {
                    parts.push(bulk(s));
                }
                RespValue::Array(parts)
            }
            Command::LastSave => {
                RespValue::Array(vec![bulk("LASTSAVE")])
            }
            Command::SubStr(key, start, end) => {
                RespValue::Array(vec![bulk("SUBSTR"), bulk(key), bulk(&start.to_string()), bulk(&end.to_string())])
            }
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
            Command::Lmove(source, dest, left_from, left_to) => {
                RespValue::Array(vec![
                    bulk("LMOVE"),
                    bulk(source),
                    bulk(dest),
                    bulk(if *left_from { "LEFT" } else { "RIGHT" }),
                    bulk(if *left_to { "LEFT" } else { "RIGHT" }),
                ])
            }
            Command::Rpoplpush(source, dest) => {
                RespValue::Array(vec![bulk("RPOPLPUSH"), bulk(source), bulk(dest)])
            }
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
            Command::BLmove(source, dest, left_from, left_to, timeout) => {
                RespValue::Array(vec![
                    bulk("BLMOVE"),
                    bulk(source),
                    bulk(dest),
                    bulk(if *left_from { "LEFT" } else { "RIGHT" }),
                    bulk(if *left_to { "LEFT" } else { "RIGHT" }),
                    bulk(&timeout.to_string()),
                ])
            }
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
            Command::BRpoplpush(source, dest, timeout) => {
                RespValue::Array(vec![bulk("BRPOPLPUSH"), bulk(source), bulk(dest), bulk(&timeout.to_string())])
            }
            Command::Quit => {
                RespValue::Array(vec![bulk("QUIT")])
            }
            Command::Unknown(cmd_name) => {
                RespValue::Array(vec![bulk(cmd_name)])
            }
        }
    }

    /// 判断是否为写操作（需要记录到 AOF）
    pub fn is_write_command(&self) -> bool {
        matches!(
            self,
            Command::Set(_, _, _)
                | Command::SetEx(_, _, _)
                | Command::Del(_)
                | Command::FlushAll
                | Command::Expire(_, _)
                | Command::MSet(_)
                | Command::Incr(_)
                | Command::Decr(_)
                | Command::IncrBy(_, _)
                | Command::DecrBy(_, _)
                | Command::Append(_, _)
                | Command::SetNx(_, _)
                | Command::SetExCmd(_, _, _)
                | Command::PSetEx(_, _, _)
                | Command::GetSet(_, _)
                | Command::GetDel(_)
                | Command::GetEx(_, _)
                | Command::MSetNx(_)
                | Command::IncrByFloat(_, _)
                | Command::SetRange(_, _, _)
                | Command::LPush(_, _)
                | Command::RPush(_, _)
                | Command::LPop(_)
                | Command::RPop(_)
                | Command::LSet(_, _, _)
                | Command::LInsert(_, _, _, _)
                | Command::LRem(_, _, _)
                | Command::LTrim(_, _, _)
                | Command::HSet(_, _)
                | Command::HDel(_, _)
                | Command::HMSet(_, _)
                | Command::HIncrBy(_, _, _)
                | Command::HIncrByFloat(_, _, _)
                | Command::HSetNx(_, _, _)
                | Command::HExpire(_, _, _)
                | Command::HPExpire(_, _, _)
                | Command::HExpireAt(_, _, _)
                | Command::HPExpireAt(_, _, _)
                | Command::HPersist(_, _)
                | Command::SAdd(_, _)
                | Command::SRem(_, _)
                | Command::SPop(_, _)
                | Command::SMove(_, _, _)
                | Command::SInterStore(_, _)
                | Command::SUnionStore(_, _)
                | Command::SDiffStore(_, _)
                | Command::ZAdd(_, _)
                | Command::ZRem(_, _)
                | Command::ZIncrBy(_, _, _)
                | Command::ZPopMin(_, _)
                | Command::ZPopMax(_, _)
                | Command::ZUnionStore(_, _, _, _)
                | Command::ZInterStore(_, _, _, _)
                | Command::ZDiffStore(_, _)
                | Command::ZRangeStore(_, _, _, _, _, _, _, _, _)
                | Command::ZMpop(_, _, _)
                | Command::Rename(_, _)
                | Command::Persist(_)
                | Command::PExpire(_, _)
                | Command::SetBit(_, _, _)
                | Command::BitOp(_, _, _)
                | Command::BitField(_, _)
                | Command::XAdd(_, _, _, _, _, _)
                | Command::XTrim(_, _, _)
                | Command::XDel(_, _)
                | Command::XSetId(_, _)
                | Command::XGroupCreate(_, _, _, _)
                | Command::XGroupDestroy(_, _)
                | Command::XGroupSetId(_, _, _)
                | Command::XGroupDelConsumer(_, _, _)
                | Command::XGroupCreateConsumer(_, _, _)
                | Command::XReadGroup(_, _, _, _, _, _)
                | Command::XAck(_, _, _)
                | Command::XClaim(_, _, _, _, _, _)
                | Command::XAutoClaim(_, _, _, _, _, _, _)
                | Command::PfAdd(_, _)
                | Command::PfMerge(_, _)
                | Command::GeoAdd(_, _)
                | Command::GeoSearchStore(_, _, _, _, _, _, _, _, _)
                | Command::Sort(_, _, _, _, _, _, _, Some(_))
                | Command::Unlink(_)
                | Command::Copy(_, _, _)
                | Command::Restore(_, _, _, _)
                | Command::Eval(_, _, _)
                | Command::EvalSha(_, _, _)
                | Command::Save
                | Command::BgSave
                | Command::ExpireAt(_, _)
                | Command::PExpireAt(_, _)
                | Command::RenameNx(_, _)
                | Command::SwapDb(_, _)
                | Command::FlushDb
                | Command::Shutdown(_)
                | Command::Lmove(_, _, _, _)
                | Command::Rpoplpush(_, _)
                | Command::Lmpop(_, _, _)
                | Command::FunctionLoad(_, _)
                | Command::FunctionDelete(_)
                | Command::FunctionFlush(_)
                | Command::FunctionRestore(_, _)
                | Command::FCall(_, _, _)
                | Command::FCallRO(_, _, _)
        )
    }
}
