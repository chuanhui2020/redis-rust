use super::*;

use crate::protocol::RespValue;

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

pub(crate) fn to_resp_h_get(cmd: &Command) -> RespValue {
    match cmd {
        Command::HGet(key, field) => {
                RespValue::Array(vec![bulk("HGET"), bulk(key), bulk(field)])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_h_str_len(cmd: &Command) -> RespValue {
    match cmd {
        Command::HStrLen(key, field) => {
                RespValue::Array(vec![bulk("HSTRLEN"), bulk(key), bulk(field)])
        }
        _ => unreachable!(),
    }
}

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

pub(crate) fn to_resp_h_exists(cmd: &Command) -> RespValue {
    match cmd {
        Command::HExists(key, field) => {
                RespValue::Array(vec![bulk("HEXISTS"), bulk(key), bulk(field)])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_h_get_all(cmd: &Command) -> RespValue {
    match cmd {
        Command::HGetAll(key) => {
                RespValue::Array(vec![bulk("HGETALL"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_h_len(cmd: &Command) -> RespValue {
    match cmd {
        Command::HLen(key) => {
                RespValue::Array(vec![bulk("HLEN"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

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

pub(crate) fn to_resp_h_keys(cmd: &Command) -> RespValue {
    match cmd {
        Command::HKeys(key) => {
                RespValue::Array(vec![bulk("HKEYS"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_h_vals(cmd: &Command) -> RespValue {
    match cmd {
        Command::HVals(key) => {
                RespValue::Array(vec![bulk("HVALS"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

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

