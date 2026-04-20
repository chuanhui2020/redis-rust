use super::*;

use crate::protocol::RespValue;

pub(crate) fn to_resp_l_push(cmd: &Command) -> RespValue {
    match cmd {
        Command::LPush(key, values) => {
                let mut parts = vec![bulk("LPUSH"), bulk(key)];
                for v in values {
                    parts.push(bulk_bytes(&v));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_r_push(cmd: &Command) -> RespValue {
    match cmd {
        Command::RPush(key, values) => {
                let mut parts = vec![bulk("RPUSH"), bulk(key)];
                for v in values {
                    parts.push(bulk_bytes(&v));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_l_pop(cmd: &Command) -> RespValue {
    match cmd {
        Command::LPop(key) => {
                RespValue::Array(vec![bulk("LPOP"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_r_pop(cmd: &Command) -> RespValue {
    match cmd {
        Command::RPop(key) => {
                RespValue::Array(vec![bulk("RPOP"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_l_len(cmd: &Command) -> RespValue {
    match cmd {
        Command::LLen(key) => {
                RespValue::Array(vec![bulk("LLEN"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

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

