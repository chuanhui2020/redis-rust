use super::*;

use crate::protocol::RespValue;

pub(crate) fn to_resp_s_add(cmd: &Command) -> RespValue {
    match cmd {
        Command::SAdd(key, members) => {
                let mut parts = vec![bulk("SADD"), bulk(key)];
                for m in members {
                    parts.push(bulk_bytes(&m));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_s_rem(cmd: &Command) -> RespValue {
    match cmd {
        Command::SRem(key, members) => {
                let mut parts = vec![bulk("SREM"), bulk(key)];
                for m in members {
                    parts.push(bulk_bytes(&m));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_s_members(cmd: &Command) -> RespValue {
    match cmd {
        Command::SMembers(key) => {
                RespValue::Array(vec![bulk("SMEMBERS"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

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

pub(crate) fn to_resp_s_card(cmd: &Command) -> RespValue {
    match cmd {
        Command::SCard(key) => {
                RespValue::Array(vec![bulk("SCARD"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

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

