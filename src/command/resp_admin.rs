use super::*;

use crate::protocol::RespValue;

pub(crate) fn to_resp_ping(cmd: &Command) -> RespValue {
    match cmd {
        Command::Ping(msg) => {
                let mut parts = vec![bulk("PING")];
                if let Some(m) = msg {
                    parts.push(bulk(m));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_config_get(cmd: &Command) -> RespValue {
    match cmd {
        Command::ConfigGet(key) => {
                RespValue::Array(vec![bulk("CONFIG"), bulk("GET"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_config_set(cmd: &Command) -> RespValue {
    match cmd {
        Command::ConfigSet(key, value) => {
                RespValue::Array(vec![bulk("CONFIG"), bulk("SET"), bulk(key), bulk(value)])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_config_rewrite(cmd: &Command) -> RespValue {
    match cmd {
        Command::ConfigRewrite => {
                RespValue::Array(vec![bulk("CONFIG"), bulk("REWRITE")])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_config_reset_stat(cmd: &Command) -> RespValue {
    match cmd {
        Command::ConfigResetStat => {
                RespValue::Array(vec![bulk("CONFIG"), bulk("RESETSTAT")])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_memory_usage(cmd: &Command) -> RespValue {
    match cmd {
        Command::MemoryUsage(key, samples) => {
                let mut parts = vec![bulk("MEMORY"), bulk("USAGE"), bulk(key)];
                if let Some(s) = samples {
                    parts.push(bulk("SAMPLES"));
                    parts.push(bulk(&s.to_string()));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_memory_doctor(cmd: &Command) -> RespValue {
    match cmd {
        Command::MemoryDoctor => {
                RespValue::Array(vec![bulk("MEMORY"), bulk("DOCTOR")])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_latency_latest(cmd: &Command) -> RespValue {
    match cmd {
        Command::LatencyLatest => {
                RespValue::Array(vec![bulk("LATENCY"), bulk("LATEST")])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_latency_history(cmd: &Command) -> RespValue {
    match cmd {
        Command::LatencyHistory(event) => {
                RespValue::Array(vec![bulk("LATENCY"), bulk("HISTORY"), bulk(event)])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_latency_reset(cmd: &Command) -> RespValue {
    match cmd {
        Command::LatencyReset(events) => {
                let mut parts = vec![bulk("LATENCY"), bulk("RESET")];
                for e in events {
                    parts.push(bulk(e));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_reset(cmd: &Command) -> RespValue {
    match cmd {
        Command::Reset => {
                RespValue::Array(vec![bulk("RESET")])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_hello(cmd: &Command) -> RespValue {
    match cmd {
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
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_monitor(cmd: &Command) -> RespValue {
    match cmd {
        Command::Monitor => {
                RespValue::Array(vec![bulk("MONITOR")])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_command_info(cmd: &Command) -> RespValue {
    match cmd {
        Command::CommandInfo => {
                RespValue::Array(vec![bulk("COMMAND")])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_command_count(cmd: &Command) -> RespValue {
    match cmd {
        Command::CommandCount => {
                RespValue::Array(vec![bulk("COMMAND"), bulk("COUNT")])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_command_list(cmd: &Command) -> RespValue {
    match cmd {
        Command::CommandList(filter) => {
                let mut parts = vec![bulk("COMMAND"), bulk("LIST")];
                if let Some(f) = filter {
                    parts.push(bulk(f));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_command_docs(cmd: &Command) -> RespValue {
    match cmd {
        Command::CommandDocs(names) => {
                let mut parts = vec![bulk("COMMAND"), bulk("DOCS")];
                parts.extend(names.iter().map(|n| bulk(n)));
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_command_get_keys(cmd: &Command) -> RespValue {
    match cmd {
        Command::CommandGetKeys(args) => {
                let mut parts = vec![bulk("COMMAND"), bulk("GETKEYS")];
                parts.extend(args.iter().map(|a| bulk(a)));
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_keys(cmd: &Command) -> RespValue {
    match cmd {
        Command::Keys(pattern) => {
                RespValue::Array(vec![bulk("KEYS"), bulk(pattern)])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_scan(cmd: &Command) -> RespValue {
    match cmd {
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
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_rename(cmd: &Command) -> RespValue {
    match cmd {
        Command::Rename(key, newkey) => {
                RespValue::Array(vec![bulk("RENAME"), bulk(key), bulk(newkey)])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_type(cmd: &Command) -> RespValue {
    match cmd {
        Command::Type(key) => {
                RespValue::Array(vec![bulk("TYPE"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_persist(cmd: &Command) -> RespValue {
    match cmd {
        Command::Persist(key) => {
                RespValue::Array(vec![bulk("PERSIST"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_p_expire(cmd: &Command) -> RespValue {
    match cmd {
        Command::PExpire(key, ms) => {
                RespValue::Array(vec![bulk("PEXPIRE"), bulk(key), bulk(&ms.to_string())])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_p_ttl(cmd: &Command) -> RespValue {
    match cmd {
        Command::PTtl(key) => {
                RespValue::Array(vec![bulk("PTTL"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_info(cmd: &Command) -> RespValue {
    match cmd {
        Command::Info(section) => {
                match section {
                    Some(s) => RespValue::Array(vec![bulk("INFO"), bulk(s)]),
                    None => RespValue::Array(vec![bulk("INFO")]),
                }
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_bg_rewrite_aof(cmd: &Command) -> RespValue {
    match cmd {
        Command::BgRewriteAof => {
                RespValue::Array(vec![bulk("BGREWRITEAOF")])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_select(cmd: &Command) -> RespValue {
    match cmd {
        Command::Select(index) => {
                RespValue::Array(vec![bulk("SELECT"), bulk(&index.to_string())])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_auth(cmd: &Command) -> RespValue {
    match cmd {
        Command::Auth(username, password) => {
                if username == "default" {
                    RespValue::Array(vec![bulk("AUTH"), bulk(password)])
                } else {
                    RespValue::Array(vec![bulk("AUTH"), bulk(username), bulk(password)])
                }
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_unlink(cmd: &Command) -> RespValue {
    match cmd {
        Command::Unlink(keys) => {
                let mut parts = vec![bulk("UNLINK")];
                for k in keys {
                    parts.push(bulk(k));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_dump(cmd: &Command) -> RespValue {
    match cmd {
        Command::Dump(key) => {
                RespValue::Array(vec![bulk("DUMP"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_eval(cmd: &Command) -> RespValue {
    match cmd {
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
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_eval_sha(cmd: &Command) -> RespValue {
    match cmd {
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
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_script_load(cmd: &Command) -> RespValue {
    match cmd {
        Command::ScriptLoad(script) => {
                RespValue::Array(vec![bulk("SCRIPT"), bulk("LOAD"), bulk(script)])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_script_exists(cmd: &Command) -> RespValue {
    match cmd {
        Command::ScriptExists(sha1s) => {
                let mut parts = vec![bulk("SCRIPT"), bulk("EXISTS")];
                for s in sha1s {
                    parts.push(bulk(s));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_script_flush(cmd: &Command) -> RespValue {
    match cmd {
        Command::ScriptFlush => {
                RespValue::Array(vec![bulk("SCRIPT"), bulk("FLUSH")])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_function_load(cmd: &Command) -> RespValue {
    match cmd {
        Command::FunctionLoad(code, replace) => {
                if *replace {
                    RespValue::Array(vec![bulk("FUNCTION"), bulk("LOAD"), bulk("REPLACE"), bulk(code)])
                } else {
                    RespValue::Array(vec![bulk("FUNCTION"), bulk("LOAD"), bulk(code)])
                }
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_function_delete(cmd: &Command) -> RespValue {
    match cmd {
        Command::FunctionDelete(lib) => {
                RespValue::Array(vec![bulk("FUNCTION"), bulk("DELETE"), bulk(lib)])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_function_list(cmd: &Command) -> RespValue {
    match cmd {
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
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_function_dump(cmd: &Command) -> RespValue {
    match cmd {
        Command::FunctionDump => {
                RespValue::Array(vec![bulk("FUNCTION"), bulk("DUMP")])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_function_restore(cmd: &Command) -> RespValue {
    match cmd {
        Command::FunctionRestore(data, policy) => {
                RespValue::Array(vec![
                    bulk("FUNCTION"), bulk("RESTORE"), bulk(data), bulk(policy),
                ])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_function_stats(cmd: &Command) -> RespValue {
    match cmd {
        Command::FunctionStats => {
                RespValue::Array(vec![bulk("FUNCTION"), bulk("STATS")])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_function_flush(cmd: &Command) -> RespValue {
    match cmd {
        Command::FunctionFlush(async_mode) => {
                if *async_mode {
                    RespValue::Array(vec![bulk("FUNCTION"), bulk("FLUSH"), bulk("ASYNC")])
                } else {
                    RespValue::Array(vec![bulk("FUNCTION"), bulk("FLUSH")])
                }
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_f_call(cmd: &Command) -> RespValue {
    match cmd {
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
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_f_call_r_o(cmd: &Command) -> RespValue {
    match cmd {
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
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_eval_r_o(cmd: &Command) -> RespValue {
    match cmd {
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
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_eval_sha_r_o(cmd: &Command) -> RespValue {
    match cmd {
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
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_slow_log_get(cmd: &Command) -> RespValue {
    match cmd {
        Command::SlowLogGet(count) => {
                RespValue::Array(vec![bulk("SLOWLOG"), bulk("GET"), bulk(&count.to_string())])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_slow_log_len(cmd: &Command) -> RespValue {
    match cmd {
        Command::SlowLogLen => {
                RespValue::Array(vec![bulk("SLOWLOG"), bulk("LEN")])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_slow_log_reset(cmd: &Command) -> RespValue {
    match cmd {
        Command::SlowLogReset => {
                RespValue::Array(vec![bulk("SLOWLOG"), bulk("RESET")])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_debug_object(cmd: &Command) -> RespValue {
    match cmd {
        Command::DebugObject(key) => {
                RespValue::Array(vec![bulk("DEBUG"), bulk("OBJECT"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_touch(cmd: &Command) -> RespValue {
    match cmd {
        Command::Touch(keys) => {
                let mut parts = vec![bulk("TOUCH")];
                for k in keys {
                    parts.push(bulk(k));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_expire_at(cmd: &Command) -> RespValue {
    match cmd {
        Command::ExpireAt(key, ts) => {
                RespValue::Array(vec![bulk("EXPIREAT"), bulk(key), bulk(&ts.to_string())])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_p_expire_at(cmd: &Command) -> RespValue {
    match cmd {
        Command::PExpireAt(key, ts) => {
                RespValue::Array(vec![bulk("PEXPIREAT"), bulk(key), bulk(&ts.to_string())])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_expire_time(cmd: &Command) -> RespValue {
    match cmd {
        Command::ExpireTime(key) => {
                RespValue::Array(vec![bulk("EXPIRETIME"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_p_expire_time(cmd: &Command) -> RespValue {
    match cmd {
        Command::PExpireTime(key) => {
                RespValue::Array(vec![bulk("PEXPIRETIME"), bulk(key)])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_rename_nx(cmd: &Command) -> RespValue {
    match cmd {
        Command::RenameNx(key, newkey) => {
                RespValue::Array(vec![bulk("RENAMENX"), bulk(key), bulk(newkey)])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_swap_db(cmd: &Command) -> RespValue {
    match cmd {
        Command::SwapDb(idx1, idx2) => {
                RespValue::Array(vec![bulk("SWAPDB"), bulk(&idx1.to_string()), bulk(&idx2.to_string())])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_flush_db(cmd: &Command) -> RespValue {
    match cmd {
        Command::FlushDb => {
                RespValue::Array(vec![bulk("FLUSHDB")])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn to_resp_shutdown(cmd: &Command) -> RespValue {
    match cmd {
        Command::Shutdown(opt) => {
                let mut parts = vec![bulk("SHUTDOWN")];
                if let Some(s) = opt {
                    parts.push(bulk(s));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

