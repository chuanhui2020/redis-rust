use super::*;

use crate::error::{AppError, Result};
use crate::protocol::RespValue;

/// 命令解析器，将 RESP 值解析为内部 Command
#[derive(Debug)]
pub struct CommandParser;

impl CommandParser {
    /// 创建新的命令解析器
    pub fn new() -> Self {
        Self
    }

    /// 将 RESP 值解析为 Command
    /// 预期输入为 RESP 数组，其中每个元素为 BulkString
    pub fn parse(&self, value: RespValue) -> Result<Command> {
        match value {
            RespValue::Array(arr) => self.parse_array(arr),
            _ => Err(AppError::Command(
                "命令必须是 RESP 数组".to_string(),
            )),
        }
    }

    /// 解析 RESP 数组为具体命令
    fn parse_array(&self, arr: Vec<RespValue>) -> Result<Command> {
        if arr.is_empty() {
            return Err(AppError::Command("空命令数组".to_string()));
        }

        // 第一个元素是命令名
        let cmd_name = match &arr[0] {
            RespValue::BulkString(Some(data)) => {
                String::from_utf8_lossy(data).to_ascii_uppercase()
            }
            RespValue::SimpleString(s) => s.to_ascii_uppercase(),
            _ => {
                return Err(AppError::Command(
                    "命令名必须是字符串".to_string(),
                ))
            }
        };

        match cmd_name.as_str() {
            "PING" => self.parse_ping(&arr),
            "GET" => self.parse_get(&arr),
            "SET" => self.parse_set(&arr),
            "DEL" => self.parse_del(&arr),
            "EXISTS" => self.parse_exists(&arr),
            "FLUSHALL" => Ok(Command::FlushAll),
            "EXPIRE" => self.parse_expire(&arr),
            "TTL" => self.parse_ttl(&arr),
            "CONFIG" => self.parse_config(&arr),
            "COMMAND" => Ok(Command::CommandInfo),
            "MGET" => self.parse_mget(&arr),
            "MSET" => self.parse_mset(&arr),
            "INCR" => self.parse_incr(&arr),
            "DECR" => self.parse_decr(&arr),
            "INCRBY" => self.parse_incrby(&arr),
            "DECRBY" => self.parse_decrby(&arr),
            "APPEND" => self.parse_append(&arr),
            "SETNX" => self.parse_setnx(&arr),
            "SETEX" => self.parse_setex(&arr),
            "PSETEX" => self.parse_psetex(&arr),
            "GETSET" => self.parse_getset(&arr),
            "GETDEL" => self.parse_getdel(&arr),
            "GETEX" => self.parse_getex(&arr),
            "MSETNX" => self.parse_msetnx(&arr),
            "INCRBYFLOAT" => self.parse_incrbyfloat(&arr),
            "SETRANGE" => self.parse_setrange(&arr),
            "GETRANGE" => self.parse_getrange(&arr),
            "STRLEN" => self.parse_strlen(&arr),
            "LPUSH" => self.parse_lpush(&arr),
            "RPUSH" => self.parse_rpush(&arr),
            "LPOP" => self.parse_lpop(&arr),
            "RPOP" => self.parse_rpop(&arr),
            "LLEN" => self.parse_llen(&arr),
            "LRANGE" => self.parse_lrange(&arr),
            "LINDEX" => self.parse_lindex(&arr),
            "LSET" => self.parse_lset(&arr),
            "LINSERT" => self.parse_linsert(&arr),
            "LREM" => self.parse_lrem(&arr),
            "LTRIM" => self.parse_ltrim(&arr),
            "LPOS" => self.parse_lpos(&arr),
            "BLPOP" => self.parse_blpop(&arr),
            "BRPOP" => self.parse_brpop(&arr),
            "HSET" => self.parse_hset(&arr),
            "HGET" => self.parse_hget(&arr),
            "HDEL" => self.parse_hdel(&arr),
            "HEXISTS" => self.parse_hexists(&arr),
            "HGETALL" => self.parse_hgetall(&arr),
            "HLEN" => self.parse_hlen(&arr),
            "HMSET" => self.parse_hmset(&arr),
            "HMGET" => self.parse_hmget(&arr),
            "HINCRBY" => self.parse_hincrby(&arr),
            "HINCRBYFLOAT" => self.parse_hincrbyfloat(&arr),
            "HKEYS" => self.parse_hkeys(&arr),
            "HVALS" => self.parse_hvals(&arr),
            "HSETNX" => self.parse_hsetnx(&arr),
            "HRANDFIELD" => self.parse_hrandfield(&arr),
            "HSCAN" => self.parse_hscan(&arr),
            "HEXPIRE" => self.parse_hexpire(&arr),
            "HPEXPIRE" => self.parse_hpexpire(&arr),
            "HEXPIREAT" => self.parse_hexpireat(&arr),
            "HPEXPIREAT" => self.parse_hpexpireat(&arr),
            "HTTL" => self.parse_httl(&arr),
            "HPTTL" => self.parse_hpttl(&arr),
            "HEXPIRETIME" => self.parse_hexpiretime(&arr),
            "HPEXPIRETIME" => self.parse_hpexpiretime(&arr),
            "HPERSIST" => self.parse_hpersist(&arr),
            "SADD" => self.parse_sadd(&arr),
            "SREM" => self.parse_srem(&arr),
            "SMEMBERS" => self.parse_smembers(&arr),
            "SISMEMBER" => self.parse_sismember(&arr),
            "SCARD" => self.parse_scard(&arr),
            "SINTER" => self.parse_sinter(&arr),
            "SUNION" => self.parse_sunion(&arr),
            "SDIFF" => self.parse_sdiff(&arr),
            "SPOP" => self.parse_spop(&arr),
            "SRANDMEMBER" => self.parse_srandmember(&arr),
            "SMOVE" => self.parse_smove(&arr),
            "SINTERSTORE" => self.parse_sinterstore(&arr),
            "SUNIONSTORE" => self.parse_sunionstore(&arr),
            "SDIFFSTORE" => self.parse_sdiffstore(&arr),
            "SSCAN" => self.parse_sscan(&arr),
            "ZADD" => self.parse_zadd(&arr),
            "ZREM" => self.parse_zrem(&arr),
            "ZSCORE" => self.parse_zscore(&arr),
            "ZRANK" => self.parse_zrank(&arr),
            "ZRANGE" => self.parse_zrange(&arr),
            "ZRANGEBYSCORE" => self.parse_zrangebyscore(&arr),
            "ZCARD" => self.parse_zcard(&arr),
            "ZREVRANGE" => self.parse_zrevrange(&arr),
            "ZREVRANK" => self.parse_zrevrank(&arr),
            "ZINCRBY" => self.parse_zincrby(&arr),
            "ZCOUNT" => self.parse_zcount(&arr),
            "ZPOPMIN" => self.parse_zpopmin(&arr),
            "ZPOPMAX" => self.parse_zpopmax(&arr),
            "ZUNIONSTORE" => self.parse_zunionstore(&arr),
            "ZINTERSTORE" => self.parse_zinterstore(&arr),
            "ZSCAN" => self.parse_zscan(&arr),
            "ZRANGEBYLEX" => self.parse_zrangebylex(&arr),
            "SINTERCARD" => self.parse_sintercard(&arr),
            "SMISMEMBER" => self.parse_smismember(&arr),
            "ZRANDMEMBER" => self.parse_zrandmember(&arr),
            "ZDIFF" => self.parse_zdiff(&arr),
            "ZDIFFSTORE" => self.parse_zdiffstore(&arr),
            "ZINTER" => self.parse_zinter(&arr),
            "ZUNION" => self.parse_zunion(&arr),
            "ZRANGESTORE" => self.parse_zrangestore(&arr),
            "ZMPOP" => self.parse_zmpop(&arr),
            "BZMPOP" => self.parse_bzmpop(&arr),
            "BZPOPMIN" => self.parse_bzpopmin(&arr),
            "BZPOPMAX" => self.parse_bzpopmax(&arr),
            "ZREVRANGEBYSCORE" => self.parse_zrevrangebyscore(&arr),
            "ZREVRANGEBYLEX" => self.parse_zrevrangebylex(&arr),
            "ZMSCORE" => self.parse_zmscore(&arr),
            "ZLEXCOUNT" => self.parse_zlexcount(&arr),
            "KEYS" => self.parse_keys(&arr),
            "SCAN" => self.parse_scan(&arr),
            "RENAME" => self.parse_rename(&arr),
            "TYPE" => self.parse_type(&arr),
            "PERSIST" => self.parse_persist(&arr),
            "PEXPIRE" => self.parse_pexpire(&arr),
            "PTTL" => self.parse_pttl(&arr),
            "DBSIZE" => self.parse_dbsize(&arr),
            "INFO" => self.parse_info(&arr),
            "SUBSCRIBE" => self.parse_subscribe(&arr),
            "UNSUBSCRIBE" => self.parse_unsubscribe(&arr),
            "PUBLISH" => self.parse_publish(&arr),
            "PSUBSCRIBE" => self.parse_psubscribe(&arr),
            "PUNSUBSCRIBE" => self.parse_punsubscribe(&arr),
            "MULTI" => self.parse_multi(&arr),
            "EXEC" => self.parse_exec(&arr),
            "DISCARD" => self.parse_discard(&arr),
            "WATCH" => self.parse_watch(&arr),
            "BGREWRITEAOF" => self.parse_bgrewriteaof(&arr),
            "SETBIT" => self.parse_setbit(&arr),
            "GETBIT" => self.parse_getbit(&arr),
            "BITCOUNT" => self.parse_bitcount(&arr),
            "BITOP" => self.parse_bitop(&arr),
            "BITPOS" => self.parse_bitpos(&arr),
            "BITFIELD" => self.parse_bitfield(&arr),
            "BITFIELD_RO" => self.parse_bitfield_ro(&arr),
            "XADD" => self.parse_xadd(&arr),
            "XLEN" => self.parse_xlen(&arr),
            "XRANGE" => self.parse_xrange(&arr),
            "XREVRANGE" => self.parse_xrevrange(&arr),
            "XTRIM" => self.parse_xtrim(&arr),
            "XDEL" => self.parse_xdel(&arr),
            "XREAD" => self.parse_xread(&arr),
            "XSETID" => self.parse_xsetid(&arr),
            "XGROUP" => self.parse_xgroup(&arr),
            "XREADGROUP" => self.parse_xreadgroup(&arr),
            "XACK" => self.parse_xack(&arr),
            "XCLAIM" => self.parse_xclaim(&arr),
            "XAUTOCLAIM" => self.parse_xautoclaim(&arr),
            "XPENDING" => self.parse_xpending(&arr),
            "XINFO" => self.parse_xinfo(&arr),
            "PFADD" => self.parse_pfadd(&arr),
            "PFCOUNT" => self.parse_pfcount(&arr),
            "PFMERGE" => self.parse_pfmerge(&arr),
            "GEOADD" => self.parse_geoadd(&arr),
            "GEODIST" => self.parse_geodist(&arr),
            "GEOHASH" => self.parse_geohash(&arr),
            "GEOPOS" => self.parse_geopos(&arr),
            "GEOSEARCH" => self.parse_geosearch(&arr),
            "GEOSEARCHSTORE" => self.parse_geosearchstore(&arr),
            "SELECT" => self.parse_select(&arr),
            "AUTH" => self.parse_auth(&arr),
            "ACL" => self.parse_acl(&arr),
            "CLIENT" => self.parse_client(&arr),
            "SORT" => self.parse_sort(&arr),
            "UNLINK" => self.parse_unlink(&arr),
            "COPY" => self.parse_copy(&arr),
            "DUMP" => self.parse_dump(&arr),
            "RESTORE" => self.parse_restore(&arr),
            "EVAL" => self.parse_eval(&arr),
            "EVALSHA" => self.parse_evalsha(&arr),
            "SCRIPT" => self.parse_script(&arr),
            "FUNCTION" => self.parse_function(&arr),
            "FCALL" => self.parse_fcall(&arr),
            "FCALL_RO" => self.parse_fcall_ro(&arr),
            "EVAL_RO" => self.parse_eval_ro(&arr),
            "EVALSHA_RO" => self.parse_evalsha_ro(&arr),
            "SAVE" => Ok(Command::Save),
            "BGSAVE" => Ok(Command::BgSave),
            "SLOWLOG" => self.parse_slowlog(&arr),
            "OBJECT" => self.parse_object(&arr),
            "DEBUG" => self.parse_debug(&arr),
            "ECHO" => self.parse_echo(&arr),
            "TIME" => Ok(Command::Time),
            "RANDOMKEY" => Ok(Command::RandomKey),
            "TOUCH" => self.parse_touch(&arr),
            "EXPIREAT" => self.parse_expireat(&arr),
            "PEXPIREAT" => self.parse_pexpireat(&arr),
            "EXPIRETIME" => self.parse_expiretime(&arr),
            "PEXPIRETIME" => self.parse_pexpiretime(&arr),
            "RENAMENX" => self.parse_renamenx(&arr),
            "SWAPDB" => self.parse_swapdb(&arr),
            "FLUSHDB" => Ok(Command::FlushDb),
            "SHUTDOWN" => self.parse_shutdown(&arr),
            "LASTSAVE" => Ok(Command::LastSave),
            "SUBSTR" => self.parse_substr(&arr),
            "LCS" => self.parse_lcs(&arr),
            "LMOVE" => self.parse_lmove(&arr),
            "RPOPLPUSH" => self.parse_rpoplpush(&arr),
            "LMPOP" => self.parse_lmpop(&arr),
            "BLMOVE" => self.parse_blmove(&arr),
            "BLMPOP" => self.parse_blmpop(&arr),
            "BRPOPLPUSH" => self.parse_brpoplpush(&arr),
            "MEMORY" => self.parse_memory(&arr),
            "LATENCY" => self.parse_latency(&arr),
            "RESET" => Ok(Command::Reset),
            "HELLO" => self.parse_hello(&arr),
            "MONITOR" => Ok(Command::Monitor),
            "QUIT" => Ok(Command::Quit),
            other => Ok(Command::Unknown(other.to_string())),
        }
    }

    /// 解析 PING 命令：PING [message]
    fn parse_ping(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() > 2 {
            return Err(AppError::Command(
                "PING 命令参数过多".to_string(),
            ));
        }

        let message = if arr.len() == 2 {
            Some(self.extract_string(&arr[1])?)
        } else {
            None
        };

        Ok(Command::Ping(message))
    }

    /// 解析 GET 命令：GET key
    fn parse_get(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "GET 命令需要 1 个参数".to_string(),
            ));
        }

        let key = self.extract_string(&arr[1])?;
        Ok(Command::Get(key))
    }

    /// 解析 SET 命令：SET key value [EX seconds]
    fn parse_set(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "SET 命令至少需要 key 和 value".to_string(),
            ));
        }

        let key = self.extract_string(&arr[1])?;
        let value = self.extract_bytes(&arr[2])?;

        let mut options = crate::storage::SetOptions::default();
        let mut i = 3;

        while i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match opt.as_str() {
                "NX" => options.nx = true,
                "XX" => options.xx = true,
                "GET" => options.get = true,
                "KEEPTTL" => options.keepttl = true,
                "EX" => {
                    i += 1;
                    if i >= arr.len() {
                        return Err(AppError::Command("EX 需要值".to_string()));
                    }
                    let seconds: u64 = self.extract_string(&arr[i])?.parse().map_err(|_| {
                        AppError::Command("EX 值必须是正整数".to_string())
                    })?;
                    options.expire = Some(crate::storage::SetExpireOption::Ex(seconds));
                }
                "PX" => {
                    i += 1;
                    if i >= arr.len() {
                        return Err(AppError::Command("PX 需要值".to_string()));
                    }
                    let millis: u64 = self.extract_string(&arr[i])?.parse().map_err(|_| {
                        AppError::Command("PX 值必须是正整数".to_string())
                    })?;
                    options.expire = Some(crate::storage::SetExpireOption::Px(millis));
                }
                "EXAT" => {
                    i += 1;
                    if i >= arr.len() {
                        return Err(AppError::Command("EXAT 需要值".to_string()));
                    }
                    let ts: u64 = self.extract_string(&arr[i])?.parse().map_err(|_| {
                        AppError::Command("EXAT 值必须是正整数".to_string())
                    })?;
                    options.expire = Some(crate::storage::SetExpireOption::ExAt(ts));
                }
                "PXAT" => {
                    i += 1;
                    if i >= arr.len() {
                        return Err(AppError::Command("PXAT 需要值".to_string()));
                    }
                    let ts: u64 = self.extract_string(&arr[i])?.parse().map_err(|_| {
                        AppError::Command("PXAT 值必须是正整数".to_string())
                    })?;
                    options.expire = Some(crate::storage::SetExpireOption::PxAt(ts));
                }
                _ => {
                    return Err(AppError::Command(
                        format!("SET 未知选项: {}", opt),
                    ));
                }
            }
            i += 1;
        }

        Ok(Command::Set(key, value, options))
    }

    /// 解析 DEL 命令：DEL key [key ...]
    fn parse_del(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "DEL 命令需要至少 1 个参数".to_string(),
            ));
        }

        let keys = arr[1..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;

        Ok(Command::Del(keys))
    }

    /// 解析 EXISTS 命令：EXISTS key [key ...]
    fn parse_exists(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "EXISTS 命令需要至少 1 个参数".to_string(),
            ));
        }

        let keys = arr[1..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;

        Ok(Command::Exists(keys))
    }

    /// 解析 EXPIRE 命令：EXPIRE key seconds
    fn parse_expire(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "EXPIRE 命令需要 2 个参数".to_string(),
            ));
        }

        let key = self.extract_string(&arr[1])?;
        let seconds: u64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("EXPIRE 的秒数必须是正整数".to_string())
        })?;

        Ok(Command::Expire(key, seconds))
    }

    /// 解析 TTL 命令：TTL key
    fn parse_ttl(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "TTL 命令需要 1 个参数".to_string(),
            ));
        }

        let key = self.extract_string(&arr[1])?;
        Ok(Command::Ttl(key))
    }

    /// 解析 MGET 命令：MGET key [key ...]
    fn parse_mget(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "MGET 命令需要至少 1 个参数".to_string(),
            ));
        }

        let keys = arr[1..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::MGet(keys))
    }

    /// 解析 MSET 命令：MSET key value [key value ...]
    fn parse_mset(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 || arr.len() % 2 == 0 {
            return Err(AppError::Command(
                "MSET 命令需要成对的 key value 参数".to_string(),
            ));
        }

        let mut pairs = Vec::new();
        for i in (1..arr.len()).step_by(2) {
            let key = self.extract_string(&arr[i])?;
            let value = self.extract_bytes(&arr[i + 1])?;
            pairs.push((key, value));
        }
        Ok(Command::MSet(pairs))
    }

    /// 解析 INCR 命令：INCR key
    fn parse_incr(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "INCR 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::Incr(key))
    }

    /// 解析 DECR 命令：DECR key
    fn parse_decr(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "DECR 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::Decr(key))
    }

    /// 解析 INCRBY 命令：INCRBY key delta
    fn parse_incrby(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "INCRBY 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let delta: i64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("INCRBY 的增量必须是整数".to_string())
        })?;
        Ok(Command::IncrBy(key, delta))
    }

    /// 解析 DECRBY 命令：DECRBY key delta
    fn parse_decrby(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "DECRBY 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let delta: i64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("DECRBY 的减量必须是整数".to_string())
        })?;
        Ok(Command::DecrBy(key, delta))
    }

    /// 解析 APPEND 命令：APPEND key value
    fn parse_append(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "APPEND 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let value = self.extract_bytes(&arr[2])?;
        Ok(Command::Append(key, value))
    }

    /// 解析 SETNX 命令：SETNX key value
    fn parse_setnx(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "SETNX 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let value = self.extract_bytes(&arr[2])?;
        Ok(Command::SetNx(key, value))
    }

    /// 解析 SETEX 命令：SETEX key seconds value
    fn parse_setex(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "SETEX 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let seconds: u64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("SETEX 的秒数必须是正整数".to_string())
        })?;
        let value = self.extract_bytes(&arr[3])?;
        Ok(Command::SetExCmd(key, value, seconds))
    }

    /// 解析 PSETEX 命令：PSETEX key milliseconds value
    fn parse_psetex(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "PSETEX 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let ms: u64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("PSETEX 的毫秒数必须是正整数".to_string())
        })?;
        let value = self.extract_bytes(&arr[3])?;
        Ok(Command::PSetEx(key, value, ms))
    }

    /// 解析 GETSET 命令：GETSET key value
    fn parse_getset(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "GETSET 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let value = self.extract_bytes(&arr[2])?;
        Ok(Command::GetSet(key, value))
    }

    /// 解析 GETDEL 命令：GETDEL key
    fn parse_getdel(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "GETDEL 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::GetDel(key))
    }

    /// 解析 GETEX 命令：GETEX key [EX seconds|PX milliseconds|EXAT timestamp|PXAT ms-timestamp|PERSIST]
    fn parse_getex(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "GETEX 命令需要至少 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        if arr.len() == 2 {
            return Ok(Command::GetEx(key, GetExOption::Persist));
        }

        let opt = self.extract_string(&arr[2])?.to_ascii_uppercase();
        match opt.as_str() {
            "EX" => {
                if arr.len() != 4 {
                    return Err(AppError::Command("GETEX EX 需要 1 个参数".to_string()));
                }
                let seconds: u64 = self.extract_string(&arr[3])?.parse().map_err(|_| {
                    AppError::Command("GETEX EX 值必须是正整数".to_string())
                })?;
                Ok(Command::GetEx(key, GetExOption::Ex(seconds)))
            }
            "PX" => {
                if arr.len() != 4 {
                    return Err(AppError::Command("GETEX PX 需要 1 个参数".to_string()));
                }
                let ms: u64 = self.extract_string(&arr[3])?.parse().map_err(|_| {
                    AppError::Command("GETEX PX 值必须是正整数".to_string())
                })?;
                Ok(Command::GetEx(key, GetExOption::Px(ms)))
            }
            "EXAT" => {
                if arr.len() != 4 {
                    return Err(AppError::Command("GETEX EXAT 需要 1 个参数".to_string()));
                }
                let ts: u64 = self.extract_string(&arr[3])?.parse().map_err(|_| {
                    AppError::Command("GETEX EXAT 值必须是正整数".to_string())
                })?;
                Ok(Command::GetEx(key, GetExOption::ExAt(ts)))
            }
            "PXAT" => {
                if arr.len() != 4 {
                    return Err(AppError::Command("GETEX PXAT 需要 1 个参数".to_string()));
                }
                let ts: u64 = self.extract_string(&arr[3])?.parse().map_err(|_| {
                    AppError::Command("GETEX PXAT 值必须是正整数".to_string())
                })?;
                Ok(Command::GetEx(key, GetExOption::PxAt(ts)))
            }
            "PERSIST" => {
                if arr.len() != 3 {
                    return Err(AppError::Command("GETEX PERSIST 不需要额外参数".to_string()));
                }
                Ok(Command::GetEx(key, GetExOption::Persist))
            }
            _ => Err(AppError::Command(format!(
                "GETEX 不支持的选项: {}",
                opt
            ))),
        }
    }

    /// 解析 MSETNX 命令：MSETNX key value [key value ...]
    fn parse_msetnx(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 || arr.len() % 2 == 0 {
            return Err(AppError::Command(
                "MSETNX 命令需要成对的 key-value 参数".to_string(),
            ));
        }
        let mut pairs = Vec::new();
        for i in (1..arr.len()).step_by(2) {
            let key = self.extract_string(&arr[i])?;
            let value = self.extract_bytes(&arr[i + 1])?;
            pairs.push((key, value));
        }
        Ok(Command::MSetNx(pairs))
    }

    /// 解析 INCRBYFLOAT 命令：INCRBYFLOAT key increment
    fn parse_incrbyfloat(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "INCRBYFLOAT 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let delta: f64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("INCRBYFLOAT 的增量必须是有效的浮点数".to_string())
        })?;
        Ok(Command::IncrByFloat(key, delta))
    }

    /// 解析 SETRANGE 命令：SETRANGE key offset value
    fn parse_setrange(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "SETRANGE 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let offset: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("SETRANGE 的 offset 必须是非负整数".to_string())
        })?;
        let value = self.extract_bytes(&arr[3])?;
        Ok(Command::SetRange(key, offset, value))
    }

    /// 解析 GETRANGE 命令：GETRANGE key start end
    fn parse_getrange(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "GETRANGE 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let start: i64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("GETRANGE 的 start 必须是整数".to_string())
        })?;
        let end: i64 = self.extract_string(&arr[3])?.parse().map_err(|_| {
            AppError::Command("GETRANGE 的 end 必须是整数".to_string())
        })?;
        Ok(Command::GetRange(key, start, end))
    }

    /// 解析 STRLEN 命令：STRLEN key
    fn parse_strlen(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "STRLEN 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::StrLen(key))
    }

    /// 解析 LPUSH 命令：LPUSH key value [value ...]
    fn parse_lpush(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "LPUSH 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let values = arr[2..]
            .iter()
            .map(|v| self.extract_bytes(v))
            .collect::<Result<Vec<Bytes>>>()?;
        Ok(Command::LPush(key, values))
    }

    /// 解析 RPUSH 命令：RPUSH key value [value ...]
    fn parse_rpush(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "RPUSH 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let values = arr[2..]
            .iter()
            .map(|v| self.extract_bytes(v))
            .collect::<Result<Vec<Bytes>>>()?;
        Ok(Command::RPush(key, values))
    }

    /// 解析 LPOP 命令：LPOP key
    fn parse_lpop(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "LPOP 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::LPop(key))
    }

    /// 解析 RPOP 命令：RPOP key
    fn parse_rpop(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "RPOP 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::RPop(key))
    }

    /// 解析 LLEN 命令：LLEN key
    fn parse_llen(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "LLEN 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::LLen(key))
    }

    /// 解析 LRANGE 命令：LRANGE key start stop
    fn parse_lrange(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "LRANGE 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let start: i64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("LRANGE 的 start 必须是整数".to_string())
        })?;
        let stop: i64 = self.extract_string(&arr[3])?.parse().map_err(|_| {
            AppError::Command("LRANGE 的 stop 必须是整数".to_string())
        })?;
        Ok(Command::LRange(key, start, stop))
    }

    /// 解析 LINDEX 命令：LINDEX key index
    fn parse_lindex(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "LINDEX 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let index: i64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("LINDEX 的 index 必须是整数".to_string())
        })?;
        Ok(Command::LIndex(key, index))
    }

    /// 解析 LSET 命令：LSET key index value
    fn parse_lset(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "LSET 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let index: i64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("LSET 的 index 必须是整数".to_string())
        })?;
        let value = self.extract_bytes(&arr[3])?;
        Ok(Command::LSet(key, index, value))
    }

    /// 解析 LINSERT 命令：LINSERT key BEFORE|AFTER pivot value
    fn parse_linsert(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 5 {
            return Err(AppError::Command(
                "LINSERT 命令需要 4 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let pos = match self.extract_string(&arr[2])?.to_ascii_uppercase().as_str() {
            "BEFORE" => crate::storage::LInsertPosition::Before,
            "AFTER" => crate::storage::LInsertPosition::After,
            other => {
                return Err(AppError::Command(format!(
                    "LINSERT 位置必须是 BEFORE 或 AFTER，得到: {}",
                    other
                )))
            }
        };
        let pivot = self.extract_bytes(&arr[3])?;
        let value = self.extract_bytes(&arr[4])?;
        Ok(Command::LInsert(key, pos, pivot, value))
    }

    /// 解析 LREM 命令：LREM key count value
    fn parse_lrem(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "LREM 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let count: i64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("LREM 的 count 必须是整数".to_string())
        })?;
        let value = self.extract_bytes(&arr[3])?;
        Ok(Command::LRem(key, count, value))
    }

    /// 解析 LTRIM 命令：LTRIM key start stop
    fn parse_ltrim(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "LTRIM 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let start: i64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("LTRIM 的 start 必须是整数".to_string())
        })?;
        let stop: i64 = self.extract_string(&arr[3])?.parse().map_err(|_| {
            AppError::Command("LTRIM 的 stop 必须是整数".to_string())
        })?;
        Ok(Command::LTrim(key, start, stop))
    }

    /// 解析 LPOS 命令：LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
    fn parse_lpos(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "LPOS 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let value = self.extract_bytes(&arr[2])?;
        let mut rank = 1i64;
        let mut count = 0i64;
        let mut maxlen = 0i64;

        let mut i = 3;
        while i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match opt.as_str() {
                "RANK" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("LPOS RANK 需要参数".to_string()));
                    }
                    rank = self.extract_string(&arr[i + 1])?.parse().map_err(|_| {
                        AppError::Command("LPOS RANK 必须是整数".to_string())
                    })?;
                    i += 2;
                }
                "COUNT" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("LPOS COUNT 需要参数".to_string()));
                    }
                    count = self.extract_string(&arr[i + 1])?.parse().map_err(|_| {
                        AppError::Command("LPOS COUNT 必须是整数".to_string())
                    })?;
                    i += 2;
                }
                "MAXLEN" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("LPOS MAXLEN 需要参数".to_string()));
                    }
                    maxlen = self.extract_string(&arr[i + 1])?.parse().map_err(|_| {
                        AppError::Command("LPOS MAXLEN 必须是整数".to_string())
                    })?;
                    i += 2;
                }
                _ => {
                    return Err(AppError::Command(format!(
                        "LPOS 不支持的选项: {}",
                        opt
                    )))
                }
            }
        }
        Ok(Command::LPos(key, value, rank, count, maxlen))
    }

    /// 解析 BLPOP 命令：BLPOP key [key ...] timeout
    fn parse_blpop(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "BLPOP 命令需要至少 2 个参数".to_string(),
            ));
        }
        let timeout: f64 = self.extract_string(&arr[arr.len() - 1])?.parse().map_err(|_| {
            AppError::Command("BLPOP 的 timeout 必须是数字".to_string())
        })?;
        let mut keys = Vec::new();
        for i in 1..arr.len() - 1 {
            keys.push(self.extract_string(&arr[i])?);
        }
        Ok(Command::BLPop(keys, timeout))
    }

    /// 解析 BRPOP 命令：BRPOP key [key ...] timeout
    fn parse_brpop(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "BRPOP 命令需要至少 2 个参数".to_string(),
            ));
        }
        let timeout: f64 = self.extract_string(&arr[arr.len() - 1])?.parse().map_err(|_| {
            AppError::Command("BRPOP 的 timeout 必须是数字".to_string())
        })?;
        let mut keys = Vec::new();
        for i in 1..arr.len() - 1 {
            keys.push(self.extract_string(&arr[i])?);
        }
        Ok(Command::BRPop(keys, timeout))
    }

    /// 解析 HSET 命令：HSET key field value [field value ...]
    fn parse_hset(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 || arr.len() % 2 == 1 {
            return Err(AppError::Command(
                "HSET 命令需要成对的 field value 参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let mut pairs = Vec::new();
        for i in (2..arr.len()).step_by(2) {
            let field = self.extract_string(&arr[i])?;
            let value = self.extract_bytes(&arr[i + 1])?;
            pairs.push((field, value));
        }
        Ok(Command::HSet(key, pairs))
    }

    /// 解析 HGET 命令：HGET key field
    fn parse_hget(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "HGET 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let field = self.extract_string(&arr[2])?;
        Ok(Command::HGet(key, field))
    }

    /// 解析 HDEL 命令：HDEL key field [field ...]
    fn parse_hdel(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "HDEL 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let fields = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::HDel(key, fields))
    }

    /// 解析 HEXISTS 命令：HEXISTS key field
    fn parse_hexists(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "HEXISTS 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let field = self.extract_string(&arr[2])?;
        Ok(Command::HExists(key, field))
    }

    /// 解析 HGETALL 命令：HGETALL key
    fn parse_hgetall(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "HGETALL 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::HGetAll(key))
    }

    /// 解析 HLEN 命令：HLEN key
    fn parse_hlen(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "HLEN 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::HLen(key))
    }

    /// 解析 HMSET 命令：HMSET key field value [field value ...]
    fn parse_hmset(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 || arr.len() % 2 == 1 {
            return Err(AppError::Command(
                "HMSET 命令需要成对的 field value 参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let mut pairs = Vec::new();
        for i in (2..arr.len()).step_by(2) {
            let field = self.extract_string(&arr[i])?;
            let value = self.extract_bytes(&arr[i + 1])?;
            pairs.push((field, value));
        }
        Ok(Command::HMSet(key, pairs))
    }

    /// 解析 HMGET 命令：HMGET key field [field ...]
    fn parse_hmget(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "HMGET 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let fields = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::HMGet(key, fields))
    }

    /// 解析 HINCRBY 命令：HINCRBY key field increment
    fn parse_hincrby(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "HINCRBY 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let field = self.extract_string(&arr[2])?;
        let delta: i64 = self.extract_string(&arr[3])?.parse().map_err(|_| {
            AppError::Command("HINCRBY 的增量必须是整数".to_string())
        })?;
        Ok(Command::HIncrBy(key, field, delta))
    }

    /// 解析 HINCRBYFLOAT 命令：HINCRBYFLOAT key field increment
    fn parse_hincrbyfloat(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "HINCRBYFLOAT 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let field = self.extract_string(&arr[2])?;
        let delta: f64 = self.extract_string(&arr[3])?.parse().map_err(|_| {
            AppError::Command("HINCRBYFLOAT 的增量必须是有效的浮点数".to_string())
        })?;
        Ok(Command::HIncrByFloat(key, field, delta))
    }

    /// 解析 HKEYS 命令：HKEYS key
    fn parse_hkeys(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "HKEYS 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::HKeys(key))
    }

    /// 解析 HVALS 命令：HVALS key
    fn parse_hvals(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "HVALS 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::HVals(key))
    }

    /// 解析 HSETNX 命令：HSETNX key field value
    fn parse_hsetnx(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "HSETNX 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let field = self.extract_string(&arr[2])?;
        let value = self.extract_bytes(&arr[3])?;
        Ok(Command::HSetNx(key, field, value))
    }

    /// 解析 HRANDFIELD 命令：HRANDFIELD key [count [WITHVALUES]]
    fn parse_hrandfield(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 || arr.len() > 4 {
            return Err(AppError::Command(
                "HRANDFIELD 命令参数错误".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let mut count = 1i64;
        let mut with_values = false;
        if arr.len() >= 3 {
            count = self.extract_string(&arr[2])?.parse().map_err(|_| {
                AppError::Command("HRANDFIELD 的 count 必须是整数".to_string())
            })?;
        }
        if arr.len() == 4 {
            let opt = self.extract_string(&arr[3])?.to_ascii_uppercase();
            if opt == "WITHVALUES" {
                with_values = true;
            } else {
                return Err(AppError::Command(format!(
                    "HRANDFIELD 不支持的选项: {}",
                    opt
                )));
            }
        }
        Ok(Command::HRandField(key, count, with_values))
    }

    /// 解析 HSCAN 命令：HSCAN key cursor [MATCH pattern] [COUNT count]
    fn parse_hscan(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "HSCAN 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let cursor: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("HSCAN 的 cursor 必须是整数".to_string())
        })?;
        let mut pattern = "*".to_string();
        let mut count = 0usize;

        let mut i = 3;
        while i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match opt.as_str() {
                "MATCH" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("HSCAN MATCH 需要参数".to_string()));
                    }
                    pattern = self.extract_string(&arr[i + 1])?;
                    i += 2;
                }
                "COUNT" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("HSCAN COUNT 需要参数".to_string()));
                    }
                    count = self.extract_string(&arr[i + 1])?.parse().map_err(|_| {
                        AppError::Command("HSCAN COUNT 必须是整数".to_string())
                    })?;
                    i += 2;
                }
                _ => {
                    return Err(AppError::Command(format!(
                        "HSCAN 不支持的选项: {}",
                        opt
                    )))
                }
            }
        }
        Ok(Command::HScan(key, cursor, pattern, count))
    }

    /// 解析 HEXPIRE 命令：HEXPIRE key field [field ...] seconds
    fn parse_hexpire(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(
                "HEXPIRE 命令需要至少 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let seconds: u64 = self.extract_string(&arr[arr.len() - 1])?.parse().map_err(|_| {
            AppError::Command("HEXPIRE 的秒数必须是正整数".to_string())
        })?;
        let fields: Vec<String> = arr[2..arr.len() - 1]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::HExpire(key, fields, seconds))
    }

    /// 解析 HPEXPIRE 命令：HPEXPIRE key field [field ...] milliseconds
    fn parse_hpexpire(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(
                "HPEXPIRE 命令需要至少 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let ms: u64 = self.extract_string(&arr[arr.len() - 1])?.parse().map_err(|_| {
            AppError::Command("HPEXPIRE 的毫秒数必须是正整数".to_string())
        })?;
        let fields: Vec<String> = arr[2..arr.len() - 1]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::HPExpire(key, fields, ms))
    }

    /// 解析 HEXPIREAT 命令：HEXPIREAT key field [field ...] timestamp-seconds
    fn parse_hexpireat(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(
                "HEXPIREAT 命令需要至少 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let ts: u64 = self.extract_string(&arr[arr.len() - 1])?.parse().map_err(|_| {
            AppError::Command("HEXPIREAT 的时间戳必须是正整数".to_string())
        })?;
        let fields: Vec<String> = arr[2..arr.len() - 1]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::HExpireAt(key, fields, ts))
    }

    /// 解析 HPEXPIREAT 命令：HPEXPIREAT key field [field ...] timestamp-ms
    fn parse_hpexpireat(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(
                "HPEXPIREAT 命令需要至少 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let ts: u64 = self.extract_string(&arr[arr.len() - 1])?.parse().map_err(|_| {
            AppError::Command("HPEXPIREAT 的时间戳必须是正整数".to_string())
        })?;
        let fields: Vec<String> = arr[2..arr.len() - 1]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::HPExpireAt(key, fields, ts))
    }

    /// 解析 HTTL 命令：HTTL key field [field ...]
    fn parse_httl(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "HTTL 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let fields: Vec<String> = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::HTtl(key, fields))
    }

    /// 解析 HPTTL 命令：HPTTL key field [field ...]
    fn parse_hpttl(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "HPTTL 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let fields: Vec<String> = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::HPTtl(key, fields))
    }

    /// 解析 HEXPIRETIME 命令：HEXPIRETIME key field [field ...]
    fn parse_hexpiretime(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "HEXPIRETIME 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let fields: Vec<String> = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::HExpireTime(key, fields))
    }

    /// 解析 HPEXPIRETIME 命令：HPEXPIRETIME key field [field ...]
    fn parse_hpexpiretime(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "HPEXPIRETIME 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let fields: Vec<String> = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::HPExpireTime(key, fields))
    }

    /// 解析 HPERSIST 命令：HPERSIST key field [field ...]
    fn parse_hpersist(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "HPERSIST 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let fields: Vec<String> = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::HPersist(key, fields))
    }

    /// 解析 SADD 命令：SADD key member [member ...]
    fn parse_sadd(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "SADD 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let members = arr[2..]
            .iter()
            .map(|v| self.extract_bytes(v))
            .collect::<Result<Vec<Bytes>>>()?;
        Ok(Command::SAdd(key, members))
    }

    /// 解析 SREM 命令：SREM key member [member ...]
    fn parse_srem(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "SREM 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let members = arr[2..]
            .iter()
            .map(|v| self.extract_bytes(v))
            .collect::<Result<Vec<Bytes>>>()?;
        Ok(Command::SRem(key, members))
    }

    /// 解析 SMEMBERS 命令：SMEMBERS key
    fn parse_smembers(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "SMEMBERS 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::SMembers(key))
    }

    /// 解析 SISMEMBER 命令：SISMEMBER key member
    fn parse_sismember(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "SISMEMBER 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let member = self.extract_bytes(&arr[2])?;
        Ok(Command::SIsMember(key, member))
    }

    /// 解析 SCARD 命令：SCARD key
    fn parse_scard(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "SCARD 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::SCard(key))
    }

    /// 解析 SINTER 命令：SINTER key [key ...]
    fn parse_sinter(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "SINTER 命令需要至少 1 个参数".to_string(),
            ));
        }
        let keys = arr[1..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::SInter(keys))
    }

    /// 解析 SUNION 命令：SUNION key [key ...]
    fn parse_sunion(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "SUNION 命令需要至少 1 个参数".to_string(),
            ));
        }
        let keys = arr[1..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::SUnion(keys))
    }

    /// 解析 SDIFF 命令：SDIFF key [key ...]
    fn parse_sdiff(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "SDIFF 命令需要至少 1 个参数".to_string(),
            ));
        }
        let keys = arr[1..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::SDiff(keys))
    }

    /// 解析 SPOP 命令：SPOP key [count]
    fn parse_spop(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 || arr.len() > 3 {
            return Err(AppError::Command(
                "SPOP 命令参数错误".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let count = if arr.len() == 3 {
            self.extract_string(&arr[2])?.parse().map_err(|_| {
                AppError::Command("SPOP 的 count 必须是整数".to_string())
            })?
        } else {
            1i64
        };
        Ok(Command::SPop(key, count))
    }

    /// 解析 SRANDMEMBER 命令：SRANDMEMBER key [count]
    fn parse_srandmember(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 || arr.len() > 3 {
            return Err(AppError::Command(
                "SRANDMEMBER 命令参数错误".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let count = if arr.len() == 3 {
            self.extract_string(&arr[2])?.parse().map_err(|_| {
                AppError::Command("SRANDMEMBER 的 count 必须是整数".to_string())
            })?
        } else {
            1i64
        };
        Ok(Command::SRandMember(key, count))
    }

    /// 解析 SMOVE 命令：SMOVE source destination member
    fn parse_smove(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "SMOVE 命令需要 3 个参数".to_string(),
            ));
        }
        let source = self.extract_string(&arr[1])?;
        let destination = self.extract_string(&arr[2])?;
        let member = self.extract_bytes(&arr[3])?;
        Ok(Command::SMove(source, destination, member))
    }

    /// 解析 SINTERSTORE 命令：SINTERSTORE destination key [key ...]
    fn parse_sinterstore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "SINTERSTORE 命令需要至少 2 个参数".to_string(),
            ));
        }
        let destination = self.extract_string(&arr[1])?;
        let keys = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::SInterStore(destination, keys))
    }

    /// 解析 SUNIONSTORE 命令：SUNIONSTORE destination key [key ...]
    fn parse_sunionstore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "SUNIONSTORE 命令需要至少 2 个参数".to_string(),
            ));
        }
        let destination = self.extract_string(&arr[1])?;
        let keys = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::SUnionStore(destination, keys))
    }

    /// 解析 SDIFFSTORE 命令：SDIFFSTORE destination key [key ...]
    fn parse_sdiffstore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "SDIFFSTORE 命令需要至少 2 个参数".to_string(),
            ));
        }
        let destination = self.extract_string(&arr[1])?;
        let keys = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::SDiffStore(destination, keys))
    }

    /// 解析 SSCAN 命令：SSCAN key cursor [MATCH pattern] [COUNT count]
    fn parse_sscan(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "SSCAN 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let cursor: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("SSCAN 的 cursor 必须是整数".to_string())
        })?;
        let mut pattern = "*".to_string();
        let mut count = 0usize;

        let mut i = 3;
        while i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match opt.as_str() {
                "MATCH" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("SSCAN MATCH 需要参数".to_string()));
                    }
                    pattern = self.extract_string(&arr[i + 1])?;
                    i += 2;
                }
                "COUNT" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("SSCAN COUNT 需要参数".to_string()));
                    }
                    count = self.extract_string(&arr[i + 1])?.parse().map_err(|_| {
                        AppError::Command("SSCAN COUNT 必须是整数".to_string())
                    })?;
                    i += 2;
                }
                _ => {
                    return Err(AppError::Command(format!(
                        "SSCAN 不支持的选项: {}",
                        opt
                    )))
                }
            }
        }
        Ok(Command::SScan(key, cursor, pattern, count))
    }

    /// 解析 ZADD 命令：ZADD key score member [score member ...]
    fn parse_zadd(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 || arr.len() % 2 == 1 {
            return Err(AppError::Command(
                "ZADD 命令需要成对的 score member 参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let mut pairs = Vec::new();
        for i in (2..arr.len()).step_by(2) {
            let score: f64 = self.extract_string(&arr[i])?.parse().map_err(|_| {
                AppError::Command("ZADD 的 score 必须是数字".to_string())
            })?;
            let member = self.extract_string(&arr[i + 1])?;
            pairs.push((score, member));
        }
        Ok(Command::ZAdd(key, pairs))
    }

    /// 解析 ZREM 命令：ZREM key member [member ...]
    fn parse_zrem(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "ZREM 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let members = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::ZRem(key, members))
    }

    /// 解析 ZSCORE 命令：ZSCORE key member
    fn parse_zscore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "ZSCORE 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let member = self.extract_string(&arr[2])?;
        Ok(Command::ZScore(key, member))
    }

    /// 解析 ZRANK 命令：ZRANK key member
    fn parse_zrank(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "ZRANK 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let member = self.extract_string(&arr[2])?;
        Ok(Command::ZRank(key, member))
    }

    /// 解析 ZRANGE 命令：ZRANGE key start stop [WITHSCORES]
    fn parse_zrange(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(
                "ZRANGE 命令需要至少 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let min = self.extract_string(&arr[2])?;
        let max = self.extract_string(&arr[3])?;

        // 检查是否使用统一语法（包含 BYSCORE/BYLEX/REV/LIMIT）
        let mut use_unified = false;
        for i in 4..arr.len() {
            let opt = self.extract_string(&arr[i]).unwrap_or_default().to_ascii_uppercase();
            if matches!(opt.as_str(), "BYSCORE" | "BYLEX" | "REV" | "LIMIT") {
                use_unified = true;
                break;
            }
        }

        if use_unified {
            let mut by_score = false;
            let mut by_lex = false;
            let mut rev = false;
            let mut with_scores = false;
            let mut limit_offset = 0usize;
            let mut limit_count = 0usize;
            let mut i = 4;
            while i < arr.len() {
                let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
                match opt.as_str() {
                    "BYSCORE" => { by_score = true; i += 1; }
                    "BYLEX" => { by_lex = true; i += 1; }
                    "REV" => { rev = true; i += 1; }
                    "WITHSCORES" => { with_scores = true; i += 1; }
                    "LIMIT" => {
                        if i + 2 >= arr.len() {
                            return Err(AppError::Command("ZRANGE LIMIT 需要 offset count".to_string()));
                        }
                        limit_offset = self.extract_string(&arr[i + 1])?.parse().map_err(|_| {
                            AppError::Command("LIMIT offset 必须是整数".to_string())
                        })?;
                        limit_count = self.extract_string(&arr[i + 2])?.parse().map_err(|_| {
                            AppError::Command("LIMIT count 必须是整数".to_string())
                        })?;
                        i += 3;
                    }
                    _ => {
                        return Err(AppError::Command(format!("ZRANGE 不支持的选项: {}", opt)));
                    }
                }
            }
            Ok(Command::ZRangeUnified(key, min, max, by_score, by_lex, rev, with_scores, limit_offset, limit_count))
        } else {
            // 传统语法：ZRANGE key start stop [WITHSCORES]
            let start: isize = min.parse().map_err(|_| {
                AppError::Command("ZRANGE 的 start 必须是整数".to_string())
            })?;
            let stop: isize = max.parse().map_err(|_| {
                AppError::Command("ZRANGE 的 stop 必须是整数".to_string())
            })?;
            let with_scores = if arr.len() == 5 {
                let flag = self.extract_string(&arr[4])?.to_ascii_uppercase();
                if flag == "WITHSCORES" {
                    true
                } else {
                    return Err(AppError::Command(
                        "ZRANGE 可选参数只能是 WITHSCORES".to_string(),
                    ));
                }
            } else {
                false
            };
            Ok(Command::ZRange(key, start, stop, with_scores))
        }
    }

    /// 解析 ZRANGEBYSCORE 命令：ZRANGEBYSCORE key min max [WITHSCORES]
    fn parse_zrangebyscore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 || arr.len() > 5 {
            return Err(AppError::Command(
                "ZRANGEBYSCORE 命令参数数量错误".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let min: f64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("ZRANGEBYSCORE 的 min 必须是数字".to_string())
        })?;
        let max: f64 = self.extract_string(&arr[3])?.parse().map_err(|_| {
            AppError::Command("ZRANGEBYSCORE 的 max 必须是数字".to_string())
        })?;
        let with_scores = if arr.len() == 5 {
            let flag = self.extract_string(&arr[4])?.to_ascii_uppercase();
            if flag == "WITHSCORES" {
                true
            } else {
                return Err(AppError::Command(
                    "ZRANGEBYSCORE 可选参数只能是 WITHSCORES".to_string(),
                ));
            }
        } else {
            false
        };
        Ok(Command::ZRangeByScore(key, min, max, with_scores))
    }

    /// 解析 ZCARD 命令：ZCARD key
    fn parse_zcard(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "ZCARD 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::ZCard(key))
    }

    /// 解析 ZREVRANGE 命令：ZREVRANGE key start stop [WITHSCORES]
    fn parse_zrevrange(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 || arr.len() > 5 {
            return Err(AppError::Command(
                "ZREVRANGE 命令参数数量错误".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let start: isize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("ZREVRANGE 的 start 必须是整数".to_string())
        })?;
        let stop: isize = self.extract_string(&arr[3])?.parse().map_err(|_| {
            AppError::Command("ZREVRANGE 的 stop 必须是整数".to_string())
        })?;
        let with_scores = if arr.len() == 5 {
            let flag = self.extract_string(&arr[4])?.to_ascii_uppercase();
            if flag == "WITHSCORES" {
                true
            } else {
                return Err(AppError::Command(
                    "ZREVRANGE 可选参数只能是 WITHSCORES".to_string(),
                ));
            }
        } else {
            false
        };
        Ok(Command::ZRevRange(key, start, stop, with_scores))
    }

    /// 解析 ZREVRANK 命令：ZREVRANK key member
    fn parse_zrevrank(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "ZREVRANK 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let member = self.extract_string(&arr[2])?;
        Ok(Command::ZRevRank(key, member))
    }

    /// 解析 ZINCRBY 命令：ZINCRBY key increment member
    fn parse_zincrby(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "ZINCRBY 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let increment: f64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("ZINCRBY 的 increment 必须是数字".to_string())
        })?;
        let member = self.extract_string(&arr[3])?;
        Ok(Command::ZIncrBy(key, increment, member))
    }

    /// 解析 ZCOUNT 命令：ZCOUNT key min max
    fn parse_zcount(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "ZCOUNT 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let min: f64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("ZCOUNT 的 min 必须是数字".to_string())
        })?;
        let max: f64 = self.extract_string(&arr[3])?.parse().map_err(|_| {
            AppError::Command("ZCOUNT 的 max 必须是数字".to_string())
        })?;
        Ok(Command::ZCount(key, min, max))
    }

    /// 解析 ZPOPMIN 命令：ZPOPMIN key [count]
    fn parse_zpopmin(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 || arr.len() > 3 {
            return Err(AppError::Command(
                "ZPOPMIN 命令参数数量错误".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let count = if arr.len() == 3 {
            self.extract_string(&arr[2])?.parse().map_err(|_| {
                AppError::Command("ZPOPMIN 的 count 必须是整数".to_string())
            })?
        } else {
            1
        };
        Ok(Command::ZPopMin(key, count))
    }

    /// 解析 ZPOPMAX 命令：ZPOPMAX key [count]
    fn parse_zpopmax(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 || arr.len() > 3 {
            return Err(AppError::Command(
                "ZPOPMAX 命令参数数量错误".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let count = if arr.len() == 3 {
            self.extract_string(&arr[2])?.parse().map_err(|_| {
                AppError::Command("ZPOPMAX 的 count 必须是整数".to_string())
            })?
        } else {
            1
        };
        Ok(Command::ZPopMax(key, count))
    }

    /// 解析 ZUNIONSTORE 命令：ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
    fn parse_zunionstore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(
                "ZUNIONSTORE 命令参数数量错误".to_string(),
            ));
        }
        let destination = self.extract_string(&arr[1])?;
        let numkeys: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("ZUNIONSTORE 的 numkeys 必须是整数".to_string())
        })?;
        if arr.len() < 3 + numkeys {
            return Err(AppError::Command(
                "ZUNIONSTORE 提供的 key 数量不足".to_string(),
            ));
        }
        let keys: Vec<String> = arr[3..3 + numkeys]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;

        let mut weights = Vec::new();
        let mut aggregate = "SUM".to_string();
        let mut idx = 3 + numkeys;
        while idx < arr.len() {
            let flag = self.extract_string(&arr[idx])?.to_ascii_uppercase();
            if flag == "WEIGHTS" {
                idx += 1;
                for _ in 0..numkeys {
                    if idx >= arr.len() {
                        return Err(AppError::Command(
                            "ZUNIONSTORE WEIGHTS 参数不足".to_string(),
                        ));
                    }
                    let w: f64 = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                        AppError::Command("ZUNIONSTORE 的 weight 必须是数字".to_string())
                    })?;
                    weights.push(w);
                    idx += 1;
                }
            } else if flag == "AGGREGATE" {
                idx += 1;
                if idx >= arr.len() {
                    return Err(AppError::Command(
                        "ZUNIONSTORE AGGREGATE 缺少参数".to_string(),
                    ));
                }
                aggregate = self.extract_string(&arr[idx])?.to_ascii_uppercase();
                if aggregate != "SUM" && aggregate != "MIN" && aggregate != "MAX" {
                    return Err(AppError::Command(
                        "ZUNIONSTORE AGGREGATE 只能是 SUM|MIN|MAX".to_string(),
                    ));
                }
                idx += 1;
            } else {
                return Err(AppError::Command(
                    format!("ZUNIONSTORE 未知参数: {}", flag),
                ));
            }
        }
        Ok(Command::ZUnionStore(destination, keys, weights, aggregate))
    }

    /// 解析 ZINTERSTORE 命令：ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
    fn parse_zinterstore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(
                "ZINTERSTORE 命令参数数量错误".to_string(),
            ));
        }
        let destination = self.extract_string(&arr[1])?;
        let numkeys: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("ZINTERSTORE 的 numkeys 必须是整数".to_string())
        })?;
        if arr.len() < 3 + numkeys {
            return Err(AppError::Command(
                "ZINTERSTORE 提供的 key 数量不足".to_string(),
            ));
        }
        let keys: Vec<String> = arr[3..3 + numkeys]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;

        let mut weights = Vec::new();
        let mut aggregate = "SUM".to_string();
        let mut idx = 3 + numkeys;
        while idx < arr.len() {
            let flag = self.extract_string(&arr[idx])?.to_ascii_uppercase();
            if flag == "WEIGHTS" {
                idx += 1;
                for _ in 0..numkeys {
                    if idx >= arr.len() {
                        return Err(AppError::Command(
                            "ZINTERSTORE WEIGHTS 参数不足".to_string(),
                        ));
                    }
                    let w: f64 = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                        AppError::Command("ZINTERSTORE 的 weight 必须是数字".to_string())
                    })?;
                    weights.push(w);
                    idx += 1;
                }
            } else if flag == "AGGREGATE" {
                idx += 1;
                if idx >= arr.len() {
                    return Err(AppError::Command(
                        "ZINTERSTORE AGGREGATE 缺少参数".to_string(),
                    ));
                }
                aggregate = self.extract_string(&arr[idx])?.to_ascii_uppercase();
                if aggregate != "SUM" && aggregate != "MIN" && aggregate != "MAX" {
                    return Err(AppError::Command(
                        "ZINTERSTORE AGGREGATE 只能是 SUM|MIN|MAX".to_string(),
                    ));
                }
                idx += 1;
            } else {
                return Err(AppError::Command(
                    format!("ZINTERSTORE 未知参数: {}", flag),
                ));
            }
        }
        Ok(Command::ZInterStore(destination, keys, weights, aggregate))
    }

    /// 解析 ZSCAN 命令：ZSCAN key cursor [MATCH pattern] [COUNT count]
    fn parse_zscan(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "ZSCAN 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let cursor: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("ZSCAN 的 cursor 必须是整数".to_string())
        })?;
        let mut pattern = String::new();
        let mut count = 0usize;
        let mut idx = 3;
        while idx < arr.len() {
            let flag = self.extract_string(&arr[idx])?.to_ascii_uppercase();
            if flag == "MATCH" {
                idx += 1;
                if idx >= arr.len() {
                    return Err(AppError::Command(
                        "ZSCAN MATCH 缺少 pattern".to_string(),
                    ));
                }
                pattern = self.extract_string(&arr[idx])?;
                idx += 1;
            } else if flag == "COUNT" {
                idx += 1;
                if idx >= arr.len() {
                    return Err(AppError::Command(
                        "ZSCAN COUNT 缺少 count".to_string(),
                    ));
                }
                count = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                    AppError::Command("ZSCAN 的 count 必须是整数".to_string())
                })?;
                idx += 1;
            } else {
                return Err(AppError::Command(
                    format!("ZSCAN 未知参数: {}", flag),
                ));
            }
        }
        Ok(Command::ZScan(key, cursor, pattern, count))
    }

    /// 解析 ZRANGEBYLEX 命令：ZRANGEBYLEX key min max
    fn parse_zrangebylex(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "ZRANGEBYLEX 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let min = self.extract_string(&arr[2])?;
        let max = self.extract_string(&arr[3])?;
        Ok(Command::ZRangeByLex(key, min, max))
    }

    /// 解析 SINTERCARD 命令：SINTERCARD numkeys key [key ...] [LIMIT limit]
    fn parse_sintercard(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "SINTERCARD 命令需要至少 2 个参数".to_string(),
            ));
        }
        let numkeys: usize = self.extract_string(&arr[1])?.parse().map_err(|_| {
            AppError::Command("SINTERCARD numkeys 必须是整数".to_string())
        })?;
        if arr.len() < 2 + numkeys {
            return Err(AppError::Command(
                "SINTERCARD 参数数量不足".to_string(),
            ));
        }
        let keys: Vec<String> = arr[2..2 + numkeys]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        let mut limit = 0usize;
        if arr.len() > 2 + numkeys {
            let opt = self.extract_string(&arr[2 + numkeys])?.to_ascii_uppercase();
            if opt == "LIMIT" {
                if arr.len() < 4 + numkeys {
                    return Err(AppError::Command("SINTERCARD LIMIT 需要参数".to_string()));
                }
                limit = self.extract_string(&arr[3 + numkeys])?.parse().map_err(|_| {
                    AppError::Command("SINTERCARD LIMIT 必须是整数".to_string())
                })?;
            }
        }
        Ok(Command::SInterCard(keys, limit))
    }

    /// 解析 SMISMEMBER 命令：SMISMEMBER key member [member ...]
    fn parse_smismember(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "SMISMEMBER 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let members: Vec<Bytes> = arr[2..]
            .iter()
            .map(|v| self.extract_bytes(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::SMisMember(key, members))
    }

    /// 解析 ZRANDMEMBER 命令：ZRANDMEMBER key [count [WITHSCORES]]
    fn parse_zrandmember(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 || arr.len() > 4 {
            return Err(AppError::Command(
                "ZRANDMEMBER 命令参数数量错误".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let mut count = 1i64;
        let mut with_scores = false;
        if arr.len() >= 3 {
            count = self.extract_string(&arr[2])?.parse().map_err(|_| {
                AppError::Command("ZRANDMEMBER count 必须是整数".to_string())
            })?;
        }
        if arr.len() == 4 {
            let flag = self.extract_string(&arr[3])?.to_ascii_uppercase();
            if flag == "WITHSCORES" {
                with_scores = true;
            } else {
                return Err(AppError::Command(
                    "ZRANDMEMBER 可选参数只能是 WITHSCORES".to_string(),
                ));
            }
        }
        Ok(Command::ZRandMember(key, count, with_scores))
    }

    /// 解析 ZDIFF 命令：ZDIFF numkeys key [key ...] [WITHSCORES]
    fn parse_zdiff(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "ZDIFF 命令需要至少 2 个参数".to_string(),
            ));
        }
        let numkeys: usize = self.extract_string(&arr[1])?.parse().map_err(|_| {
            AppError::Command("ZDIFF numkeys 必须是整数".to_string())
        })?;
        if arr.len() < 2 + numkeys {
            return Err(AppError::Command("ZDIFF 参数数量不足".to_string()));
        }
        let keys: Vec<String> = arr[2..2 + numkeys]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        let mut with_scores = false;
        if arr.len() > 2 + numkeys {
            let flag = self.extract_string(&arr[2 + numkeys])?.to_ascii_uppercase();
            if flag == "WITHSCORES" {
                with_scores = true;
            }
        }
        Ok(Command::ZDiff(keys, with_scores))
    }

    /// 解析 ZDIFFSTORE 命令：ZDIFFSTORE destination numkeys key [key ...]
    fn parse_zdiffstore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(
                "ZDIFFSTORE 命令需要至少 3 个参数".to_string(),
            ));
        }
        let destination = self.extract_string(&arr[1])?;
        let numkeys: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("ZDIFFSTORE numkeys 必须是整数".to_string())
        })?;
        if arr.len() < 3 + numkeys {
            return Err(AppError::Command("ZDIFFSTORE 参数数量不足".to_string()));
        }
        let keys: Vec<String> = arr[3..3 + numkeys]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::ZDiffStore(destination, keys))
    }

    /// 解析 ZINTER 命令：ZINTER numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
    fn parse_zinter(&self, arr: &[RespValue]) -> Result<Command> {
        self.parse_zset_union_inter(arr, true)
    }

    /// 解析 ZUNION 命令：ZUNION numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
    fn parse_zunion(&self, arr: &[RespValue]) -> Result<Command> {
        self.parse_zset_union_inter(arr, false)
    }

    /// 辅助方法：解析 ZINTER/ZUNION
    fn parse_zset_union_inter(&self, arr: &[RespValue], is_inter: bool) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                if is_inter { "ZINTER 命令需要至少 2 个参数".to_string() } else { "ZUNION 命令需要至少 2 个参数".to_string() },
            ));
        }
        let numkeys: usize = self.extract_string(&arr[1])?.parse().map_err(|_| {
            AppError::Command("numkeys 必须是整数".to_string())
        })?;
        if arr.len() < 2 + numkeys {
            return Err(AppError::Command("参数数量不足".to_string()));
        }
        let keys: Vec<String> = arr[2..2 + numkeys]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        let mut weights = Vec::new();
        let mut aggregate = "SUM".to_string();
        let mut with_scores = false;
        let mut i = 2 + numkeys;
        while i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match opt.as_str() {
                "WEIGHTS" => {
                    i += 1;
                    while i < arr.len() {
                        let next = self.extract_string(&arr[i])?;
                        if let Ok(w) = next.parse::<f64>() {
                            weights.push(w);
                            i += 1;
                        } else {
                            break;
                        }
                    }
                }
                "AGGREGATE" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("AGGREGATE 需要参数".to_string()));
                    }
                    aggregate = self.extract_string(&arr[i + 1])?.to_ascii_uppercase();
                    if !matches!(aggregate.as_str(), "SUM" | "MIN" | "MAX") {
                        return Err(AppError::Command("AGGREGATE 必须是 SUM|MIN|MAX".to_string()));
                    }
                    i += 2;
                }
                "WITHSCORES" => {
                    with_scores = true;
                    i += 1;
                }
                _ => {
                    return Err(AppError::Command(format!("不支持的选项: {}", opt)));
                }
            }
        }
        if is_inter {
            Ok(Command::ZInter(keys, weights, aggregate, with_scores))
        } else {
            Ok(Command::ZUnion(keys, weights, aggregate, with_scores))
        }
    }

    /// 解析 ZRANGESTORE 命令：ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]
    fn parse_zrangestore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 6 {
            return Err(AppError::Command(
                "ZRANGESTORE 命令需要至少 5 个参数".to_string(),
            ));
        }
        let dst = self.extract_string(&arr[1])?;
        let src = self.extract_string(&arr[2])?;
        let min = self.extract_string(&arr[3])?;
        let max = self.extract_string(&arr[4])?;
        let mut by_score = false;
        let mut by_lex = false;
        let mut rev = false;
        let mut limit_offset = 0usize;
        let mut limit_count = 0usize;
        let mut i = 5;
        while i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match opt.as_str() {
                "BYSCORE" => { by_score = true; i += 1; }
                "BYLEX" => { by_lex = true; i += 1; }
                "REV" => { rev = true; i += 1; }
                "LIMIT" => {
                    if i + 2 >= arr.len() {
                        return Err(AppError::Command("ZRANGESTORE LIMIT 需要 offset count".to_string()));
                    }
                    limit_offset = self.extract_string(&arr[i + 1])?.parse().map_err(|_| {
                        AppError::Command("LIMIT offset 必须是整数".to_string())
                    })?;
                    limit_count = self.extract_string(&arr[i + 2])?.parse().map_err(|_| {
                        AppError::Command("LIMIT count 必须是整数".to_string())
                    })?;
                    i += 3;
                }
                _ => {
                    return Err(AppError::Command(format!("ZRANGESTORE 不支持的选项: {}", opt)));
                }
            }
        }
        Ok(Command::ZRangeStore(dst, src, min, max, by_score, by_lex, rev, limit_offset, limit_count))
    }

    /// 解析 ZMPOP 命令：ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
    fn parse_zmpop(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(
                "ZMPOP 命令需要至少 3 个参数".to_string(),
            ));
        }
        let numkeys: usize = self.extract_string(&arr[1])?.parse().map_err(|_| {
            AppError::Command("ZMPOP numkeys 必须是整数".to_string())
        })?;
        if arr.len() < 2 + numkeys + 1 {
            return Err(AppError::Command("ZMPOP 参数数量不足".to_string()));
        }
        let keys: Vec<String> = arr[2..2 + numkeys]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        let min_or_max_str = self.extract_string(&arr[2 + numkeys])?.to_ascii_uppercase();
        let min_or_max = match min_or_max_str.as_str() {
            "MIN" => true,
            "MAX" => false,
            _ => return Err(AppError::Command("ZMPOP 必须是 MIN 或 MAX".to_string())),
        };
        let mut count = 1usize;
        let mut i = 3 + numkeys;
        if i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            if opt == "COUNT" {
                if i + 1 >= arr.len() {
                    return Err(AppError::Command("ZMPOP COUNT 需要参数".to_string()));
                }
                count = self.extract_string(&arr[i + 1])?.parse().map_err(|_| {
                    AppError::Command("ZMPOP COUNT 必须是整数".to_string())
                })?;
            }
        }
        Ok(Command::ZMpop(keys, min_or_max, count))
    }

    /// 解析 BZMPOP 命令：BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
    fn parse_bzmpop(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 5 {
            return Err(AppError::Command(
                "BZMPOP 命令需要至少 4 个参数".to_string(),
            ));
        }
        let timeout: f64 = self.extract_string(&arr[1])?.parse().map_err(|_| {
            AppError::Command("BZMPOP timeout 必须是数字".to_string())
        })?;
        let numkeys: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("BZMPOP numkeys 必须是整数".to_string())
        })?;
        if arr.len() < 3 + numkeys + 1 {
            return Err(AppError::Command("BZMPOP 参数数量不足".to_string()));
        }
        let keys: Vec<String> = arr[3..3 + numkeys]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        let min_or_max_str = self.extract_string(&arr[3 + numkeys])?.to_ascii_uppercase();
        let min_or_max = match min_or_max_str.as_str() {
            "MIN" => true,
            "MAX" => false,
            _ => return Err(AppError::Command("BZMPOP 必须是 MIN 或 MAX".to_string())),
        };
        let mut count = 1usize;
        let mut i = 4 + numkeys;
        if i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            if opt == "COUNT" {
                if i + 1 >= arr.len() {
                    return Err(AppError::Command("BZMPOP COUNT 需要参数".to_string()));
                }
                count = self.extract_string(&arr[i + 1])?.parse().map_err(|_| {
                    AppError::Command("BZMPOP COUNT 必须是整数".to_string())
                })?;
            }
        }
        Ok(Command::BZMpop(timeout, keys, min_or_max, count))
    }

    /// 解析 BZPOPMIN 命令：BZPOPMIN key [key ...] timeout
    fn parse_bzpopmin(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "BZPOPMIN 命令需要至少 2 个参数".to_string(),
            ));
        }
        let timeout: f64 = self.extract_string(&arr[arr.len() - 1])?.parse().map_err(|_| {
            AppError::Command("BZPOPMIN timeout 必须是数字".to_string())
        })?;
        let keys: Vec<String> = arr[1..arr.len() - 1]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::BZPopMin(keys, timeout))
    }

    /// 解析 BZPOPMAX 命令：BZPOPMAX key [key ...] timeout
    fn parse_bzpopmax(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "BZPOPMAX 命令需要至少 2 个参数".to_string(),
            ));
        }
        let timeout: f64 = self.extract_string(&arr[arr.len() - 1])?.parse().map_err(|_| {
            AppError::Command("BZPOPMAX timeout 必须是数字".to_string())
        })?;
        let keys: Vec<String> = arr[1..arr.len() - 1]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::BZPopMax(keys, timeout))
    }

    /// 解析 ZREVRANGEBYSCORE 命令：ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
    fn parse_zrevrangebyscore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 5 {
            return Err(AppError::Command(
                "ZREVRANGEBYSCORE 命令需要至少 4 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let max: f64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("ZREVRANGEBYSCORE max 必须是数字".to_string())
        })?;
        let min: f64 = self.extract_string(&arr[3])?.parse().map_err(|_| {
            AppError::Command("ZREVRANGEBYSCORE min 必须是数字".to_string())
        })?;
        let mut with_scores = false;
        let mut limit_offset = 0usize;
        let mut limit_count = 0usize;
        let mut i = 4;
        while i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match opt.as_str() {
                "WITHSCORES" => { with_scores = true; i += 1; }
                "LIMIT" => {
                    if i + 2 >= arr.len() {
                        return Err(AppError::Command("ZREVRANGEBYSCORE LIMIT 需要 offset count".to_string()));
                    }
                    limit_offset = self.extract_string(&arr[i + 1])?.parse().map_err(|_| {
                        AppError::Command("LIMIT offset 必须是整数".to_string())
                    })?;
                    limit_count = self.extract_string(&arr[i + 2])?.parse().map_err(|_| {
                        AppError::Command("LIMIT count 必须是整数".to_string())
                    })?;
                    i += 3;
                }
                _ => {
                    return Err(AppError::Command(format!("ZREVRANGEBYSCORE 不支持的选项: {}", opt)));
                }
            }
        }
        Ok(Command::ZRevRangeByScore(key, max, min, with_scores, limit_offset, limit_count))
    }

    /// 解析 ZREVRANGEBYLEX 命令：ZREVRANGEBYLEX key max min [LIMIT offset count]
    fn parse_zrevrangebylex(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 5 {
            return Err(AppError::Command(
                "ZREVRANGEBYLEX 命令需要至少 4 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let max = self.extract_string(&arr[2])?;
        let min = self.extract_string(&arr[3])?;
        let mut limit_offset = 0usize;
        let mut limit_count = 0usize;
        if arr.len() >= 7 {
            let opt = self.extract_string(&arr[4])?.to_ascii_uppercase();
            if opt == "LIMIT" {
                limit_offset = self.extract_string(&arr[5])?.parse().map_err(|_| {
                    AppError::Command("LIMIT offset 必须是整数".to_string())
                })?;
                limit_count = self.extract_string(&arr[6])?.parse().map_err(|_| {
                    AppError::Command("LIMIT count 必须是整数".to_string())
                })?;
            }
        }
        Ok(Command::ZRevRangeByLex(key, max, min, limit_offset, limit_count))
    }

    /// 解析 ZMSCORE 命令：ZMSCORE key member [member ...]
    fn parse_zmscore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "ZMSCORE 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let members: Vec<String> = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::ZMScore(key, members))
    }

    /// 解析 ZLEXCOUNT 命令：ZLEXCOUNT key min max
    fn parse_zlexcount(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "ZLEXCOUNT 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let min = self.extract_string(&arr[2])?;
        let max = self.extract_string(&arr[3])?;
        Ok(Command::ZLexCount(key, min, max))
    }

    /// 解析 KEYS 命令：KEYS pattern
    fn parse_keys(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "KEYS 命令需要 1 个参数".to_string(),
            ));
        }
        let pattern = self.extract_string(&arr[1])?;
        Ok(Command::Keys(pattern))
    }

    /// 解析 SCAN 命令：SCAN cursor [MATCH pattern] [COUNT count]
    fn parse_scan(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "SCAN 命令需要至少 1 个参数".to_string(),
            ));
        }
        let cursor = self.extract_string(&arr[1])?.parse::<usize>()
            .map_err(|_| AppError::Command("SCAN cursor 必须是数字".to_string()))?;
        let mut pattern = String::new();
        let mut count = 0usize;
        let mut i = 2;
        while i < arr.len() {
            let flag = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match flag.as_str() {
                "MATCH" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("SCAN MATCH 需要参数".to_string()));
                    }
                    pattern = self.extract_string(&arr[i + 1])?;
                    i += 2;
                }
                "COUNT" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("SCAN COUNT 需要参数".to_string()));
                    }
                    count = self.extract_string(&arr[i + 1])?.parse::<usize>()
                        .map_err(|_| AppError::Command("SCAN COUNT 必须是数字".to_string()))?;
                    i += 2;
                }
                _ => {
                    return Err(AppError::Command(
                        format!("SCAN 不支持参数 {}", flag),
                    ));
                }
            }
        }
        Ok(Command::Scan(cursor, pattern, count))
    }

    /// 解析 RENAME 命令：RENAME key newkey
    fn parse_rename(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "RENAME 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let newkey = self.extract_string(&arr[2])?;
        Ok(Command::Rename(key, newkey))
    }

    /// 解析 TYPE 命令：TYPE key
    fn parse_type(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "TYPE 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::Type(key))
    }

    /// 解析 PERSIST 命令：PERSIST key
    fn parse_persist(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "PERSIST 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::Persist(key))
    }

    /// 解析 PEXPIRE 命令：PEXPIRE key milliseconds
    fn parse_pexpire(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "PEXPIRE 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let ms = self.extract_string(&arr[2])?.parse::<u64>()
            .map_err(|_| AppError::Command("PEXPIRE 时间必须是数字".to_string()))?;
        Ok(Command::PExpire(key, ms))
    }

    /// 解析 PTTL 命令：PTTL key
    fn parse_pttl(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "PTTL 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::PTtl(key))
    }

    /// 解析 DBSIZE 命令：DBSIZE
    fn parse_dbsize(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 1 {
            return Err(AppError::Command(
                "DBSIZE 命令不需要参数".to_string(),
            ));
        }
        Ok(Command::DbSize)
    }

    /// 解析 INFO 命令：INFO [section]
    fn parse_info(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() > 2 {
            return Err(AppError::Command(
                "INFO 命令参数过多".to_string(),
            ));
        }
        let section = if arr.len() == 2 {
            Some(self.extract_string(&arr[1])?)
        } else {
            None
        };
        Ok(Command::Info(section))
    }

    /// 解析 SUBSCRIBE 命令：SUBSCRIBE channel [channel ...]
    fn parse_subscribe(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "SUBSCRIBE 命令需要至少 1 个参数".to_string(),
            ));
        }
        let channels = arr[1..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::Subscribe(channels))
    }

    /// 解析 UNSUBSCRIBE 命令：UNSUBSCRIBE [channel ...]
    fn parse_unsubscribe(&self, arr: &[RespValue]) -> Result<Command> {
        let channels = if arr.len() > 1 {
            arr[1..]
                .iter()
                .map(|v| self.extract_string(v))
                .collect::<Result<Vec<String>>>()?
        } else {
            vec![]
        };
        Ok(Command::Unsubscribe(channels))
    }

    /// 解析 PUBLISH 命令：PUBLISH channel message
    fn parse_publish(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "PUBLISH 命令需要 2 个参数".to_string(),
            ));
        }
        let channel = self.extract_string(&arr[1])?;
        let message = self.extract_bytes(&arr[2])?;
        Ok(Command::Publish(channel, message))
    }

    /// 解析 PSUBSCRIBE 命令：PSUBSCRIBE pattern [pattern ...]
    fn parse_psubscribe(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "PSUBSCRIBE 命令需要至少 1 个参数".to_string(),
            ));
        }
        let patterns = arr[1..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::PSubscribe(patterns))
    }

    /// 解析 PUNSUBSCRIBE 命令：PUNSUBSCRIBE [pattern ...]
    fn parse_punsubscribe(&self, arr: &[RespValue]) -> Result<Command> {
        let patterns = if arr.len() > 1 {
            arr[1..]
                .iter()
                .map(|v| self.extract_string(v))
                .collect::<Result<Vec<String>>>()?
        } else {
            vec![]
        };
        Ok(Command::PUnsubscribe(patterns))
    }

    /// 解析 MULTI 命令：MULTI
    fn parse_multi(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 1 {
            return Err(AppError::Command(
                "MULTI 命令不需要参数".to_string(),
            ));
        }
        Ok(Command::Multi)
    }

    /// 解析 EXEC 命令：EXEC
    fn parse_exec(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 1 {
            return Err(AppError::Command(
                "EXEC 命令不需要参数".to_string(),
            ));
        }
        Ok(Command::Exec)
    }

    /// 解析 DISCARD 命令：DISCARD
    fn parse_discard(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 1 {
            return Err(AppError::Command(
                "DISCARD 命令不需要参数".to_string(),
            ));
        }
        Ok(Command::Discard)
    }

    /// 解析 WATCH 命令：WATCH key [key ...]
    fn parse_watch(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "WATCH 命令需要至少 1 个参数".to_string(),
            ));
        }
        let keys = arr[1..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::Watch(keys))
    }

    /// 解析 BGREWRITEAOF 命令：BGREWRITEAOF
    fn parse_bgrewriteaof(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 1 {
            return Err(AppError::Command(
                "BGREWRITEAOF 命令不需要参数".to_string(),
            ));
        }
        Ok(Command::BgRewriteAof)
    }

    /// 解析 SETBIT 命令：SETBIT key offset value
    fn parse_setbit(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "SETBIT 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let offset: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("SETBIT 的 offset 必须是非负整数".to_string())
        })?;
        let value: u8 = self.extract_string(&arr[3])?.parse().map_err(|_| {
            AppError::Command("SETBIT 的 value 必须是 0 或 1".to_string())
        })?;
        if value != 0 && value != 1 {
            return Err(AppError::Command(
                "SETBIT 的 value 必须是 0 或 1".to_string(),
            ));
        }
        Ok(Command::SetBit(key, offset, value == 1))
    }

    /// 解析 GETBIT 命令：GETBIT key offset
    fn parse_getbit(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "GETBIT 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let offset: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("GETBIT 的 offset 必须是非负整数".to_string())
        })?;
        Ok(Command::GetBit(key, offset))
    }

    /// 解析 BITCOUNT 命令：BITCOUNT key [start end [BYTE|BIT]]
    fn parse_bitcount(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 || arr.len() > 5 {
            return Err(AppError::Command(
                "BITCOUNT 命令参数数量错误".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let start = if arr.len() > 2 {
            self.extract_string(&arr[2])?.parse().map_err(|_| {
                AppError::Command("BITCOUNT 的 start 必须是整数".to_string())
            })?
        } else {
            0
        };
        let end = if arr.len() > 3 {
            self.extract_string(&arr[3])?.parse().map_err(|_| {
                AppError::Command("BITCOUNT 的 end 必须是整数".to_string())
            })?
        } else {
            -1
        };
        let is_byte = if arr.len() > 4 {
            let unit = self.extract_string(&arr[4])?.to_ascii_uppercase();
            if unit == "BYTE" {
                true
            } else if unit == "BIT" {
                false
            } else {
                return Err(AppError::Command(
                    "BITCOUNT 的单位只能是 BYTE 或 BIT".to_string(),
                ));
            }
        } else {
            true
        };
        Ok(Command::BitCount(key, start, end, is_byte))
    }

    /// 解析 BITOP 命令：BITOP AND|OR|XOR|NOT destkey key [key ...]
    fn parse_bitop(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(
                "BITOP 命令参数数量错误".to_string(),
            ));
        }
        let op = self.extract_string(&arr[1])?.to_ascii_uppercase();
        if op != "AND" && op != "OR" && op != "XOR" && op != "NOT" {
            return Err(AppError::Command(
                "BITOP 的操作只能是 AND|OR|XOR|NOT".to_string(),
            ));
        }
        let destkey = self.extract_string(&arr[2])?;
        let keys: Vec<String> = arr[3..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        if op == "NOT" && keys.len() != 1 {
            return Err(AppError::Command(
                "BITOP NOT 只能接受一个 key".to_string(),
            ));
        }
        Ok(Command::BitOp(op, destkey, keys))
    }

    /// 解析 BITPOS 命令：BITPOS key bit [start [end [BYTE|BIT]]]
    fn parse_bitpos(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 || arr.len() > 6 {
            return Err(AppError::Command(
                "BITPOS 命令参数数量错误".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let bit: u8 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("BITPOS 的 bit 必须是 0 或 1".to_string())
        })?;
        if bit != 0 && bit != 1 {
            return Err(AppError::Command(
                "BITPOS 的 bit 必须是 0 或 1".to_string(),
            ));
        }
        let start = if arr.len() > 3 {
            self.extract_string(&arr[3])?.parse().map_err(|_| {
                AppError::Command("BITPOS 的 start 必须是整数".to_string())
            })?
        } else {
            0
        };
        let end = if arr.len() > 4 {
            self.extract_string(&arr[4])?.parse().map_err(|_| {
                AppError::Command("BITPOS 的 end 必须是整数".to_string())
            })?
        } else {
            -1
        };
        let is_byte = if arr.len() > 5 {
            let unit = self.extract_string(&arr[5])?.to_ascii_uppercase();
            if unit == "BYTE" {
                true
            } else if unit == "BIT" {
                false
            } else {
                return Err(AppError::Command(
                    "BITPOS 的单位只能是 BYTE 或 BIT".to_string(),
                ));
            }
        } else {
            true
        };
        Ok(Command::BitPos(key, bit, start, end, is_byte))
    }

    /// 解析 BITFIELD 命令：BITFIELD key [GET type offset] [SET type offset value] [INCRBY type offset increment] [OVERFLOW WRAP|SAT|FAIL] ...
    fn parse_bitfield(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "BITFIELD 命令需要至少 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let mut ops = Vec::new();
        let mut i = 2;
        while i < arr.len() {
            let cmd = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match cmd.as_str() {
                "GET" => {
                    if i + 2 >= arr.len() {
                        return Err(AppError::Command("BITFIELD GET 需要 type 和 offset".to_string()));
                    }
                    let enc = crate::storage::BitFieldEncoding::parse(&self.extract_string(&arr[i + 1])?)?;
                    let off = crate::storage::BitFieldOffset::parse(&self.extract_string(&arr[i + 2])?)?;
                    ops.push(crate::storage::BitFieldOp::Get(enc, off));
                    i += 3;
                }
                "SET" => {
                    if i + 3 >= arr.len() {
                        return Err(AppError::Command("BITFIELD SET 需要 type offset value".to_string()));
                    }
                    let enc = crate::storage::BitFieldEncoding::parse(&self.extract_string(&arr[i + 1])?)?;
                    let off = crate::storage::BitFieldOffset::parse(&self.extract_string(&arr[i + 2])?)?;
                    let value: i64 = self.extract_string(&arr[i + 3])?.parse().map_err(|_| {
                        AppError::Command("BITFIELD SET value 必须是整数".to_string())
                    })?;
                    ops.push(crate::storage::BitFieldOp::Set(enc, off, value));
                    i += 4;
                }
                "INCRBY" => {
                    if i + 3 >= arr.len() {
                        return Err(AppError::Command("BITFIELD INCRBY 需要 type offset increment".to_string()));
                    }
                    let enc = crate::storage::BitFieldEncoding::parse(&self.extract_string(&arr[i + 1])?)?;
                    let off = crate::storage::BitFieldOffset::parse(&self.extract_string(&arr[i + 2])?)?;
                    let inc: i64 = self.extract_string(&arr[i + 3])?.parse().map_err(|_| {
                        AppError::Command("BITFIELD INCRBY increment 必须是整数".to_string())
                    })?;
                    ops.push(crate::storage::BitFieldOp::IncrBy(enc, off, inc));
                    i += 4;
                }
                "OVERFLOW" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("BITFIELD OVERFLOW 需要策略".to_string()));
                    }
                    let strategy = self.extract_string(&arr[i + 1])?.to_ascii_uppercase();
                    let overflow = match strategy.as_str() {
                        "WRAP" => crate::storage::BitFieldOverflow::Wrap,
                        "SAT" => crate::storage::BitFieldOverflow::Sat,
                        "FAIL" => crate::storage::BitFieldOverflow::Fail,
                        _ => return Err(AppError::Command("BITFIELD OVERFLOW 必须是 WRAP|SAT|FAIL".to_string())),
                    };
                    ops.push(crate::storage::BitFieldOp::Overflow(overflow));
                    i += 2;
                }
                _ => {
                    return Err(AppError::Command(format!("BITFIELD 不支持的子命令: {}", cmd)));
                }
            }
        }
        Ok(Command::BitField(key, ops))
    }

    /// 解析 BITFIELD_RO 命令：BITFIELD_RO key [GET type offset] ...
    fn parse_bitfield_ro(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "BITFIELD_RO 命令需要至少 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let mut ops = Vec::new();
        let mut i = 2;
        while i < arr.len() {
            let cmd = self.extract_string(&arr[i])?.to_ascii_uppercase();
            if cmd != "GET" {
                return Err(AppError::Command("BITFIELD_RO 只支持 GET 操作".to_string()));
            }
            if i + 2 >= arr.len() {
                return Err(AppError::Command("BITFIELD_RO GET 需要 type 和 offset".to_string()));
            }
            let enc = crate::storage::BitFieldEncoding::parse(&self.extract_string(&arr[i + 1])?)?;
            let off = crate::storage::BitFieldOffset::parse(&self.extract_string(&arr[i + 2])?)?;
            ops.push(crate::storage::BitFieldOp::Get(enc, off));
            i += 3;
        }
        Ok(Command::BitFieldRo(key, ops))
    }

    /// 解析 XADD 命令：XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold] *|id field value [field value ...]
    fn parse_xadd(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 5 {
            return Err(AppError::Command(
                "XADD 命令需要至少 4 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let mut i = 2;
        let mut nomkstream = false;
        let mut max_len = None;
        let mut min_id = None;

        // 解析选项
        while i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match opt.as_str() {
                "NOMKSTREAM" => {
                    nomkstream = true;
                    i += 1;
                }
                "MAXLEN" => {
                    i += 1;
                    if i < arr.len() {
                        let next = self.extract_string(&arr[i])?;
                        if next == "~" || next == "=" {
                            i += 1; // 忽略 ~ 或 =
                        }
                        if i < arr.len() {
                            max_len = Some(next.parse().map_err(|_| {
                                AppError::Command("XADD MAXLEN threshold 必须是整数".to_string())
                            })?);
                            if next == "~" || next == "=" {
                                max_len = Some(self.extract_string(&arr[i])?.parse().map_err(|_| {
                                    AppError::Command("XADD MAXLEN threshold 必须是整数".to_string())
                                })?);
                                i += 1;
                            } else {
                                max_len = Some(next.parse().map_err(|_| {
                                    AppError::Command("XADD MAXLEN threshold 必须是整数".to_string())
                                })?);
                                i += 1;
                            }
                        } else {
                            return Err(AppError::Command("XADD MAXLEN 需要 threshold".to_string()));
                        }
                    } else {
                        return Err(AppError::Command("XADD MAXLEN 需要 threshold".to_string()));
                    }
                }
                "MINID" => {
                    i += 1;
                    if i < arr.len() {
                        let next = self.extract_string(&arr[i])?;
                        if next == "~" || next == "=" {
                            i += 1;
                        }
                        if i < arr.len() {
                            min_id = Some(self.extract_string(&arr[i])?);
                            i += 1;
                        } else {
                            return Err(AppError::Command("XADD MINID 需要 id".to_string()));
                        }
                    } else {
                        return Err(AppError::Command("XADD MINID 需要 id".to_string()));
                    }
                }
                _ => break,
            }
        }

        if i >= arr.len() {
            return Err(AppError::Command("XADD 需要 ID 和字段".to_string()));
        }
        let id = self.extract_string(&arr[i])?;
        i += 1;

        if (arr.len() - i) % 2 != 0 {
            return Err(AppError::Command("XADD 字段必须是 field-value 对".to_string()));
        }

        let mut fields = Vec::new();
        while i < arr.len() {
            let field = self.extract_string(&arr[i])?;
            let value = self.extract_string(&arr[i + 1])?;
            fields.push((field, value));
            i += 2;
        }

        Ok(Command::XAdd(key, id, fields, nomkstream, max_len, min_id))
    }

    /// 解析 XLEN 命令：XLEN key
    fn parse_xlen(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command("XLEN 命令需要 1 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::XLen(key))
    }

    /// 解析 XRANGE 命令：XRANGE key start end [COUNT count]
    fn parse_xrange(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command("XRANGE 命令需要至少 3 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let start = self.extract_string(&arr[2])?;
        let end = self.extract_string(&arr[3])?;
        let mut count = None;
        if arr.len() >= 6 {
            let opt = self.extract_string(&arr[4])?.to_ascii_uppercase();
            if opt == "COUNT" {
                count = Some(self.extract_string(&arr[5])?.parse().map_err(|_| {
                    AppError::Command("XRANGE COUNT 必须是整数".to_string())
                })?);
            }
        }
        Ok(Command::XRange(key, start, end, count))
    }

    /// 解析 XREVRANGE 命令：XREVRANGE key end start [COUNT count]
    fn parse_xrevrange(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command("XREVRANGE 命令需要至少 3 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let end = self.extract_string(&arr[2])?;
        let start = self.extract_string(&arr[3])?;
        let mut count = None;
        if arr.len() >= 6 {
            let opt = self.extract_string(&arr[4])?.to_ascii_uppercase();
            if opt == "COUNT" {
                count = Some(self.extract_string(&arr[5])?.parse().map_err(|_| {
                    AppError::Command("XREVRANGE COUNT 必须是整数".to_string())
                })?);
            }
        }
        Ok(Command::XRevRange(key, end, start, count))
    }

    /// 解析 XTRIM 命令：XTRIM key MAXLEN|MINID [=|~] threshold
    fn parse_xtrim(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command("XTRIM 命令需要至少 3 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let strategy = self.extract_string(&arr[2])?;
        let mut threshold_idx = 3;
        let threshold_str = self.extract_string(&arr[3])?;
        if threshold_str == "~" || threshold_str == "=" {
            threshold_idx = 4;
        }
        let threshold = self.extract_string(&arr[threshold_idx])?;
        Ok(Command::XTrim(key, strategy, threshold))
    }

    /// 解析 XDEL 命令：XDEL key id [id ...]
    fn parse_xdel(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command("XDEL 命令需要至少 2 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let ids: Vec<String> = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::XDel(key, ids))
    }

    /// 解析 XREAD 命令：XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
    fn parse_xread(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command("XREAD 命令需要至少 3 个参数".to_string()));
        }
        let mut count = None;
        let mut i = 1;
        while i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match opt.as_str() {
                "COUNT" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("XREAD COUNT 需要参数".to_string()));
                    }
                    count = Some(self.extract_string(&arr[i + 1])?.parse().map_err(|_| {
                        AppError::Command("XREAD COUNT 必须是整数".to_string())
                    })?);
                    i += 2;
                }
                "BLOCK" => {
                    // BLOCK 由 server.rs 处理，这里只解析 COUNT 和 STREAMS
                    i += 2;
                }
                "STREAMS" => {
                    break;
                }
                _ => {
                    return Err(AppError::Command(format!("XREAD 不支持的选项: {}", opt)));
                }
            }
        }
        if i >= arr.len() || self.extract_string(&arr[i])?.to_ascii_uppercase() != "STREAMS" {
            return Err(AppError::Command("XREAD 需要 STREAMS 关键字".to_string()));
        }
        i += 1;
        let remaining = arr.len() - i;
        if remaining % 2 != 0 {
            return Err(AppError::Command("XREAD STREAMS 后面需要成对的 key 和 id".to_string()));
        }
        let num_streams = remaining / 2;
        let keys: Vec<String> = arr[i..i + num_streams]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        let ids: Vec<String> = arr[i + num_streams..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::XRead(keys, ids, count))
    }

    /// 解析 XSETID 命令：XSETID key id
    fn parse_xsetid(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command("XSETID 命令需要 2 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let id = self.extract_string(&arr[2])?;
        Ok(Command::XSetId(key, id))
    }

    fn parse_xgroup(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command("XGROUP 需要子命令".to_string()));
        }
        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "CREATE" => {
                if arr.len() < 5 {
                    return Err(AppError::Command("XGROUP CREATE 需要 key groupname id".to_string()));
                }
                let key = self.extract_string(&arr[2])?;
                let group = self.extract_string(&arr[3])?;
                let id = self.extract_string(&arr[4])?;
                let mut mkstream = false;
                for i in 5..arr.len() {
                    if self.extract_string(&arr[i])?.to_ascii_uppercase() == "MKSTREAM" {
                        mkstream = true;
                    }
                }
                Ok(Command::XGroupCreate(key, group, id, mkstream))
            }
            "DESTROY" => {
                if arr.len() < 4 {
                    return Err(AppError::Command("XGROUP DESTROY 需要 key groupname".to_string()));
                }
                let key = self.extract_string(&arr[2])?;
                let group = self.extract_string(&arr[3])?;
                Ok(Command::XGroupDestroy(key, group))
            }
            "SETID" => {
                if arr.len() < 5 {
                    return Err(AppError::Command("XGROUP SETID 需要 key groupname id".to_string()));
                }
                let key = self.extract_string(&arr[2])?;
                let group = self.extract_string(&arr[3])?;
                let id = self.extract_string(&arr[4])?;
                Ok(Command::XGroupSetId(key, group, id))
            }
            "DELCONSUMER" => {
                if arr.len() < 5 {
                    return Err(AppError::Command("XGROUP DELCONSUMER 需要 key groupname consumername".to_string()));
                }
                let key = self.extract_string(&arr[2])?;
                let group = self.extract_string(&arr[3])?;
                let consumer = self.extract_string(&arr[4])?;
                Ok(Command::XGroupDelConsumer(key, group, consumer))
            }
            "CREATECONSUMER" => {
                if arr.len() < 5 {
                    return Err(AppError::Command("XGROUP CREATECONSUMER 需要 key groupname consumername".to_string()));
                }
                let key = self.extract_string(&arr[2])?;
                let group = self.extract_string(&arr[3])?;
                let consumer = self.extract_string(&arr[4])?;
                Ok(Command::XGroupCreateConsumer(key, group, consumer))
            }
            _ => Err(AppError::Command(format!("XGROUP 不支持的子命令: {}", sub))),
        }
    }

    fn parse_xreadgroup(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 7 {
            return Err(AppError::Command("XREADGROUP 参数不足".to_string()));
        }
        let g = self.extract_string(&arr[1])?.to_ascii_uppercase();
        if g != "GROUP" {
            return Err(AppError::Command("XREADGROUP 需要 GROUP 关键字".to_string()));
        }
        let group = self.extract_string(&arr[2])?;
        let consumer = self.extract_string(&arr[3])?;
        let mut i = 4;
        let mut count = None;
        let mut noack = false;
        while i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match opt.as_str() {
                "COUNT" => {
                    i += 1;
                    count = Some(self.extract_string(&arr[i])?.parse::<usize>().map_err(|_| {
                        AppError::Command("COUNT 必须是整数".to_string())
                    })?);
                }
                "NOACK" => { noack = true; }
                "STREAMS" => { i += 1; break; }
                _ => {}
            }
            i += 1;
        }
        let remaining = arr.len() - i;
        if remaining == 0 || remaining % 2 != 0 {
            return Err(AppError::Command("XREADGROUP STREAMS 参数不匹配".to_string()));
        }
        let num_streams = remaining / 2;
        let keys: Vec<String> = arr[i..i + num_streams]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        let ids: Vec<String> = arr[i + num_streams..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::XReadGroup(group, consumer, keys, ids, count, noack))
    }

    fn parse_xack(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command("XACK 需要 key group id".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let group = self.extract_string(&arr[2])?;
        let ids: Vec<String> = arr[3..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::XAck(key, group, ids))
    }

    fn parse_xclaim(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 6 {
            return Err(AppError::Command("XCLAIM 参数不足".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let group = self.extract_string(&arr[2])?;
        let consumer = self.extract_string(&arr[3])?;
        let min_idle: u64 = self.extract_string(&arr[4])?.parse().map_err(|_| {
            AppError::Command("min-idle-time 必须是整数".to_string())
        })?;
        let mut ids = Vec::new();
        let mut justid = false;
        for j in 5..arr.len() {
            let s = self.extract_string(&arr[j])?;
            if s.to_ascii_uppercase() == "JUSTID" {
                justid = true;
            } else {
                ids.push(s);
            }
        }
        Ok(Command::XClaim(key, group, consumer, min_idle, ids, justid))
    }

    fn parse_xautoclaim(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 6 {
            return Err(AppError::Command("XAUTOCLAIM 参数不足".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let group = self.extract_string(&arr[2])?;
        let consumer = self.extract_string(&arr[3])?;
        let min_idle: u64 = self.extract_string(&arr[4])?.parse().map_err(|_| {
            AppError::Command("min-idle-time 必须是整数".to_string())
        })?;
        let start = self.extract_string(&arr[5])?;
        let mut count = 100usize;
        let mut justid = false;
        let mut i = 6;
        while i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match opt.as_str() {
                "COUNT" => {
                    i += 1;
                    count = self.extract_string(&arr[i])?.parse().map_err(|_| {
                        AppError::Command("COUNT 必须是整数".to_string())
                    })?;
                }
                "JUSTID" => { justid = true; }
                _ => {}
            }
            i += 1;
        }
        Ok(Command::XAutoClaim(key, group, consumer, min_idle, start, count, justid))
    }

    fn parse_xpending(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command("XPENDING 需要 key group".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let group = self.extract_string(&arr[2])?;
        if arr.len() == 3 {
            return Ok(Command::XPending(key, group, None, None, None, None));
        }
        if arr.len() >= 6 {
            let start = self.extract_string(&arr[3])?;
            let end = self.extract_string(&arr[4])?;
            let count: usize = self.extract_string(&arr[5])?.parse().map_err(|_| {
                AppError::Command("COUNT 必须是整数".to_string())
            })?;
            let consumer = if arr.len() >= 7 {
                Some(self.extract_string(&arr[6])?)
            } else {
                None
            };
            Ok(Command::XPending(key, group, Some(start), Some(end), Some(count), consumer))
        } else {
            Err(AppError::Command("XPENDING 参数不足".to_string()))
        }
    }

    fn parse_xinfo(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command("XINFO 需要子命令".to_string()));
        }
        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "STREAM" => {
                let key = self.extract_string(&arr[2])?;
                let full = arr.len() > 3 && self.extract_string(&arr[3])?.to_ascii_uppercase() == "FULL";
                Ok(Command::XInfoStream(key, full))
            }
            "GROUPS" => {
                let key = self.extract_string(&arr[2])?;
                Ok(Command::XInfoGroups(key))
            }
            "CONSUMERS" => {
                if arr.len() < 4 {
                    return Err(AppError::Command("XINFO CONSUMERS 需要 key groupname".to_string()));
                }
                let key = self.extract_string(&arr[2])?;
                let group = self.extract_string(&arr[3])?;
                Ok(Command::XInfoConsumers(key, group))
            }
            _ => Err(AppError::Command(format!("XINFO 不支持的子命令: {}", sub))),
        }
    }

    /// 解析 PFADD 命令：PFADD key element [element ...]
    fn parse_pfadd(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "PFADD 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let elements: Vec<String> = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::PfAdd(key, elements))
    }

    /// 解析 PFCOUNT 命令：PFCOUNT key [key ...]
    fn parse_pfcount(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "PFCOUNT 命令需要至少 1 个参数".to_string(),
            ));
        }
        let keys: Vec<String> = arr[1..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::PfCount(keys))
    }

    /// 解析 PFMERGE 命令：PFMERGE destkey sourcekey [sourcekey ...]
    fn parse_pfmerge(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "PFMERGE 命令需要至少 2 个参数".to_string(),
            ));
        }
        let destkey = self.extract_string(&arr[1])?;
        let sourcekeys: Vec<String> = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::PfMerge(destkey, sourcekeys))
    }

    /// 解析 GEOADD 命令：GEOADD key longitude latitude member [longitude latitude member ...]
    fn parse_geoadd(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 5 || (arr.len() - 2) % 3 != 0 {
            return Err(AppError::Command(
                "GEOADD 命令参数数量错误，需要 key 和至少一组 (lon lat member)".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let mut items = Vec::new();
        let mut idx = 2;
        while idx < arr.len() {
            let lon: f64 = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                AppError::Command("GEOADD 的 longitude 必须是数字".to_string())
            })?;
            let lat: f64 = self.extract_string(&arr[idx + 1])?.parse().map_err(|_| {
                AppError::Command("GEOADD 的 latitude 必须是数字".to_string())
            })?;
            let member = self.extract_string(&arr[idx + 2])?;
            items.push((lon, lat, member));
            idx += 3;
        }
        Ok(Command::GeoAdd(key, items))
    }

    /// 解析 GEODIST 命令：GEODIST key member1 member2 [m|km|ft|mi]
    fn parse_geodist(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 || arr.len() > 5 {
            return Err(AppError::Command(
                "GEODIST 命令参数数量错误".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let member1 = self.extract_string(&arr[2])?;
        let member2 = self.extract_string(&arr[3])?;
        let unit = if arr.len() == 5 {
            self.extract_string(&arr[4])?
        } else {
            "m".to_string()
        };
        Ok(Command::GeoDist(key, member1, member2, unit))
    }

    /// 解析 GEOHASH 命令：GEOHASH key member [member ...]
    fn parse_geohash(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "GEOHASH 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let members: Vec<String> = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::GeoHash(key, members))
    }

    /// 解析 GEOPOS 命令：GEOPOS key member [member ...]
    fn parse_geopos(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "GEOPOS 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let members: Vec<String> = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::GeoPos(key, members))
    }

    /// 解析 GEOSEARCH 命令
    fn parse_geosearch(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 7 {
            return Err(AppError::Command(
                "GEOSEARCH 命令参数数量错误".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;

        let mut idx = 2;
        let mut center_lon = 0.0f64;
        let mut center_lat = 0.0f64;
        let mut by_radius: Option<f64> = None;
        let mut by_box: Option<(f64, f64)> = None;
        let mut order: Option<String> = None;
        let mut count = 0usize;
        let mut withcoord = false;
        let mut withdist = false;
        let mut withhash = false;

        // FROMMEMBER member | FROMLONLAT lon lat
        let from_type = self.extract_string(&arr[idx])?.to_ascii_uppercase();
        if from_type == "FROMMEMBER" {
            return Err(AppError::Command(
                "GEOSEARCH FROMMEMBER 暂不支持，请使用 FROMLONLAT".to_string(),
            ));
        } else if from_type == "FROMLONLAT" {
            idx += 1;
            center_lon = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                AppError::Command("GEOSEARCH 的 lon 必须是数字".to_string())
            })?;
            idx += 1;
            center_lat = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                AppError::Command("GEOSEARCH 的 lat 必须是数字".to_string())
            })?;
            idx += 1;
        } else {
            return Err(AppError::Command(
                format!("GEOSEARCH 需要 FROMMEMBER 或 FROMLONLAT，得到: {}", from_type),
            ));
        }

        // BYRADIUS radius unit | BYBOX width height unit
        let by_type = self.extract_string(&arr[idx])?.to_ascii_uppercase();
        if by_type == "BYRADIUS" {
            idx += 1;
            let radius: f64 = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                AppError::Command("GEOSEARCH 的 radius 必须是数字".to_string())
            })?;
            idx += 1;
            let unit = self.extract_string(&arr[idx])?.to_ascii_lowercase();
            let radius_m = match unit.as_str() {
                "m" => radius,
                "km" => radius * 1000.0,
                "mi" => radius * 1609.344,
                "ft" => radius * 0.3048,
                _ => return Err(AppError::Command("GEOSEARCH 单位必须是 m|km|mi|ft".to_string())),
            };
            by_radius = Some(radius_m);
            idx += 1;
        } else if by_type == "BYBOX" {
            idx += 1;
            let width: f64 = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                AppError::Command("GEOSEARCH 的 width 必须是数字".to_string())
            })?;
            idx += 1;
            let height: f64 = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                AppError::Command("GEOSEARCH 的 height 必须是数字".to_string())
            })?;
            idx += 1;
            let unit = self.extract_string(&arr[idx])?.to_ascii_lowercase();
            let width_m = match unit.as_str() {
                "m" => width,
                "km" => width * 1000.0,
                "mi" => width * 1609.344,
                "ft" => width * 0.3048,
                _ => return Err(AppError::Command("GEOSEARCH 单位必须是 m|km|mi|ft".to_string())),
            };
            let height_m = match unit.as_str() {
                "m" => height,
                "km" => height * 1000.0,
                "mi" => height * 1609.344,
                "ft" => height * 0.3048,
                _ => return Err(AppError::Command("GEOSEARCH 单位必须是 m|km|mi|ft".to_string())),
            };
            by_box = Some((width_m, height_m));
            idx += 1;
        } else {
            return Err(AppError::Command(
                format!("GEOSEARCH 需要 BYRADIUS 或 BYBOX，得到: {}", by_type),
            ));
        }

        // 可选参数
        while idx < arr.len() {
            let flag = self.extract_string(&arr[idx])?.to_ascii_uppercase();
            if flag == "ASC" || flag == "DESC" {
                order = Some(flag);
                idx += 1;
            } else if flag == "COUNT" {
                idx += 1;
                if idx >= arr.len() {
                    return Err(AppError::Command("GEOSEARCH COUNT 缺少数量".to_string()));
                }
                count = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                    AppError::Command("GEOSEARCH COUNT 必须是整数".to_string())
                })?;
                idx += 1;
            } else if flag == "WITHCOORD" {
                withcoord = true;
                idx += 1;
            } else if flag == "WITHDIST" {
                withdist = true;
                idx += 1;
            } else if flag == "WITHHASH" {
                withhash = true;
                idx += 1;
            } else {
                return Err(AppError::Command(format!("GEOSEARCH 未知参数: {}", flag)));
            }
        }

        Ok(Command::GeoSearch(
            key, center_lon, center_lat, by_radius, by_box,
            order, count, withcoord, withdist, withhash,
        ))
    }

    /// 解析 GEOSEARCHSTORE 命令
    fn parse_geosearchstore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 8 {
            return Err(AppError::Command(
                "GEOSEARCHSTORE 命令参数数量错误".to_string(),
            ));
        }
        let destination = self.extract_string(&arr[1])?;
        let source = self.extract_string(&arr[2])?;

        let mut idx = 3;
        let mut center_lon = 0.0f64;
        let mut center_lat = 0.0f64;
        let mut by_radius: Option<f64> = None;
        let mut by_box: Option<(f64, f64)> = None;
        let mut order: Option<String> = None;
        let mut count = 0usize;
        let mut storedist = false;

        // FROMMEMBER member | FROMLONLAT lon lat
        let from_type = self.extract_string(&arr[idx])?.to_ascii_uppercase();
        if from_type == "FROMMEMBER" {
            return Err(AppError::Command(
                "GEOSEARCHSTORE FROMMEMBER 暂不支持，请使用 FROMLONLAT".to_string(),
            ));
        } else if from_type == "FROMLONLAT" {
            idx += 1;
            center_lon = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                AppError::Command("GEOSEARCHSTORE 的 lon 必须是数字".to_string())
            })?;
            idx += 1;
            center_lat = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                AppError::Command("GEOSEARCHSTORE 的 lat 必须是数字".to_string())
            })?;
            idx += 1;
        } else {
            return Err(AppError::Command(
                format!("GEOSEARCHSTORE 需要 FROMMEMBER 或 FROMLONLAT，得到: {}", from_type),
            ));
        }

        // BYRADIUS radius unit | BYBOX width height unit
        let by_type = self.extract_string(&arr[idx])?.to_ascii_uppercase();
        if by_type == "BYRADIUS" {
            idx += 1;
            let radius: f64 = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                AppError::Command("GEOSEARCHSTORE 的 radius 必须是数字".to_string())
            })?;
            idx += 1;
            let unit = self.extract_string(&arr[idx])?.to_ascii_lowercase();
            let radius_m = match unit.as_str() {
                "m" => radius,
                "km" => radius * 1000.0,
                "mi" => radius * 1609.344,
                "ft" => radius * 0.3048,
                _ => return Err(AppError::Command("GEOSEARCHSTORE 单位必须是 m|km|mi|ft".to_string())),
            };
            by_radius = Some(radius_m);
            idx += 1;
        } else if by_type == "BYBOX" {
            idx += 1;
            let width: f64 = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                AppError::Command("GEOSEARCHSTORE 的 width 必须是数字".to_string())
            })?;
            idx += 1;
            let height: f64 = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                AppError::Command("GEOSEARCHSTORE 的 height 必须是数字".to_string())
            })?;
            idx += 1;
            let unit = self.extract_string(&arr[idx])?.to_ascii_lowercase();
            let width_m = match unit.as_str() {
                "m" => width,
                "km" => width * 1000.0,
                "mi" => width * 1609.344,
                "ft" => width * 0.3048,
                _ => return Err(AppError::Command("GEOSEARCHSTORE 单位必须是 m|km|mi|ft".to_string())),
            };
            let height_m = match unit.as_str() {
                "m" => height,
                "km" => height * 1000.0,
                "mi" => height * 1609.344,
                "ft" => height * 0.3048,
                _ => return Err(AppError::Command("GEOSEARCHSTORE 单位必须是 m|km|mi|ft".to_string())),
            };
            by_box = Some((width_m, height_m));
            idx += 1;
        } else {
            return Err(AppError::Command(
                format!("GEOSEARCHSTORE 需要 BYRADIUS 或 BYBOX，得到: {}", by_type),
            ));
        }

        // 可选参数
        while idx < arr.len() {
            let flag = self.extract_string(&arr[idx])?.to_ascii_uppercase();
            if flag == "ASC" || flag == "DESC" {
                order = Some(flag);
                idx += 1;
            } else if flag == "COUNT" {
                idx += 1;
                if idx >= arr.len() {
                    return Err(AppError::Command("GEOSEARCHSTORE COUNT 缺少数量".to_string()));
                }
                count = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                    AppError::Command("GEOSEARCHSTORE COUNT 必须是整数".to_string())
                })?;
                idx += 1;
            } else if flag == "STOREDIST" {
                storedist = true;
                idx += 1;
            } else {
                return Err(AppError::Command(format!("GEOSEARCHSTORE 未知参数: {}", flag)));
            }
        }

        Ok(Command::GeoSearchStore(
            destination, source, center_lon, center_lat, by_radius, by_box,
            order, count, storedist,
        ))
    }

    /// 解析 SELECT 命令：SELECT index
    fn parse_select(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "SELECT 命令需要 1 个参数".to_string(),
            ));
        }
        let index: usize = self.extract_string(&arr[1])?.parse().map_err(|_| {
            AppError::Command("SELECT 的参数必须是整数".to_string())
        })?;
        Ok(Command::Select(index))
    }

    /// 解析 AUTH 命令：AUTH [username] password
    fn parse_auth(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() == 2 {
            // AUTH password（默认用户 default）
            let password = self.extract_string(&arr[1])?;
            Ok(Command::Auth("default".to_string(), password))
        } else if arr.len() == 3 {
            // AUTH username password
            let username = self.extract_string(&arr[1])?;
            let password = self.extract_string(&arr[2])?;
            Ok(Command::Auth(username, password))
        } else {
            Err(AppError::Command(
                "AUTH 命令需要 1 或 2 个参数".to_string(),
            ))
        }
    }

    /// 解析 ACL 命令
    fn parse_acl(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "ACL 命令需要子命令".to_string(),
            ));
        }
        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "SETUSER" => {
                if arr.len() < 3 {
                    return Err(AppError::Command(
                        "ACL SETUSER 需要用户名".to_string(),
                    ));
                }
                let username = self.extract_string(&arr[2])?;
                let rules: Vec<String> = arr[3..].iter()
                    .map(|v| self.extract_string(v))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Command::AclSetUser(username, rules))
            }
            "GETUSER" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "ACL GETUSER 需要 1 个参数".to_string(),
                    ));
                }
                let username = self.extract_string(&arr[2])?;
                Ok(Command::AclGetUser(username))
            }
            "DELUSER" => {
                if arr.len() < 3 {
                    return Err(AppError::Command(
                        "ACL DELUSER 需要至少 1 个用户名".to_string(),
                    ));
                }
                let names: Vec<String> = arr[2..].iter()
                    .map(|v| self.extract_string(v))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Command::AclDelUser(names))
            }
            "LIST" => Ok(Command::AclList),
            "CAT" => {
                if arr.len() == 2 {
                    Ok(Command::AclCat(None))
                } else {
                    let category = self.extract_string(&arr[2])?;
                    Ok(Command::AclCat(Some(category)))
                }
            }
            "WHOAMI" => Ok(Command::AclWhoAmI),
            "LOG" => {
                if arr.len() == 2 {
                    Ok(Command::AclLog(None))
                } else {
                    let arg = self.extract_string(&arr[2])?.to_ascii_uppercase();
                    Ok(Command::AclLog(Some(arg)))
                }
            }
            "GENPASS" => {
                if arr.len() == 2 {
                    Ok(Command::AclGenPass(None))
                } else {
                    let bits: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
                        AppError::Command("ACL GENPASS bits 必须是整数".to_string())
                    })?;
                    Ok(Command::AclGenPass(Some(bits)))
                }
            }
            _ => Err(AppError::Command(
                format!("ACL 未知子命令: {}", sub),
            )),
        }
    }

    /// 解析 CLIENT 命令：CLIENT SETNAME name | CLIENT GETNAME | CLIENT LIST | CLIENT ID
    fn parse_client(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "CLIENT 命令需要子命令".to_string(),
            ));
        }
        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "SETNAME" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "CLIENT SETNAME 命令需要 1 个参数".to_string(),
                    ));
                }
                let name = self.extract_string(&arr[2])?;
                Ok(Command::ClientSetName(name))
            }
            "GETNAME" => {
                if arr.len() != 2 {
                    return Err(AppError::Command(
                        "CLIENT GETNAME 命令不需要参数".to_string(),
                    ));
                }
                Ok(Command::ClientGetName)
            }
            "LIST" => {
                if arr.len() != 2 {
                    return Err(AppError::Command(
                        "CLIENT LIST 命令不需要参数".to_string(),
                    ));
                }
                Ok(Command::ClientList)
            }
            "ID" => {
                if arr.len() != 2 {
                    return Err(AppError::Command(
                        "CLIENT ID 命令不需要参数".to_string(),
                    ));
                }
                Ok(Command::ClientId)
            }
            "INFO" => {
                if arr.len() != 2 {
                    return Err(AppError::Command(
                        "CLIENT INFO 命令不需要参数".to_string(),
                    ));
                }
                Ok(Command::ClientInfo)
            }
            "KILL" => {
                let mut id = None;
                let mut addr = None;
                let mut user = None;
                let mut skipme = true;
                let mut i = 2;
                while i < arr.len() {
                    let arg = self.extract_string(&arr[i])?.to_ascii_uppercase();
                    match arg.as_str() {
                        "ID" => {
                            i += 1;
                            if i < arr.len() {
                                id = Some(self.extract_string(&arr[i])?.parse().map_err(|_| {
                                    AppError::Command("CLIENT KILL ID 必须是整数".to_string())
                                })?);
                            }
                        }
                        "ADDR" => {
                            i += 1;
                            if i < arr.len() {
                                addr = Some(self.extract_string(&arr[i])?);
                            }
                        }
                        "USER" => {
                            i += 1;
                            if i < arr.len() {
                                user = Some(self.extract_string(&arr[i])?);
                            }
                        }
                        "SKIPME" => {
                            i += 1;
                            if i < arr.len() {
                                let val = self.extract_string(&arr[i])?.to_ascii_lowercase();
                                skipme = val == "yes";
                            }
                        }
                        _ => {}
                    }
                    i += 1;
                }
                Ok(Command::ClientKill { id, addr, user, skipme })
            }
            "PAUSE" => {
                if arr.len() < 3 {
                    return Err(AppError::Command(
                        "CLIENT PAUSE 需要 timeout 参数".to_string(),
                    ));
                }
                let timeout: u64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
                    AppError::Command("CLIENT PAUSE timeout 必须是整数".to_string())
                })?;
                let mode = if arr.len() >= 4 {
                    self.extract_string(&arr[3])?.to_ascii_uppercase()
                } else {
                    "ALL".to_string()
                };
                Ok(Command::ClientPause(timeout, mode))
            }
            "UNPAUSE" => {
                if arr.len() != 2 {
                    return Err(AppError::Command(
                        "CLIENT UNPAUSE 命令不需要参数".to_string(),
                    ));
                }
                Ok(Command::ClientUnpause)
            }
            "NO-EVICT" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "CLIENT NO-EVICT 需要 ON|OFF 参数".to_string(),
                    ));
                }
                let flag = match self.extract_string(&arr[2])?.to_ascii_uppercase().as_str() {
                    "ON" => true,
                    "OFF" => false,
                    _ => return Err(AppError::Command(
                        "CLIENT NO-EVICT 参数必须是 ON 或 OFF".to_string(),
                    )),
                };
                Ok(Command::ClientNoEvict(flag))
            }
            "NO-TOUCH" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "CLIENT NO-TOUCH 需要 ON|OFF 参数".to_string(),
                    ));
                }
                let flag = match self.extract_string(&arr[2])?.to_ascii_uppercase().as_str() {
                    "ON" => true,
                    "OFF" => false,
                    _ => return Err(AppError::Command(
                        "CLIENT NO-TOUCH 参数必须是 ON 或 OFF".to_string(),
                    )),
                };
                Ok(Command::ClientNoTouch(flag))
            }
            "REPLY" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "CLIENT REPLY 需要 ON|OFF|SKIP 参数".to_string(),
                    ));
                }
                let mode = match self.extract_string(&arr[2])?.to_ascii_uppercase().as_str() {
                    "ON" => crate::server::ReplyMode::On,
                    "OFF" => crate::server::ReplyMode::Off,
                    "SKIP" => crate::server::ReplyMode::Skip,
                    _ => return Err(AppError::Command(
                        "CLIENT REPLY 参数必须是 ON、OFF 或 SKIP".to_string(),
                    )),
                };
                Ok(Command::ClientReply(mode))
            }
            "UNBLOCK" => {
                if arr.len() < 3 {
                    return Err(AppError::Command(
                        "CLIENT UNBLOCK 需要 client-id 参数".to_string(),
                    ));
                }
                let target_id: u64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
                    AppError::Command("CLIENT UNBLOCK client-id 必须是整数".to_string())
                })?;
                let reason = if arr.len() >= 4 {
                    self.extract_string(&arr[3])?.to_ascii_uppercase()
                } else {
                    "TIMEOUT".to_string()
                };
                Ok(Command::ClientUnblock(target_id, reason))
            }
            _ => Err(AppError::Command(
                format!("CLIENT 未知子命令: {}", sub),
            )),
        }
    }

    /// 解析 SORT 命令：SORT key [BY pattern] [LIMIT offset count] [GET pattern ...] [ASC|DESC] [ALPHA] [STORE destination]
    fn parse_sort(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "SORT 命令需要至少 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let mut by_pattern = None;
        let mut get_patterns = Vec::new();
        let mut limit_offset = None;
        let mut limit_count = None;
        let mut asc = true;
        let mut alpha = false;
        let mut store_key = None;

        let mut i = 2;
        while i < arr.len() {
            let arg = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match arg.as_str() {
                "BY" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("SORT BY 需要参数".to_string()));
                    }
                    by_pattern = Some(self.extract_string(&arr[i + 1])?);
                    i += 2;
                }
                "LIMIT" => {
                    if i + 2 >= arr.len() {
                        return Err(AppError::Command("SORT LIMIT 需要 2 个参数".to_string()));
                    }
                    limit_offset = Some(self.extract_string(&arr[i + 1])?.parse::<isize>().map_err(|_| {
                        AppError::Command("SORT LIMIT offset 必须是整数".to_string())
                    })?);
                    limit_count = Some(self.extract_string(&arr[i + 2])?.parse::<isize>().map_err(|_| {
                        AppError::Command("SORT LIMIT count 必须是整数".to_string())
                    })?);
                    i += 3;
                }
                "GET" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("SORT GET 需要参数".to_string()));
                    }
                    get_patterns.push(self.extract_string(&arr[i + 1])?);
                    i += 2;
                }
                "ASC" => {
                    asc = true;
                    i += 1;
                }
                "DESC" => {
                    asc = false;
                    i += 1;
                }
                "ALPHA" => {
                    alpha = true;
                    i += 1;
                }
                "STORE" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("SORT STORE 需要参数".to_string()));
                    }
                    store_key = Some(self.extract_string(&arr[i + 1])?);
                    i += 2;
                }
                _ => {
                    return Err(AppError::Command(format!("SORT 未知参数: {}", arg)));
                }
            }
        }

        Ok(Command::Sort(key, by_pattern, get_patterns, limit_offset, limit_count, asc, alpha, store_key))
    }

    /// 解析 UNLINK 命令：UNLINK key [key ...]
    fn parse_unlink(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "UNLINK 命令需要至少 1 个参数".to_string(),
            ));
        }
        let mut keys = Vec::new();
        for i in 1..arr.len() {
            keys.push(self.extract_string(&arr[i])?);
        }
        Ok(Command::Unlink(keys))
    }

    /// 解析 COPY 命令：COPY source destination [REPLACE]
    fn parse_copy(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "COPY 命令需要 2 个参数".to_string(),
            ));
        }
        let source = self.extract_string(&arr[1])?;
        let destination = self.extract_string(&arr[2])?;
        let mut replace = false;
        if arr.len() == 4 {
            let arg = self.extract_string(&arr[3])?.to_ascii_uppercase();
            if arg == "REPLACE" {
                replace = true;
            } else {
                return Err(AppError::Command(format!("COPY 未知参数: {}", arg)));
            }
        } else if arr.len() > 4 {
            return Err(AppError::Command("COPY 参数过多".to_string()));
        }
        Ok(Command::Copy(source, destination, replace))
    }

    /// 解析 DUMP 命令：DUMP key
    fn parse_dump(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "DUMP 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::Dump(key))
    }

    /// 解析 RESTORE 命令：RESTORE key ttl serialized-value [REPLACE]
    fn parse_restore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(
                "RESTORE 命令需要至少 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let ttl_ms: u64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("RESTORE ttl 必须是整数".to_string())
        })?;
        let serialized = self.extract_bytes(&arr[3])?;
        let mut replace = false;
        if arr.len() == 5 {
            let arg = self.extract_string(&arr[4])?.to_ascii_uppercase();
            if arg == "REPLACE" {
                replace = true;
            } else {
                return Err(AppError::Command(format!("RESTORE 未知参数: {}", arg)));
            }
        } else if arr.len() > 5 {
            return Err(AppError::Command("RESTORE 参数过多".to_string()));
        }
        Ok(Command::Restore(key, ttl_ms, serialized.to_vec(), replace))
    }

    /// 解析 EVAL 命令：EVAL script numkeys key [key ...] arg [arg ...]
    fn parse_eval(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "EVAL 命令需要至少 1 个参数".to_string(),
            ));
        }
        let script = self.extract_string(&arr[1])?;
        if arr.len() < 3 {
            return Ok(Command::Eval(script, Vec::new(), Vec::new()));
        }
        let numkeys: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("EVAL numkeys 必须是整数".to_string())
        })?;
        if arr.len() < 3 + numkeys {
            return Err(AppError::Command(
                "EVAL 提供的 key 数量不足".to_string(),
            ));
        }
        let mut keys = Vec::new();
        for i in 0..numkeys {
            keys.push(self.extract_string(&arr[3 + i])?);
        }
        let mut args = Vec::new();
        for i in (3 + numkeys)..arr.len() {
            args.push(self.extract_string(&arr[i])?);
        }
        Ok(Command::Eval(script, keys, args))
    }

    /// 解析 EVALSHA 命令：EVALSHA sha1 numkeys key [key ...] arg [arg ...]
    fn parse_evalsha(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "EVALSHA 命令需要至少 1 个参数".to_string(),
            ));
        }
        let sha1 = self.extract_string(&arr[1])?;
        if arr.len() < 3 {
            return Ok(Command::EvalSha(sha1, Vec::new(), Vec::new()));
        }
        let numkeys: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("EVALSHA numkeys 必须是整数".to_string())
        })?;
        if arr.len() < 3 + numkeys {
            return Err(AppError::Command(
                "EVALSHA 提供的 key 数量不足".to_string(),
            ));
        }
        let mut keys = Vec::new();
        for i in 0..numkeys {
            keys.push(self.extract_string(&arr[3 + i])?);
        }
        let mut args = Vec::new();
        for i in (3 + numkeys)..arr.len() {
            args.push(self.extract_string(&arr[i])?);
        }
        Ok(Command::EvalSha(sha1, keys, args))
    }

    /// 解析 SCRIPT 命令：SCRIPT LOAD script | SCRIPT EXISTS sha1 [sha1 ...] | SCRIPT FLUSH
    fn parse_script(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "SCRIPT 命令需要子命令".to_string(),
            ));
        }
        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "LOAD" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "SCRIPT LOAD 命令需要 1 个参数".to_string(),
                    ));
                }
                let script = self.extract_string(&arr[2])?;
                Ok(Command::ScriptLoad(script))
            }
            "EXISTS" => {
                if arr.len() < 3 {
                    return Err(AppError::Command(
                        "SCRIPT EXISTS 命令需要至少 1 个参数".to_string(),
                    ));
                }
                let mut sha1s = Vec::new();
                for i in 2..arr.len() {
                    sha1s.push(self.extract_string(&arr[i])?);
                }
                Ok(Command::ScriptExists(sha1s))
            }
            "FLUSH" => {
                if arr.len() != 2 {
                    return Err(AppError::Command(
                        "SCRIPT FLUSH 命令不需要参数".to_string(),
                    ));
                }
                Ok(Command::ScriptFlush)
            }
            _ => Err(AppError::Command(
                format!("SCRIPT 未知子命令: {}", sub),
            )),
        }
    }

    /// 解析 FUNCTION 命令
    fn parse_function(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "FUNCTION 命令需要子命令".to_string(),
            ));
        }
        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "LOAD" => {
                let mut replace = false;
                let mut code_idx = 2;
                if arr.len() > 2 {
                    let arg = self.extract_string(&arr[2])?.to_ascii_uppercase();
                    if arg == "REPLACE" {
                        replace = true;
                        code_idx = 3;
                    }
                }
                if arr.len() <= code_idx {
                    return Err(AppError::Command(
                        "FUNCTION LOAD 需要函数代码".to_string(),
                    ));
                }
                let code = self.extract_string(&arr[code_idx])?;
                Ok(Command::FunctionLoad(code, replace))
            }
            "DELETE" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "FUNCTION DELETE 需要 1 个参数".to_string(),
                    ));
                }
                let lib = self.extract_string(&arr[2])?;
                Ok(Command::FunctionDelete(lib))
            }
            "LIST" => {
                let mut pattern = None;
                let mut withcode = false;
                let mut i = 2;
                while i < arr.len() {
                    let arg = self.extract_string(&arr[i])?.to_ascii_uppercase();
                    match arg.as_str() {
                        "LIBRARYNAME" => {
                            i += 1;
                            if i < arr.len() {
                                pattern = Some(self.extract_string(&arr[i])?);
                            }
                        }
                        "WITHCODE" => withcode = true,
                        _ => {}
                    }
                    i += 1;
                }
                Ok(Command::FunctionList(pattern, withcode))
            }
            "DUMP" => Ok(Command::FunctionDump),
            "RESTORE" => {
                if arr.len() < 3 {
                    return Err(AppError::Command(
                        "FUNCTION RESTORE 需要序列化数据".to_string(),
                    ));
                }
                let data = self.extract_string(&arr[2])?;
                let policy = if arr.len() >= 4 {
                    self.extract_string(&arr[3])?.to_ascii_uppercase()
                } else {
                    "FLUSH".to_string()
                };
                Ok(Command::FunctionRestore(data, policy))
            }
            "STATS" => Ok(Command::FunctionStats),
            "FLUSH" => {
                let async_mode = if arr.len() >= 3 {
                    let arg = self.extract_string(&arr[2])?.to_ascii_uppercase();
                    arg == "ASYNC"
                } else {
                    false
                };
                Ok(Command::FunctionFlush(async_mode))
            }
            _ => Err(AppError::Command(
                format!("FUNCTION 未知子命令: {}", sub),
            )),
        }
    }

    /// 解析 FCALL 命令：FCALL function numkeys key [key ...] arg [arg ...]
    fn parse_fcall(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "FCALL 命令需要函数名".to_string(),
            ));
        }
        let name = self.extract_string(&arr[1])?;
        if arr.len() < 3 {
            return Ok(Command::FCall(name, vec![], vec![]));
        }
        let numkeys: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("FCALL numkeys 必须是整数".to_string())
        })?;
        if arr.len() < 3 + numkeys {
            return Err(AppError::Command(
                "FCALL 提供的 key 数量不足".to_string(),
            ));
        }
        let mut keys = Vec::new();
        for i in 0..numkeys {
            keys.push(self.extract_string(&arr[3 + i])?);
        }
        let mut args = Vec::new();
        for i in (3 + numkeys)..arr.len() {
            args.push(self.extract_string(&arr[i])?);
        }
        Ok(Command::FCall(name, keys, args))
    }

    /// 解析 FCALL_RO 命令
    fn parse_fcall_ro(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "FCALL_RO 命令需要函数名".to_string(),
            ));
        }
        let name = self.extract_string(&arr[1])?;
        if arr.len() < 3 {
            return Ok(Command::FCallRO(name, vec![], vec![]));
        }
        let numkeys: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("FCALL_RO numkeys 必须是整数".to_string())
        })?;
        if arr.len() < 3 + numkeys {
            return Err(AppError::Command(
                "FCALL_RO 提供的 key 数量不足".to_string(),
            ));
        }
        let mut keys = Vec::new();
        for i in 0..numkeys {
            keys.push(self.extract_string(&arr[3 + i])?);
        }
        let mut args = Vec::new();
        for i in (3 + numkeys)..arr.len() {
            args.push(self.extract_string(&arr[i])?);
        }
        Ok(Command::FCallRO(name, keys, args))
    }

    /// 解析 EVAL_RO 命令：EVAL_RO script numkeys key [key ...] arg [arg ...]
    fn parse_eval_ro(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "EVAL_RO 命令需要脚本".to_string(),
            ));
        }
        let script = self.extract_string(&arr[1])?;
        if arr.len() < 3 {
            return Ok(Command::EvalRO(script, Vec::new(), Vec::new()));
        }
        let numkeys: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("EVAL_RO numkeys 必须是整数".to_string())
        })?;
        if arr.len() < 3 + numkeys {
            return Err(AppError::Command(
                "EVAL_RO 提供的 key 数量不足".to_string(),
            ));
        }
        let mut keys = Vec::new();
        for i in 0..numkeys {
            keys.push(self.extract_string(&arr[3 + i])?);
        }
        let mut args = Vec::new();
        for i in (3 + numkeys)..arr.len() {
            args.push(self.extract_string(&arr[i])?);
        }
        Ok(Command::EvalRO(script, keys, args))
    }

    /// 解析 EVALSHA_RO 命令：EVALSHA_RO sha1 numkeys key [key ...] arg [arg ...]
    fn parse_evalsha_ro(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "EVALSHA_RO 命令需要 sha1".to_string(),
            ));
        }
        let sha1 = self.extract_string(&arr[1])?;
        if arr.len() < 3 {
            return Ok(Command::EvalShaRO(sha1, Vec::new(), Vec::new()));
        }
        let numkeys: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("EVALSHA_RO numkeys 必须是整数".to_string())
        })?;
        if arr.len() < 3 + numkeys {
            return Err(AppError::Command(
                "EVALSHA_RO 提供的 key 数量不足".to_string(),
            ));
        }
        let mut keys = Vec::new();
        for i in 0..numkeys {
            keys.push(self.extract_string(&arr[3 + i])?);
        }
        let mut args = Vec::new();
        for i in (3 + numkeys)..arr.len() {
            args.push(self.extract_string(&arr[i])?);
        }
        Ok(Command::EvalShaRO(sha1, keys, args))
    }

    /// 解析 CONFIG 命令：CONFIG GET key / CONFIG SET key value
    fn parse_config(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "CONFIG 命令需要子命令".to_string(),
            ));
        }

        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "GET" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "CONFIG GET 需要 1 个参数".to_string(),
                    ));
                }
                let key = self.extract_string(&arr[2])?;
                Ok(Command::ConfigGet(key))
            }
            "SET" => {
                if arr.len() != 4 {
                    return Err(AppError::Command(
                        "CONFIG SET 需要 2 个参数".to_string(),
                    ));
                }
                let key = self.extract_string(&arr[2])?;
                let value = self.extract_string(&arr[3])?;
                Ok(Command::ConfigSet(key, value))
            }
            "REWRITE" => Ok(Command::ConfigRewrite),
            "RESETSTAT" => Ok(Command::ConfigResetStat),
            _ => Ok(Command::Unknown(format!("CONFIG {}", sub))),
        }
    }

    /// 解析 MEMORY 命令：MEMORY USAGE key [SAMPLES count] | MEMORY DOCTOR
    fn parse_memory(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "MEMORY 命令需要子命令".to_string(),
            ));
        }
        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "USAGE" => {
                if arr.len() < 3 {
                    return Err(AppError::Command(
                        "MEMORY USAGE 需要 key 参数".to_string(),
                    ));
                }
                let key = self.extract_string(&arr[2])?;
                let mut samples = None;
                let mut i = 3;
                while i < arr.len() {
                    let arg = self.extract_string(&arr[i])?.to_ascii_uppercase();
                    if arg == "SAMPLES" && i + 1 < arr.len() {
                        samples = Some(self.extract_string(&arr[i + 1])?.parse().map_err(|_| {
                            AppError::Command("MEMORY USAGE SAMPLES 必须是整数".to_string())
                        })?);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                Ok(Command::MemoryUsage(key, samples))
            }
            "DOCTOR" => Ok(Command::MemoryDoctor),
            _ => Err(AppError::Command(format!("MEMORY 未知子命令: {}", sub))),
        }
    }

    /// 解析 LATENCY 命令：LATENCY LATEST | LATENCY HISTORY event | LATENCY RESET [event ...]
    fn parse_latency(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "LATENCY 命令需要子命令".to_string(),
            ));
        }
        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "LATEST" => Ok(Command::LatencyLatest),
            "HISTORY" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "LATENCY HISTORY 需要事件名".to_string(),
                    ));
                }
                let event = self.extract_string(&arr[2])?;
                Ok(Command::LatencyHistory(event))
            }
            "RESET" => {
                let events: Vec<String> = arr[2..].iter()
                    .map(|v| self.extract_string(v))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Command::LatencyReset(events))
            }
            _ => Err(AppError::Command(format!("LATENCY 未知子命令: {}", sub))),
        }
    }

    /// 解析 HELLO 命令：HELLO [protover [AUTH username password] [SETNAME clientname]]
    fn parse_hello(&self, arr: &[RespValue]) -> Result<Command> {
        let protover: u8 = if arr.len() >= 2 {
            self.extract_string(&arr[1])?.parse().map_err(|_| {
                AppError::Command("HELLO protover 必须是整数".to_string())
            })?
        } else {
            2
        };
        let mut auth = None;
        let mut setname = None;
        let mut i = 2;
        while i < arr.len() {
            let arg = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match arg.as_str() {
                "AUTH" => {
                    if i + 2 < arr.len() {
                        let username = self.extract_string(&arr[i + 1])?;
                        let password = self.extract_string(&arr[i + 2])?;
                        auth = Some((username, password));
                        i += 3;
                        continue;
                    }
                }
                "SETNAME" => {
                    if i + 1 < arr.len() {
                        setname = Some(self.extract_string(&arr[i + 1])?);
                        i += 2;
                        continue;
                    }
                }
                _ => {}
            }
            i += 1;
        }
        Ok(Command::Hello(protover, auth, setname))
    }

    /// 解析 SLOWLOG 命令：SLOWLOG GET [count] | SLOWLOG LEN | SLOWLOG RESET
    fn parse_slowlog(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "SLOWLOG 命令需要子命令".to_string(),
            ));
        }
        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "GET" => {
                let count = if arr.len() >= 3 {
                    self.extract_string(&arr[2])?.parse().map_err(|_| {
                        AppError::Command("SLOWLOG GET count 必须是整数".to_string())
                    })?
                } else {
                    10
                };
                Ok(Command::SlowLogGet(count))
            }
            "LEN" => {
                if arr.len() != 2 {
                    return Err(AppError::Command(
                        "SLOWLOG LEN 不需要参数".to_string(),
                    ));
                }
                Ok(Command::SlowLogLen)
            }
            "RESET" => {
                if arr.len() != 2 {
                    return Err(AppError::Command(
                        "SLOWLOG RESET 不需要参数".to_string(),
                    ));
                }
                Ok(Command::SlowLogReset)
            }
            _ => Err(AppError::Command(
                format!("SLOWLOG 未知子命令: {}", sub),
            )),
        }
    }

    fn parse_object(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "OBJECT 命令需要子命令".to_string(),
            ));
        }
        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "ENCODING" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "OBJECT ENCODING 需要 1 个 key 参数".to_string(),
                    ));
                }
                Ok(Command::ObjectEncoding(self.extract_string(&arr[2])?))
            }
            "REFCOUNT" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "OBJECT REFCOUNT 需要 1 个 key 参数".to_string(),
                    ));
                }
                Ok(Command::ObjectRefCount(self.extract_string(&arr[2])?))
            }
            "IDLETIME" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "OBJECT IDLETIME 需要 1 个 key 参数".to_string(),
                    ));
                }
                Ok(Command::ObjectIdleTime(self.extract_string(&arr[2])?))
            }
            "HELP" => Ok(Command::ObjectHelp),
            _ => Err(AppError::Command(
                format!("OBJECT 未知子命令: {}", sub),
            )),
        }
    }

    fn parse_debug(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "DEBUG 命令需要子命令".to_string(),
            ));
        }
        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "SET-ACTIVE-EXPIRE" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "DEBUG SET-ACTIVE-EXPIRE 需要 1 个参数 (0|1)".to_string(),
                    ));
                }
                let flag = match self.extract_string(&arr[2])?.as_str() {
                    "0" => false,
                    "1" => true,
                    _ => return Err(AppError::Command(
                        "DEBUG SET-ACTIVE-EXPIRE 参数必须是 0 或 1".to_string(),
                    )),
                };
                Ok(Command::DebugSetActiveExpire(flag))
            }
            "SLEEP" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "DEBUG SLEEP 需要 1 个参数 (seconds)".to_string(),
                    ));
                }
                let seconds: f64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
                    AppError::Command("DEBUG SLEEP 参数必须是数字".to_string())
                })?;
                Ok(Command::DebugSleep(seconds))
            }
            "OBJECT" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "DEBUG OBJECT 需要 1 个 key 参数".to_string(),
                    ));
                }
                Ok(Command::DebugObject(self.extract_string(&arr[2])?))
            }
            _ => Err(AppError::Command(
                format!("DEBUG 未知子命令: {}", sub),
            )),
        }
    }

    fn parse_echo(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "ECHO 命令需要 1 个参数".to_string(),
            ));
        }
        Ok(Command::Echo(self.extract_string(&arr[1])?))
    }

    fn parse_touch(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "TOUCH 命令需要至少 1 个 key".to_string(),
            ));
        }
        let keys: Vec<String> = arr[1..].iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<_>>()?;
        Ok(Command::Touch(keys))
    }

    fn parse_expireat(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "EXPIREAT 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let timestamp: u64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("EXPIREAT 时间戳必须是整数".to_string())
        })?;
        Ok(Command::ExpireAt(key, timestamp))
    }

    fn parse_pexpireat(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "PEXPIREAT 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let timestamp: u64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("PEXPIREAT 时间戳必须是整数".to_string())
        })?;
        Ok(Command::PExpireAt(key, timestamp))
    }

    fn parse_expiretime(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "EXPIRETIME 命令需要 1 个参数".to_string(),
            ));
        }
        Ok(Command::ExpireTime(self.extract_string(&arr[1])?))
    }

    fn parse_pexpiretime(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "PEXPIRETIME 命令需要 1 个参数".to_string(),
            ));
        }
        Ok(Command::PExpireTime(self.extract_string(&arr[1])?))
    }

    fn parse_renamenx(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "RENAMENX 命令需要 2 个参数".to_string(),
            ));
        }
        Ok(Command::RenameNx(
            self.extract_string(&arr[1])?,
            self.extract_string(&arr[2])?,
        ))
    }

    fn parse_swapdb(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "SWAPDB 命令需要 2 个参数".to_string(),
            ));
        }
        let idx1: usize = self.extract_string(&arr[1])?.parse().map_err(|_| {
            AppError::Command("SWAPDB 索引必须是整数".to_string())
        })?;
        let idx2: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("SWAPDB 索引必须是整数".to_string())
        })?;
        Ok(Command::SwapDb(idx1, idx2))
    }

    fn parse_shutdown(&self, arr: &[RespValue]) -> Result<Command> {
        let opt = if arr.len() >= 2 {
            Some(self.extract_string(&arr[1])?.to_ascii_uppercase())
        } else {
            None
        };
        Ok(Command::Shutdown(opt))
    }

    fn parse_substr(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "SUBSTR 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let start: i64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("SUBSTR start 必须是整数".to_string())
        })?;
        let end: i64 = self.extract_string(&arr[3])?.parse().map_err(|_| {
            AppError::Command("SUBSTR end 必须是整数".to_string())
        })?;
        Ok(Command::SubStr(key, start, end))
    }

    fn parse_lcs(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "LCS 命令需要至少 2 个 key".to_string(),
            ));
        }
        let key1 = self.extract_string(&arr[1])?;
        let key2 = self.extract_string(&arr[2])?;
        let mut len = false;
        let mut idx = false;
        let mut minmatchlen = 0usize;
        let mut withmatchlen = false;

        let mut i = 3;
        while i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match opt.as_str() {
                "LEN" => len = true,
                "IDX" => idx = true,
                "MINMATCHLEN" => {
                    i += 1;
                    if i >= arr.len() {
                        return Err(AppError::Command("MINMATCHLEN 需要值".to_string()));
                    }
                    minmatchlen = self.extract_string(&arr[i])?.parse().map_err(|_| {
                        AppError::Command("MINMATCHLEN 必须是整数".to_string())
                    })?;
                }
                "WITHMATCHLEN" => withmatchlen = true,
                _ => {
                    return Err(AppError::Command(
                        format!("LCS 未知选项: {}", opt),
                    ));
                }
            }
            i += 1;
        }

        Ok(Command::Lcs(key1, key2, len, idx, minmatchlen, withmatchlen))
    }

    /// 从 RespValue 中提取字符串（支持 BulkString 和 SimpleString）
    fn extract_string(&self, value: &RespValue) -> Result<String> {
        match value {
            RespValue::BulkString(Some(data)) => {
                Ok(String::from_utf8_lossy(data).to_string())
            }
            RespValue::SimpleString(s) => Ok(s.clone()),
            _ => Err(AppError::Command(
                "期望字符串类型的参数".to_string(),
            )),
        }
    }

    /// 从 RespValue 中提取原始字节（支持 BulkString 和 SimpleString）
    fn extract_bytes(&self, value: &RespValue) -> Result<Bytes> {
        match value {
            RespValue::BulkString(Some(data)) => Ok(data.clone()),
            RespValue::SimpleString(s) => Ok(Bytes::from(s.clone())),
            _ => Err(AppError::Command(
                "期望字符串类型的参数".to_string(),
            )),
        }
    }
    fn parse_lmove(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 5 {
            return Err(AppError::Command("LMOVE 命令需要 4 个参数".to_string()));
        }
        let source = self.extract_string(&arr[1])?;
        let dest = self.extract_string(&arr[2])?;
        let from = self.extract_string(&arr[3])?.to_ascii_uppercase();
        let to = self.extract_string(&arr[4])?.to_ascii_uppercase();
        let left_from = match from.as_str() {
            "LEFT" => true,
            "RIGHT" => false,
            _ => return Err(AppError::Command("LMOVE wherefrom 必须是 LEFT 或 RIGHT".to_string())),
        };
        let left_to = match to.as_str() {
            "LEFT" => true,
            "RIGHT" => false,
            _ => return Err(AppError::Command("LMOVE whereto 必须是 LEFT 或 RIGHT".to_string())),
        };
        Ok(Command::Lmove(source, dest, left_from, left_to))
    }

    fn parse_rpoplpush(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command("RPOPLPUSH 命令需要 2 个参数".to_string()));
        }
        Ok(Command::Rpoplpush(
            self.extract_string(&arr[1])?,
            self.extract_string(&arr[2])?,
        ))
    }

    fn parse_lmpop(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command("LMPOP 命令需要至少 3 个参数".to_string()));
        }
        let numkeys: usize = self.extract_string(&arr[1])?.parse().map_err(|_| {
            AppError::Command("LMPOP numkeys 必须是整数".to_string())
        })?;
        if arr.len() < 2 + numkeys + 1 {
            return Err(AppError::Command("LMPOP 参数不足".to_string()));
        }
        let mut keys = Vec::new();
        for i in 0..numkeys {
            keys.push(self.extract_string(&arr[2 + i])?);
        }
        let mut pos = 2 + numkeys;
        let direction = self.extract_string(&arr[pos])?.to_ascii_uppercase();
        let left = match direction.as_str() {
            "LEFT" => true,
            "RIGHT" => false,
            _ => return Err(AppError::Command("LMPOP 方向必须是 LEFT 或 RIGHT".to_string())),
        };
        pos += 1;
        let mut count = 1usize;
        if pos < arr.len() {
            let opt = self.extract_string(&arr[pos])?.to_ascii_uppercase();
            if opt == "COUNT" {
                pos += 1;
                if pos >= arr.len() {
                    return Err(AppError::Command("LMPOP COUNT 需要值".to_string()));
                }
                count = self.extract_string(&arr[pos])?.parse().map_err(|_| {
                    AppError::Command("LMPOP COUNT 必须是整数".to_string())
                })?;
            }
        }
        Ok(Command::Lmpop(keys, left, count))
    }

    fn parse_blmove(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 6 {
            return Err(AppError::Command("BLMOVE 命令需要 5 个参数".to_string()));
        }
        let source = self.extract_string(&arr[1])?;
        let dest = self.extract_string(&arr[2])?;
        let from = self.extract_string(&arr[3])?.to_ascii_uppercase();
        let to = self.extract_string(&arr[4])?.to_ascii_uppercase();
        let timeout: f64 = self.extract_string(&arr[5])?.parse().map_err(|_| {
            AppError::Command("BLMOVE timeout 必须是数字".to_string())
        })?;
        let left_from = match from.as_str() {
            "LEFT" => true,
            "RIGHT" => false,
            _ => return Err(AppError::Command("BLMOVE wherefrom 必须是 LEFT 或 RIGHT".to_string())),
        };
        let left_to = match to.as_str() {
            "LEFT" => true,
            "RIGHT" => false,
            _ => return Err(AppError::Command("BLMOVE whereto 必须是 LEFT 或 RIGHT".to_string())),
        };
        Ok(Command::BLmove(source, dest, left_from, left_to, timeout))
    }

    fn parse_blmpop(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 5 {
            return Err(AppError::Command("BLMPOP 命令需要至少 4 个参数".to_string()));
        }
        let timeout: f64 = self.extract_string(&arr[1])?.parse().map_err(|_| {
            AppError::Command("BLMPOP timeout 必须是数字".to_string())
        })?;
        let numkeys: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("BLMPOP numkeys 必须是整数".to_string())
        })?;
        if arr.len() < 3 + numkeys + 1 {
            return Err(AppError::Command("BLMPOP 参数不足".to_string()));
        }
        let mut keys = Vec::new();
        for i in 0..numkeys {
            keys.push(self.extract_string(&arr[3 + i])?);
        }
        let mut pos = 3 + numkeys;
        let direction = self.extract_string(&arr[pos])?.to_ascii_uppercase();
        let left = match direction.as_str() {
            "LEFT" => true,
            "RIGHT" => false,
            _ => return Err(AppError::Command("BLMPOP 方向必须是 LEFT 或 RIGHT".to_string())),
        };
        pos += 1;
        let mut count = 1usize;
        if pos < arr.len() {
            let opt = self.extract_string(&arr[pos])?.to_ascii_uppercase();
            if opt == "COUNT" {
                pos += 1;
                if pos >= arr.len() {
                    return Err(AppError::Command("BLMPOP COUNT 需要值".to_string()));
                }
                count = self.extract_string(&arr[pos])?.parse().map_err(|_| {
                    AppError::Command("BLMPOP COUNT 必须是整数".to_string())
                })?;
            }
        }
        Ok(Command::BLmpop(keys, left, count, timeout))
    }

    fn parse_brpoplpush(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command("BRPOPLPUSH 命令需要 3 个参数".to_string()));
        }
        let source = self.extract_string(&arr[1])?;
        let dest = self.extract_string(&arr[2])?;
        let timeout: f64 = self.extract_string(&arr[3])?.parse().map_err(|_| {
            AppError::Command("BRPOPLPUSH timeout 必须是数字".to_string())
        })?;
        Ok(Command::BRpoplpush(source, dest, timeout))
    }
}

impl Default for CommandParser {
    fn default() -> Self {
        Self::new()
    }
}
