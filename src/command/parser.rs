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

    /// 从 RespValue 中提取字符串（支持 BulkString 和 SimpleString）
    pub(crate) fn extract_string(&self, value: &RespValue) -> Result<String> {
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
    pub(crate) fn extract_bytes(&self, value: &RespValue) -> Result<Bytes> {
        match value {
            RespValue::BulkString(Some(data)) => Ok(data.clone()),
            RespValue::SimpleString(s) => Ok(Bytes::from(s.clone())),
            _ => Err(AppError::Command(
                "期望字符串类型的参数".to_string(),
            )),
        }
    }
}

impl Default for CommandParser {
    fn default() -> Self {
        Self::new()
    }
}
