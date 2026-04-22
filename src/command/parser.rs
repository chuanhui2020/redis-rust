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
            "COMMAND" => self.parse_command(&arr),
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
            "PUBSUB" => self.parse_pubsub(&arr),
            "SSUBSCRIBE" => self.parse_ssubscribe(&arr),
            "SUNSUBSCRIBE" => self.parse_sunsubscribe(&arr),
            "SPUBLISH" => self.parse_spublish(&arr),
            "MULTI" => self.parse_multi(&arr),
            "EXEC" => self.parse_exec(&arr),
            "DISCARD" => self.parse_discard(&arr),
            "WATCH" => self.parse_watch(&arr),
            "UNWATCH" => self.parse_unwatch(&arr),
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
            "REPLCONF" => self.parse_replconf(&arr),
            "PSYNC" => self.parse_psync(&arr),
            "ROLE" => self.parse_role(&arr),
            "REPLICAOF" => self.parse_replicaof(&arr),
            "SLAVEOF" => self.parse_replicaof(&arr),
            other => Ok(Command::Unknown(other.to_string())),
        }
    }

    /// 解析 PING 命令：PING [message]

    /// 解析 DEL 命令：DEL key [key ...]

    /// 解析 EXISTS 命令：EXISTS key [key ...]

    /// 解析 EXPIRE 命令：EXPIRE key seconds

    /// 解析 TTL 命令：TTL key

    /// 解析 KEYS 命令：KEYS pattern

    /// 解析 SCAN 命令：SCAN cursor [MATCH pattern] [COUNT count]

    /// 解析 RENAME 命令：RENAME key newkey

    /// 解析 TYPE 命令：TYPE key

    /// 解析 PERSIST 命令：PERSIST key

    /// 解析 PEXPIRE 命令：PEXPIRE key milliseconds

    /// 解析 PTTL 命令：PTTL key

    /// 解析 DBSIZE 命令：DBSIZE

    /// 解析 INFO 命令：INFO [section]

    /// 解析 SUBSCRIBE 命令：SUBSCRIBE channel [channel ...]

    /// 解析 UNSUBSCRIBE 命令：UNSUBSCRIBE [channel ...]

    /// 解析 PUBLISH 命令：PUBLISH channel message

    /// 解析 PSUBSCRIBE 命令：PSUBSCRIBE pattern [pattern ...]

    /// 解析 PUNSUBSCRIBE 命令：PUNSUBSCRIBE [pattern ...]

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

    /// 解析 UNWATCH 命令：UNWATCH
    fn parse_unwatch(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 1 {
            return Err(AppError::Command(
                "UNWATCH 命令不需要参数".to_string(),
            ));
        }
        Ok(Command::Unwatch)
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

    /// 解析 SELECT 命令：SELECT index

    /// 解析 AUTH 命令：AUTH [username] password

    /// 解析 ACL 命令

    /// 解析 CLIENT 命令：CLIENT SETNAME name | CLIENT GETNAME | CLIENT LIST | CLIENT ID

    /// 解析 SORT 命令：SORT key [BY pattern] [LIMIT offset count] [GET pattern ...] [ASC|DESC] [ALPHA] [STORE destination]

    /// 解析 UNLINK 命令：UNLINK key [key ...]

    /// 解析 COPY 命令：COPY source destination [REPLACE]

    /// 解析 DUMP 命令：DUMP key

    /// 解析 RESTORE 命令：RESTORE key ttl serialized-value [REPLACE]

    /// 解析 EVAL 命令：EVAL script numkeys key [key ...] arg [arg ...]

    /// 解析 EVALSHA 命令：EVALSHA sha1 numkeys key [key ...] arg [arg ...]

    /// 解析 SCRIPT 命令：SCRIPT LOAD script | SCRIPT EXISTS sha1 [sha1 ...] | SCRIPT FLUSH

    /// 解析 FUNCTION 命令

    /// 解析 FCALL 命令：FCALL function numkeys key [key ...] arg [arg ...]

    /// 解析 FCALL_RO 命令

    /// 解析 EVAL_RO 命令：EVAL_RO script numkeys key [key ...] arg [arg ...]

    /// 解析 EVALSHA_RO 命令：EVALSHA_RO sha1 numkeys key [key ...] arg [arg ...]

    /// 解析 CONFIG 命令：CONFIG GET key / CONFIG SET key value

    /// 解析 MEMORY 命令：MEMORY USAGE key [SAMPLES count] | MEMORY DOCTOR

    /// 解析 LATENCY 命令：LATENCY LATEST | LATENCY HISTORY event | LATENCY RESET [event ...]

    /// 解析 HELLO 命令：HELLO [protover [AUTH username password] [SETNAME clientname]]

    /// 解析 SLOWLOG 命令：SLOWLOG GET [count] | SLOWLOG LEN | SLOWLOG RESET












    /// 解析 ROLE 命令：ROLE
    fn parse_role(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 1 {
            return Err(AppError::Command(
                "ROLE 命令不需要参数".to_string(),
            ));
        }
        Ok(Command::Role)
    }

    /// 解析 REPLCONF 命令：REPLCONF arg [arg ...]
    fn parse_replconf(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "REPLCONF 命令需要至少 1 个参数".to_string(),
            ));
        }
        let args = arr[1..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::ReplConf { args })
    }

    /// 解析 PSYNC 命令：PSYNC replid offset
    fn parse_psync(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "PSYNC 命令需要 2 个参数".to_string(),
            ));
        }
        let replid = self.extract_string(&arr[1])?;
        let offset = self.extract_string(&arr[2])?.parse::<i64>().map_err(|_| {
            AppError::Command("PSYNC offset 必须是有效的整数".to_string())
        })?;
        Ok(Command::Psync { replid, offset })
    }

    /// 解析 COMMAND 子命令
    fn parse_command(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() == 1 {
            return Ok(Command::CommandInfo);
        }
        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "COUNT" => Ok(Command::CommandCount),
            "LIST" => {
                // 简化实现：忽略复杂过滤器，仅记录可能的模式
                let filter = if arr.len() > 2 {
                    Some(self.extract_string(&arr[arr.len() - 1])?)
                } else {
                    None
                };
                Ok(Command::CommandList(filter))
            }
            "DOCS" => {
                let names = arr[2..]
                    .iter()
                    .map(|v| self.extract_string(v))
                    .collect::<Result<Vec<String>>>()?;
                Ok(Command::CommandDocs(names))
            }
            "GETKEYS" => {
                let args = arr[2..]
                    .iter()
                    .map(|v| self.extract_string(v))
                    .collect::<Result<Vec<String>>>()?;
                Ok(Command::CommandGetKeys(args))
            }
            _ => Ok(Command::CommandInfo),
        }
    }

    /// 解析 REPLICAOF 命令：REPLICAOF host port | REPLICAOF NO ONE
    fn parse_replicaof(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "REPLICAOF 命令需要 2 个参数".to_string(),
            ));
        }
        let arg1 = self.extract_string(&arr[1])?;
        let arg2 = self.extract_string(&arr[2])?;
        if arg1.to_ascii_uppercase() == "NO" && arg2.to_ascii_uppercase() == "ONE" {
            Ok(Command::ReplicaOfNoOne)
        } else {
            let port = arg2.parse::<u16>().map_err(|_| {
                AppError::Command("REPLICAOF 端口必须是有效的整数".to_string())
            })?;
            Ok(Command::ReplicaOf { host: arg1, port })
        }
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
