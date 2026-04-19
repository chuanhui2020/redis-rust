// 命令解析与分发模块

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::aof::AofWriter;
use crate::error::{AppError, Result};
use crate::keyspace::KeyspaceNotifier;
use crate::protocol::RespValue;
use crate::scripting::ScriptEngine;
use crate::slowlog::SlowLog;
use crate::storage::{EvictionPolicy, GetExOption, StorageEngine};

/// Redis 支持的命令枚举
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    /// GET key
    Get(String),
    /// SET key value [NX|XX] [GET] [EX seconds|PX milliseconds|EXAT timestamp|PXAT ms-timestamp|KEEPTTL]
    Set(String, Bytes, crate::storage::SetOptions),
    /// SET key value EX seconds
    SetEx(String, Bytes, u64),
    /// DEL key [key ...]
    Del(Vec<String>),
    /// EXISTS key [key ...]
    Exists(Vec<String>),
    /// PING [message]
    Ping(Option<String>),
    /// FLUSHALL
    FlushAll,
    /// EXPIRE key seconds
    Expire(String, u64),
    /// TTL key
    Ttl(String),
    /// CONFIG GET key
    ConfigGet(String),
    /// CONFIG SET key value
    ConfigSet(String, String),
    /// CONFIG REWRITE
    ConfigRewrite,
    /// CONFIG RESETSTAT
    ConfigResetStat,
    /// MEMORY USAGE key [SAMPLES count]
    MemoryUsage(String, Option<usize>),
    /// MEMORY DOCTOR
    MemoryDoctor,
    /// LATENCY LATEST
    LatencyLatest,
    /// LATENCY HISTORY event-name
    LatencyHistory(String),
    /// LATENCY RESET [event-name ...]
    LatencyReset(Vec<String>),
    /// RESET
    Reset,
    /// HELLO protover [AUTH username password] [SETNAME clientname]
    Hello(u8, Option<(String, String)>, Option<String>),
    /// MONITOR
    Monitor,
    /// COMMAND（redis-benchmark 查询支持的命令列表）
    CommandInfo,
    /// MGET key [key ...]
    MGet(Vec<String>),
    /// MSET key value [key value ...]
    MSet(Vec<(String, Bytes)>),
    /// INCR key
    Incr(String),
    /// DECR key
    Decr(String),
    /// INCRBY key delta
    IncrBy(String, i64),
    /// DECRBY key delta
    DecrBy(String, i64),
    /// APPEND key value
    Append(String, Bytes),
    /// SETNX key value
    SetNx(String, Bytes),
    /// SETEX key seconds value
    SetExCmd(String, Bytes, u64),
    /// PSETEX key milliseconds value
    PSetEx(String, Bytes, u64),
    /// GETSET key value
    GetSet(String, Bytes),
    /// GETDEL key
    GetDel(String),
    /// GETEX key [EX seconds|PX milliseconds|EXAT timestamp|PXAT ms-timestamp|PERSIST]
    GetEx(String, GetExOption),
    /// MSETNX key value [key value ...]
    MSetNx(Vec<(String, Bytes)>),
    /// INCRBYFLOAT key increment
    IncrByFloat(String, f64),
    /// SETRANGE key offset value
    SetRange(String, usize, Bytes),
    /// GETRANGE key start end
    GetRange(String, i64, i64),
    /// STRLEN key
    StrLen(String),
    /// LPUSH key value [value ...]
    LPush(String, Vec<Bytes>),
    /// RPUSH key value [value ...]
    RPush(String, Vec<Bytes>),
    /// LPOP key
    LPop(String),
    /// RPOP key
    RPop(String),
    /// LLEN key
    LLen(String),
    /// LRANGE key start stop
    LRange(String, i64, i64),
    /// LINDEX key index
    LIndex(String, i64),
    /// LSET key index value
    LSet(String, i64, Bytes),
    /// LINSERT key BEFORE|AFTER pivot value
    LInsert(String, crate::storage::LInsertPosition, Bytes, Bytes),
    /// LREM key count value
    LRem(String, i64, Bytes),
    /// LTRIM key start stop
    LTrim(String, i64, i64),
    /// LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
    LPos(String, Bytes, i64, i64, i64),
    /// BLPOP key [key ...] timeout
    BLPop(Vec<String>, f64),
    /// BRPOP key [key ...] timeout
    BRPop(Vec<String>, f64),
    /// HSET key field value [field value ...]
    HSet(String, Vec<(String, Bytes)>),
    /// HGET key field
    HGet(String, String),
    /// HDEL key field [field ...]
    HDel(String, Vec<String>),
    /// HEXISTS key field
    HExists(String, String),
    /// HGETALL key
    HGetAll(String),
    /// HLEN key
    HLen(String),
    /// HMSET key field value [field value ...]
    HMSet(String, Vec<(String, Bytes)>),
    /// HMGET key field [field ...]
    HMGet(String, Vec<String>),
    /// HINCRBY key field increment
    HIncrBy(String, String, i64),
    /// HINCRBYFLOAT key field increment
    HIncrByFloat(String, String, f64),
    /// HKEYS key
    HKeys(String),
    /// HVALS key
    HVals(String),
    /// HSETNX key field value
    HSetNx(String, String, Bytes),
    /// HRANDFIELD key [count [WITHVALUES]]
    HRandField(String, i64, bool),
    /// HSCAN key cursor [MATCH pattern] [COUNT count]
    HScan(String, usize, String, usize),
    /// HEXPIRE key field [field ...] seconds
    HExpire(String, Vec<String>, u64),
    /// HPEXPIRE key field [field ...] milliseconds
    HPExpire(String, Vec<String>, u64),
    /// HEXPIREAT key field [field ...] timestamp-seconds
    HExpireAt(String, Vec<String>, u64),
    /// HPEXPIREAT key field [field ...] timestamp-ms
    HPExpireAt(String, Vec<String>, u64),
    /// HTTL key field [field ...]
    HTtl(String, Vec<String>),
    /// HPTTL key field [field ...]
    HPTtl(String, Vec<String>),
    /// HEXPIRETIME key field [field ...]
    HExpireTime(String, Vec<String>),
    /// HPEXPIRETIME key field [field ...]
    HPExpireTime(String, Vec<String>),
    /// HPERSIST key field [field ...]
    HPersist(String, Vec<String>),
    /// SADD key member [member ...]
    SAdd(String, Vec<Bytes>),
    /// SREM key member [member ...]
    SRem(String, Vec<Bytes>),
    /// SMEMBERS key
    SMembers(String),
    /// SISMEMBER key member
    SIsMember(String, Bytes),
    /// SCARD key
    SCard(String),
    /// SINTER key [key ...]
    SInter(Vec<String>),
    /// SUNION key [key ...]
    SUnion(Vec<String>),
    /// SDIFF key [key ...]
    SDiff(Vec<String>),
    /// SPOP key [count]
    SPop(String, i64),
    /// SRANDMEMBER key [count]
    SRandMember(String, i64),
    /// SMOVE source destination member
    SMove(String, String, Bytes),
    /// SINTERSTORE destination key [key ...]
    SInterStore(String, Vec<String>),
    /// SUNIONSTORE destination key [key ...]
    SUnionStore(String, Vec<String>),
    /// SDIFFSTORE destination key [key ...]
    SDiffStore(String, Vec<String>),
    /// SSCAN key cursor [MATCH pattern] [COUNT count]
    SScan(String, usize, String, usize),
    /// ZADD key score member [score member ...]
    ZAdd(String, Vec<(f64, String)>),
    /// ZREM key member [member ...]
    ZRem(String, Vec<String>),
    /// ZSCORE key member
    ZScore(String, String),
    /// ZRANK key member
    ZRank(String, String),
    /// ZRANGE key start stop [WITHSCORES]
    ZRange(String, isize, isize, bool),
    /// ZRANGEBYSCORE key min max [WITHSCORES]
    ZRangeByScore(String, f64, f64, bool),
    /// ZCARD key
    ZCard(String),
    /// ZREVRANGE key start stop [WITHSCORES]
    ZRevRange(String, isize, isize, bool),
    /// ZREVRANK key member
    ZRevRank(String, String),
    /// ZINCRBY key increment member
    ZIncrBy(String, f64, String),
    /// ZCOUNT key min max
    ZCount(String, f64, f64),
    /// ZPOPMIN key [count]
    ZPopMin(String, usize),
    /// ZPOPMAX key [count]
    ZPopMax(String, usize),
    /// ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
    ZUnionStore(String, Vec<String>, Vec<f64>, String),
    /// ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
    ZInterStore(String, Vec<String>, Vec<f64>, String),
    /// ZSCAN key cursor [MATCH pattern] [COUNT count]
    ZScan(String, usize, String, usize),
    /// ZRANGEBYLEX key min max [LIMIT offset count]
    ZRangeByLex(String, String, String),
    /// SINTERCARD numkeys key [key ...] [LIMIT limit]
    SInterCard(Vec<String>, usize),
    /// SMISMEMBER key member [member ...]
    SMisMember(String, Vec<Bytes>),
    /// ZRANDMEMBER key [count [WITHSCORES]]
    ZRandMember(String, i64, bool),
    /// ZDIFF key [key ...] [WITHSCORES]
    ZDiff(Vec<String>, bool),
    /// ZDIFFSTORE destination numkeys key [key ...]
    ZDiffStore(String, Vec<String>),
    /// ZINTER numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
    ZInter(Vec<String>, Vec<f64>, String, bool),
    /// ZUNION numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
    ZUnion(Vec<String>, Vec<f64>, String, bool),
    /// ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]
    ZRangeStore(String, String, String, String, bool, bool, bool, usize, usize),
    /// ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
    ZMpop(Vec<String>, bool, usize),
    /// BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
    BZMpop(f64, Vec<String>, bool, usize),
    /// BZPOPMIN key [key ...] timeout
    BZPopMin(Vec<String>, f64),
    /// BZPOPMAX key [key ...] timeout
    BZPopMax(Vec<String>, f64),
    /// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
    ZRevRangeByScore(String, f64, f64, bool, usize, usize),
    /// ZREVRANGEBYLEX key max min [LIMIT offset count]
    ZRevRangeByLex(String, String, String, usize, usize),
    /// ZMSCORE key member [member ...]
    ZMScore(String, Vec<String>),
    /// ZLEXCOUNT key min max
    ZLexCount(String, String, String),
    /// ZRANGE 统一语法: ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
    ZRangeUnified(String, String, String, bool, bool, bool, bool, usize, usize),
    /// KEYS pattern
    Keys(String),
    /// SCAN cursor [MATCH pattern] [COUNT count]
    Scan(usize, String, usize),
    /// RENAME key newkey
    Rename(String, String),
    /// TYPE key
    Type(String),
    /// PERSIST key
    Persist(String),
    /// PEXPIRE key milliseconds
    PExpire(String, u64),
    /// PTTL key
    PTtl(String),
    /// DBSIZE
    DbSize,
    /// INFO [section]
    Info(Option<String>),
    /// SUBSCRIBE channel [channel ...]
    Subscribe(Vec<String>),
    /// UNSUBSCRIBE [channel ...]
    Unsubscribe(Vec<String>),
    /// PUBLISH channel message
    Publish(String, Bytes),
    /// PSUBSCRIBE pattern [pattern ...]
    PSubscribe(Vec<String>),
    /// PUNSUBSCRIBE [pattern ...]
    PUnsubscribe(Vec<String>),
    /// MULTI
    Multi,
    /// EXEC
    Exec,
    /// DISCARD
    Discard,
    /// WATCH key [key ...]
    Watch(Vec<String>),
    /// BGREWRITEAOF
    BgRewriteAof,
    /// SETBIT key offset value
    SetBit(String, usize, bool),
    /// GETBIT key offset
    GetBit(String, usize),
    /// BITCOUNT key [start end [BYTE|BIT]]
    BitCount(String, isize, isize, bool),
    /// BITOP AND|OR|XOR|NOT destkey key [key ...]
    BitOp(String, String, Vec<String>),
    /// BITPOS key bit [start [end [BYTE|BIT]]]
    BitPos(String, u8, isize, isize, bool),
    /// BITFIELD key [GET type offset] [SET type offset value] [INCRBY type offset increment] [OVERFLOW WRAP|SAT|FAIL] ...
    BitField(String, Vec<crate::storage::BitFieldOp>),
    /// BITFIELD_RO key [GET type offset] ...
    BitFieldRo(String, Vec<crate::storage::BitFieldOp>),
    /// XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold] *|id field value [field value ...]
    XAdd(String, String, Vec<(String, String)>, bool, Option<usize>, Option<String>),
    /// XLEN key
    XLen(String),
    /// XRANGE key start end [COUNT count]
    XRange(String, String, String, Option<usize>),
    /// XREVRANGE key end start [COUNT count]
    XRevRange(String, String, String, Option<usize>),
    /// XTRIM key MAXLEN|MINID [=|~] threshold
    XTrim(String, String, String),
    /// XDEL key id [id ...]
    XDel(String, Vec<String>),
    /// XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
    XRead(Vec<String>, Vec<String>, Option<usize>),
    /// XSETID key id
    XSetId(String, String),
    /// XGROUP CREATE key groupname id [MKSTREAM]
    XGroupCreate(String, String, String, bool),
    /// XGROUP DESTROY key groupname
    XGroupDestroy(String, String),
    /// XGROUP SETID key groupname id
    XGroupSetId(String, String, String),
    /// XGROUP DELCONSUMER key groupname consumername
    XGroupDelConsumer(String, String, String),
    /// XGROUP CREATECONSUMER key groupname consumername
    XGroupCreateConsumer(String, String, String),
    /// XREADGROUP GROUP group consumer [COUNT count] [NOACK] STREAMS key [key ...] id [id ...]
    XReadGroup(String, String, Vec<String>, Vec<String>, Option<usize>, bool),
    /// XACK key group id [id ...]
    XAck(String, String, Vec<String>),
    /// XCLAIM key group consumer min-idle-time id [id ...] [JUSTID]
    XClaim(String, String, String, u64, Vec<String>, bool),
    /// XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
    XAutoClaim(String, String, String, u64, String, usize, bool),
    /// XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
    XPending(String, String, Option<String>, Option<String>, Option<usize>, Option<String>),
    /// XINFO STREAM key [FULL]
    XInfoStream(String, bool),
    /// XINFO GROUPS key
    XInfoGroups(String),
    /// XINFO CONSUMERS key groupname
    XInfoConsumers(String, String),
    /// PFADD key element [element ...]
    PfAdd(String, Vec<String>),
    /// PFCOUNT key [key ...]
    PfCount(Vec<String>),
    /// PFMERGE destkey sourcekey [sourcekey ...]
    PfMerge(String, Vec<String>),
    /// GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]
    GeoAdd(String, Vec<(f64, f64, String)>),
    /// GEODIST key member1 member2 [m|km|ft|mi]
    GeoDist(String, String, String, String),
    /// GEOHASH key member [member ...]
    GeoHash(String, Vec<String>),
    /// GEOPOS key member [member ...]
    GeoPos(String, Vec<String>),
    /// GEOSEARCH key [FROMMEMBER member|FROMLONLAT lon lat] [BYRADIUS radius unit|BYBOX width height unit] [ASC|DESC] [COUNT count] [WITHCOORD] [WITHDIST] [WITHHASH]
    GeoSearch(String, f64, f64, Option<f64>, Option<(f64, f64)>, Option<String>, usize, bool, bool, bool),
    /// GEOSEARCHSTORE destination source [FROMMEMBER|FROMLONLAT ...] [BYRADIUS|BYBOX ...] [ASC|DESC] [COUNT count] [STOREDIST]
    GeoSearchStore(String, String, f64, f64, Option<f64>, Option<(f64, f64)>, Option<String>, usize, bool),
    /// SELECT index
    Select(usize),
    /// AUTH [username] password
    Auth(String, String),
    /// ACL SETUSER username [rule ...]
    AclSetUser(String, Vec<String>),
    /// ACL GETUSER username
    AclGetUser(String),
    /// ACL DELUSER username [username ...]
    AclDelUser(Vec<String>),
    /// ACL LIST
    AclList,
    /// ACL CAT [category]
    AclCat(Option<String>),
    /// ACL WHOAMI
    AclWhoAmI,
    /// ACL LOG [count|RESET]
    AclLog(Option<String>),
    /// ACL GENPASS [bits]
    AclGenPass(Option<usize>),
    /// CLIENT SETNAME connection-name
    ClientSetName(String),
    /// CLIENT GETNAME
    ClientGetName,
    /// CLIENT LIST
    ClientList,
    /// CLIENT ID
    ClientId,
    /// CLIENT INFO
    ClientInfo,
    /// CLIENT KILL [ID id | ADDR addr | USER user | SKIPME yes|no]
    ClientKill {
        id: Option<u64>,
        addr: Option<String>,
        user: Option<String>,
        skipme: bool,
    },
    /// CLIENT PAUSE timeout [WRITE|ALL]
    ClientPause(u64, String),
    /// CLIENT UNPAUSE
    ClientUnpause,
    /// CLIENT NO-EVICT ON|OFF
    ClientNoEvict(bool),
    /// CLIENT NO-TOUCH ON|OFF
    ClientNoTouch(bool),
    /// CLIENT REPLY ON|OFF|SKIP
    ClientReply(crate::server::ReplyMode),
    /// CLIENT UNBLOCK client-id [TIMEOUT|ERROR]
    ClientUnblock(u64, String),
    /// SORT key [BY pattern] [LIMIT offset count] [GET pattern ...] [ASC|DESC] [ALPHA] [STORE destination]
    Sort(String, Option<String>, Vec<String>, Option<isize>, Option<isize>, bool, bool, Option<String>),
    /// UNLINK key [key ...]
    Unlink(Vec<String>),
    /// COPY source destination [REPLACE]
    Copy(String, String, bool),
    /// DUMP key
    Dump(String),
    /// RESTORE key ttl serialized-value [REPLACE]
    Restore(String, u64, Vec<u8>, bool),
    /// EVAL script numkeys key [key ...] arg [arg ...]
    Eval(String, Vec<String>, Vec<String>),
    /// EVALSHA sha1 numkeys key [key ...] arg [arg ...]
    EvalSha(String, Vec<String>, Vec<String>),
    /// SCRIPT LOAD script
    ScriptLoad(String),
    /// SCRIPT EXISTS sha1 [sha1 ...]
    ScriptExists(Vec<String>),
    /// SCRIPT FLUSH
    ScriptFlush,
    /// FUNCTION LOAD [REPLACE] function-code
    FunctionLoad(String, bool),
    /// FUNCTION DELETE library-name
    FunctionDelete(String),
    /// FUNCTION LIST [LIBRARYNAME pattern] [WITHCODE]
    FunctionList(Option<String>, bool),
    /// FUNCTION DUMP
    FunctionDump,
    /// FUNCTION RESTORE serialized-value policy
    FunctionRestore(String, String),
    /// FUNCTION STATS
    FunctionStats,
    /// FUNCTION FLUSH [ASYNC|SYNC]
    FunctionFlush(bool),
    /// FCALL function numkeys key [key ...] arg [arg ...]
    FCall(String, Vec<String>, Vec<String>),
    /// FCALL_RO function numkeys key [key ...] arg [arg ...]
    FCallRO(String, Vec<String>, Vec<String>),
    /// EVAL_RO script numkeys key [key ...] arg [arg ...]
    EvalRO(String, Vec<String>, Vec<String>),
    /// EVALSHA_RO sha1 numkeys key [key ...] arg [arg ...]
    EvalShaRO(String, Vec<String>, Vec<String>),
    /// SAVE → 同步保存 RDB 快照
    Save,
    /// BGSAVE → 后台保存 RDB 快照
    BgSave,
    /// SLOWLOG GET [count]
    SlowLogGet(usize),
    /// SLOWLOG LEN
    SlowLogLen,
    /// SLOWLOG RESET
    SlowLogReset,
    /// OBJECT ENCODING key
    ObjectEncoding(String),
    /// OBJECT REFCOUNT key
    ObjectRefCount(String),
    /// OBJECT IDLETIME key
    ObjectIdleTime(String),
    /// OBJECT HELP
    ObjectHelp,
    /// DEBUG SET-ACTIVE-EXPIRE 0|1
    DebugSetActiveExpire(bool),
    /// DEBUG SLEEP seconds
    DebugSleep(f64),
    /// DEBUG OBJECT key
    DebugObject(String),
    /// ECHO message
    Echo(String),
    /// TIME
    Time,
    /// RANDOMKEY
    RandomKey,
    /// TOUCH key [key ...]
    Touch(Vec<String>),
    /// EXPIREAT key timestamp
    ExpireAt(String, u64),
    /// PEXPIREAT key ms-timestamp
    PExpireAt(String, u64),
    /// EXPIRETIME key
    ExpireTime(String),
    /// PEXPIRETIME key
    PExpireTime(String),
    /// RENAMENX key newkey
    RenameNx(String, String),
    /// SWAPDB index1 index2
    SwapDb(usize, usize),
    /// FLUSHDB
    FlushDb,
    /// SHUTDOWN [NOSAVE|SAVE]
    Shutdown(Option<String>),
    /// LASTSAVE
    LastSave,
    /// SUBSTR key start end
    SubStr(String, i64, i64),
    /// LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN]
    Lcs(String, String, bool, bool, usize, bool),
    /// LMOVE source destination LEFT|RIGHT LEFT|RIGHT
    Lmove(String, String, bool, bool),
    /// RPOPLPUSH source destination
    Rpoplpush(String, String),
    /// LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
    Lmpop(Vec<String>, bool, usize),
    /// BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
    BLmove(String, String, bool, bool, f64),
    /// BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
    BLmpop(Vec<String>, bool, usize, f64),
    /// BRPOPLPUSH source destination timeout
    BRpoplpush(String, String, f64),
    /// QUIT
    Quit,
    /// 未知命令
    Unknown(String),
}

pub mod resp;
pub mod parser;
pub mod executor;
pub mod info;
pub mod parser_bitmap;
pub mod parser_geo;
pub mod parser_hash;
pub mod parser_hll;
pub mod parser_list;
pub mod parser_set;
pub mod parser_stream;
pub mod parser_string;
pub mod parser_zset;

pub use executor::CommandExecutor;
pub use parser::CommandParser;
pub(crate) use info::extract_cmd_info;

/// 辅助函数：创建 BulkString
pub(crate) fn bulk(s: &str) -> RespValue {
    RespValue::BulkString(Some(Bytes::copy_from_slice(s.as_bytes())))
}

/// 辅助函数：从 Bytes 创建 BulkString
pub(crate) fn bulk_bytes(b: &Bytes) -> RespValue {
    RespValue::BulkString(Some(b.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_bulk_array(items: &[&str]) -> RespValue {
        RespValue::Array(
            items
                .iter()
                .map(|s| RespValue::BulkString(Some(Bytes::copy_from_slice(s.as_bytes()))))
                .collect(),
        )
    }

    #[test]
    fn test_parse_ping_no_args() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["PING"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::Ping(None));
    }

    #[test]
    fn test_parse_ping_with_arg() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["PING", "hello"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::Ping(Some("hello".to_string())));
    }

    #[test]
    fn test_parse_set() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["SET", "key", "value"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(
            cmd,
            Command::Set("key".to_string(), Bytes::from("value"), crate::storage::SetOptions::default())
        );
    }

    #[test]
    fn test_parse_set_ex() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["SET", "key", "value", "EX", "10"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(
            cmd,
            Command::Set(
                "key".to_string(),
                Bytes::from("value"),
                crate::storage::SetOptions {
                    expire: Some(crate::storage::SetExpireOption::Ex(10)),
                    ..Default::default()
                }
            )
        );
    }

    #[test]
    fn test_parse_set_px() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["SET", "key", "value", "PX", "500"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(
            cmd,
            Command::Set(
                "key".to_string(),
                Bytes::from("value"),
                crate::storage::SetOptions {
                    expire: Some(crate::storage::SetExpireOption::Px(500)),
                    ..Default::default()
                }
            )
        );
    }

    #[test]
    fn test_parse_get() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["GET", "key"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::Get("key".to_string()));
    }

    #[test]
    fn test_parse_del() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["DEL", "a", "b", "c"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(
            cmd,
            Command::Del(vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string()
            ])
        );
    }

    #[test]
    fn test_parse_exists() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["EXISTS", "x", "y"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(
            cmd,
            Command::Exists(vec!["x".to_string(), "y".to_string()])
        );
    }

    #[test]
    fn test_parse_unknown() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["FOOBAR"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::Unknown("FOOBAR".to_string()));
    }

    #[test]
    fn test_parse_expire() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["EXPIRE", "key", "10"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::Expire("key".to_string(), 10));
    }

    #[test]
    fn test_parse_ttl() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["TTL", "key"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::Ttl("key".to_string()));
    }

    #[test]
    fn test_execute_ping() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        let resp = executor.execute(Command::Ping(None)).unwrap();
        assert_eq!(resp, RespValue::SimpleString("PONG".to_string()));

        let resp = executor.execute(Command::Ping(Some("hi".to_string()))).unwrap();
        assert_eq!(resp, RespValue::SimpleString("hi".to_string()));
    }

    #[test]
    fn test_execute_set_get() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::Set("name".to_string(), Bytes::from("redis"), crate::storage::SetOptions::default()))
            .unwrap();
        let resp = executor.execute(Command::Get("name".to_string())).unwrap();
        assert_eq!(
            resp,
            RespValue::BulkString(Some(Bytes::from("redis")))
        );
    }

    #[test]
    fn test_execute_get_nonexistent() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        let resp = executor.execute(Command::Get("missing".to_string())).unwrap();
        assert_eq!(resp, RespValue::BulkString(None));
    }

    #[test]
    fn test_execute_del() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::Set("a".to_string(), Bytes::from("1"), crate::storage::SetOptions::default()))
            .unwrap();
        executor
            .execute(Command::Set("b".to_string(), Bytes::from("2"), crate::storage::SetOptions::default()))
            .unwrap();

        let resp = executor
            .execute(Command::Del(vec!["a".to_string(), "c".to_string()]))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(1));
    }

    #[test]
    fn test_execute_exists() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::Set("x".to_string(), Bytes::from("1"), crate::storage::SetOptions::default()))
            .unwrap();

        let resp = executor
            .execute(Command::Exists(vec!["x".to_string(), "y".to_string()]))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(1));
    }

    #[test]
    fn test_execute_unknown() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        let resp = executor
            .execute(Command::Unknown("FOOBAR".to_string()))
            .unwrap();
        assert_eq!(
            resp,
            RespValue::Error("ERR unknown command 'FOOBAR'".to_string())
        );
    }

    #[test]
    fn test_execute_flushall() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::Set("k".to_string(), Bytes::from("v"), crate::storage::SetOptions::default()))
            .unwrap();
        let resp = executor.execute(Command::FlushAll).unwrap();
        assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

        let resp = executor.execute(Command::Get("k".to_string())).unwrap();
        assert_eq!(resp, RespValue::BulkString(None));
    }

    #[test]
    fn test_execute_expire_ttl() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::Set("key".to_string(), Bytes::from("val"), crate::storage::SetOptions::default()))
            .unwrap();

        let resp = executor
            .execute(Command::Expire("key".to_string(), 10))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(1));

        let resp = executor
            .execute(Command::Ttl("key".to_string()))
            .unwrap();
        // TTL 返回秒数，可能是 9 或 10，取决于执行时机
        match resp {
            RespValue::Integer(n) => assert!(n >= 9 && n <= 10),
            other => panic!("期望 Integer，得到 {:?}", other),
        }
    }

    #[test]
    fn test_execute_expire_nonexistent() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        let resp = executor
            .execute(Command::Expire("missing".to_string(), 10))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(0));

        let resp = executor
            .execute(Command::Ttl("missing".to_string()))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(-2));
    }

    #[test]
    fn test_execute_ttl_no_expire() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::Set("key".to_string(), Bytes::from("val"), crate::storage::SetOptions::default()))
            .unwrap();

        let resp = executor
            .execute(Command::Ttl("key".to_string()))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(-1));
    }

    #[test]
    fn test_execute_ttl_after_expire() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::Set("key".to_string(), Bytes::from("val"), crate::storage::SetOptions::default()))
            .unwrap();
        executor
            .execute(Command::Expire("key".to_string(), 0))
            .unwrap();

        // 等待一小段时间确保过期
        std::thread::sleep(std::time::Duration::from_millis(50));

        let resp = executor
            .execute(Command::Get("key".to_string()))
            .unwrap();
        assert_eq!(resp, RespValue::BulkString(None));

        let resp = executor
            .execute(Command::Ttl("key".to_string()))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(-2));
    }

    // ---------- 边界测试 ----------

    #[test]
    fn test_empty_key_and_value() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::Set("".to_string(), Bytes::from(""), crate::storage::SetOptions::default()))
            .unwrap();
        let resp = executor.execute(Command::Get("".to_string())).unwrap();
        assert_eq!(
            resp,
            RespValue::BulkString(Some(Bytes::from_static(b"")))
        );
    }

    #[test]
    fn test_long_key() {
        let long_key = "a".repeat(1000);
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::Set(long_key.clone(), Bytes::from("val"), crate::storage::SetOptions::default()))
            .unwrap();
        let resp = executor.execute(Command::Get(long_key)).unwrap();
        assert_eq!(
            resp,
            RespValue::BulkString(Some(Bytes::from("val")))
        );
    }

    #[test]
    fn test_parse_set_missing_value() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["SET", "key"]);
        let result = parser.parse(resp);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_get_too_many_args() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["GET", "key", "extra"]);
        let result = parser.parse(resp);
        assert!(result.is_err());
    }

    // ---------- 字符串扩展命令测试 ----------

    #[test]
    fn test_execute_mset_mget() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::MSet(vec![
                ("a".to_string(), Bytes::from("1")),
                ("b".to_string(), Bytes::from("2")),
            ]))
            .unwrap();

        let resp = executor
            .execute(Command::MGet(vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
            ]))
            .unwrap();

        assert_eq!(
            resp,
            RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from("1"))),
                RespValue::BulkString(Some(Bytes::from("2"))),
                RespValue::BulkString(None),
            ])
        );
    }

    #[test]
    fn test_execute_incr_decr() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        assert_eq!(
            executor.execute(Command::Incr("c".to_string())).unwrap(),
            RespValue::Integer(1)
        );
        assert_eq!(
            executor.execute(Command::Incr("c".to_string())).unwrap(),
            RespValue::Integer(2)
        );
        assert_eq!(
            executor.execute(Command::Decr("c".to_string())).unwrap(),
            RespValue::Integer(1)
        );
        assert_eq!(
            executor
                .execute(Command::IncrBy("c".to_string(), 10))
                .unwrap(),
            RespValue::Integer(11)
        );
        assert_eq!(
            executor
                .execute(Command::DecrBy("c".to_string(), 5))
                .unwrap(),
            RespValue::Integer(6)
        );
    }

    #[test]
    fn test_execute_incr_non_integer() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::Set("x".to_string(), Bytes::from("abc"), crate::storage::SetOptions::default()))
            .unwrap();
        let result = executor.execute(Command::Incr("x".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_execute_append() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        let resp = executor
            .execute(Command::Append(
                "s".to_string(),
                Bytes::from("hello"),
            ))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(5));

        let resp = executor
            .execute(Command::Append(
                "s".to_string(),
                Bytes::from(" world"),
            ))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(11));

        let resp = executor.execute(Command::Get("s".to_string())).unwrap();
        assert_eq!(
            resp,
            RespValue::BulkString(Some(Bytes::from("hello world")))
        );
    }

    #[test]
    fn test_execute_setnx() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        let resp = executor
            .execute(Command::SetNx(
                "key".to_string(),
                Bytes::from("first"),
            ))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(1));

        let resp = executor
            .execute(Command::SetNx(
                "key".to_string(),
                Bytes::from("second"),
            ))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(0));

        let resp = executor.execute(Command::Get("key".to_string())).unwrap();
        assert_eq!(
            resp,
            RespValue::BulkString(Some(Bytes::from("first")))
        );
    }

    #[test]
    fn test_execute_getrange() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::Set(
                "s".to_string(),
                Bytes::from("hello world"),
                crate::storage::SetOptions::default(),
))
            .unwrap();

        let resp = executor
            .execute(Command::GetRange("s".to_string(), 0, 4))
            .unwrap();
        assert_eq!(
            resp,
            RespValue::BulkString(Some(Bytes::from("hello")))
        );

        let resp = executor
            .execute(Command::GetRange("s".to_string(), -5, -1))
            .unwrap();
        assert_eq!(
            resp,
            RespValue::BulkString(Some(Bytes::from("world")))
        );
    }

    #[test]
    fn test_execute_strlen() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        let resp = executor.execute(Command::StrLen("missing".to_string())).unwrap();
        assert_eq!(resp, RespValue::Integer(0));

        executor
            .execute(Command::Set("s".to_string(), Bytes::from("hello"), crate::storage::SetOptions::default()))
            .unwrap();

        let resp = executor.execute(Command::StrLen("s".to_string())).unwrap();
        assert_eq!(resp, RespValue::Integer(5));
    }

    #[test]
    fn test_parse_mset_mget() {
        let parser = CommandParser::new();

        let resp = make_bulk_array(&["MSET", "a", "1", "b", "2"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(
            cmd,
            Command::MSet(vec![
                ("a".to_string(), Bytes::from("1")),
                ("b".to_string(), Bytes::from("2")),
            ])
        );

        let resp = make_bulk_array(&["MGET", "a", "b"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(
            cmd,
            Command::MGet(vec!["a".to_string(), "b".to_string()])
        );
    }

    #[test]
    fn test_parse_incr_decr_family() {
        let parser = CommandParser::new();

        assert_eq!(
            parser.parse(make_bulk_array(&["INCR", "k"])).unwrap(),
            Command::Incr("k".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["DECR", "k"])).unwrap(),
            Command::Decr("k".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["INCRBY", "k", "5"])).unwrap(),
            Command::IncrBy("k".to_string(), 5)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["DECRBY", "k", "3"])).unwrap(),
            Command::DecrBy("k".to_string(), 3)
        );
    }

    #[test]
    fn test_parse_append_setnx_getrange_strlen() {
        let parser = CommandParser::new();

        assert_eq!(
            parser.parse(make_bulk_array(&["APPEND", "k", "v"])).unwrap(),
            Command::Append("k".to_string(), Bytes::from("v"))
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["SETNX", "k", "v"])).unwrap(),
            Command::SetNx("k".to_string(), Bytes::from("v"))
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["GETRANGE", "k", "0", "3"]))
                .unwrap(),
            Command::GetRange("k".to_string(), 0, 3)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["STRLEN", "k"])).unwrap(),
            Command::StrLen("k".to_string())
        );
    }

    // ---------- List 命令测试 ----------

    #[test]
    fn test_execute_lpush_rpush_lpop_rpop() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        let resp = executor
            .execute(Command::LPush(
                "list".to_string(),
                vec![Bytes::from("a"), Bytes::from("b")],
            ))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(2));

        let resp = executor
            .execute(Command::RPush("list".to_string(), vec![Bytes::from("c")]))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(3));

        let resp = executor.execute(Command::LLen("list".to_string())).unwrap();
        assert_eq!(resp, RespValue::Integer(3));

        let resp = executor.execute(Command::LPop("list".to_string())).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("b"))));

        let resp = executor.execute(Command::RPop("list".to_string())).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("c"))));
    }

    #[test]
    fn test_execute_lrange_lindex() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::RPush(
                "list".to_string(),
                vec![
                    Bytes::from("a"),
                    Bytes::from("b"),
                    Bytes::from("c"),
                    Bytes::from("d"),
                ],
            ))
            .unwrap();

        let resp = executor
            .execute(Command::LRange("list".to_string(), 0, 1))
            .unwrap();
        assert_eq!(
            resp,
            RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from("a"))),
                RespValue::BulkString(Some(Bytes::from("b"))),
            ])
        );

        let resp = executor
            .execute(Command::LIndex("list".to_string(), 2))
            .unwrap();
        assert_eq!(
            resp,
            RespValue::BulkString(Some(Bytes::from("c")))
        );

        let resp = executor
            .execute(Command::LIndex("list".to_string(), -1))
            .unwrap();
        assert_eq!(
            resp,
            RespValue::BulkString(Some(Bytes::from("d")))
        );

        let resp = executor
            .execute(Command::LIndex("list".to_string(), 10))
            .unwrap();
        assert_eq!(resp, RespValue::BulkString(None));
    }

    #[test]
    fn test_execute_list_wrongtype() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::Set("s".to_string(), Bytes::from("hello"), crate::storage::SetOptions::default()))
            .unwrap();

        let result = executor.execute(Command::LPush(
            "s".to_string(),
            vec![Bytes::from("x")],
        ));
        assert!(result.is_err());

        let result = executor.execute(Command::LPop("s".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_list_commands() {
        let parser = CommandParser::new();

        assert_eq!(
            parser.parse(make_bulk_array(&["LPUSH", "k", "v1", "v2"]))
                .unwrap(),
            Command::LPush(
                "k".to_string(),
                vec![Bytes::from("v1"), Bytes::from("v2")]
            )
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["RPUSH", "k", "v"])).unwrap(),
            Command::RPush("k".to_string(), vec![Bytes::from("v")])
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["LPOP", "k"])).unwrap(),
            Command::LPop("k".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["RPOP", "k"])).unwrap(),
            Command::RPop("k".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["LLEN", "k"])).unwrap(),
            Command::LLen("k".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["LRANGE", "k", "0", "-1"]))
                .unwrap(),
            Command::LRange("k".to_string(), 0, -1)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["LINDEX", "k", "2"])).unwrap(),
            Command::LIndex("k".to_string(), 2)
        );
    }

    // ---------- Hash 命令测试 ----------

    #[test]
    fn test_execute_hset_hget() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        let resp = executor
            .execute(Command::HSet(
                "hash".to_string(),
                vec![("name".to_string(), Bytes::from("redis"))],
            ))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(1));

        let resp = executor
            .execute(Command::HSet(
                "hash".to_string(),
                vec![("name".to_string(), Bytes::from("redis2"))],
            ))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(0));

        let resp = executor
            .execute(Command::HGet("hash".to_string(), "name".to_string()))
            .unwrap();
        assert_eq!(
            resp,
            RespValue::BulkString(Some(Bytes::from("redis2")))
        );
    }

    #[test]
    fn test_execute_hdel_hexists_hlen() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::HSet(
                "hash".to_string(),
                vec![
                    ("a".to_string(), Bytes::from("1")),
                    ("b".to_string(), Bytes::from("2")),
                ],
            ))
            .unwrap();

        let resp = executor
            .execute(Command::HExists("hash".to_string(), "a".to_string()))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(1));

        let resp = executor
            .execute(Command::HLen("hash".to_string()))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(2));

        let resp = executor
            .execute(Command::HDel(
                "hash".to_string(),
                vec!["a".to_string()],
            ))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(1));

        let resp = executor
            .execute(Command::HExists("hash".to_string(), "a".to_string()))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(0));
    }

    #[test]
    fn test_execute_hgetall() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::HSet(
                "hash".to_string(),
                vec![
                    ("a".to_string(), Bytes::from("1")),
                ],
            ))
            .unwrap();

        let resp = executor
            .execute(Command::HGetAll("hash".to_string()))
            .unwrap();
        // HGETALL 返回 [field, value, field, value, ...]
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from("a"))));
                assert_eq!(arr[1], RespValue::BulkString(Some(Bytes::from("1"))));
            }
            other => panic!("期望 Array，得到 {:?}", other),
        }
    }

    #[test]
    fn test_execute_hmset_hmget() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        let resp = executor
            .execute(Command::HMSet(
                "hash".to_string(),
                vec![
                    ("a".to_string(), Bytes::from("1")),
                    ("b".to_string(), Bytes::from("2")),
                ],
            ))
            .unwrap();
        assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

        let resp = executor
            .execute(Command::HMGet(
                "hash".to_string(),
                vec!["a".to_string(), "missing".to_string(), "b".to_string()],
            ))
            .unwrap();
        assert_eq!(
            resp,
            RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from("1"))),
                RespValue::BulkString(None),
                RespValue::BulkString(Some(Bytes::from("2"))),
            ])
        );
    }

    #[test]
    fn test_execute_hash_wrongtype() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::Set("s".to_string(), Bytes::from("hello"), crate::storage::SetOptions::default()))
            .unwrap();

        let result = executor.execute(Command::HSet(
            "s".to_string(),
            vec![("field".to_string(), Bytes::from("val"))],
        ));
        assert!(result.is_err());

        let result = executor.execute(Command::HGet(
            "s".to_string(),
            "field".to_string(),
        ));
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_hash_commands() {
        let parser = CommandParser::new();

        assert_eq!(
            parser.parse(make_bulk_array(&["HSET", "k", "f", "v"])).unwrap(),
            Command::HSet(
                "k".to_string(),
                vec![("f".to_string(), Bytes::from("v"))]
            )
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["HGET", "k", "f"])).unwrap(),
            Command::HGet("k".to_string(), "f".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["HDEL", "k", "f1", "f2"])).unwrap(),
            Command::HDel("k".to_string(), vec!["f1".to_string(), "f2".to_string()])
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["HEXISTS", "k", "f"])).unwrap(),
            Command::HExists("k".to_string(), "f".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["HGETALL", "k"])).unwrap(),
            Command::HGetAll("k".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["HLEN", "k"])).unwrap(),
            Command::HLen("k".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["HMSET", "k", "f", "v"])).unwrap(),
            Command::HMSet(
                "k".to_string(),
                vec![("f".to_string(), Bytes::from("v"))]
            )
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["HMGET", "k", "f1", "f2"])).unwrap(),
            Command::HMGet(
                "k".to_string(),
                vec!["f1".to_string(), "f2".to_string()]
            )
        );
    }

    // ---------- Set 命令测试 ----------

    #[test]
    fn test_execute_sadd_srem_smembers() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        let resp = executor
            .execute(Command::SAdd(
                "set".to_string(),
                vec![Bytes::from("a"), Bytes::from("b")],
            ))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(2));

        let resp = executor
            .execute(Command::SAdd(
                "set".to_string(),
                vec![Bytes::from("a"), Bytes::from("c")],
            ))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(1));

        let resp = executor
            .execute(Command::SIsMember(
                "set".to_string(),
                Bytes::from("a"),
            ))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(1));

        let resp = executor
            .execute(Command::SCard("set".to_string()))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(3));

        let resp = executor
            .execute(Command::SRem(
                "set".to_string(),
                vec![Bytes::from("a")],
            ))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(1));

        let resp = executor
            .execute(Command::SMembers("set".to_string()))
            .unwrap();
        match resp {
            RespValue::Array(arr) => assert_eq!(arr.len(), 2),
            other => panic!("期望 Array，得到 {:?}", other),
        }
    }

    #[test]
    fn test_execute_set_ops() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::SAdd(
                "a".to_string(),
                vec![
                    Bytes::from("1"),
                    Bytes::from("2"),
                    Bytes::from("3"),
                ],
            ))
            .unwrap();
        executor
            .execute(Command::SAdd(
                "b".to_string(),
                vec![
                    Bytes::from("2"),
                    Bytes::from("3"),
                    Bytes::from("4"),
                ],
            ))
            .unwrap();

        let resp = executor
            .execute(Command::SInter(vec![
                "a".to_string(),
                "b".to_string(),
            ]))
            .unwrap();
        match resp {
            RespValue::Array(arr) => assert_eq!(arr.len(), 2),
            other => panic!("期望 Array，得到 {:?}", other),
        }

        let resp = executor
            .execute(Command::SUnion(vec![
                "a".to_string(),
                "b".to_string(),
            ]))
            .unwrap();
        match resp {
            RespValue::Array(arr) => assert_eq!(arr.len(), 4),
            other => panic!("期望 Array，得到 {:?}", other),
        }

        let resp = executor
            .execute(Command::SDiff(vec![
                "a".to_string(),
                "b".to_string(),
            ]))
            .unwrap();
        match resp {
            RespValue::Array(arr) => assert_eq!(arr.len(), 1),
            other => panic!("期望 Array，得到 {:?}", other),
        }
    }

    #[test]
    fn test_execute_set_wrongtype() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::Set("s".to_string(), Bytes::from("hello"), crate::storage::SetOptions::default()))
            .unwrap();

        let result = executor.execute(Command::SAdd(
            "s".to_string(),
            vec![Bytes::from("x")],
        ));
        assert!(result.is_err());

        let result = executor.execute(Command::SMembers("s".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_set_commands() {
        let parser = CommandParser::new();

        assert_eq!(
            parser.parse(make_bulk_array(&["SADD", "k", "m1", "m2"])).unwrap(),
            Command::SAdd("k".to_string(), vec![Bytes::from("m1"), Bytes::from("m2")])
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["SREM", "k", "m"])).unwrap(),
            Command::SRem("k".to_string(), vec![Bytes::from("m")])
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["SMEMBERS", "k"])).unwrap(),
            Command::SMembers("k".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["SISMEMBER", "k", "m"])).unwrap(),
            Command::SIsMember("k".to_string(), Bytes::from("m"))
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["SCARD", "k"])).unwrap(),
            Command::SCard("k".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["SINTER", "a", "b"])).unwrap(),
            Command::SInter(vec!["a".to_string(), "b".to_string()])
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["SUNION", "a", "b"])).unwrap(),
            Command::SUnion(vec!["a".to_string(), "b".to_string()])
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["SDIFF", "a", "b"])).unwrap(),
            Command::SDiff(vec!["a".to_string(), "b".to_string()])
        );
    }

    // ---------- ZSet 命令测试 ----------

    #[test]
    fn test_execute_zadd_zscore_zrank() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        let resp = executor
            .execute(Command::ZAdd(
                "zset".to_string(),
                vec![
                    (10.0, "a".to_string()),
                    (20.0, "b".to_string()),
                    (30.0, "c".to_string()),
                ],
            ))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(3));

        let resp = executor
            .execute(Command::ZScore("zset".to_string(), "b".to_string()))
            .unwrap();
        assert_eq!(
            resp,
            RespValue::BulkString(Some(Bytes::from("20")))
        );

        let resp = executor
            .execute(Command::ZRank("zset".to_string(), "b".to_string()))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(1));

        let resp = executor
            .execute(Command::ZCard("zset".to_string()))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(3));
    }

    #[test]
    fn test_execute_zrange() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::ZAdd(
                "zset".to_string(),
                vec![
                    (30.0, "c".to_string()),
                    (10.0, "a".to_string()),
                    (20.0, "b".to_string()),
                ],
            ))
            .unwrap();

        let resp = executor
            .execute(Command::ZRange("zset".to_string(), 0, 1, false))
            .unwrap();
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from("a"))));
                assert_eq!(arr[1], RespValue::BulkString(Some(Bytes::from("b"))));
            }
            other => panic!("期望 Array，得到 {:?}", other),
        }
    }

    #[test]
    fn test_execute_zrangebyscore() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::ZAdd(
                "zset".to_string(),
                vec![
                    (10.0, "a".to_string()),
                    (20.0, "b".to_string()),
                    (30.0, "c".to_string()),
                ],
            ))
            .unwrap();

        let resp = executor
            .execute(Command::ZRangeByScore("zset".to_string(), 15.0, 25.0, false))
            .unwrap();
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 1);
                assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from("b"))));
            }
            other => panic!("期望 Array，得到 {:?}", other),
        }
    }

    #[test]
    fn test_execute_zrem() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::ZAdd(
                "zset".to_string(),
                vec![
                    (1.0, "a".to_string()),
                    (2.0, "b".to_string()),
                ],
            ))
            .unwrap();

        let resp = executor
            .execute(Command::ZRem(
                "zset".to_string(),
                vec!["a".to_string(), "missing".to_string()],
            ))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(1));

        let resp = executor
            .execute(Command::ZCard("zset".to_string()))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(1));
    }

    #[test]
    fn test_execute_zset_wrongtype() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::Set("s".to_string(), Bytes::from("hello"), crate::storage::SetOptions::default()))
            .unwrap();

        let result = executor.execute(Command::ZAdd(
            "s".to_string(),
            vec![(1.0, "x".to_string())],
        ));
        assert!(result.is_err());

        let result = executor.execute(Command::ZScore("s".to_string(), "x".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_zset_commands() {
        let parser = CommandParser::new();

        assert_eq!(
            parser.parse(make_bulk_array(&["ZADD", "k", "1", "a", "2", "b"])).unwrap(),
            Command::ZAdd(
                "k".to_string(),
                vec![(1.0, "a".to_string()), (2.0, "b".to_string())]
            )
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["ZREM", "k", "a", "b"])).unwrap(),
            Command::ZRem("k".to_string(), vec!["a".to_string(), "b".to_string()])
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["ZSCORE", "k", "a"])).unwrap(),
            Command::ZScore("k".to_string(), "a".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["ZRANK", "k", "a"])).unwrap(),
            Command::ZRank("k".to_string(), "a".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["ZRANGE", "k", "0", "-1"])).unwrap(),
            Command::ZRange("k".to_string(), 0, -1, false)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["ZRANGE", "k", "0", "-1", "WITHSCORES"])).unwrap(),
            Command::ZRange("k".to_string(), 0, -1, true)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["ZRANGEBYSCORE", "k", "0", "100"])).unwrap(),
            Command::ZRangeByScore("k".to_string(), 0.0, 100.0, false)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["ZCARD", "k"])).unwrap(),
            Command::ZCard("k".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["ZREVRANGE", "k", "0", "-1"])).unwrap(),
            Command::ZRevRange("k".to_string(), 0, -1, false)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["ZREVRANK", "k", "a"])).unwrap(),
            Command::ZRevRank("k".to_string(), "a".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["ZINCRBY", "k", "1.5", "a"])).unwrap(),
            Command::ZIncrBy("k".to_string(), 1.5, "a".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["ZCOUNT", "k", "0", "100"])).unwrap(),
            Command::ZCount("k".to_string(), 0.0, 100.0)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["ZPOPMIN", "k"])).unwrap(),
            Command::ZPopMin("k".to_string(), 1)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["ZPOPMAX", "k", "2"])).unwrap(),
            Command::ZPopMax("k".to_string(), 2)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["ZUNIONSTORE", "d", "2", "a", "b", "WEIGHTS", "1", "2", "AGGREGATE", "SUM"])).unwrap(),
            Command::ZUnionStore("d".to_string(), vec!["a".to_string(), "b".to_string()], vec![1.0, 2.0], "SUM".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["ZINTERSTORE", "d", "1", "a"])).unwrap(),
            Command::ZInterStore("d".to_string(), vec!["a".to_string()], vec![], "SUM".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["ZSCAN", "k", "0", "MATCH", "a*", "COUNT", "5"])).unwrap(),
            Command::ZScan("k".to_string(), 0, "a*".to_string(), 5)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["ZRANGEBYLEX", "k", "[a", "[z"])).unwrap(),
            Command::ZRangeByLex("k".to_string(), "[a".to_string(), "[z".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["SETBIT", "k", "7", "1"])).unwrap(),
            Command::SetBit("k".to_string(), 7, true)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["GETBIT", "k", "7"])).unwrap(),
            Command::GetBit("k".to_string(), 7)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["BITCOUNT", "k", "0", "-1", "BYTE"])).unwrap(),
            Command::BitCount("k".to_string(), 0, -1, true)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["BITOP", "AND", "dest", "a", "b"])).unwrap(),
            Command::BitOp("AND".to_string(), "dest".to_string(), vec!["a".to_string(), "b".to_string()])
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["BITPOS", "k", "1", "0", "-1", "BYTE"])).unwrap(),
            Command::BitPos("k".to_string(), 1, 0, -1, true)
        );
    }

    #[test]
    fn test_execute_setbit_getbit() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        let resp = executor.execute(Command::SetBit("k".to_string(), 0, true)).unwrap();
        assert_eq!(resp, RespValue::Integer(0));

        let resp = executor.execute(Command::SetBit("k".to_string(), 0, false)).unwrap();
        assert_eq!(resp, RespValue::Integer(1));

        let resp = executor.execute(Command::GetBit("k".to_string(), 0)).unwrap();
        assert_eq!(resp, RespValue::Integer(0));

        let resp = executor.execute(Command::GetBit("k".to_string(), 100)).unwrap();
        assert_eq!(resp, RespValue::Integer(0));
    }

    #[test]
    fn test_execute_bitcount() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor.execute(Command::Set("k".to_string(), Bytes::from(vec![0b10101010, 0b11110000]), crate::storage::SetOptions::default())).unwrap();

        let resp = executor.execute(Command::BitCount("k".to_string(), 0, -1, true)).unwrap();
        assert_eq!(resp, RespValue::Integer(8));

        let resp = executor.execute(Command::BitCount("k".to_string(), 0, 0, true)).unwrap();
        assert_eq!(resp, RespValue::Integer(4));
    }

    #[test]
    fn test_execute_bitop() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor.execute(Command::Set("a".to_string(), Bytes::from(vec![0b11110000]), crate::storage::SetOptions::default())).unwrap();
        executor.execute(Command::Set("b".to_string(), Bytes::from(vec![0b10101010]), crate::storage::SetOptions::default())).unwrap();

        let resp = executor.execute(Command::BitOp("AND".to_string(), "dest".to_string(), vec!["a".to_string(), "b".to_string()])).unwrap();
        assert_eq!(resp, RespValue::Integer(1));

        let resp = executor.execute(Command::Get("dest".to_string())).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from(vec![0b10100000]))));
    }

    #[test]
    fn test_execute_bitpos() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor.execute(Command::Set("k".to_string(), Bytes::from(vec![0b10101010]), crate::storage::SetOptions::default())).unwrap();

        let resp = executor.execute(Command::BitPos("k".to_string(), 1, 0, -1, true)).unwrap();
        assert_eq!(resp, RespValue::Integer(0));

        let resp = executor.execute(Command::BitPos("k".to_string(), 0, 0, -1, true)).unwrap();
        assert_eq!(resp, RespValue::Integer(1));
    }

    #[test]
    fn test_execute_geoadd_geodist() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        let resp = executor.execute(Command::GeoAdd(
            "cities".to_string(),
            vec![
                (116.4074, 39.9042, "北京".to_string()),
                (121.4737, 31.2304, "上海".to_string()),
            ],
        )).unwrap();
        assert_eq!(resp, RespValue::Integer(2));

        let resp = executor.execute(Command::GeoDist(
            "cities".to_string(),
            "北京".to_string(),
            "上海".to_string(),
            "km".to_string(),
        )).unwrap();
        match resp {
            RespValue::BulkString(Some(b)) => {
                let dist: f64 = String::from_utf8_lossy(&b).parse().unwrap();
                assert!((dist - 1067.0).abs() < 20.0);
            }
            other => panic!("期望 BulkString，得到 {:?}", other),
        }
    }

    #[test]
    fn test_execute_geohash_geopos() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor.execute(Command::GeoAdd(
            "cities".to_string(),
            vec![(116.4074, 39.9042, "北京".to_string())],
        )).unwrap();

        let resp = executor.execute(Command::GeoHash(
            "cities".to_string(),
            vec!["北京".to_string()],
        )).unwrap();
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 1);
            }
            other => panic!("期望 Array，得到 {:?}", other),
        }

        let resp = executor.execute(Command::GeoPos(
            "cities".to_string(),
            vec!["北京".to_string()],
        )).unwrap();
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 1);
            }
            other => panic!("期望 Array，得到 {:?}", other),
        }
    }

    #[test]
    fn test_parse_geo_commands() {
        let parser = CommandParser::new();

        assert_eq!(
            parser.parse(make_bulk_array(&["GEOADD", "k", "116.4", "39.9", "北京"])).unwrap(),
            Command::GeoAdd("k".to_string(), vec![(116.4, 39.9, "北京".to_string())])
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["GEODIST", "k", "a", "b", "km"])).unwrap(),
            Command::GeoDist("k".to_string(), "a".to_string(), "b".to_string(), "km".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["GEOHASH", "k", "a"])).unwrap(),
            Command::GeoHash("k".to_string(), vec!["a".to_string()])
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["GEOPOS", "k", "a"])).unwrap(),
            Command::GeoPos("k".to_string(), vec!["a".to_string()])
        );
    }

    #[test]
    fn test_execute_pfadd_pfcount() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        let resp = executor.execute(Command::PfAdd(
            "hll".to_string(),
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        )).unwrap();
        assert_eq!(resp, RespValue::Integer(1));

        let resp = executor.execute(Command::PfCount(vec!["hll".to_string()])).unwrap();
        match resp {
            RespValue::Integer(n) => assert!(n >= 3),
            other => panic!("期望 Integer，得到 {:?}", other),
        }

        // 重复元素
        let resp = executor.execute(Command::PfAdd(
            "hll".to_string(),
            vec!["a".to_string(), "b".to_string()],
        )).unwrap();
        assert_eq!(resp, RespValue::Integer(0));
    }

    #[test]
    fn test_execute_pfmerge() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor.execute(Command::PfAdd(
            "hll1".to_string(),
            vec!["a".to_string(), "b".to_string()],
        )).unwrap();
        executor.execute(Command::PfAdd(
            "hll2".to_string(),
            vec!["b".to_string(), "c".to_string()],
        )).unwrap();

        let resp = executor.execute(Command::PfMerge(
            "merged".to_string(),
            vec!["hll1".to_string(), "hll2".to_string()],
        )).unwrap();
        assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

        let resp = executor.execute(Command::PfCount(vec!["merged".to_string()])).unwrap();
        match resp {
            RespValue::Integer(n) => assert!(n >= 3),
            other => panic!("期望 Integer，得到 {:?}", other),
        }
    }

    #[test]
    fn test_parse_hll_commands() {
        let parser = CommandParser::new();

        assert_eq!(
            parser.parse(make_bulk_array(&["PFADD", "k", "a", "b"])).unwrap(),
            Command::PfAdd("k".to_string(), vec!["a".to_string(), "b".to_string()])
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["PFCOUNT", "k"])).unwrap(),
            Command::PfCount(vec!["k".to_string()])
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["PFCOUNT", "k1", "k2"])).unwrap(),
            Command::PfCount(vec!["k1".to_string(), "k2".to_string()])
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["PFMERGE", "dest", "k1", "k2"])).unwrap(),
            Command::PfMerge("dest".to_string(), vec!["k1".to_string(), "k2".to_string()])
        );
    }

    #[test]
    fn test_execute_zrevrange() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::ZAdd(
                "zset".to_string(),
                vec![
                    (10.0, "a".to_string()),
                    (20.0, "b".to_string()),
                    (30.0, "c".to_string()),
                ],
            ))
            .unwrap();

        let resp = executor
            .execute(Command::ZRevRange("zset".to_string(), 0, 1, false))
            .unwrap();
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from("c"))));
                assert_eq!(arr[1], RespValue::BulkString(Some(Bytes::from("b"))));
            }
            other => panic!("期望 Array，得到 {:?}", other),
        }
    }

    #[test]
    fn test_execute_zrevrank() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::ZAdd(
                "zset".to_string(),
                vec![
                    (10.0, "a".to_string()),
                    (20.0, "b".to_string()),
                    (30.0, "c".to_string()),
                ],
            ))
            .unwrap();

        let resp = executor
            .execute(Command::ZRevRank("zset".to_string(), "a".to_string()))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(2));

        let resp = executor
            .execute(Command::ZRevRank("zset".to_string(), "c".to_string()))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(0));
    }

    #[test]
    fn test_execute_zincrby() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::ZAdd(
                "zset".to_string(),
                vec![(10.0, "a".to_string())],
            ))
            .unwrap();

        let resp = executor
            .execute(Command::ZIncrBy("zset".to_string(), 5.5, "a".to_string()))
            .unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("15.5"))));

        let resp = executor
            .execute(Command::ZIncrBy("zset".to_string(), 2.0, "b".to_string()))
            .unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("2"))));
    }

    #[test]
    fn test_execute_zcount() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::ZAdd(
                "zset".to_string(),
                vec![
                    (10.0, "a".to_string()),
                    (20.0, "b".to_string()),
                    (30.0, "c".to_string()),
                ],
            ))
            .unwrap();

        let resp = executor
            .execute(Command::ZCount("zset".to_string(), 15.0, 25.0))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(1));
    }

    #[test]
    fn test_execute_zpopmin() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::ZAdd(
                "zset".to_string(),
                vec![
                    (30.0, "c".to_string()),
                    (10.0, "a".to_string()),
                    (20.0, "b".to_string()),
                ],
            ))
            .unwrap();

        let resp = executor
            .execute(Command::ZPopMin("zset".to_string(), 2))
            .unwrap();
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 4);
                assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from("a"))));
                assert_eq!(arr[1], RespValue::BulkString(Some(Bytes::from("10"))));
                assert_eq!(arr[2], RespValue::BulkString(Some(Bytes::from("b"))));
                assert_eq!(arr[3], RespValue::BulkString(Some(Bytes::from("20"))));
            }
            other => panic!("期望 Array，得到 {:?}", other),
        }

        let resp = executor
            .execute(Command::ZCard("zset".to_string()))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(1));
    }

    #[test]
    fn test_execute_zpopmax() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::ZAdd(
                "zset".to_string(),
                vec![
                    (30.0, "c".to_string()),
                    (10.0, "a".to_string()),
                    (20.0, "b".to_string()),
                ],
            ))
            .unwrap();

        let resp = executor
            .execute(Command::ZPopMax("zset".to_string(), 1))
            .unwrap();
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from("c"))));
                assert_eq!(arr[1], RespValue::BulkString(Some(Bytes::from("30"))));
            }
            other => panic!("期望 Array，得到 {:?}", other),
        }
    }

    #[test]
    fn test_execute_zunionstore() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::ZAdd(
                "z1".to_string(),
                vec![
                    (1.0, "a".to_string()),
                    (2.0, "b".to_string()),
                ],
            ))
            .unwrap();
        executor
            .execute(Command::ZAdd(
                "z2".to_string(),
                vec![
                    (2.0, "b".to_string()),
                    (3.0, "c".to_string()),
                ],
            ))
            .unwrap();

        let resp = executor
            .execute(Command::ZUnionStore(
                "z3".to_string(),
                vec!["z1".to_string(), "z2".to_string()],
                vec![],
                "SUM".to_string(),
            ))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(3));

        let resp = executor
            .execute(Command::ZRange("z3".to_string(), 0, -1, true))
            .unwrap();
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 6);
                assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from("a"))));
                assert_eq!(arr[1], RespValue::BulkString(Some(Bytes::from("1"))));
                assert_eq!(arr[2], RespValue::BulkString(Some(Bytes::from("c"))));
                assert_eq!(arr[3], RespValue::BulkString(Some(Bytes::from("3"))));
                assert_eq!(arr[4], RespValue::BulkString(Some(Bytes::from("b"))));
                assert_eq!(arr[5], RespValue::BulkString(Some(Bytes::from("4"))));
            }
            other => panic!("期望 Array，得到 {:?}", other),
        }
    }

    #[test]
    fn test_execute_zinterstore() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::ZAdd(
                "z1".to_string(),
                vec![
                    (1.0, "a".to_string()),
                    (2.0, "b".to_string()),
                ],
            ))
            .unwrap();
        executor
            .execute(Command::ZAdd(
                "z2".to_string(),
                vec![
                    (2.0, "b".to_string()),
                    (3.0, "c".to_string()),
                ],
            ))
            .unwrap();

        let resp = executor
            .execute(Command::ZInterStore(
                "z3".to_string(),
                vec!["z1".to_string(), "z2".to_string()],
                vec![],
                "SUM".to_string(),
            ))
            .unwrap();
        assert_eq!(resp, RespValue::Integer(1));

        let resp = executor
            .execute(Command::ZScore("z3".to_string(), "b".to_string()))
            .unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("4"))));
    }

    #[test]
    fn test_execute_zscan() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::ZAdd(
                "zset".to_string(),
                vec![
                    (1.0, "alpha".to_string()),
                    (2.0, "beta".to_string()),
                ],
            ))
            .unwrap();

        let resp = executor
            .execute(Command::ZScan("zset".to_string(), 0, "a*".to_string(), 10))
            .unwrap();
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2);
                match &arr[1] {
                    RespValue::Array(items) => {
                        assert_eq!(items.len(), 2);
                    }
                    _ => panic!("期望 ZSCAN 返回成员数组"),
                }
            }
            _ => panic!("期望 ZSCAN 返回数组"),
        }
    }

    #[test]
    fn test_execute_zrangebylex() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        executor
            .execute(Command::ZAdd(
                "zset".to_string(),
                vec![
                    (0.0, "a".to_string()),
                    (0.0, "b".to_string()),
                    (0.0, "c".to_string()),
                    (0.0, "d".to_string()),
                ],
            ))
            .unwrap();

        let resp = executor
            .execute(Command::ZRangeByLex("zset".to_string(), "[b".to_string(), "[c".to_string()))
            .unwrap();
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from("b"))));
                assert_eq!(arr[1], RespValue::BulkString(Some(Bytes::from("c"))));
            }
            other => panic!("期望 Array，得到 {:?}", other),
        }
    }

    #[test]
    fn test_execute_keys_scan_rename_type_persist_pexpire_pttl_dbsize_info() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        // 设置一些键
        executor.execute(Command::Set("hello".to_string(), Bytes::from("v1"), crate::storage::SetOptions::default())).unwrap();
        executor.execute(Command::Set("hallo".to_string(), Bytes::from("v2"), crate::storage::SetOptions::default())).unwrap();
        executor.execute(Command::Set("world".to_string(), Bytes::from("v3"), crate::storage::SetOptions::default())).unwrap();

        // KEYS
        let resp = executor.execute(Command::Keys("h*lo".to_string())).unwrap();
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("Expected array"),
        }

        // SCAN
        let resp = executor.execute(Command::Scan(0, "*".to_string(), 10)).unwrap();
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2); // cursor + keys array
            }
            _ => panic!("Expected array"),
        }

        // RENAME
        let resp = executor.execute(Command::Rename("hello".to_string(), "hello2".to_string())).unwrap();
        assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

        // TYPE
        let resp = executor.execute(Command::Type("hello2".to_string())).unwrap();
        assert_eq!(resp, RespValue::SimpleString("string".to_string()));
        let resp = executor.execute(Command::Type("missing".to_string())).unwrap();
        assert_eq!(resp, RespValue::SimpleString("none".to_string()));

        // PERSIST / PEXPIRE / PTTL
        executor.execute(Command::SetEx("temp".to_string(), Bytes::from("v"), 10000)).unwrap();
        let resp = executor.execute(Command::PTtl("temp".to_string())).unwrap();
        assert!(matches!(resp, RespValue::Integer(n) if n > 0 && n <= 10000));

        let resp = executor.execute(Command::Persist("temp".to_string())).unwrap();
        assert_eq!(resp, RespValue::Integer(1));

        let resp = executor.execute(Command::PTtl("temp".to_string())).unwrap();
        assert_eq!(resp, RespValue::Integer(-1));

        let resp = executor.execute(Command::PExpire("temp".to_string(), 5000)).unwrap();
        assert_eq!(resp, RespValue::Integer(1));

        // DBSIZE
        let resp = executor.execute(Command::DbSize).unwrap();
        assert!(matches!(resp, RespValue::Integer(n) if n >= 4));

        // INFO
        let resp = executor.execute(Command::Info(None)).unwrap();
        match resp {
            RespValue::BulkString(Some(s)) => {
                assert!(std::str::from_utf8(&s).unwrap().contains("redis_version"));
            }
            _ => panic!("Expected bulk string"),
        }
    }

    #[test]
    fn test_parse_key_management_commands() {
        let parser = CommandParser::new();

        assert_eq!(
            parser.parse(make_bulk_array(&["KEYS", "*"])).unwrap(),
            Command::Keys("*".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["SCAN", "0"])).unwrap(),
            Command::Scan(0, String::new(), 0)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["SCAN", "0", "MATCH", "a*", "COUNT", "5"])).unwrap(),
            Command::Scan(0, "a*".to_string(), 5)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["RENAME", "a", "b"])).unwrap(),
            Command::Rename("a".to_string(), "b".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["TYPE", "k"])).unwrap(),
            Command::Type("k".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["PERSIST", "k"])).unwrap(),
            Command::Persist("k".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["PEXPIRE", "k", "1000"])).unwrap(),
            Command::PExpire("k".to_string(), 1000)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["PTTL", "k"])).unwrap(),
            Command::PTtl("k".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["DBSIZE"])).unwrap(),
            Command::DbSize
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["INFO"])).unwrap(),
            Command::Info(None)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["INFO", "server"])).unwrap(),
            Command::Info(Some("server".to_string()))
        );
    }

    #[test]
    fn test_parse_pubsub_commands() {
        let parser = CommandParser::new();

        assert_eq!(
            parser.parse(make_bulk_array(&["SUBSCRIBE", "ch1", "ch2"])).unwrap(),
            Command::Subscribe(vec!["ch1".to_string(), "ch2".to_string()])
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["UNSUBSCRIBE"])).unwrap(),
            Command::Unsubscribe(vec![])
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["UNSUBSCRIBE", "ch1"])).unwrap(),
            Command::Unsubscribe(vec!["ch1".to_string()])
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["PUBLISH", "ch1", "hello"])).unwrap(),
            Command::Publish("ch1".to_string(), Bytes::from("hello"))
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["PSUBSCRIBE", "news.*"])).unwrap(),
            Command::PSubscribe(vec!["news.*".to_string()])
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["PUNSUBSCRIBE"])).unwrap(),
            Command::PUnsubscribe(vec![])
        );
    }

    #[test]
    fn test_parse_transaction_commands() {
        let parser = CommandParser::new();

        assert_eq!(
            parser.parse(make_bulk_array(&["MULTI"])).unwrap(),
            Command::Multi
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["EXEC"])).unwrap(),
            Command::Exec
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["DISCARD"])).unwrap(),
            Command::Discard
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["WATCH", "a", "b"])).unwrap(),
            Command::Watch(vec!["a".to_string(), "b".to_string()])
        );
    }

    #[test]
    fn test_parse_config_maxmemory() {
        let parser = CommandParser::new();

        assert_eq!(
            parser.parse(make_bulk_array(&["CONFIG", "GET", "maxmemory"])).unwrap(),
            Command::ConfigGet("maxmemory".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["CONFIG", "SET", "maxmemory", "1024"])).unwrap(),
            Command::ConfigSet("maxmemory".to_string(), "1024".to_string())
        );
    }

    #[test]
    fn test_parse_string_tail_commands() {
        let parser = CommandParser::new();

        // SETEX
        assert_eq!(
            parser.parse(make_bulk_array(&["SETEX", "k", "10", "v"])).unwrap(),
            Command::SetExCmd("k".to_string(), Bytes::from("v"), 10)
        );
        // PSETEX
        assert_eq!(
            parser.parse(make_bulk_array(&["PSETEX", "k", "100", "v"])).unwrap(),
            Command::PSetEx("k".to_string(), Bytes::from("v"), 100)
        );
        // GETSET
        assert_eq!(
            parser.parse(make_bulk_array(&["GETSET", "k", "v"])).unwrap(),
            Command::GetSet("k".to_string(), Bytes::from("v"))
        );
        // GETDEL
        assert_eq!(
            parser.parse(make_bulk_array(&["GETDEL", "k"])).unwrap(),
            Command::GetDel("k".to_string())
        );
        // GETEX default (no option)
        assert_eq!(
            parser.parse(make_bulk_array(&["GETEX", "k"])).unwrap(),
            Command::GetEx("k".to_string(), GetExOption::Persist)
        );
        // GETEX EX
        assert_eq!(
            parser.parse(make_bulk_array(&["GETEX", "k", "EX", "10"])).unwrap(),
            Command::GetEx("k".to_string(), GetExOption::Ex(10))
        );
        // GETEX PX
        assert_eq!(
            parser.parse(make_bulk_array(&["GETEX", "k", "PX", "100"])).unwrap(),
            Command::GetEx("k".to_string(), GetExOption::Px(100))
        );
        // GETEX PERSIST
        assert_eq!(
            parser.parse(make_bulk_array(&["GETEX", "k", "PERSIST"])).unwrap(),
            Command::GetEx("k".to_string(), GetExOption::Persist)
        );
        // MSETNX
        assert_eq!(
            parser.parse(make_bulk_array(&["MSETNX", "a", "1", "b", "2"])).unwrap(),
            Command::MSetNx(vec![
                ("a".to_string(), Bytes::from("1")),
                ("b".to_string(), Bytes::from("2")),
            ])
        );
        // INCRBYFLOAT
        assert_eq!(
            parser.parse(make_bulk_array(&["INCRBYFLOAT", "k", "0.5"])).unwrap(),
            Command::IncrByFloat("k".to_string(), 0.5)
        );
        // SETRANGE
        assert_eq!(
            parser.parse(make_bulk_array(&["SETRANGE", "k", "6", "Redis"])).unwrap(),
            Command::SetRange("k".to_string(), 6, Bytes::from("Redis"))
        );
    }

    #[test]
    fn test_execute_string_tail_commands() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        // SETEX + GET
        executor.execute(Command::SetExCmd("k1".to_string(), Bytes::from("v1"), 3600)).unwrap();
        let resp = executor.execute(Command::Get("k1".to_string())).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("v1"))));

        // PSETEX + GET
        executor.execute(Command::PSetEx("k2".to_string(), Bytes::from("v2"), 3600000)).unwrap();
        let resp = executor.execute(Command::Get("k2".to_string())).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("v2"))));

        // GETSET
        executor.execute(Command::Set("k3".to_string(), Bytes::from("old"), crate::storage::SetOptions::default())).unwrap();
        let resp = executor.execute(Command::GetSet("k3".to_string(), Bytes::from("new"))).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("old"))));
        let resp = executor.execute(Command::Get("k3".to_string())).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("new"))));

        // GETDEL
        executor.execute(Command::Set("k4".to_string(), Bytes::from("v4"), crate::storage::SetOptions::default())).unwrap();
        let resp = executor.execute(Command::GetDel("k4".to_string())).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("v4"))));
        let resp = executor.execute(Command::Get("k4".to_string())).unwrap();
        assert_eq!(resp, RespValue::BulkString(None));

        // GETEX PERSIST
        executor.execute(Command::SetExCmd("k5".to_string(), Bytes::from("v5"), 3600)).unwrap();
        let resp = executor.execute(Command::GetEx("k5".to_string(), GetExOption::Persist)).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("v5"))));
        let resp = executor.execute(Command::Ttl("k5".to_string())).unwrap();
        assert_eq!(resp, RespValue::Integer(-1));

        // MSETNX
        let resp = executor.execute(Command::MSetNx(vec![
            ("m1".to_string(), Bytes::from("1")),
            ("m2".to_string(), Bytes::from("2")),
        ])).unwrap();
        assert_eq!(resp, RespValue::Integer(1));
        let resp = executor.execute(Command::MSetNx(vec![
            ("m1".to_string(), Bytes::from("x")),
            ("m3".to_string(), Bytes::from("3")),
        ])).unwrap();
        assert_eq!(resp, RespValue::Integer(0));

        // INCRBYFLOAT
        let resp = executor.execute(Command::IncrByFloat("f".to_string(), 0.5)).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("0.5"))));
        let resp = executor.execute(Command::IncrByFloat("f".to_string(), 0.25)).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("0.75"))));

        // SETRANGE
        executor.execute(Command::Set("s".to_string(), Bytes::from("Hello World"), crate::storage::SetOptions::default())).unwrap();
        let resp = executor.execute(Command::SetRange("s".to_string(), 6, Bytes::from("Redis"))).unwrap();
        assert_eq!(resp, RespValue::Integer(11));
        let resp = executor.execute(Command::Get("s".to_string())).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("Hello Redis"))));
    }

    #[test]
    fn test_parse_list_tail_commands() {
        let parser = CommandParser::new();

        assert_eq!(
            parser.parse(make_bulk_array(&["LSET", "k", "1", "v"])).unwrap(),
            Command::LSet("k".to_string(), 1, Bytes::from("v"))
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["LINSERT", "k", "BEFORE", "p", "v"])).unwrap(),
            Command::LInsert("k".to_string(), crate::storage::LInsertPosition::Before, Bytes::from("p"), Bytes::from("v"))
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["LINSERT", "k", "AFTER", "p", "v"])).unwrap(),
            Command::LInsert("k".to_string(), crate::storage::LInsertPosition::After, Bytes::from("p"), Bytes::from("v"))
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["LREM", "k", "2", "v"])).unwrap(),
            Command::LRem("k".to_string(), 2, Bytes::from("v"))
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["LTRIM", "k", "1", "3"])).unwrap(),
            Command::LTrim("k".to_string(), 1, 3)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["LPOS", "k", "v"])).unwrap(),
            Command::LPos("k".to_string(), Bytes::from("v"), 1, 0, 0)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["LPOS", "k", "v", "RANK", "2", "COUNT", "3", "MAXLEN", "100"])).unwrap(),
            Command::LPos("k".to_string(), Bytes::from("v"), 2, 3, 100)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["BLPOP", "k1", "k2", "5"])).unwrap(),
            Command::BLPop(vec!["k1".to_string(), "k2".to_string()], 5.0)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["BRPOP", "k1", "k2", "0"])).unwrap(),
            Command::BRPop(vec!["k1".to_string(), "k2".to_string()], 0.0)
        );
    }

    #[test]
    fn test_execute_list_tail_commands() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        // LSET
        executor.execute(Command::RPush("list".to_string(), vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")])).unwrap();
        let resp = executor.execute(Command::LSet("list".to_string(), 1, Bytes::from("x"))).unwrap();
        assert_eq!(resp, RespValue::SimpleString("OK".to_string()));
        let resp = executor.execute(Command::LIndex("list".to_string(), 1)).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("x"))));

        // LINSERT
        let resp = executor.execute(Command::LInsert("list".to_string(), crate::storage::LInsertPosition::Before, Bytes::from("x"), Bytes::from("y"))).unwrap();
        assert_eq!(resp, RespValue::Integer(4));

        // LREM
        executor.execute(Command::RPush("list2".to_string(), vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("a")])).unwrap();
        let resp = executor.execute(Command::LRem("list2".to_string(), 1, Bytes::from("a"))).unwrap();
        assert_eq!(resp, RespValue::Integer(1));

        // LTRIM
        executor.execute(Command::RPush("list3".to_string(), vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c"), Bytes::from("d")])).unwrap();
        let resp = executor.execute(Command::LTrim("list3".to_string(), 1, 2)).unwrap();
        assert_eq!(resp, RespValue::SimpleString("OK".to_string()));
        let resp = executor.execute(Command::LLen("list3".to_string())).unwrap();
        assert_eq!(resp, RespValue::Integer(2));

        // LPOS (without COUNT)
        executor.execute(Command::RPush("list4".to_string(), vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("a")])).unwrap();
        let resp = executor.execute(Command::LPos("list4".to_string(), Bytes::from("a"), 1, 0, 0)).unwrap();
        assert_eq!(resp, RespValue::Integer(0));

        // LPOS (with COUNT)
        let resp = executor.execute(Command::LPos("list4".to_string(), Bytes::from("a"), 1, 2, 0)).unwrap();
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], RespValue::Integer(0));
                assert_eq!(arr[1], RespValue::Integer(2));
            }
            _ => panic!("期望 LPOS COUNT 返回数组"),
        }

        // BLPOP (non-blocking, list empty)
        let resp = executor.execute(Command::BLPop(vec!["empty".to_string()], 1.0)).unwrap();
        assert_eq!(resp, RespValue::BulkString(None));

        // BLPOP (non-blocking, list has data)
        executor.execute(Command::LPush("blist".to_string(), vec![Bytes::from("a"), Bytes::from("b")])).unwrap();
        let resp = executor.execute(Command::BLPop(vec!["blist".to_string()], 1.0)).unwrap();
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from("blist"))));
                assert_eq!(arr[1], RespValue::BulkString(Some(Bytes::from("b"))));
            }
            _ => panic!("期望 BLPOP 返回数组"),
        }
    }

    #[test]
    fn test_parse_hash_tail_commands() {
        let parser = CommandParser::new();

        assert_eq!(
            parser.parse(make_bulk_array(&["HINCRBY", "k", "f", "5"])).unwrap(),
            Command::HIncrBy("k".to_string(), "f".to_string(), 5)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["HINCRBYFLOAT", "k", "f", "0.5"])).unwrap(),
            Command::HIncrByFloat("k".to_string(), "f".to_string(), 0.5)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["HKEYS", "k"])).unwrap(),
            Command::HKeys("k".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["HVALS", "k"])).unwrap(),
            Command::HVals("k".to_string())
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["HSETNX", "k", "f", "v"])).unwrap(),
            Command::HSetNx("k".to_string(), "f".to_string(), Bytes::from("v"))
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["HRANDFIELD", "k"])).unwrap(),
            Command::HRandField("k".to_string(), 1, false)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["HRANDFIELD", "k", "2", "WITHVALUES"])).unwrap(),
            Command::HRandField("k".to_string(), 2, true)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["HSCAN", "k", "0", "MATCH", "f*", "COUNT", "10"])).unwrap(),
            Command::HScan("k".to_string(), 0, "f*".to_string(), 10)
        );
    }

    #[test]
    fn test_execute_hash_tail_commands() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        // HINCRBY
        let resp = executor.execute(Command::HIncrBy("h".to_string(), "f".to_string(), 5)).unwrap();
        assert_eq!(resp, RespValue::Integer(5));
        let resp = executor.execute(Command::HIncrBy("h".to_string(), "f".to_string(), 3)).unwrap();
        assert_eq!(resp, RespValue::Integer(8));

        // HINCRBYFLOAT
        let resp = executor.execute(Command::HIncrByFloat("h".to_string(), "g".to_string(), 0.5)).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("0.5"))));

        // HKEYS / HVALS
        executor.execute(Command::HSet("h2".to_string(), vec![("a".to_string(), Bytes::from("1")), ("b".to_string(), Bytes::from("2"))])).unwrap();
        let resp = executor.execute(Command::HKeys("h2".to_string())).unwrap();
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from("a"))));
                assert_eq!(arr[1], RespValue::BulkString(Some(Bytes::from("b"))));
            }
            _ => panic!("期望 HKEYS 返回数组"),
        }
        let resp = executor.execute(Command::HVals("h2".to_string())).unwrap();
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from("1"))));
                assert_eq!(arr[1], RespValue::BulkString(Some(Bytes::from("2"))));
            }
            _ => panic!("期望 HVALS 返回数组"),
        }

        // HSETNX
        let resp = executor.execute(Command::HSetNx("h3".to_string(), "f".to_string(), Bytes::from("v1"))).unwrap();
        assert_eq!(resp, RespValue::Integer(1));
        let resp = executor.execute(Command::HSetNx("h3".to_string(), "f".to_string(), Bytes::from("v2"))).unwrap();
        assert_eq!(resp, RespValue::Integer(0));

        // HSCAN
        executor.execute(Command::HSet("h4".to_string(), vec![
            ("a".to_string(), Bytes::from("1")),
            ("b".to_string(), Bytes::from("2")),
            ("c".to_string(), Bytes::from("3")),
        ])).unwrap();
        let resp = executor.execute(Command::HScan("h4".to_string(), 0, "*".to_string(), 2)).unwrap();
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2); // cursor + array
                match &arr[1] {
                    RespValue::Array(fields) => {
                        assert_eq!(fields.len(), 4); // 2 fields * 2 (field + value)
                    }
                    _ => panic!("期望 HSCAN 返回字段数组"),
                }
            }
            _ => panic!("期望 HSCAN 返回数组"),
        }
    }

    #[test]
    fn test_parse_set_tail_commands() {
        let parser = CommandParser::new();

        assert_eq!(
            parser.parse(make_bulk_array(&["SPOP", "k", "2"])).unwrap(),
            Command::SPop("k".to_string(), 2)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["SRANDMEMBER", "k", "3"])).unwrap(),
            Command::SRandMember("k".to_string(), 3)
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["SMOVE", "s1", "s2", "m"])).unwrap(),
            Command::SMove("s1".to_string(), "s2".to_string(), Bytes::from("m"))
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["SINTERSTORE", "dest", "k1", "k2"])).unwrap(),
            Command::SInterStore("dest".to_string(), vec!["k1".to_string(), "k2".to_string()])
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["SUNIONSTORE", "dest", "k1", "k2"])).unwrap(),
            Command::SUnionStore("dest".to_string(), vec!["k1".to_string(), "k2".to_string()])
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["SDIFFSTORE", "dest", "k1", "k2"])).unwrap(),
            Command::SDiffStore("dest".to_string(), vec!["k1".to_string(), "k2".to_string()])
        );
        assert_eq!(
            parser.parse(make_bulk_array(&["SSCAN", "k", "0", "MATCH", "f*", "COUNT", "10"])).unwrap(),
            Command::SScan("k".to_string(), 0, "f*".to_string(), 10)
        );
    }

    #[test]
    fn test_execute_set_tail_commands() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        // SPOP
        executor.execute(Command::SAdd("s1".to_string(), vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")])).unwrap();
        let resp = executor.execute(Command::SPop("s1".to_string(), 2)).unwrap();
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("期望 SPOP 返回数组"),
        }
        let resp = executor.execute(Command::SCard("s1".to_string())).unwrap();
        assert_eq!(resp, RespValue::Integer(1));

        // SRANDMEMBER
        executor.execute(Command::SAdd("s2".to_string(), vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")])).unwrap();
        let resp = executor.execute(Command::SRandMember("s2".to_string(), 2)).unwrap();
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("期望 SRANDMEMBER 返回数组"),
        }
        let resp = executor.execute(Command::SCard("s2".to_string())).unwrap();
        assert_eq!(resp, RespValue::Integer(3)); // 不删除

        // SMOVE
        executor.execute(Command::SAdd("s3".to_string(), vec![Bytes::from("a"), Bytes::from("b")])).unwrap();
        executor.execute(Command::SAdd("s4".to_string(), vec![Bytes::from("c")])).unwrap();
        let resp = executor.execute(Command::SMove("s3".to_string(), "s4".to_string(), Bytes::from("a"))).unwrap();
        assert_eq!(resp, RespValue::Integer(1));

        // SINTERSTORE
        executor.execute(Command::SAdd("s5".to_string(), vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")])).unwrap();
        executor.execute(Command::SAdd("s6".to_string(), vec![Bytes::from("b"), Bytes::from("c"), Bytes::from("d")])).unwrap();
        let resp = executor.execute(Command::SInterStore("dest".to_string(), vec!["s5".to_string(), "s6".to_string()])).unwrap();
        assert_eq!(resp, RespValue::Integer(2));

        // SUNIONSTORE
        let resp = executor.execute(Command::SUnionStore("dest2".to_string(), vec!["s5".to_string(), "s6".to_string()])).unwrap();
        assert_eq!(resp, RespValue::Integer(4));

        // SDIFFSTORE
        let resp = executor.execute(Command::SDiffStore("dest3".to_string(), vec!["s5".to_string(), "s6".to_string()])).unwrap();
        assert_eq!(resp, RespValue::Integer(1));

        // SSCAN
        executor.execute(Command::SAdd("s7".to_string(), vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")])).unwrap();
        let resp = executor.execute(Command::SScan("s7".to_string(), 0, "*".to_string(), 2)).unwrap();
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2); // cursor + array
                match &arr[1] {
                    RespValue::Array(members) => {
                        assert_eq!(members.len(), 2);
                    }
                    _ => panic!("期望 SSCAN 返回成员数组"),
                }
            }
            _ => panic!("期望 SSCAN 返回数组"),
        }
    }

    #[test]
    fn test_parse_select() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["SELECT", "2"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::Select(2));
    }

    #[test]
    fn test_parse_auth() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["AUTH", "mypassword"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::Auth("default".to_string(), "mypassword".to_string()));
    }

    #[test]
    fn test_parse_client_setname() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["CLIENT", "SETNAME", "my-client"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::ClientSetName("my-client".to_string()));
    }

    #[test]
    fn test_parse_client_getname() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["CLIENT", "GETNAME"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::ClientGetName);
    }

    #[test]
    fn test_parse_client_list() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["CLIENT", "LIST"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::ClientList);
    }

    #[test]
    fn test_parse_client_id() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["CLIENT", "ID"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::ClientId);
    }

    #[test]
    fn test_parse_quit() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["QUIT"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::Quit);
    }

    #[test]
    fn test_execute_select() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage.clone());

        // 默认在 db 0（通过 executor 的 storage 检查）
        assert_eq!(executor.storage().current_db(), 0);

        let resp = executor.execute(Command::Select(2)).unwrap();
        assert_eq!(resp, RespValue::SimpleString("OK".to_string()));
        assert_eq!(executor.storage().current_db(), 2);

        // 切换回 db 0
        let resp = executor.execute(Command::Select(0)).unwrap();
        assert_eq!(resp, RespValue::SimpleString("OK".to_string()));
        assert_eq!(executor.storage().current_db(), 0);
    }

    #[test]
    fn test_execute_select_out_of_range() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);

        let result = executor.execute(Command::Select(16));
        assert!(result.is_err());
    }

    #[test]
    fn test_is_write_command_select_auth_client() {
        assert!(!Command::Select(0).is_write_command());
        assert!(!Command::Auth("default".to_string(), "pass".to_string()).is_write_command());
        assert!(!Command::ClientSetName("name".to_string()).is_write_command());
        assert!(!Command::ClientGetName.is_write_command());
        assert!(!Command::ClientList.is_write_command());
        assert!(!Command::ClientId.is_write_command());
        assert!(!Command::Quit.is_write_command());
    }

    #[test]
    fn test_to_resp_value_select_auth_client() {
        assert_eq!(
            Command::Select(2).to_resp_value(),
            RespValue::Array(vec![bulk("SELECT"), bulk("2")])
        );
        assert_eq!(
            Command::Auth("default".to_string(), "pass".to_string()).to_resp_value(),
            RespValue::Array(vec![bulk("AUTH"), bulk("pass")])
        );
        assert_eq!(
            Command::ClientSetName("name".to_string()).to_resp_value(),
            RespValue::Array(vec![bulk("CLIENT"), bulk("SETNAME"), bulk("name")])
        );
        assert_eq!(
            Command::Quit.to_resp_value(),
            RespValue::Array(vec![bulk("QUIT")])
        );
    }

    // ---------- SORT / UNLINK / COPY / DUMP / RESTORE 解析测试 ----------

    #[test]
    fn test_parse_sort() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["SORT", "key", "DESC", "ALPHA", "LIMIT", "1", "2"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::Sort(
            "key".to_string(), None, Vec::new(),
            Some(1), Some(2), false, true, None
        ));
    }

    #[test]
    fn test_parse_sort_store() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["SORT", "key", "STORE", "dest"]);
        let cmd = parser.parse(resp).unwrap();
        match cmd {
            Command::Sort(_, _, _, _, _, _, _, Some(dest)) => assert_eq!(dest, "dest"),
            _ => panic!("期望 Sort 带 STORE"),
        }
    }

    #[test]
    fn test_parse_unlink() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["UNLINK", "a", "b"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::Unlink(vec!["a".to_string(), "b".to_string()]));
    }

    #[test]
    fn test_parse_copy() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["COPY", "src", "dest", "REPLACE"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::Copy("src".to_string(), "dest".to_string(), true));
    }

    #[test]
    fn test_parse_dump() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["DUMP", "key"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::Dump("key".to_string()));
    }

    #[test]
    fn test_parse_restore() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["RESTORE", "key", "0", "data"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::Restore("key".to_string(), 0, b"data".to_vec(), false));
    }

    // ---------- SORT / UNLINK / COPY / DUMP / RESTORE 执行测试 ----------

    #[test]
    fn test_execute_sort() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);
        executor.execute(Command::RPush("list".to_string(), vec![Bytes::from("3"), Bytes::from("1"), Bytes::from("2")])).unwrap();

        let resp = executor.execute(Command::Sort("list".to_string(), None, Vec::new(), None, None, true, false, None)).unwrap();
        match resp {
            RespValue::Array(arr) => {
                let strs: Vec<String> = arr.into_iter().map(|v| match v {
                    RespValue::BulkString(Some(b)) => String::from_utf8_lossy(&b).to_string(),
                    _ => panic!("期望 BulkString"),
                }).collect();
                assert_eq!(strs, vec!["1", "2", "3"]);
            }
            _ => panic!("期望 Array"),
        }
    }

    #[test]
    fn test_execute_sort_store() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);
        executor.execute(Command::RPush("list".to_string(), vec![Bytes::from("3"), Bytes::from("1")])).unwrap();

        let resp = executor.execute(Command::Sort("list".to_string(), None, Vec::new(), None, None, true, false, Some("dest".to_string()))).unwrap();
        assert_eq!(resp, RespValue::Integer(2));
    }

    #[test]
    fn test_execute_unlink() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);
        executor.execute(Command::Set("a".to_string(), Bytes::from("1"), crate::storage::SetOptions::default())).unwrap();
        executor.execute(Command::Set("b".to_string(), Bytes::from("2"), crate::storage::SetOptions::default())).unwrap();

        let resp = executor.execute(Command::Unlink(vec!["a".to_string(), "b".to_string(), "c".to_string()])).unwrap();
        assert_eq!(resp, RespValue::Integer(2));
    }

    #[test]
    fn test_execute_copy() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);
        executor.execute(Command::Set("src".to_string(), Bytes::from("value"), crate::storage::SetOptions::default())).unwrap();

        let resp = executor.execute(Command::Copy("src".to_string(), "dest".to_string(), false)).unwrap();
        assert_eq!(resp, RespValue::Integer(1));

        let get_resp = executor.execute(Command::Get("dest".to_string())).unwrap();
        assert_eq!(get_resp, RespValue::BulkString(Some(Bytes::from("value"))));
    }

    #[test]
    fn test_execute_dump_restore() {
        let storage = StorageEngine::new();
        let executor = CommandExecutor::new(storage);
        executor.execute(Command::Set("key".to_string(), Bytes::from("hello"), crate::storage::SetOptions::default())).unwrap();

        let dump_resp = executor.execute(Command::Dump("key".to_string())).unwrap();
        let data = match dump_resp {
            RespValue::BulkString(Some(b)) => b.to_vec(),
            _ => panic!("期望 BulkString"),
        };

        let restore_resp = executor.execute(Command::Restore("restored".to_string(), 0, data, false)).unwrap();
        assert_eq!(restore_resp, RespValue::SimpleString("OK".to_string()));

        let get_resp = executor.execute(Command::Get("restored".to_string())).unwrap();
        assert_eq!(get_resp, RespValue::BulkString(Some(Bytes::from("hello"))));
    }

    #[test]
    fn test_is_write_command_new() {
        assert!(Command::Sort("k".to_string(), None, Vec::new(), None, None, true, false, Some("d".to_string())).is_write_command());
        assert!(!Command::Sort("k".to_string(), None, Vec::new(), None, None, true, false, None).is_write_command());
        assert!(Command::Unlink(vec!["a".to_string()]).is_write_command());
        assert!(Command::Copy("a".to_string(), "b".to_string(), false).is_write_command());
        assert!(!Command::Dump("a".to_string()).is_write_command());
        assert!(Command::Restore("a".to_string(), 0, vec![], false).is_write_command());
    }

    // ---------- EVAL / EVALSHA / SCRIPT 测试 ----------

    #[test]
    fn test_parse_eval() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["EVAL", "return 1", "0"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::Eval("return 1".to_string(), Vec::new(), Vec::new()));
    }

    #[test]
    fn test_parse_eval_with_keys_args() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["EVAL", "return KEYS[1]", "1", "key1", "arg1"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::Eval(
            "return KEYS[1]".to_string(),
            vec!["key1".to_string()],
            vec!["arg1".to_string()]
        ));
    }

    #[test]
    fn test_parse_evalsha() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["EVALSHA", "abc123", "0"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::EvalSha("abc123".to_string(), Vec::new(), Vec::new()));
    }

    #[test]
    fn test_parse_script_load() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["SCRIPT", "LOAD", "return 1"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::ScriptLoad("return 1".to_string()));
    }

    #[test]
    fn test_parse_script_exists() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["SCRIPT", "EXISTS", "abc", "def"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::ScriptExists(vec!["abc".to_string(), "def".to_string()]));
    }

    #[test]
    fn test_parse_script_flush() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["SCRIPT", "FLUSH"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::ScriptFlush);
    }

    #[test]
    fn test_execute_eval() {
        let storage = StorageEngine::new();
        let mut executor = CommandExecutor::new(storage);
        executor.set_script_engine(ScriptEngine::new());

        let resp = executor.execute(Command::Eval("return 42".to_string(), vec![], vec![])).unwrap();
        assert_eq!(resp, RespValue::Integer(42));
    }

    #[test]
    fn test_execute_script_load_exists_flush() {
        let storage = StorageEngine::new();
        let mut executor = CommandExecutor::new(storage);
        executor.set_script_engine(ScriptEngine::new());

        let load_resp = executor.execute(Command::ScriptLoad("return 1".to_string())).unwrap();
        let sha1 = match load_resp {
            RespValue::BulkString(Some(b)) => String::from_utf8_lossy(&b).to_string(),
            _ => panic!("期望 BulkString"),
        };

        let exists_resp = executor.execute(Command::ScriptExists(vec![sha1.clone()])).unwrap();
        match exists_resp {
            RespValue::Array(arr) => assert_eq!(arr, vec![RespValue::Integer(1)]),
            _ => panic!("期望 Array"),
        }

        let flush_resp = executor.execute(Command::ScriptFlush).unwrap();
        assert_eq!(flush_resp, RespValue::SimpleString("OK".to_string()));

        let exists_resp2 = executor.execute(Command::ScriptExists(vec![sha1])).unwrap();
        match exists_resp2 {
            RespValue::Array(arr) => assert_eq!(arr, vec![RespValue::Integer(0)]),
            _ => panic!("期望 Array"),
        }
    }

    #[test]
    fn test_is_write_command_script() {
        assert!(Command::Eval("s".to_string(), vec![], vec![]).is_write_command());
        assert!(Command::EvalSha("s".to_string(), vec![], vec![]).is_write_command());
        assert!(!Command::ScriptLoad("s".to_string()).is_write_command());
        assert!(!Command::ScriptExists(vec!["s".to_string()]).is_write_command());
        assert!(!Command::ScriptFlush.is_write_command());
    }

    // ---------- SAVE / BGSAVE 测试 ----------

    #[test]
    fn test_parse_save() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["SAVE"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::Save);
    }

    #[test]
    fn test_parse_bgsave() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["BGSAVE"]);
        let cmd = parser.parse(resp).unwrap();
        assert_eq!(cmd, Command::BgSave);
    }

    #[test]
    fn test_is_write_command_save() {
        assert!(Command::Save.is_write_command());
        assert!(Command::BgSave.is_write_command());
    }

    #[test]
    fn test_to_resp_value_save_bgsave() {
        assert_eq!(Command::Save.to_resp_value(), RespValue::Array(vec![bulk("SAVE")]));
        assert_eq!(Command::BgSave.to_resp_value(), RespValue::Array(vec![bulk("BGSAVE")]));
    }

    // ---------- 阶段 35: 零散命令测试 ----------

    #[test]
    fn test_parse_echo() {
        let parser = CommandParser::new();
        let cmd = parser.parse(make_bulk_array(&["ECHO", "hello"])).unwrap();
        assert_eq!(cmd, Command::Echo("hello".to_string()));
    }

    #[test]
    fn test_execute_echo() {
        let executor = CommandExecutor::new(StorageEngine::new());
        let resp = executor.execute(Command::Echo("hello".to_string())).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("hello"))));
    }

    #[test]
    fn test_execute_time() {
        let executor = CommandExecutor::new(StorageEngine::new());
        let resp = executor.execute(Command::Time).unwrap();
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert!(matches!(arr[0], RespValue::Integer(_)));
                assert!(matches!(arr[1], RespValue::Integer(_)));
            }
            _ => panic!("期望数组"),
        }
    }

    #[test]
    fn test_execute_randomkey() {
        let executor = CommandExecutor::new(StorageEngine::new());
        let resp = executor.execute(Command::RandomKey).unwrap();
        assert_eq!(resp, RespValue::BulkString(None));
    }

    #[test]
    fn test_execute_expire_at() {
        let executor = CommandExecutor::new(StorageEngine::new());
        executor.execute(Command::Set("k".to_string(), Bytes::from("v"), crate::storage::SetOptions::default())).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let resp = executor.execute(Command::ExpireAt("k".to_string(), now + 10)).unwrap();
        assert_eq!(resp, RespValue::Integer(1));
        let resp2 = executor.execute(Command::ExpireTime("k".to_string())).unwrap();
        assert!(matches!(resp2, RespValue::Integer(t) if t >= now as i64));
    }

    #[test]
    fn test_execute_pexpire_at() {
        let executor = CommandExecutor::new(StorageEngine::new());
        executor.execute(Command::Set("k".to_string(), Bytes::from("v"), crate::storage::SetOptions::default())).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let resp = executor.execute(Command::PExpireAt("k".to_string(), now + 10000)).unwrap();
        assert_eq!(resp, RespValue::Integer(1));
        let resp2 = executor.execute(Command::PExpireTime("k".to_string())).unwrap();
        assert!(matches!(resp2, RespValue::Integer(t) if t >= now as i64));
    }

    #[test]
    fn test_execute_renamenx() {
        let executor = CommandExecutor::new(StorageEngine::new());
        executor.execute(Command::Set("a".to_string(), Bytes::from("1"), crate::storage::SetOptions::default())).unwrap();
        executor.execute(Command::Set("b".to_string(), Bytes::from("2"), crate::storage::SetOptions::default())).unwrap();
        let resp = executor.execute(Command::RenameNx("a".to_string(), "b".to_string())).unwrap();
        assert_eq!(resp, RespValue::Integer(0));
        let resp2 = executor.execute(Command::RenameNx("a".to_string(), "c".to_string())).unwrap();
        assert_eq!(resp2, RespValue::Integer(1));
    }

    #[test]
    fn test_execute_swapdb() {
        let executor = CommandExecutor::new(StorageEngine::new());
        executor.execute(Command::Set("k".to_string(), Bytes::from("db0"), crate::storage::SetOptions::default())).unwrap();
        executor.select_db(1).unwrap();
        executor.execute(Command::Set("k".to_string(), Bytes::from("db1"), crate::storage::SetOptions::default())).unwrap();
        let resp = executor.execute(Command::SwapDb(0, 1)).unwrap();
        assert_eq!(resp, RespValue::SimpleString("OK".to_string()));
        executor.select_db(0).unwrap();
        assert_eq!(executor.execute(Command::Get("k".to_string())).unwrap(), RespValue::BulkString(Some(Bytes::from("db1"))));
    }

    #[test]
    fn test_execute_flushdb() {
        let executor = CommandExecutor::new(StorageEngine::new());
        executor.execute(Command::Set("k".to_string(), Bytes::from("v"), crate::storage::SetOptions::default())).unwrap();
        let resp = executor.execute(Command::FlushDb).unwrap();
        assert_eq!(resp, RespValue::SimpleString("OK".to_string()));
        assert_eq!(executor.execute(Command::Get("k".to_string())).unwrap(), RespValue::BulkString(None));
    }

    #[test]
    fn test_execute_lastsave() {
        let executor = CommandExecutor::new(StorageEngine::new());
        let resp = executor.execute(Command::LastSave).unwrap();
        assert_eq!(resp, RespValue::Integer(0));
    }

    #[test]
    fn test_execute_substr() {
        let executor = CommandExecutor::new(StorageEngine::new());
        executor.execute(Command::Set("k".to_string(), Bytes::from("hello world"), crate::storage::SetOptions::default())).unwrap();
        let resp = executor.execute(Command::SubStr("k".to_string(), 0, 4)).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("hello"))));
    }

    #[test]
    fn test_execute_touch() {
        let executor = CommandExecutor::new(StorageEngine::new());
        executor.execute(Command::Set("a".to_string(), Bytes::from("1"), crate::storage::SetOptions::default())).unwrap();
        executor.execute(Command::Set("b".to_string(), Bytes::from("2"), crate::storage::SetOptions::default())).unwrap();
        let resp = executor.execute(Command::Touch(vec!["a".to_string(), "b".to_string(), "c".to_string()])).unwrap();
        assert_eq!(resp, RespValue::Integer(2));
    }

    // ---------- 阶段 36: SET 扩展选项 + LCS 测试 ----------

    #[test]
    fn test_set_nx_success() {
        let executor = CommandExecutor::new(StorageEngine::new());
        let opts = crate::storage::SetOptions { nx: true, ..Default::default() };
        let resp = executor.execute(Command::Set("k".to_string(), Bytes::from("v"), opts)).unwrap();
        assert_eq!(resp, RespValue::SimpleString("OK".to_string()));
        assert_eq!(executor.execute(Command::Get("k".to_string())).unwrap(), RespValue::BulkString(Some(Bytes::from("v"))));
    }

    #[test]
    fn test_set_nx_fail() {
        let executor = CommandExecutor::new(StorageEngine::new());
        executor.execute(Command::Set("k".to_string(), Bytes::from("old"), crate::storage::SetOptions::default())).unwrap();
        let opts = crate::storage::SetOptions { nx: true, ..Default::default() };
        let resp = executor.execute(Command::Set("k".to_string(), Bytes::from("new"), opts)).unwrap();
        assert_eq!(resp, RespValue::BulkString(None));
        assert_eq!(executor.execute(Command::Get("k".to_string())).unwrap(), RespValue::BulkString(Some(Bytes::from("old"))));
    }

    #[test]
    fn test_set_xx_success() {
        let executor = CommandExecutor::new(StorageEngine::new());
        executor.execute(Command::Set("k".to_string(), Bytes::from("old"), crate::storage::SetOptions::default())).unwrap();
        let opts = crate::storage::SetOptions { xx: true, ..Default::default() };
        let resp = executor.execute(Command::Set("k".to_string(), Bytes::from("new"), opts)).unwrap();
        assert_eq!(resp, RespValue::SimpleString("OK".to_string()));
        assert_eq!(executor.execute(Command::Get("k".to_string())).unwrap(), RespValue::BulkString(Some(Bytes::from("new"))));
    }

    #[test]
    fn test_set_xx_fail() {
        let executor = CommandExecutor::new(StorageEngine::new());
        let opts = crate::storage::SetOptions { xx: true, ..Default::default() };
        let resp = executor.execute(Command::Set("k".to_string(), Bytes::from("v"), opts)).unwrap();
        assert_eq!(resp, RespValue::BulkString(None));
    }

    #[test]
    fn test_set_get_return_old() {
        let executor = CommandExecutor::new(StorageEngine::new());
        executor.execute(Command::Set("k".to_string(), Bytes::from("old"), crate::storage::SetOptions::default())).unwrap();
        let opts = crate::storage::SetOptions { get: true, ..Default::default() };
        let resp = executor.execute(Command::Set("k".to_string(), Bytes::from("new"), opts)).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("old"))));
        assert_eq!(executor.execute(Command::Get("k".to_string())).unwrap(), RespValue::BulkString(Some(Bytes::from("new"))));
    }

    #[test]
    fn test_set_get_no_key() {
        let executor = CommandExecutor::new(StorageEngine::new());
        let opts = crate::storage::SetOptions { get: true, ..Default::default() };
        let resp = executor.execute(Command::Set("k".to_string(), Bytes::from("v"), opts)).unwrap();
        assert_eq!(resp, RespValue::BulkString(None));
    }

    #[test]
    fn test_set_keepttl() {
        let executor = CommandExecutor::new(StorageEngine::new());
        executor.execute(Command::SetEx("k".to_string(), Bytes::from("old"), 10000)).unwrap();
        let ttl_before = match executor.execute(Command::Ttl("k".to_string())).unwrap() {
            RespValue::Integer(t) => t,
            _ => panic!("期望整数"),
        };
        assert!(ttl_before > 0);

        let opts = crate::storage::SetOptions { keepttl: true, ..Default::default() };
        executor.execute(Command::Set("k".to_string(), Bytes::from("new"), opts)).unwrap();
        let ttl_after = match executor.execute(Command::Ttl("k".to_string())).unwrap() {
            RespValue::Integer(t) => t,
            _ => panic!("期望整数"),
        };
        assert!(ttl_after > 0);
    }

    #[test]
    fn test_set_exat() {
        let executor = CommandExecutor::new(StorageEngine::new());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let opts = crate::storage::SetOptions {
            expire: Some(crate::storage::SetExpireOption::ExAt(now + 10)),
            ..Default::default()
        };
        executor.execute(Command::Set("k".to_string(), Bytes::from("v"), opts)).unwrap();
        let ttl = match executor.execute(Command::Ttl("k".to_string())).unwrap() {
            RespValue::Integer(t) => t,
            _ => panic!("期望整数"),
        };
        assert!(ttl > 0 && ttl <= 10000);
    }

    #[test]
    fn test_set_pxat() {
        let executor = CommandExecutor::new(StorageEngine::new());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let opts = crate::storage::SetOptions {
            expire: Some(crate::storage::SetExpireOption::PxAt(now + 10000)),
            ..Default::default()
        };
        executor.execute(Command::Set("k".to_string(), Bytes::from("v"), opts)).unwrap();
        let pttl = match executor.execute(Command::PTtl("k".to_string())).unwrap() {
            RespValue::Integer(t) => t,
            _ => panic!("期望整数"),
        };
        assert!(pttl > 0 && pttl <= 10000);
    }

    #[test]
    fn test_set_nx_get() {
        let executor = CommandExecutor::new(StorageEngine::new());
        // key 不存在：NX + GET → 设置成功，旧值 nil
        let opts = crate::storage::SetOptions { nx: true, get: true, ..Default::default() };
        let resp = executor.execute(Command::Set("k".to_string(), Bytes::from("v"), opts)).unwrap();
        assert_eq!(resp, RespValue::BulkString(None));
        assert_eq!(executor.execute(Command::Get("k".to_string())).unwrap(), RespValue::BulkString(Some(Bytes::from("v"))));

        // key 已存在：NX + GET → 不设置，返回旧值
        let opts2 = crate::storage::SetOptions { nx: true, get: true, ..Default::default() };
        let resp2 = executor.execute(Command::Set("k".to_string(), Bytes::from("new"), opts2)).unwrap();
        assert_eq!(resp2, RespValue::BulkString(Some(Bytes::from("v"))));
    }

    #[test]
    fn test_parse_set_with_options() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["SET", "k", "v", "NX", "EX", "10"]);
        let cmd = parser.parse(resp).unwrap();
        match cmd {
            Command::Set(key, value, opts) => {
                assert_eq!(key, "k");
                assert_eq!(value, Bytes::from("v"));
                assert!(opts.nx);
                assert_eq!(opts.expire, Some(crate::storage::SetExpireOption::Ex(10)));
            }
            _ => panic!("期望 Set"),
        }
    }

    #[test]
    fn test_execute_lcs_basic() {
        let executor = CommandExecutor::new(StorageEngine::new());
        executor.execute(Command::Set("k1".to_string(), Bytes::from("hello world"), crate::storage::SetOptions::default())).unwrap();
        executor.execute(Command::Set("k2".to_string(), Bytes::from("hello rust"), crate::storage::SetOptions::default())).unwrap();
        let resp = executor.execute(Command::Lcs("k1".to_string(), "k2".to_string(), false, false, 0, false)).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("hello "))));
    }

    #[test]
    fn test_execute_lcs_len() {
        let executor = CommandExecutor::new(StorageEngine::new());
        executor.execute(Command::Set("k1".to_string(), Bytes::from("hello world"), crate::storage::SetOptions::default())).unwrap();
        executor.execute(Command::Set("k2".to_string(), Bytes::from("hello rust"), crate::storage::SetOptions::default())).unwrap();
        let resp = executor.execute(Command::Lcs("k1".to_string(), "k2".to_string(), true, false, 0, false)).unwrap();
        assert_eq!(resp, RespValue::Integer(6));
    }

    #[test]
    fn test_execute_lcs_idx() {
        let executor = CommandExecutor::new(StorageEngine::new());
        executor.execute(Command::Set("k1".to_string(), Bytes::from("hello world"), crate::storage::SetOptions::default())).unwrap();
        executor.execute(Command::Set("k2".to_string(), Bytes::from("hello rust"), crate::storage::SetOptions::default())).unwrap();
        let resp = executor.execute(Command::Lcs("k1".to_string(), "k2".to_string(), false, true, 0, false)).unwrap();
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 3); // matches, len1, len2
            }
            _ => panic!("期望数组"),
        }
    }

    #[test]
    fn test_storage_lcs() {
        let engine = StorageEngine::new();
        engine.set("k1".to_string(), Bytes::from("hello world")).unwrap();
        engine.set("k2".to_string(), Bytes::from("hello rust")).unwrap();
        let lcs = engine.lcs("k1", "k2").unwrap();
        assert_eq!(lcs, Some("hello ".to_string()));
    }

    // ---------- 阶段 44: Function 系统测试 ----------

    #[test]
    fn test_parse_function_load() {
        let parser = CommandParser::new();
        let code = "#!lua name=mylib\nredis.register_function('f1', function(k, a) return 1 end)";
        let resp = make_bulk_array(&["FUNCTION", "LOAD", code]);
        let cmd = parser.parse(resp).unwrap();
        assert!(matches!(cmd, Command::FunctionLoad(_, false)));
    }

    #[test]
    fn test_parse_function_load_replace() {
        let parser = CommandParser::new();
        let code = "#!lua name=mylib\nredis.register_function('f1', function(k, a) return 1 end)";
        let resp = make_bulk_array(&["FUNCTION", "LOAD", "REPLACE", code]);
        let cmd = parser.parse(resp).unwrap();
        assert!(matches!(cmd, Command::FunctionLoad(_, true)));
    }

    #[test]
    fn test_parse_fcall() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["FCALL", "myfunc", "1", "key1", "arg1"]);
        let cmd = parser.parse(resp).unwrap();
        assert!(matches!(cmd, Command::FCall(name, keys, args) if name == "myfunc" && keys == vec!["key1"] && args == vec!["arg1"]));
    }

    #[test]
    fn test_parse_fcall_ro() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["FCALL_RO", "myfunc", "0"]);
        let cmd = parser.parse(resp).unwrap();
        assert!(matches!(cmd, Command::FCallRO(name, _, _) if name == "myfunc"));
    }

    #[test]
    fn test_parse_eval_ro() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["EVAL_RO", "return 1", "0"]);
        let cmd = parser.parse(resp).unwrap();
        assert!(matches!(cmd, Command::EvalRO(script, _, _) if script == "return 1"));
    }

    #[test]
    fn test_parse_evalsha_ro() {
        let parser = CommandParser::new();
        let resp = make_bulk_array(&["EVALSHA_RO", "abc123", "0"]);
        let cmd = parser.parse(resp).unwrap();
        assert!(matches!(cmd, Command::EvalShaRO(sha, _, _) if sha == "abc123"));
    }

    #[test]
    fn test_execute_function_load_and_fcall() {
        let storage = StorageEngine::new();
        let mut executor = CommandExecutor::new(storage.clone());
        executor.set_script_engine(ScriptEngine::new());

        let code = "#!lua name=mylib\nredis.register_function('hello', function(keys, args)\n    return 'world'\nend)\n";
        let load_resp = executor.execute(Command::FunctionLoad(code.to_string(), false)).unwrap();
        assert_eq!(load_resp, RespValue::SimpleString("mylib".to_string()));

        let fcall_resp = executor.execute(Command::FCall("hello".to_string(), vec![], vec![])).unwrap();
        assert_eq!(fcall_resp, RespValue::BulkString(Some(Bytes::from("world"))));
    }

    #[test]
    fn test_execute_function_delete() {
        let storage = StorageEngine::new();
        let mut executor = CommandExecutor::new(storage.clone());
        executor.set_script_engine(ScriptEngine::new());

        let code = "#!lua name=mylib\nredis.register_function('f1', function(k, a) return 1 end)\n";
        executor.execute(Command::FunctionLoad(code.to_string(), false)).unwrap();

        let del_resp = executor.execute(Command::FunctionDelete("mylib".to_string())).unwrap();
        assert_eq!(del_resp, RespValue::Integer(1));

        let del_resp2 = executor.execute(Command::FunctionDelete("mylib".to_string())).unwrap();
        assert_eq!(del_resp2, RespValue::Integer(0));
    }

    #[test]
    fn test_execute_function_list() {
        let storage = StorageEngine::new();
        let mut executor = CommandExecutor::new(storage.clone());
        executor.set_script_engine(ScriptEngine::new());

        let code = "#!lua name=mylib\nredis.register_function('f1', function(k, a) return 1 end, 'no-writes')\n";
        executor.execute(Command::FunctionLoad(code.to_string(), false)).unwrap();

        let list_resp = executor.execute(Command::FunctionList(None, false)).unwrap();
        assert!(matches!(list_resp, RespValue::Array(_)));
    }

    #[test]
    fn test_execute_function_dump_restore() {
        let storage = StorageEngine::new();
        let mut executor = CommandExecutor::new(storage.clone());
        executor.set_script_engine(ScriptEngine::new());

        let code = "#!lua name=mylib\nredis.register_function('f1', function(k, a) return 1 end)\n";
        executor.execute(Command::FunctionLoad(code.to_string(), false)).unwrap();

        let dump_resp = executor.execute(Command::FunctionDump).unwrap();
        let dump_data = match dump_resp {
            RespValue::BulkString(Some(b)) => String::from_utf8_lossy(&b).to_string(),
            _ => panic!("期望 BulkString"),
        };

        executor.execute(Command::FunctionFlush(false)).unwrap();
        let list_before = executor.execute(Command::FunctionList(None, false)).unwrap();
        match list_before {
            RespValue::Array(arr) => assert_eq!(arr.len(), 0),
            _ => panic!("期望空数组"),
        }

        executor.execute(Command::FunctionRestore(dump_data, "FLUSH".to_string())).unwrap();
        let list_after = executor.execute(Command::FunctionList(None, false)).unwrap();
        match list_after {
            RespValue::Array(arr) => assert_eq!(arr.len(), 1),
            _ => panic!("期望 1 个库"),
        }
    }

    #[test]
    fn test_execute_function_flush() {
        let storage = StorageEngine::new();
        let mut executor = CommandExecutor::new(storage.clone());
        executor.set_script_engine(ScriptEngine::new());

        let code = "#!lua name=mylib\nredis.register_function('f1', function(k, a) return 1 end)\n";
        executor.execute(Command::FunctionLoad(code.to_string(), false)).unwrap();

        let flush_resp = executor.execute(Command::FunctionFlush(false)).unwrap();
        assert_eq!(flush_resp, RespValue::SimpleString("OK".to_string()));

        let list_resp = executor.execute(Command::FunctionList(None, false)).unwrap();
        match list_resp {
            RespValue::Array(arr) => assert_eq!(arr.len(), 0),
            _ => panic!("期望空数组"),
        }
    }

    #[test]
    fn test_execute_eval_ro() {
        let storage = StorageEngine::new();
        let mut executor = CommandExecutor::new(storage.clone());
        executor.set_script_engine(ScriptEngine::new());

        let resp = executor.execute(Command::EvalRO("return 42".to_string(), vec![], vec![])).unwrap();
        assert_eq!(resp, RespValue::Integer(42));
    }

    #[test]
    fn test_execute_fcall_ro() {
        let storage = StorageEngine::new();
        let mut executor = CommandExecutor::new(storage.clone());
        executor.set_script_engine(ScriptEngine::new());

        let code = "#!lua name=mylib\nredis.register_function('readonly_fn', function(k, a)\n    return 'ro'\nend)\n";
        executor.execute(Command::FunctionLoad(code.to_string(), false)).unwrap();

        let resp = executor.execute(Command::FCallRO("readonly_fn".to_string(), vec![], vec![])).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("ro"))));
    }

    // ---------- 阶段 45: 客户端命令补全测试 ----------

    #[test]
    fn test_parse_client_info() {
        let parser = CommandParser::new();
        let cmd = parser.parse(make_bulk_array(&["CLIENT", "INFO"])).unwrap();
        assert_eq!(cmd, Command::ClientInfo);
    }

    #[test]
    fn test_parse_client_kill() {
        let parser = CommandParser::new();
        let cmd = parser.parse(make_bulk_array(&["CLIENT", "KILL", "ID", "42"])).unwrap();
        assert!(matches!(cmd, Command::ClientKill { id: Some(42), .. }));

        let cmd = parser.parse(make_bulk_array(&["CLIENT", "KILL", "ADDR", "127.0.0.1:1234"])).unwrap();
        assert!(matches!(cmd, Command::ClientKill { addr: Some(ref a), .. } if a == "127.0.0.1:1234"));
    }

    #[test]
    fn test_parse_client_pause() {
        let parser = CommandParser::new();
        let cmd = parser.parse(make_bulk_array(&["CLIENT", "PAUSE", "100", "WRITE"])).unwrap();
        assert!(matches!(cmd, Command::ClientPause(100, ref m) if m == "WRITE"));
    }

    #[test]
    fn test_parse_client_unpause() {
        let parser = CommandParser::new();
        let cmd = parser.parse(make_bulk_array(&["CLIENT", "UNPAUSE"])).unwrap();
        assert_eq!(cmd, Command::ClientUnpause);
    }

    #[test]
    fn test_parse_client_no_evict() {
        let parser = CommandParser::new();
        let cmd = parser.parse(make_bulk_array(&["CLIENT", "NO-EVICT", "ON"])).unwrap();
        assert!(matches!(cmd, Command::ClientNoEvict(true)));

        let cmd = parser.parse(make_bulk_array(&["CLIENT", "NO-EVICT", "OFF"])).unwrap();
        assert!(matches!(cmd, Command::ClientNoEvict(false)));
    }

    #[test]
    fn test_parse_client_no_touch() {
        let parser = CommandParser::new();
        let cmd = parser.parse(make_bulk_array(&["CLIENT", "NO-TOUCH", "ON"])).unwrap();
        assert!(matches!(cmd, Command::ClientNoTouch(true)));
    }

    #[test]
    fn test_parse_client_reply() {
        let parser = CommandParser::new();
        let cmd = parser.parse(make_bulk_array(&["CLIENT", "REPLY", "OFF"])).unwrap();
        assert!(matches!(cmd, Command::ClientReply(crate::server::ReplyMode::Off)));

        let cmd = parser.parse(make_bulk_array(&["CLIENT", "REPLY", "SKIP"])).unwrap();
        assert!(matches!(cmd, Command::ClientReply(crate::server::ReplyMode::Skip)));
    }

    #[test]
    fn test_parse_client_unblock() {
        let parser = CommandParser::new();
        let cmd = parser.parse(make_bulk_array(&["CLIENT", "UNBLOCK", "42", "ERROR"])).unwrap();
        assert!(matches!(cmd, Command::ClientUnblock(42, ref r) if r == "ERROR"));
    }
}
