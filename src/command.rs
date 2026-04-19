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

/// 辅助函数：创建 BulkString
fn bulk(s: &str) -> RespValue {
    RespValue::BulkString(Some(Bytes::copy_from_slice(s.as_bytes())))
}

/// 辅助函数：从 Bytes 创建 BulkString
fn bulk_bytes(b: &Bytes) -> RespValue {
    RespValue::BulkString(Some(b.clone()))
}

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

/// 命令执行器，负责分发命令到存储引擎
pub struct CommandExecutor {
    /// 存储引擎引用
    storage: StorageEngine,
    /// AOF 写入器（可选），用于持久化写操作
    aof: Option<Arc<Mutex<AofWriter>>>,
    /// Lua 脚本引擎（可选）
    script_engine: Option<ScriptEngine>,
    /// 慢查询日志（可选）
    slowlog: Option<SlowLog>,
    /// ACL 管理器（可选）
    acl: Option<crate::acl::AclManager>,
    /// 延迟追踪器（可选）
    latency: Option<crate::latency::LatencyTracker>,
    /// Keyspace 通知器（可选）
    keyspace_notifier: Option<Arc<KeyspaceNotifier>>,
    /// 是否使用 AOF RDB preamble（混合持久化）
    aof_use_rdb_preamble: Arc<AtomicBool>,
}

impl CommandExecutor {
    /// 创建不带 AOF 的命令执行器（用于 AOF 重放）
    pub fn new(storage: StorageEngine) -> Self {
        Self {
            storage,
            aof: None,
            script_engine: None,
            slowlog: None,
            acl: None,
            latency: None,
            keyspace_notifier: None,
            aof_use_rdb_preamble: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 创建带 AOF 的命令执行器（用于正常服务）
    pub fn new_with_aof(
        storage: StorageEngine,
        aof: Arc<Mutex<AofWriter>>,
    ) -> Self {
        Self {
            storage,
            aof: Some(aof),
            script_engine: None,
            slowlog: None,
            acl: None,
            latency: None,
            keyspace_notifier: None,
            aof_use_rdb_preamble: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 设置 Lua 脚本引擎
    pub fn set_script_engine(&mut self, engine: ScriptEngine) {
        self.script_engine = Some(engine);
    }

    /// 设置慢查询日志
    pub fn set_slowlog(&mut self, slowlog: SlowLog) {
        self.slowlog = Some(slowlog);
    }

    /// 设置 ACL 管理器
    pub fn set_acl(&mut self, acl: crate::acl::AclManager) {
        self.acl = Some(acl);
    }

    /// 设置延迟追踪器
    pub fn set_latency(&mut self, latency: crate::latency::LatencyTracker) {
        self.latency = Some(latency);
    }

    /// 设置 Keyspace 通知器
    pub fn set_keyspace_notifier(&mut self, notifier: Arc<KeyspaceNotifier>) {
        self.keyspace_notifier = Some(notifier);
    }

    /// 设置是否使用 AOF RDB preamble
    pub fn set_aof_use_rdb_preamble(&self, enabled: bool) {
        self.aof_use_rdb_preamble.store(enabled, Ordering::Relaxed);
    }

    /// 获取是否使用 AOF RDB preamble
    pub fn aof_use_rdb_preamble(&self) -> bool {
        self.aof_use_rdb_preamble.load(Ordering::Relaxed)
    }

    /// 获取存储引擎的克隆引用
    pub fn storage(&self) -> StorageEngine {
        self.storage.clone()
    }

    /// 获取 ACL 管理器
    pub fn acl(&self) -> Option<crate::acl::AclManager> {
        self.acl.clone()
    }

    /// 切换当前数据库
    pub fn select_db(&self, index: usize) -> Result<()> {
        self.storage.select(index)
    }

    /// 构建 DEBUG OBJECT key 的返回信息
    fn build_debug_object_info(&self, key: &str) -> Result<String> {
        let encoding = self.storage.object_encoding(key)?
            .unwrap_or_else(|| "none".to_string());
        let refcount = self.storage.object_refcount(key)?
            .unwrap_or(0);
        let idletime = self.storage.object_idletime(key)?
            .unwrap_or(0);
        let serialized_len = match self.storage.dump(key)? {
            Some(data) => data.len(),
            None => 0,
        };

        Ok(format!(
            "Value at:0x{:p} refcount:{} encoding:{} serializedlength:{} lru_seconds_idle:{}",
            key.as_ptr(), refcount, encoding, serialized_len, idletime
        ))
    }

    /// 将写操作追加到 AOF 文件
    fn append_to_aof(&self, cmd: &Command) {
        if !cmd.is_write_command() {
            return;
        }
        if let Some(ref aof) = self.aof {
            match aof.lock() {
                Ok(mut writer) => {
                    if let Err(e) = writer.append(cmd) {
                        log::error!("AOF 写入失败: {}", e);
                    }
                }
                Err(e) => {
                    log::error!("AOF writer 锁中毒: {}", e);
                }
            }
        }
    }

    /// 执行命令并返回 RESP 结果（自动记录慢查询）
    pub fn execute(&self, cmd: Command) -> Result<RespValue> {
        let start = std::time::Instant::now();
        let (cmd_name, args) = extract_cmd_info(&cmd);

        // 写操作先记录到 AOF，再执行
        self.append_to_aof(&cmd);

        // 保留命令引用用于 keyspace 通知
        let cmd_for_notify = cmd.clone();
        let result = self.do_execute(cmd);

        // 命令执行成功后发送 Keyspace 通知
        if result.is_ok() {
            if let Some(ref notifier) = self.keyspace_notifier {
                let db = self.storage.current_db();
                notifier.notify_command(&cmd_for_notify, db);
            }
        }

        let duration_us = start.elapsed().as_micros() as u64;
        if let Some(ref slowlog) = self.slowlog {
            slowlog.record(&cmd_name, args, duration_us);
        }
        if let Some(ref latency) = self.latency {
            // LATENCY 命令自身不记录延迟，避免 RESET 后立即被重新填充
            if !cmd_name.starts_with("LATENCY ") {
                let _ = latency.record("command", duration_us / 1000);
            }
        }

        result
    }

    /// 内部执行命令（不包含计时）
    fn do_execute(&self, cmd: Command) -> Result<RespValue> {
        match cmd {
            Command::Ping(message) => {
                // PING 命令：有参数返回参数，否则返回 PONG
                let reply = message.unwrap_or_else(|| "PONG".to_string());
                Ok(RespValue::SimpleString(reply))
            }
            Command::Get(key) => {
                match self.storage.get(&key)? {
                    Some(value) => {
                        Ok(RespValue::BulkString(Some(value)))
                    }
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::Set(key, value, options) => {
                let (ok, old_value) = self.storage.set_with_options(key, value, &options)?;
                if options.get {
                    Ok(RespValue::BulkString(old_value))
                } else if ok {
                    Ok(RespValue::SimpleString("OK".to_string()))
                } else {
                    Ok(RespValue::BulkString(None))
                }
            }
            Command::SetEx(key, value, ttl_ms) => {
                self.storage.set_with_ttl(key, value, ttl_ms)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::Del(keys) => {
                let mut count = 0i64;
                for key in keys {
                    if self.storage.del(&key)? {
                        count += 1;
                    }
                }
                Ok(RespValue::Integer(count))
            }
            Command::Exists(keys) => {
                let mut count = 0i64;
                for key in keys {
                    if self.storage.exists(&key)? {
                        count += 1;
                    }
                }
                Ok(RespValue::Integer(count))
            }
            Command::FlushAll => {
                self.storage.flush()?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::Expire(key, seconds) => {
                let result = if self.storage.expire(&key, seconds)? {
                    1i64
                } else {
                    0i64
                };
                Ok(RespValue::Integer(result))
            }
            Command::Ttl(key) => {
                let ttl_ms = self.storage.ttl(&key)?;
                // Redis TTL 返回秒数，-1 和 -2 保持不变
                let ttl_sec = if ttl_ms >= 0 {
                    ttl_ms / 1000
                } else {
                    ttl_ms
                };
                Ok(RespValue::Integer(ttl_sec))
            }
            // redis-benchmark 兼容：CONFIG GET 返回空数组
            // 对 maxmemory 返回实际值
            Command::ConfigGet(key) => {
                let key_lower = key.to_ascii_lowercase();
                if key_lower == "maxmemory" {
                    let maxmem = self.storage.get_maxmemory();
                    Ok(RespValue::Array(vec![
                        RespValue::BulkString(Some(bytes::Bytes::from(key))),
                        RespValue::BulkString(Some(bytes::Bytes::from(maxmem.to_string()))),
                    ]))
                } else if key_lower == "maxmemory-policy" {
                    let policy = self.storage.get_eviction_policy();
                    let policy_str = format!("{:?}", policy).to_ascii_lowercase();
                    Ok(RespValue::Array(vec![
                        RespValue::BulkString(Some(bytes::Bytes::from(key))),
                        RespValue::BulkString(Some(bytes::Bytes::from(policy_str))),
                    ]))
                } else if key_lower == "notify-keyspace-events" {
                    let raw = self.keyspace_notifier.as_ref()
                        .map(|n| n.config().raw().to_string())
                        .unwrap_or_default();
                    Ok(RespValue::Array(vec![
                        RespValue::BulkString(Some(bytes::Bytes::from(key))),
                        RespValue::BulkString(Some(bytes::Bytes::from(raw))),
                    ]))
                } else if key_lower == "aof-use-rdb-preamble" {
                    let val = if self.aof_use_rdb_preamble.load(Ordering::Relaxed) {
                        "yes"
                    } else {
                        "no"
                    };
                    Ok(RespValue::Array(vec![
                        RespValue::BulkString(Some(bytes::Bytes::from(key))),
                        RespValue::BulkString(Some(bytes::Bytes::from(val))),
                    ]))
                } else {
                    Ok(RespValue::Array(vec![]))
                }
            }
            Command::ConfigSet(key, value) => {
                let key_lower = key.to_ascii_lowercase();
                if key_lower == "maxmemory" {
                    match value.parse::<u64>() {
                        Ok(bytes) => {
                            self.storage.set_maxmemory(bytes);
                            Ok(RespValue::SimpleString("OK".to_string()))
                        }
                        Err(_) => {
                            Ok(RespValue::Error("ERR value is not an integer or out of range".to_string()))
                        }
                    }
                } else if key_lower == "maxmemory-policy" {
                    let policy = match value.to_ascii_lowercase().as_str() {
                        "noeviction" => EvictionPolicy::NoEviction,
                        "allkeys-lru" => EvictionPolicy::AllKeysLru,
                        "allkeys-random" => EvictionPolicy::AllKeysRandom,
                        "allkeys-lfu" => EvictionPolicy::AllKeysLfu,
                        "volatile-lru" => EvictionPolicy::VolatileLru,
                        "volatile-ttl" => EvictionPolicy::VolatileTtl,
                        "volatile-random" => EvictionPolicy::VolatileRandom,
                        "volatile-lfu" => EvictionPolicy::VolatileLfu,
                        _ => {
                            return Ok(RespValue::Error(format!(
                                "ERR Invalid maxmemory-policy: {}", value
                            )));
                        }
                    };
                    self.storage.set_eviction_policy(policy);
                    Ok(RespValue::SimpleString("OK".to_string()))
                } else if key_lower == "notify-keyspace-events" {
                    if let Some(ref notifier) = self.keyspace_notifier {
                        notifier.set_config(crate::keyspace::NotifyKeyspaceEvents::from_str(&value));
                        Ok(RespValue::SimpleString("OK".to_string()))
                    } else {
                        Ok(RespValue::Error("ERR Keyspace notifier not initialized".to_string()))
                    }
                } else if key_lower == "aof-use-rdb-preamble" {
                    let enabled = match value.to_ascii_lowercase().as_str() {
                        "yes" => true,
                        "no" => false,
                        _ => {
                            return Ok(RespValue::Error(
                                "ERR Invalid argument 'yes' or 'no' expected".to_string()
                            ));
                        }
                    };
                    self.aof_use_rdb_preamble.store(enabled, Ordering::Relaxed);
                    Ok(RespValue::SimpleString("OK".to_string()))
                } else {
                    Ok(RespValue::Error(format!("ERR Unsupported CONFIG parameter: {}", key)))
                }
            }
            // redis-benchmark 兼容：COMMAND 返回空数组
            Command::ConfigRewrite => {
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::ConfigResetStat => {
                self.slowlog.as_ref().map(|s| s.reset());
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::MemoryUsage(key, _samples) => {
                match self.storage.memory_key_usage(&key, _samples)? {
                    Some(size) => Ok(RespValue::Integer(size as i64)),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::MemoryDoctor => {
                let info = self.storage.memory_doctor()?;
                Ok(RespValue::BulkString(Some(bytes::Bytes::from(info))))
            }
            Command::LatencyLatest => {
                let tracker = self.latency.as_ref().ok_or_else(|| {
                    AppError::Command("延迟追踪器未初始化".to_string())
                })?;
                let latest = tracker.latest()?;
                let parts: Vec<RespValue> = latest.into_iter()
                    .map(|(name, lat, ts, max)| {
                        RespValue::Array(vec![
                            RespValue::BulkString(Some(bytes::Bytes::from(name))),
                            RespValue::Integer(ts as i64),
                            RespValue::Integer(lat as i64),
                            RespValue::Integer(max as i64),
                        ])
                    })
                    .collect();
                Ok(RespValue::Array(parts))
            }
            Command::LatencyHistory(event) => {
                let tracker = self.latency.as_ref().ok_or_else(|| {
                    AppError::Command("延迟追踪器未初始化".to_string())
                })?;
                let history = tracker.history(&event)?;
                let parts: Vec<RespValue> = history.into_iter()
                    .map(|(ts, lat)| {
                        RespValue::Array(vec![
                            RespValue::Integer(ts as i64),
                            RespValue::Integer(lat as i64),
                        ])
                    })
                    .collect();
                Ok(RespValue::Array(parts))
            }
            Command::LatencyReset(events) => {
                let tracker = self.latency.as_ref().ok_or_else(|| {
                    AppError::Command("延迟追踪器未初始化".to_string())
                })?;
                let count = if events.is_empty() {
                    tracker.reset_all()?
                } else {
                    let refs: Vec<&str> = events.iter().map(|s| s.as_str()).collect();
                    tracker.reset(&refs)?
                };
                Ok(RespValue::Integer(count as i64))
            }
            Command::Reset => {
                Err(AppError::Command("RESET 应在连接层处理".to_string()))
            }
            Command::Hello(_protover, _auth, _setname) => {
                Err(AppError::Command("HELLO 应在连接层处理".to_string()))
            }
            Command::Monitor => {
                Err(AppError::Command("MONITOR 应在连接层处理".to_string()))
            }
            Command::CommandInfo => {
                Ok(RespValue::Array(vec![]))
            }
            Command::BgRewriteAof => {
                // BGREWRITEAOF 在 server.rs 中处理，需要 AOF writer
                Err(AppError::Command("BGREWRITEAOF 应在连接层处理".to_string()))
            }
            Command::SetBit(key, offset, value) => {
                let old_val = self.storage.setbit(&key, offset, value)?;
                Ok(RespValue::Integer(old_val))
            }
            Command::GetBit(key, offset) => {
                let val = self.storage.getbit(&key, offset)?;
                Ok(RespValue::Integer(val))
            }
            Command::BitCount(key, start, end, is_byte) => {
                let count = self.storage.bitcount(&key, start, end, is_byte)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::BitOp(op, destkey, keys) => {
                let len = self.storage.bitop(&op, &destkey, &keys)?;
                Ok(RespValue::Integer(len as i64))
            }
            Command::BitPos(key, bit, start, end, is_byte) => {
                let pos = self.storage.bitpos(&key, bit, start, end, is_byte)?;
                Ok(RespValue::Integer(pos))
            }
            Command::BitField(key, ops) => {
                let results = self.storage.bitfield(&key, &ops)?;
                let resp_values: Vec<RespValue> = results
                    .into_iter()
                    .map(|r| match r {
                        crate::storage::BitFieldResult::Value(v) => RespValue::Integer(v),
                        crate::storage::BitFieldResult::Nil => RespValue::BulkString(None),
                    })
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::BitFieldRo(key, ops) => {
                let results = self.storage.bitfield_ro(&key, &ops)?;
                let resp_values: Vec<RespValue> = results
                    .into_iter()
                    .map(|r| match r {
                        crate::storage::BitFieldResult::Value(v) => RespValue::Integer(v),
                        crate::storage::BitFieldResult::Nil => RespValue::BulkString(None),
                    })
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::XAdd(key, id, fields, nomkstream, max_len, min_id) => {
                let min_id_parsed = match min_id {
                    Some(s) => Some(crate::storage::StreamId::parse(s.as_str())?),
                    None => None,
                };
                match self.storage.xadd(&key, &id, fields, nomkstream, max_len, min_id_parsed)? {
                    Some(new_id) => Ok(RespValue::BulkString(Some(Bytes::from(new_id)))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::XLen(key) => {
                let len = self.storage.xlen(&key)?;
                Ok(RespValue::Integer(len as i64))
            }
            Command::XRange(key, start, end, count) => {
                let entries = self.storage.xrange(&key, &start, &end, count)?;
                let mut resp_values = Vec::new();
                for (id, fields) in entries {
                    let mut field_values = Vec::new();
                    for (f, v) in fields {
                        field_values.push(RespValue::BulkString(Some(Bytes::from(f))));
                        field_values.push(RespValue::BulkString(Some(Bytes::from(v))));
                    }
                    resp_values.push(RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from(id.to_string()))),
                        RespValue::Array(field_values),
                    ]));
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::XRevRange(key, end, start, count) => {
                let entries = self.storage.xrevrange(&key, &end, &start, count)?;
                let mut resp_values = Vec::new();
                for (id, fields) in entries {
                    let mut field_values = Vec::new();
                    for (f, v) in fields {
                        field_values.push(RespValue::BulkString(Some(Bytes::from(f))));
                        field_values.push(RespValue::BulkString(Some(Bytes::from(v))));
                    }
                    resp_values.push(RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from(id.to_string()))),
                        RespValue::Array(field_values),
                    ]));
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::XTrim(key, strategy, threshold) => {
                let removed = self.storage.xtrim(&key, &strategy, &threshold, false)?;
                Ok(RespValue::Integer(removed as i64))
            }
            Command::XDel(key, ids) => {
                let id_vec: Vec<crate::storage::StreamId> = ids
                    .iter()
                    .map(|s| crate::storage::StreamId::parse(s))
                    .collect::<Result<Vec<_>>>()?;
                let removed = self.storage.xdel(&key, &id_vec)?;
                Ok(RespValue::Integer(removed as i64))
            }
            Command::XRead(keys, ids, count) => {
                let result = self.storage.xread(&keys, &ids, count)?;
                let mut resp_values = Vec::new();
                for (key, entries) in result {
                    let mut stream_entries = Vec::new();
                    for (id, fields) in entries {
                        let mut field_values = Vec::new();
                        for (f, v) in fields {
                            field_values.push(RespValue::BulkString(Some(Bytes::from(f))));
                            field_values.push(RespValue::BulkString(Some(Bytes::from(v))));
                        }
                        stream_entries.push(RespValue::Array(vec![
                            RespValue::BulkString(Some(Bytes::from(id.to_string()))),
                            RespValue::Array(field_values),
                        ]));
                    }
                    resp_values.push(RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from(key))),
                        RespValue::Array(stream_entries),
                    ]));
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::XSetId(key, id) => {
                let sid = crate::storage::StreamId::parse(&id)?;
                let ok = self.storage.xsetid(&key, sid)?;
                if ok {
                    Ok(RespValue::SimpleString("OK".to_string()))
                } else {
                    Ok(RespValue::Error("ERR 键不存在".to_string()))
                }
            }
            Command::XGroupCreate(key, group, id, mkstream) => {
                self.storage.xgroup_create(&key, &group, &id, mkstream)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::XGroupDestroy(key, group) => {
                let removed = self.storage.xgroup_destroy(&key, &group)?;
                Ok(RespValue::Integer(if removed { 1 } else { 0 }))
            }
            Command::XGroupSetId(key, group, id) => {
                self.storage.xgroup_setid(&key, &group, &id)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::XGroupDelConsumer(key, group, consumer) => {
                let pending = self.storage.xgroup_delconsumer(&key, &group, &consumer)?;
                Ok(RespValue::Integer(pending as i64))
            }
            Command::XGroupCreateConsumer(key, group, consumer) => {
                let created = self.storage.xgroup_createconsumer(&key, &group, &consumer)?;
                Ok(RespValue::Integer(if created { 1 } else { 0 }))
            }
            Command::XReadGroup(group, consumer, keys, ids, count, noack) => {
                let result = self.storage.xreadgroup(&group, &consumer, &keys, &ids, count, noack)?;
                let mut resp_values = Vec::new();
                for (key, entries) in result {
                    let mut stream_entries = Vec::new();
                    for (id, fields) in entries {
                        let mut field_values = Vec::new();
                        for (f, v) in fields {
                            field_values.push(RespValue::BulkString(Some(Bytes::from(f))));
                            field_values.push(RespValue::BulkString(Some(Bytes::from(v))));
                        }
                        stream_entries.push(RespValue::Array(vec![
                            RespValue::BulkString(Some(Bytes::from(id.to_string()))),
                            RespValue::Array(field_values),
                        ]));
                    }
                    resp_values.push(RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from(key))),
                        RespValue::Array(stream_entries),
                    ]));
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::XAck(key, group, ids) => {
                let id_vec: Vec<crate::storage::StreamId> = ids
                    .iter()
                    .map(|s| crate::storage::StreamId::parse(s))
                    .collect::<Result<Vec<_>>>()?;
                let acked = self.storage.xack(&key, &group, &id_vec)?;
                Ok(RespValue::Integer(acked as i64))
            }
            Command::XClaim(key, group, consumer, min_idle, ids, justid) => {
                let id_vec: Vec<crate::storage::StreamId> = ids
                    .iter()
                    .map(|s| crate::storage::StreamId::parse(s))
                    .collect::<Result<Vec<_>>>()?;
                let claimed = self.storage.xclaim(&key, &group, &consumer, min_idle, &id_vec, justid)?;
                let mut resp = Vec::new();
                for (id, fields) in claimed {
                    if justid {
                        resp.push(RespValue::BulkString(Some(Bytes::from(id.to_string()))));
                    } else if let Some(flds) = fields {
                        let mut fv = Vec::new();
                        for (f, v) in flds {
                            fv.push(RespValue::BulkString(Some(Bytes::from(f))));
                            fv.push(RespValue::BulkString(Some(Bytes::from(v))));
                        }
                        resp.push(RespValue::Array(vec![
                            RespValue::BulkString(Some(Bytes::from(id.to_string()))),
                            RespValue::Array(fv),
                        ]));
                    }
                }
                Ok(RespValue::Array(resp))
            }
            Command::XAutoClaim(key, group, consumer, min_idle, start, count, justid) => {
                let start_id = crate::storage::StreamId::parse(&start)?;
                let (next_id, claimed) = self.storage.xautoclaim(&key, &group, &consumer, min_idle, start_id, count, justid)?;
                let mut entries = Vec::new();
                for (id, fields) in claimed {
                    if justid {
                        entries.push(RespValue::BulkString(Some(Bytes::from(id.to_string()))));
                    } else if let Some(flds) = fields {
                        let mut fv = Vec::new();
                        for (f, v) in flds {
                            fv.push(RespValue::BulkString(Some(Bytes::from(f))));
                            fv.push(RespValue::BulkString(Some(Bytes::from(v))));
                        }
                        entries.push(RespValue::Array(vec![
                            RespValue::BulkString(Some(Bytes::from(id.to_string()))),
                            RespValue::Array(fv),
                        ]));
                    }
                }
                Ok(RespValue::Array(vec![
                    RespValue::BulkString(Some(Bytes::from(next_id.to_string()))),
                    RespValue::Array(entries),
                    RespValue::Array(vec![]),
                ]))
            }
            Command::XPending(key, group, start, end, count, consumer) => {
                let start_id = match &start {
                    Some(s) => {
                        if s == "-" { Some(crate::storage::StreamId::new(0, 0)) }
                        else { Some(crate::storage::StreamId::parse(s)?) }
                    }
                    None => None,
                };
                let end_id = match &end {
                    Some(s) => {
                        if s == "+" { Some(crate::storage::StreamId::new(u64::MAX, u64::MAX)) }
                        else { Some(crate::storage::StreamId::parse(s)?) }
                    }
                    None => None,
                };
                let (total, min_id, max_id, consumers, details) = self.storage.xpending(
                    &key, &group, start_id, end_id, count, consumer.as_deref(),
                )?;
                if start.is_none() {
                    let mut resp = vec![RespValue::Integer(total as i64)];
                    match min_id {
                        Some(id) => resp.push(RespValue::BulkString(Some(Bytes::from(id.to_string())))),
                        None => resp.push(RespValue::BulkString(None)),
                    }
                    match max_id {
                        Some(id) => resp.push(RespValue::BulkString(Some(Bytes::from(id.to_string())))),
                        None => resp.push(RespValue::BulkString(None)),
                    }
                    if consumers.is_empty() {
                        resp.push(RespValue::BulkString(None));
                    } else {
                        let mut consumer_list = Vec::new();
                        for (name, cnt) in consumers {
                            consumer_list.push(RespValue::Array(vec![
                                RespValue::BulkString(Some(Bytes::from(name))),
                                RespValue::BulkString(Some(Bytes::from(cnt.to_string()))),
                            ]));
                        }
                        resp.push(RespValue::Array(consumer_list));
                    }
                    Ok(RespValue::Array(resp))
                } else {
                    let mut resp = Vec::new();
                    for (id, consumer_name, idle, delivery_count) in details {
                        resp.push(RespValue::Array(vec![
                            RespValue::BulkString(Some(Bytes::from(id.to_string()))),
                            RespValue::BulkString(Some(Bytes::from(consumer_name))),
                            RespValue::Integer(idle as i64),
                            RespValue::Integer(delivery_count as i64),
                        ]));
                    }
                    Ok(RespValue::Array(resp))
                }
            }
            Command::XInfoStream(key, full) => {
                match self.storage.xinfo_stream(&key, full)? {
                    Some((length, groups, last_id, first_id, group_names)) => {
                        let resp = vec![
                            RespValue::BulkString(Some(Bytes::from("length"))),
                            RespValue::Integer(length as i64),
                            RespValue::BulkString(Some(Bytes::from("groups"))),
                            RespValue::Integer(groups as i64),
                            RespValue::BulkString(Some(Bytes::from("last-generated-id"))),
                            RespValue::BulkString(Some(Bytes::from(last_id.to_string()))),
                            RespValue::BulkString(Some(Bytes::from("first-entry"))),
                            RespValue::BulkString(Some(Bytes::from(first_id.to_string()))),
                        ];
                        Ok(RespValue::Array(resp))
                    }
                    None => Ok(RespValue::Error("ERR no such key".to_string())),
                }
            }
            Command::XInfoGroups(key) => {
                let groups = self.storage.xinfo_groups(&key)?;
                let mut resp = Vec::new();
                for (name, consumers, pending, last_id, entries_read) in groups {
                    resp.push(RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from("name"))),
                        RespValue::BulkString(Some(Bytes::from(name))),
                        RespValue::BulkString(Some(Bytes::from("consumers"))),
                        RespValue::Integer(consumers as i64),
                        RespValue::BulkString(Some(Bytes::from("pending"))),
                        RespValue::Integer(pending as i64),
                        RespValue::BulkString(Some(Bytes::from("last-delivered-id"))),
                        RespValue::BulkString(Some(Bytes::from(last_id.to_string()))),
                        RespValue::BulkString(Some(Bytes::from("entries-read"))),
                        RespValue::Integer(entries_read as i64),
                    ]));
                }
                Ok(RespValue::Array(resp))
            }
            Command::XInfoConsumers(key, group) => {
                let consumers = self.storage.xinfo_consumers(&key, &group)?;
                let mut resp = Vec::new();
                for (name, pending, idle, inactive) in consumers {
                    resp.push(RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from("name"))),
                        RespValue::BulkString(Some(Bytes::from(name))),
                        RespValue::BulkString(Some(Bytes::from("pending"))),
                        RespValue::Integer(pending as i64),
                        RespValue::BulkString(Some(Bytes::from("idle"))),
                        RespValue::Integer(idle as i64),
                        RespValue::BulkString(Some(Bytes::from("inactive"))),
                        RespValue::Integer(inactive as i64),
                    ]));
                }
                Ok(RespValue::Array(resp))
            }
            Command::PfAdd(key, elements) => {
                let updated = self.storage.pfadd(&key, &elements)?;
                Ok(RespValue::Integer(updated))
            }
            Command::PfCount(keys) => {
                let count = self.storage.pfcount(&keys)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::PfMerge(destkey, sourcekeys) => {
                self.storage.pfmerge(&destkey, &sourcekeys)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::GeoAdd(key, items) => {
                let count = self.storage.geoadd(&key, items)?;
                Ok(RespValue::Integer(count))
            }
            Command::GeoDist(key, member1, member2, unit) => {
                match self.storage.geodist(&key, &member1, &member2, &unit)? {
                    Some(dist) => {
                        let trimmed = format!("{:.17}", dist).trim_end_matches('0').trim_end_matches('.').to_string();
                        Ok(RespValue::BulkString(Some(Bytes::from(trimmed))))
                    }
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::GeoHash(key, members) => {
                let hashes = self.storage.geohash(&key, &members)?;
                let resp_values: Vec<RespValue> = hashes
                    .into_iter()
                    .map(|h| RespValue::BulkString(h.map(|s| Bytes::from(s))))
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::GeoPos(key, members) => {
                let positions = self.storage.geopos(&key, &members)?;
                let resp_values: Vec<RespValue> = positions
                    .into_iter()
                    .map(|opt| {
                        match opt {
                            Some((lon, lat)) => {
                                let parts = vec![
                                    RespValue::BulkString(Some(Bytes::from(
                                        format!("{:.17}", lon).trim_end_matches('0').trim_end_matches('.').to_string()
                                    ))),
                                    RespValue::BulkString(Some(Bytes::from(
                                        format!("{:.17}", lat).trim_end_matches('0').trim_end_matches('.').to_string()
                                    ))),
                                ];
                                RespValue::Array(parts)
                            }
                            None => RespValue::BulkString(None),
                        }
                    })
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::GeoSearch(key, center_lon, center_lat, by_radius, by_box, order, count, withcoord, withdist, withhash) => {
                let results = self.storage.geosearch(
                    &key, center_lon, center_lat, by_radius, by_box,
                    order.as_deref(), count,
                )?;
                let mut resp_values = Vec::new();
                for (member, dist, lon, lat, hash) in results {
                    let mut item_parts = Vec::new();
                    // 成员名
                    item_parts.push(RespValue::BulkString(Some(Bytes::from(member))));
                    // 距离
                    if withdist {
                        let dist_s = format!("{:.17}", dist / 1000.0);
                        let dist_str = dist_s.trim_end_matches('0').trim_end_matches('.').to_string();
                        item_parts.push(RespValue::BulkString(Some(Bytes::from(dist_str))));
                    }
                    // hash
                    if withhash {
                        item_parts.push(RespValue::Integer(hash as i64));
                    }
                    // 坐标
                    if withcoord {
                        let coord_parts = vec![
                            RespValue::BulkString(Some(Bytes::from(
                                format!("{:.17}", lon).trim_end_matches('0').trim_end_matches('.').to_string()
                            ))),
                            RespValue::BulkString(Some(Bytes::from(
                                format!("{:.17}", lat).trim_end_matches('0').trim_end_matches('.').to_string()
                            ))),
                        ];
                        item_parts.push(RespValue::Array(coord_parts));
                    }
                    if item_parts.len() == 1 {
                        resp_values.push(item_parts.into_iter().next().unwrap());
                    } else {
                        resp_values.push(RespValue::Array(item_parts));
                    }
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::GeoSearchStore(destination, source, center_lon, center_lat, by_radius, by_box, order, count, storedist) => {
                let count_result = self.storage.geosearchstore(
                    &destination, &source, center_lon, center_lat, by_radius, by_box,
                    order.as_deref(), count, storedist,
                )?;
                Ok(RespValue::Integer(count_result as i64))
            }
            Command::Select(index) => {
                self.storage.select(index)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::Auth(_, _) => {
                Err(AppError::Command("AUTH 应在连接层处理".to_string()))
            }
            Command::AclSetUser(username, rules) => {
                let acl = self.acl.as_ref().ok_or_else(|| {
                    AppError::Command("ACL 未启用".to_string())
                })?;
                let rules_ref: Vec<&str> = rules.iter().map(|s| s.as_str()).collect();
                acl.setuser(&username, &rules_ref)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::AclGetUser(username) => {
                let acl = self.acl.as_ref().ok_or_else(|| {
                    AppError::Command("ACL 未启用".to_string())
                })?;
                match acl.getuser(&username)? {
                    Some(user) => {
                        let rules = user.to_rules();
                        let parts: Vec<RespValue> = rules.into_iter()
                            .map(|r| RespValue::BulkString(Some(Bytes::from(r))))
                            .collect();
                        Ok(RespValue::Array(parts))
                    }
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::AclDelUser(names) => {
                let acl = self.acl.as_ref().ok_or_else(|| {
                    AppError::Command("ACL 未启用".to_string())
                })?;
                let names_ref: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
                let count = acl.deluser(&names_ref)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::AclList => {
                let acl = self.acl.as_ref().ok_or_else(|| {
                    AppError::Command("ACL 未启用".to_string())
                })?;
                let list = acl.list()?;
                let parts: Vec<RespValue> = list.into_iter()
                    .map(|s| RespValue::BulkString(Some(Bytes::from(s))))
                    .collect();
                Ok(RespValue::Array(parts))
            }
            Command::AclCat(category) => {
                let acl = self.acl.as_ref().ok_or_else(|| {
                    AppError::Command("ACL 未启用".to_string())
                })?;
                let cmds = acl.cat(category.as_deref())?;
                let parts: Vec<RespValue> = cmds.into_iter()
                    .map(|s| RespValue::BulkString(Some(Bytes::from(s))))
                    .collect();
                Ok(RespValue::Array(parts))
            }
            Command::AclWhoAmI => {
                Ok(RespValue::SimpleString("default".to_string()))
            }
            Command::AclLog(arg) => {
                let acl = self.acl.as_ref().ok_or_else(|| {
                    AppError::Command("ACL 未启用".to_string())
                })?;
                if let Some(arg) = arg {
                    if arg.to_ascii_uppercase() == "RESET" {
                        acl.log_reset()?;
                        Ok(RespValue::SimpleString("OK".to_string()))
                    } else {
                        let count = arg.parse::<usize>().map_err(|_| {
                            AppError::Command("ACL LOG count 必须是整数".to_string())
                        })?;
                        let logs = acl.log(Some(count))?;
                        let parts: Vec<RespValue> = logs.into_iter()
                            .map(|entry| {
                                RespValue::Array(vec![
                                    RespValue::BulkString(Some(Bytes::from("reason"))),
                                    RespValue::BulkString(Some(Bytes::from(entry.reason))),
                                    RespValue::BulkString(Some(Bytes::from("context"))),
                                    RespValue::BulkString(Some(Bytes::from(entry.context))),
                                    RespValue::BulkString(Some(Bytes::from("object"))),
                                    RespValue::BulkString(Some(Bytes::from(entry.object))),
                                    RespValue::BulkString(Some(Bytes::from("username"))),
                                    RespValue::BulkString(Some(Bytes::from(entry.username))),
                                ])
                            })
                            .collect();
                        Ok(RespValue::Array(parts))
                    }
                } else {
                    let logs = acl.log(None)?;
                    let parts: Vec<RespValue> = logs.into_iter()
                        .map(|entry| {
                            RespValue::Array(vec![
                                RespValue::BulkString(Some(Bytes::from("reason"))),
                                RespValue::BulkString(Some(Bytes::from(entry.reason))),
                                RespValue::BulkString(Some(Bytes::from("context"))),
                                RespValue::BulkString(Some(Bytes::from(entry.context))),
                                RespValue::BulkString(Some(Bytes::from("object"))),
                                RespValue::BulkString(Some(Bytes::from(entry.object))),
                                RespValue::BulkString(Some(Bytes::from("username"))),
                                RespValue::BulkString(Some(Bytes::from(entry.username))),
                            ])
                        })
                        .collect();
                    Ok(RespValue::Array(parts))
                }
            }
            Command::AclGenPass(bits) => {
                let acl = self.acl.as_ref().ok_or_else(|| {
                    AppError::Command("ACL 未启用".to_string())
                })?;
                let pass = acl.genpass(bits)?;
                Ok(RespValue::BulkString(Some(Bytes::from(pass))))
            }
            Command::ClientSetName(_) | Command::ClientGetName | Command::ClientList | Command::ClientId
            | Command::ClientInfo | Command::ClientKill { .. } | Command::ClientPause(_, _)
            | Command::ClientUnpause | Command::ClientNoEvict(_) | Command::ClientNoTouch(_)
            | Command::ClientReply(_) | Command::ClientUnblock(_, _) => {
                Err(AppError::Command("CLIENT 应在连接层处理".to_string()))
            }
            Command::Quit => {
                Err(AppError::Command("QUIT 应在连接层处理".to_string()))
            }
            Command::Eval(script, keys, args) => {
                match &self.script_engine {
                    Some(engine) => engine.eval(&script, keys, args, self.storage.clone()),
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::EvalSha(sha1, keys, args) => {
                match &self.script_engine {
                    Some(engine) => engine.evalsha(&sha1, keys, args, self.storage.clone()),
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::ScriptLoad(script) => {
                match &self.script_engine {
                    Some(engine) => {
                        let sha1 = engine.script_load(&script)?;
                        Ok(RespValue::BulkString(Some(Bytes::from(sha1))))
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::ScriptExists(sha1s) => {
                match &self.script_engine {
                    Some(engine) => {
                        let exists = engine.script_exists(&sha1s)?;
                        let arr: Vec<RespValue> = exists.into_iter().map(|b| RespValue::Integer(if b { 1 } else { 0 })).collect();
                        Ok(RespValue::Array(arr))
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::ScriptFlush => {
                match &self.script_engine {
                    Some(engine) => {
                        engine.script_flush()?;
                        Ok(RespValue::SimpleString("OK".to_string()))
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::FunctionLoad(code, replace) => {
                match &self.script_engine {
                    Some(engine) => {
                        let name = engine.function_load(&code, replace)?;
                        Ok(RespValue::SimpleString(name))
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::FunctionDelete(lib) => {
                match &self.script_engine {
                    Some(engine) => {
                        let ok = engine.function_delete(&lib)?;
                        if ok {
                            Ok(RespValue::Integer(1))
                        } else {
                            Ok(RespValue::Integer(0))
                        }
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::FunctionList(pattern, withcode) => {
                match &self.script_engine {
                    Some(engine) => {
                        let list = engine.function_list(pattern.as_deref(), withcode)?;
                        let mut parts = Vec::new();
                        for (name, engine_name, code, funcs) in list {
                            let mut lib_parts = Vec::new();
                            lib_parts.push(RespValue::BulkString(Some(Bytes::from("library_name"))));
                            lib_parts.push(RespValue::BulkString(Some(Bytes::from(name))));
                            lib_parts.push(RespValue::BulkString(Some(Bytes::from("engine"))));
                            lib_parts.push(RespValue::BulkString(Some(Bytes::from(engine_name))));
                            if withcode {
                                lib_parts.push(RespValue::BulkString(Some(Bytes::from("library_code"))));
                                lib_parts.push(RespValue::BulkString(Some(Bytes::from(code))));
                            }
                            lib_parts.push(RespValue::BulkString(Some(Bytes::from("functions"))));
                            let mut func_parts = Vec::new();
                            for (fname, flags) in funcs {
                                let mut f = Vec::new();
                                f.push(RespValue::BulkString(Some(Bytes::from("name"))));
                                f.push(RespValue::BulkString(Some(Bytes::from(fname))));
                                f.push(RespValue::BulkString(Some(Bytes::from("flags"))));
                                let flag_parts: Vec<RespValue> = flags.into_iter()
                                    .map(|s| RespValue::BulkString(Some(Bytes::from(s))))
                                    .collect();
                                f.push(RespValue::Array(flag_parts));
                                func_parts.push(RespValue::Array(f));
                            }
                            lib_parts.push(RespValue::Array(func_parts));
                            parts.push(RespValue::Array(lib_parts));
                        }
                        Ok(RespValue::Array(parts))
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::FunctionDump => {
                match &self.script_engine {
                    Some(engine) => {
                        let dump = engine.function_dump()?;
                        Ok(RespValue::BulkString(Some(Bytes::from(dump))))
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::FunctionRestore(data, policy) => {
                match &self.script_engine {
                    Some(engine) => {
                        engine.function_restore(&data, &policy)?;
                        Ok(RespValue::SimpleString("OK".to_string()))
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::FunctionStats => {
                match &self.script_engine {
                    Some(engine) => {
                        let (libs, funcs) = engine.function_stats()?;
                        let mut parts = Vec::new();
                        parts.push(RespValue::BulkString(Some(Bytes::from("libraries_count"))));
                        parts.push(RespValue::Integer(libs as i64));
                        parts.push(RespValue::BulkString(Some(Bytes::from("functions_count"))));
                        parts.push(RespValue::Integer(funcs as i64));
                        Ok(RespValue::Array(parts))
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::FunctionFlush(async_mode) => {
                match &self.script_engine {
                    Some(engine) => {
                        engine.function_flush(async_mode)?;
                        Ok(RespValue::SimpleString("OK".to_string()))
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::FCall(name, keys, args) => {
                match &self.script_engine {
                    Some(engine) => {
                        let resp = engine.fcall(&name, keys.clone(), args.clone(), self.storage.clone())?;
                        Ok(resp)
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::FCallRO(name, keys, args) => {
                match &self.script_engine {
                    Some(engine) => {
                        let resp = engine.fcall_ro(&name, keys.clone(), args.clone(), self.storage.clone())?;
                        Ok(resp)
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::EvalRO(script, keys, args) => {
                match &self.script_engine {
                    Some(engine) => {
                        let resp = engine.eval(&script, keys.clone(), args.clone(), self.storage.clone())?;
                        Ok(resp)
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::EvalShaRO(sha1, keys, args) => {
                match &self.script_engine {
                    Some(engine) => {
                        let resp = engine.evalsha(&sha1, keys.clone(), args.clone(), self.storage.clone())?;
                        Ok(resp)
                    }
                    None => Err(AppError::Command("脚本引擎未初始化".to_string())),
                }
            }
            Command::Save => {
                Err(AppError::Command("SAVE 应在连接层处理".to_string()))
            }
            Command::BgSave => {
                Err(AppError::Command("BGSAVE 应在连接层处理".to_string()))
            }
            Command::SlowLogGet(count) => {
                match &self.slowlog {
                    Some(log) => {
                        let entries = log.get(count);
                        let arr: Vec<RespValue> = entries.iter()
                            .map(|e| SlowLog::entry_to_resp(e))
                            .collect();
                        Ok(RespValue::Array(arr))
                    }
                    None => Ok(RespValue::Array(vec![])),
                }
            }
            Command::SlowLogLen => {
                match &self.slowlog {
                    Some(log) => Ok(RespValue::Integer(log.len() as i64)),
                    None => Ok(RespValue::Integer(0)),
                }
            }
            Command::SlowLogReset => {
                match &self.slowlog {
                    Some(log) => {
                        log.reset();
                        Ok(RespValue::SimpleString("OK".to_string()))
                    }
                    None => Ok(RespValue::SimpleString("OK".to_string())),
                }
            }
            Command::ObjectEncoding(key) => {
                match self.storage.object_encoding(&key)? {
                    Some(enc) => Ok(RespValue::BulkString(Some(Bytes::from(enc)))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::ObjectRefCount(key) => {
                match self.storage.object_refcount(&key)? {
                    Some(count) => Ok(RespValue::Integer(count)),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::ObjectIdleTime(key) => {
                match self.storage.object_idletime(&key)? {
                    Some(secs) => Ok(RespValue::Integer(secs as i64)),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::ObjectHelp => {
                let help = crate::storage::StorageEngine::object_help();
                let arr: Vec<RespValue> = help
                    .into_iter()
                    .map(|s| RespValue::BulkString(Some(Bytes::from(s))))
                    .collect();
                Ok(RespValue::Array(arr))
            }
            Command::DebugSetActiveExpire(enabled) => {
                self.storage.set_active_expire(enabled);
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::DebugSleep(seconds) => {
                std::thread::sleep(std::time::Duration::from_secs_f64(seconds));
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::DebugObject(key) => {
                let info = self.build_debug_object_info(&key)?;
                Ok(RespValue::SimpleString(info))
            }
            Command::Echo(msg) => {
                Ok(RespValue::BulkString(Some(Bytes::from(msg))))
            }
            Command::Time => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                let secs = now.as_secs() as i64;
                let micros = (now.as_micros() % 1_000_000) as i64;
                Ok(RespValue::Array(vec![
                    RespValue::Integer(secs),
                    RespValue::Integer(micros),
                ]))
            }
            Command::RandomKey => {
                match self.storage.randomkey()? {
                    Some(key) => Ok(RespValue::BulkString(Some(Bytes::from(key)))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::Touch(keys) => {
                let count = self.storage.touch_keys(&keys)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::ExpireAt(key, timestamp) => {
                let ok = self.storage.expire_at(&key, timestamp)?;
                Ok(RespValue::Integer(if ok { 1 } else { 0 }))
            }
            Command::PExpireAt(key, timestamp) => {
                let ok = self.storage.pexpire_at(&key, timestamp)?;
                Ok(RespValue::Integer(if ok { 1 } else { 0 }))
            }
            Command::ExpireTime(key) => {
                Ok(RespValue::Integer(self.storage.expire_time(&key)?))
            }
            Command::PExpireTime(key) => {
                Ok(RespValue::Integer(self.storage.pexpire_time(&key)?))
            }
            Command::RenameNx(key, newkey) => {
                let ok = self.storage.renamenx(&key, &newkey)?;
                Ok(RespValue::Integer(if ok { 1 } else { 0 }))
            }
            Command::SwapDb(idx1, idx2) => {
                self.storage.swap_db(idx1, idx2)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::FlushDb => {
                self.storage.flush_db(self.storage.current_db())?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::Shutdown(_) => {
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::LastSave => {
                Ok(RespValue::Integer(self.storage.get_last_save_time() as i64))
            }
            Command::SubStr(key, start, end) => {
                // SUBSTR 是 GETRANGE 的别名
                match self.storage.getrange(&key, start, end)? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::Lcs(key1, key2, len, idx, _minmatchlen, withmatchlen) => {
                let lcs_str = self.storage.lcs(&key1, &key2)?;
                if len {
                    // LEN 模式：只返回长度
                    let length = lcs_str.as_ref().map(|s| s.len()).unwrap_or(0);
                    Ok(RespValue::Integer(length as i64))
                } else if idx {
                    // IDX 模式：返回匹配位置
                    // 简化实现：返回整个匹配的起点和终点
                    let mut arr = Vec::new();
                    if let Some(s) = lcs_str {
                        let v1 = match self.storage.get(&key1)? {
                            Some(b) => String::from_utf8_lossy(&b).to_string(),
                            None => String::new(),
                        };
                        let v2 = match self.storage.get(&key2)? {
                            Some(b) => String::from_utf8_lossy(&b).to_string(),
                            None => String::new(),
                        };
                        // 找到 LCS 在 v1 和 v2 中的位置
                        if !s.is_empty() && !v1.is_empty() && !v2.is_empty() {
                            let pos1 = v1.find(&s).unwrap_or(0) as i64;
                            let pos2 = v2.find(&s).unwrap_or(0) as i64;
                            let match_len = s.len() as i64;
                            let mut match_arr = Vec::new();
                            let mut m1 = Vec::new();
                            m1.push(RespValue::Integer(pos1));
                            m1.push(RespValue::Integer(pos1 + match_len - 1));
                            let mut m2 = Vec::new();
                            m2.push(RespValue::Integer(pos2));
                            m2.push(RespValue::Integer(pos2 + match_len - 1));
                            match_arr.push(RespValue::Array(m1));
                            match_arr.push(RespValue::Array(m2));
                            if withmatchlen {
                                match_arr.push(RespValue::Integer(match_len));
                            }
                            arr.push(RespValue::Array(match_arr));
                        }
                        arr.push(RespValue::Integer(v1.len() as i64));
                        arr.push(RespValue::Integer(v2.len() as i64));
                    } else {
                        arr.push(RespValue::Array(vec![]));
                        arr.push(RespValue::Integer(0));
                        arr.push(RespValue::Integer(0));
                    }
                    Ok(RespValue::Array(arr))
                } else {
                    // 默认模式：返回最长公共子串
                    match lcs_str {
                        Some(s) => Ok(RespValue::BulkString(Some(Bytes::from(s)))),
                        None => Ok(RespValue::BulkString(None)),
                    }
                }
            }
            Command::Lmove(source, dest, left_from, left_to) => {
                match self.storage.lmove(&source, &dest, left_from, left_to)? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::Rpoplpush(source, dest) => {
                match self.storage.rpoplpush(&source, &dest)? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::Lmpop(keys, left, count) => {
                match self.storage.lmpop(&keys, left, count)? {
                    Some((key, values)) => {
                        let mut arr = Vec::new();
                        arr.push(RespValue::BulkString(Some(Bytes::from(key))));
                        let vals: Vec<RespValue> = values.into_iter()
                            .map(|v| RespValue::BulkString(Some(v)))
                            .collect();
                        arr.push(RespValue::Array(vals));
                        Ok(RespValue::Array(arr))
                    }
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::Sort(key, by_pattern, get_patterns, limit_offset, limit_count, asc, alpha, store_key) => {
                let result = self.storage.sort(
                    &key, by_pattern, get_patterns, limit_offset, limit_count, asc, alpha, store_key.clone(),
                )?;
                if let Some(dest) = store_key {
                    // 有 STORE 时返回存入的元素数量
                    let count = self.storage.llen(&dest)?;
                    Ok(RespValue::Integer(count as i64))
                } else {
                    let resp_values: Vec<RespValue> = result
                        .into_iter()
                        .map(|s| RespValue::BulkString(Some(Bytes::from(s))))
                        .collect();
                    Ok(RespValue::Array(resp_values))
                }
            }
            Command::Unlink(keys) => {
                let count = self.storage.unlink(&keys)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::Copy(source, destination, replace) => {
                let ok = self.storage.copy(&source, &destination, replace)?;
                Ok(RespValue::Integer(if ok { 1 } else { 0 }))
            }
            Command::Dump(key) => {
                match self.storage.dump(&key)? {
                    Some(data) => Ok(RespValue::BulkString(Some(Bytes::from(data)))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::Restore(key, ttl_ms, serialized, replace) => {
                self.storage.restore(&key, ttl_ms, &serialized, replace)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::MGet(keys) => {
                let values = self.storage.mget(&keys)?;
                let resp_values: Vec<RespValue> = values
                    .into_iter()
                    .map(|v| RespValue::BulkString(v))
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::MSet(pairs) => {
                self.storage.mset(&pairs)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::Incr(key) => {
                let new_val = self.storage.incr(&key)?;
                Ok(RespValue::Integer(new_val))
            }
            Command::Decr(key) => {
                let new_val = self.storage.decr(&key)?;
                Ok(RespValue::Integer(new_val))
            }
            Command::IncrBy(key, delta) => {
                let new_val = self.storage.incrby(&key, delta)?;
                Ok(RespValue::Integer(new_val))
            }
            Command::DecrBy(key, delta) => {
                let new_val = self.storage.decrby(&key, delta)?;
                Ok(RespValue::Integer(new_val))
            }
            Command::Append(key, value) => {
                let new_len = self.storage.append(&key, value)?;
                Ok(RespValue::Integer(new_len as i64))
            }
            Command::SetNx(key, value) => {
                let result = if self.storage.setnx(key, value)? {
                    1i64
                } else {
                    0i64
                };
                Ok(RespValue::Integer(result))
            }
            Command::SetExCmd(key, value, seconds) => {
                self.storage.setex(key, seconds, value)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::PSetEx(key, value, ms) => {
                self.storage.psetex(key, ms, value)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::GetSet(key, value) => {
                match self.storage.getset(&key, value)? {
                    Some(data) => Ok(RespValue::BulkString(Some(data))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::GetDel(key) => {
                match self.storage.getdel(&key)? {
                    Some(data) => Ok(RespValue::BulkString(Some(data))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::GetEx(key, opt) => {
                match self.storage.getex(&key, opt)? {
                    Some(data) => Ok(RespValue::BulkString(Some(data))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::MSetNx(pairs) => {
                let result = self.storage.msetnx(&pairs)?;
                Ok(RespValue::Integer(result))
            }
            Command::IncrByFloat(key, delta) => {
                let new_val = self.storage.incrbyfloat(&key, delta)?;
                Ok(RespValue::BulkString(Some(Bytes::from(new_val))))
            }
            Command::SetRange(key, offset, value) => {
                let new_len = self.storage.setrange(&key, offset, value)?;
                Ok(RespValue::Integer(new_len as i64))
            }
            Command::GetRange(key, start, end) => {
                match self.storage.getrange(&key, start, end)? {
                    Some(data) => Ok(RespValue::BulkString(Some(data))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::StrLen(key) => {
                let len = self.storage.strlen(&key)?;
                Ok(RespValue::Integer(len as i64))
            }
            Command::LPush(key, values) => {
                let len = self.storage.lpush(&key, values)?;
                Ok(RespValue::Integer(len as i64))
            }
            Command::RPush(key, values) => {
                let len = self.storage.rpush(&key, values)?;
                Ok(RespValue::Integer(len as i64))
            }
            Command::LPop(key) => {
                match self.storage.lpop(&key)? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::RPop(key) => {
                match self.storage.rpop(&key)? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::LLen(key) => {
                let len = self.storage.llen(&key)?;
                Ok(RespValue::Integer(len as i64))
            }
            Command::LRange(key, start, stop) => {
                let values = self.storage.lrange(&key, start, stop)?;
                let resp_values: Vec<RespValue> = values
                    .into_iter()
                    .map(|v| RespValue::BulkString(Some(v)))
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::LIndex(key, index) => {
                match self.storage.lindex(&key, index)? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::LSet(key, index, value) => {
                self.storage.lset(&key, index, value)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::LInsert(key, pos, pivot, value) => {
                let result = self.storage.linsert(&key, pos, pivot, value)?;
                Ok(RespValue::Integer(result))
            }
            Command::LRem(key, count, value) => {
                let removed = self.storage.lrem(&key, count, value)?;
                Ok(RespValue::Integer(removed))
            }
            Command::LTrim(key, start, stop) => {
                self.storage.ltrim(&key, start, stop)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::LPos(key, value, rank, count, maxlen) => {
                let positions = self.storage.lpos(&key, value, rank, count, maxlen)?;
                if count == 0 {
                    // 不指定 COUNT，返回第一个匹配位置或 nil
                    if let Some(&pos) = positions.first() {
                        Ok(RespValue::Integer(pos))
                    } else {
                        Ok(RespValue::BulkString(None))
                    }
                } else {
                    // 指定了 COUNT，返回位置数组
                    let arr: Vec<RespValue> = positions.into_iter().map(|p| RespValue::Integer(p)).collect();
                    Ok(RespValue::Array(arr))
                }
            }
            Command::BLPop(keys, _timeout) => {
                // 非阻塞版本：直接尝试弹出（用于事务和 AOF 重放）
                for key in &keys {
                    if let Ok(Some(value)) = self.storage.lpop(key) {
                        // 记录等效 LPOP 到 AOF
                        let lpop_cmd = Command::LPop(key.clone());
                        self.append_to_aof(&lpop_cmd);
                        return Ok(RespValue::Array(vec![
                            RespValue::BulkString(Some(bytes::Bytes::from(key.clone()))),
                            RespValue::BulkString(Some(value)),
                        ]));
                    }
                }
                Ok(RespValue::BulkString(None))
            }
            Command::BRPop(keys, _timeout) => {
                // 非阻塞版本：直接尝试弹出（用于事务和 AOF 重放）
                for key in &keys {
                    if let Ok(Some(value)) = self.storage.rpop(key) {
                        // 记录等效 RPOP 到 AOF
                        let rpop_cmd = Command::RPop(key.clone());
                        self.append_to_aof(&rpop_cmd);
                        return Ok(RespValue::Array(vec![
                            RespValue::BulkString(Some(bytes::Bytes::from(key.clone()))),
                            RespValue::BulkString(Some(value)),
                        ]));
                    }
                }
                Ok(RespValue::BulkString(None))
            }
            Command::BLmove(source, dest, left_from, left_to, _timeout) => {
                // 非阻塞版本：直接尝试移动（用于事务和 AOF 重放）
                if let Ok(Some(value)) = self.storage.lmove(&source, &dest, left_from, left_to) {
                    self.append_to_aof(&Command::Lmove(source.clone(), dest.clone(), left_from, left_to));
                    Ok(RespValue::BulkString(Some(value)))
                } else {
                    Ok(RespValue::BulkString(None))
                }
            }
            Command::BLmpop(keys, left, count, _timeout) => {
                // 非阻塞版本：直接尝试弹出（用于事务和 AOF 重放）
                if let Ok(Some((key, values))) = self.storage.lmpop(&keys, left, count) {
                    self.append_to_aof(&Command::Lmpop(keys.clone(), left, count));
                    let mut arr = Vec::new();
                    arr.push(RespValue::BulkString(Some(bytes::Bytes::from(key))));
                    let vals: Vec<RespValue> = values.into_iter()
                        .map(|v| RespValue::BulkString(Some(v)))
                        .collect();
                    arr.push(RespValue::Array(vals));
                    Ok(RespValue::Array(arr))
                } else {
                    Ok(RespValue::BulkString(None))
                }
            }
            Command::BRpoplpush(source, dest, _timeout) => {
                // 非阻塞版本：直接尝试移动（用于事务和 AOF 重放）
                if let Ok(Some(value)) = self.storage.rpoplpush(&source, &dest) {
                    self.append_to_aof(&Command::Rpoplpush(source.clone(), dest.clone()));
                    Ok(RespValue::BulkString(Some(value)))
                } else {
                    Ok(RespValue::BulkString(None))
                }
            }
            Command::HSet(key, pairs) => {
                let mut count = 0i64;
                for (field, value) in pairs {
                    count += self.storage.hset(&key, field, value)?;
                }
                Ok(RespValue::Integer(count))
            }
            Command::HGet(key, field) => {
                match self.storage.hget(&key, &field)? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::HDel(key, fields) => {
                let count = self.storage.hdel(&key, &fields)?;
                Ok(RespValue::Integer(count))
            }
            Command::HExists(key, field) => {
                let result = if self.storage.hexists(&key, &field)? {
                    1i64
                } else {
                    0i64
                };
                Ok(RespValue::Integer(result))
            }
            Command::HGetAll(key) => {
                let pairs = self.storage.hgetall(&key)?;
                let mut resp_values = Vec::new();
                for (field, value) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(field))));
                    resp_values.push(RespValue::BulkString(Some(value)));
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::HLen(key) => {
                let len = self.storage.hlen(&key)?;
                Ok(RespValue::Integer(len as i64))
            }
            Command::HMSet(key, pairs) => {
                self.storage.hmset(&key, &pairs)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::HMGet(key, fields) => {
                let values = self.storage.hmget(&key, &fields)?;
                let resp_values: Vec<RespValue> = values
                    .into_iter()
                    .map(|v| RespValue::BulkString(v))
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::HIncrBy(key, field, delta) => {
                let new_val = self.storage.hincrby(&key, field, delta)?;
                Ok(RespValue::Integer(new_val))
            }
            Command::HIncrByFloat(key, field, delta) => {
                let new_val = self.storage.hincrbyfloat(&key, field, delta)?;
                Ok(RespValue::BulkString(Some(Bytes::from(new_val))))
            }
            Command::HKeys(key) => {
                let keys = self.storage.hkeys(&key)?;
                let resp_values: Vec<RespValue> = keys.into_iter().map(|k| RespValue::BulkString(Some(Bytes::from(k)))).collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::HVals(key) => {
                let vals = self.storage.hvals(&key)?;
                let resp_values: Vec<RespValue> = vals.into_iter().map(|v| RespValue::BulkString(Some(v))).collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::HSetNx(key, field, value) => {
                let result = self.storage.hsetnx(&key, field, value)?;
                Ok(RespValue::Integer(result))
            }
            Command::HRandField(key, count, with_values) => {
                let result = self.storage.hrandfield(&key, count, with_values)?;
                if count == 1 && !with_values {
                    // 单字段不带值，返回单个 BulkString
                    if let Some((field, _)) = result.first() {
                        Ok(RespValue::BulkString(Some(Bytes::from(field.clone()))))
                    } else {
                        Ok(RespValue::BulkString(None))
                    }
                } else {
                    let mut parts = Vec::new();
                    for (field, value) in result {
                        parts.push(RespValue::BulkString(Some(Bytes::from(field))));
                        if let Some(v) = value {
                            parts.push(RespValue::BulkString(Some(v)));
                        }
                    }
                    Ok(RespValue::Array(parts))
                }
            }
            Command::HScan(key, cursor, pattern, count) => {
                let (new_cursor, fields) = self.storage.hscan(&key, cursor, &pattern, count)?;
                let mut parts: Vec<RespValue> = vec![
                    RespValue::BulkString(Some(Bytes::from(new_cursor.to_string()))),
                ];
                let field_values: Vec<RespValue> = fields
                    .into_iter()
                    .flat_map(|(f, v)| {
                        vec![
                            RespValue::BulkString(Some(Bytes::from(f))),
                            RespValue::BulkString(Some(v)),
                        ]
                    })
                    .collect();
                parts.push(RespValue::Array(field_values));
                Ok(RespValue::Array(parts))
            }
            Command::HExpire(key, fields, seconds) => {
                let result = self.storage.hexpire(&key, &fields, seconds)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(|v| RespValue::Integer(v))
                    .collect();
                Ok(RespValue::Array(arr))
            }
            Command::HPExpire(key, fields, ms) => {
                let result = self.storage.hpexpire(&key, &fields, ms)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(|v| RespValue::Integer(v))
                    .collect();
                Ok(RespValue::Array(arr))
            }
            Command::HExpireAt(key, fields, ts) => {
                let result = self.storage.hexpireat(&key, &fields, ts)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(|v| RespValue::Integer(v))
                    .collect();
                Ok(RespValue::Array(arr))
            }
            Command::HPExpireAt(key, fields, ts) => {
                let result = self.storage.hpexpireat(&key, &fields, ts)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(|v| RespValue::Integer(v))
                    .collect();
                Ok(RespValue::Array(arr))
            }
            Command::HTtl(key, fields) => {
                let result = self.storage.httl(&key, &fields)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(|v| RespValue::Integer(v))
                    .collect();
                Ok(RespValue::Array(arr))
            }
            Command::HPTtl(key, fields) => {
                let result = self.storage.hpttl(&key, &fields)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(|v| RespValue::Integer(v))
                    .collect();
                Ok(RespValue::Array(arr))
            }
            Command::HExpireTime(key, fields) => {
                let result = self.storage.hexpiretime(&key, &fields)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(|v| RespValue::Integer(v))
                    .collect();
                Ok(RespValue::Array(arr))
            }
            Command::HPExpireTime(key, fields) => {
                let result = self.storage.hpexpiretime(&key, &fields)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(|v| RespValue::Integer(v))
                    .collect();
                Ok(RespValue::Array(arr))
            }
            Command::HPersist(key, fields) => {
                let result = self.storage.hpersist(&key, &fields)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(|v| RespValue::Integer(v))
                    .collect();
                Ok(RespValue::Array(arr))
            }
            Command::SAdd(key, members) => {
                let count = self.storage.sadd(&key, members)?;
                Ok(RespValue::Integer(count))
            }
            Command::SRem(key, members) => {
                let count = self.storage.srem(&key, &members)?;
                Ok(RespValue::Integer(count))
            }
            Command::SMembers(key) => {
                let members = self.storage.smembers(&key)?;
                let resp_values: Vec<RespValue> = members
                    .into_iter()
                    .map(|m| RespValue::BulkString(Some(m)))
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::SIsMember(key, member) => {
                let result = if self.storage.sismember(&key, &member)? {
                    1i64
                } else {
                    0i64
                };
                Ok(RespValue::Integer(result))
            }
            Command::SCard(key) => {
                let len = self.storage.scard(&key)?;
                Ok(RespValue::Integer(len as i64))
            }
            Command::SInter(keys) => {
                let members = self.storage.sinter(&keys)?;
                let resp_values: Vec<RespValue> = members
                    .into_iter()
                    .map(|m| RespValue::BulkString(Some(m)))
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::SUnion(keys) => {
                let members = self.storage.sunion(&keys)?;
                let resp_values: Vec<RespValue> = members
                    .into_iter()
                    .map(|m| RespValue::BulkString(Some(m)))
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::SDiff(keys) => {
                let members = self.storage.sdiff(&keys)?;
                let resp_values: Vec<RespValue> = members
                    .into_iter()
                    .map(|m| RespValue::BulkString(Some(m)))
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::SPop(key, count) => {
                let members = self.storage.spop(&key, count)?;
                if count == 1 {
                    if let Some(m) = members.into_iter().next() {
                        Ok(RespValue::BulkString(Some(m)))
                    } else {
                        Ok(RespValue::BulkString(None))
                    }
                } else {
                    let resp_values: Vec<RespValue> = members
                        .into_iter()
                        .map(|m| RespValue::BulkString(Some(m)))
                        .collect();
                    Ok(RespValue::Array(resp_values))
                }
            }
            Command::SRandMember(key, count) => {
                let members = self.storage.srandmember(&key, count)?;
                if count == 1 {
                    if let Some(m) = members.into_iter().next() {
                        Ok(RespValue::BulkString(Some(m)))
                    } else {
                        Ok(RespValue::BulkString(None))
                    }
                } else {
                    let resp_values: Vec<RespValue> = members
                        .into_iter()
                        .map(|m| RespValue::BulkString(Some(m)))
                        .collect();
                    Ok(RespValue::Array(resp_values))
                }
            }
            Command::SMove(source, destination, member) => {
                let result = if self.storage.smove(&source, &destination, member)? {
                    1i64
                } else {
                    0i64
                };
                Ok(RespValue::Integer(result))
            }
            Command::SInterStore(destination, keys) => {
                let count = self.storage.sinterstore(&destination, &keys)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::SUnionStore(destination, keys) => {
                let count = self.storage.sunionstore(&destination, &keys)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::SDiffStore(destination, keys) => {
                let count = self.storage.sdiffstore(&destination, &keys)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::SScan(key, cursor, pattern, count) => {
                let (new_cursor, members) = self.storage.sscan(&key, cursor, &pattern, count)?;
                let mut parts: Vec<RespValue> = vec![
                    RespValue::BulkString(Some(Bytes::from(new_cursor.to_string()))),
                ];
                let member_values: Vec<RespValue> = members
                    .into_iter()
                    .map(|m| RespValue::BulkString(Some(m)))
                    .collect();
                parts.push(RespValue::Array(member_values));
                Ok(RespValue::Array(parts))
            }
            Command::ZAdd(key, pairs) => {
                let count = self.storage.zadd(&key, pairs)?;
                Ok(RespValue::Integer(count))
            }
            Command::ZRem(key, members) => {
                let count = self.storage.zrem(&key, &members)?;
                Ok(RespValue::Integer(count))
            }
            Command::ZScore(key, member) => {
                match self.storage.zscore(&key, &member)? {
                    Some(score) => {
                        Ok(RespValue::BulkString(Some(Bytes::from(
                            format!("{}", score),
                        ))))
                    }
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::ZRank(key, member) => {
                match self.storage.zrank(&key, &member)? {
                    Some(rank) => Ok(RespValue::Integer(rank as i64)),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::ZRange(key, start, stop, with_scores) => {
                let pairs = self.storage.zrange(&key, start, stop, with_scores)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(
                            format!("{}", score),
                        ))));
                    }
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::ZRangeByScore(key, min, max, with_scores) => {
                let pairs = self.storage.zrangebyscore(&key, min, max, with_scores)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(
                            format!("{}", score),
                        ))));
                    }
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::ZCard(key) => {
                let len = self.storage.zcard(&key)?;
                Ok(RespValue::Integer(len as i64))
            }
            Command::ZRevRange(key, start, stop, with_scores) => {
                let pairs = self.storage.zrevrange(&key, start, stop, with_scores)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(
                            format!("{}", score),
                        ))));
                    }
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::ZRevRank(key, member) => {
                match self.storage.zrevrank(&key, &member)? {
                    Some(rank) => Ok(RespValue::Integer(rank as i64)),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::ZIncrBy(key, increment, member) => {
                let new_score = self.storage.zincrby(&key, increment, member)?;
                Ok(RespValue::BulkString(Some(Bytes::from(new_score))))
            }
            Command::ZCount(key, min, max) => {
                let count = self.storage.zcount(&key, min, max)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::ZPopMin(key, count) => {
                let pairs = self.storage.zpopmin(&key, count)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(
                        format!("{}", score),
                    ))));
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::ZPopMax(key, count) => {
                let pairs = self.storage.zpopmax(&key, count)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(
                        format!("{}", score),
                    ))));
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::ZUnionStore(destination, keys, weights, aggregate) => {
                let weights_slice = if weights.is_empty() { None } else { Some(weights.as_slice()) };
                let count = self.storage.zunionstore(&destination, &keys, weights_slice, &aggregate)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::ZInterStore(destination, keys, weights, aggregate) => {
                let weights_slice = if weights.is_empty() { None } else { Some(weights.as_slice()) };
                let count = self.storage.zinterstore(&destination, &keys, weights_slice, &aggregate)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::ZScan(key, cursor, pattern, count) => {
                let (next_cursor, items) = self.storage.zscan(&key, cursor, &pattern, count)?;
                let mut resp_values = Vec::new();
                resp_values.push(RespValue::BulkString(Some(Bytes::from(
                    next_cursor.to_string(),
                ))));
                let mut item_values = Vec::new();
                for (member, score) in items {
                    item_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    item_values.push(RespValue::BulkString(Some(Bytes::from(
                        format!("{}", score),
                    ))));
                }
                resp_values.push(RespValue::Array(item_values));
                Ok(RespValue::Array(resp_values))
            }
            Command::ZRangeByLex(key, min, max) => {
                let members = self.storage.zrangebylex(&key, &min, &max)?;
                let resp_values: Vec<RespValue> = members
                    .into_iter()
                    .map(|m| RespValue::BulkString(Some(Bytes::from(m))))
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::SInterCard(keys, limit) => {
                let count = self.storage.sintercard(&keys, limit)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::SMisMember(key, members) => {
                let result = self.storage.smismember(&key, &members)?;
                let arr: Vec<RespValue> = result.into_iter().map(|v| RespValue::Integer(v)).collect();
                Ok(RespValue::Array(arr))
            }
            Command::ZRandMember(key, count, with_scores) => {
                let pairs = self.storage.zrandmember(&key, count, with_scores)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(format!("{}", score)))));
                    }
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::ZDiff(keys, with_scores) => {
                let pairs = self.storage.zdiff(&keys, with_scores)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(format!("{}", score)))));
                    }
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::ZDiffStore(destination, keys) => {
                let count = self.storage.zdiffstore(&destination, &keys)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::ZInter(keys, weights, aggregate, with_scores) => {
                let weights_slice = if weights.is_empty() { None } else { Some(weights.as_slice()) };
                let pairs = self.storage.zinter(&keys, weights_slice, &aggregate, with_scores)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(format!("{}", score)))));
                    }
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::ZUnion(keys, weights, aggregate, with_scores) => {
                let weights_slice = if weights.is_empty() { None } else { Some(weights.as_slice()) };
                let pairs = self.storage.zunion(&keys, weights_slice, &aggregate, with_scores)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(format!("{}", score)))));
                    }
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::ZRangeStore(dst, src, min, max, by_score, by_lex, rev, limit_offset, limit_count) => {
                let count = self.storage.zrangestore(&dst, &src, &min, &max, by_score, by_lex, rev, limit_offset, limit_count)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::ZMpop(keys, min_or_max, count) => {
                match self.storage.zmpop(&keys, min_or_max, count)? {
                    Some((key, pairs)) => {
                        let mut pair_values = Vec::new();
                        for (member, score) in pairs {
                            pair_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                            pair_values.push(RespValue::BulkString(Some(Bytes::from(format!("{}", score)))));
                        }
                        let resp = RespValue::Array(vec![
                            RespValue::BulkString(Some(Bytes::from(key))),
                            RespValue::Array(pair_values),
                        ]);
                        Ok(resp)
                    }
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::ZRevRangeByScore(key, max, min, with_scores, limit_offset, limit_count) => {
                let pairs = self.storage.zrevrangebyscore(&key, max, min, with_scores, limit_offset, limit_count)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(format!("{}", score)))));
                    }
                }
                Ok(RespValue::Array(resp_values))
            }
            Command::ZRevRangeByLex(key, max, min, limit_offset, limit_count) => {
                let members = self.storage.zrevrangebylex(&key, &max, &min, limit_offset, limit_count)?;
                let resp_values: Vec<RespValue> = members
                    .into_iter()
                    .map(|m| RespValue::BulkString(Some(Bytes::from(m))))
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::ZMScore(key, members) => {
                let scores = self.storage.zmscore(&key, &members)?;
                let resp_values: Vec<RespValue> = scores
                    .into_iter()
                    .map(|s| match s {
                        Some(score) => RespValue::BulkString(Some(Bytes::from(format!("{}", score)))),
                        None => RespValue::BulkString(None),
                    })
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::ZLexCount(key, min, max) => {
                let count = self.storage.zlexcount(&key, &min, &max)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::ZRangeUnified(key, min, max, by_score, by_lex, rev, with_scores, limit_offset, limit_count) => {
                let pairs = self.storage.zrange_unified(&key, &min, &max, by_score, by_lex, rev, with_scores, limit_offset, limit_count)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(format!("{}", score)))));
                    }
                }
                Ok(RespValue::Array(resp_values))
            }
            // 阻塞命令在非阻塞上下文中由 server.rs 处理
            Command::BZMpop(_, _, _, _) | Command::BZPopMin(_, _) | Command::BZPopMax(_, _) => {
                Ok(RespValue::BulkString(None))
            }
            Command::Keys(pattern) => {
                let keys = self.storage.keys(&pattern)?;
                let resp_values: Vec<RespValue> = keys
                    .into_iter()
                    .map(|k| RespValue::BulkString(Some(Bytes::from(k))))
                    .collect();
                Ok(RespValue::Array(resp_values))
            }
            Command::Scan(cursor, pattern, count) => {
                let (next_cursor, keys) = self.storage.scan(cursor, &pattern, count)?;
                let mut resp_values = Vec::new();
                resp_values.push(RespValue::BulkString(Some(Bytes::from(
                    next_cursor.to_string(),
                ))));
                let key_values: Vec<RespValue> = keys
                    .into_iter()
                    .map(|k| RespValue::BulkString(Some(Bytes::from(k))))
                    .collect();
                resp_values.push(RespValue::Array(key_values));
                Ok(RespValue::Array(resp_values))
            }
            Command::Rename(key, newkey) => {
                self.storage.rename(&key, &newkey)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::Type(key) => {
                let key_type = self.storage.key_type(&key)?;
                Ok(RespValue::SimpleString(key_type))
            }
            Command::Persist(key) => {
                let removed = self.storage.persist(&key)?;
                Ok(RespValue::Integer(if removed { 1 } else { 0 }))
            }
            Command::PExpire(key, ms) => {
                let success = self.storage.pexpire(&key, ms)?;
                Ok(RespValue::Integer(if success { 1 } else { 0 }))
            }
            Command::PTtl(key) => {
                let ttl_ms = self.storage.pttl(&key)?;
                Ok(RespValue::Integer(ttl_ms))
            }
            Command::DbSize => {
                let size = self.storage.dbsize()?;
                Ok(RespValue::Integer(size as i64))
            }
            Command::Info(section) => {
                let info = self.storage.info(section.as_deref())?;
                Ok(RespValue::BulkString(Some(Bytes::from(info))))
            }
            Command::Subscribe(_) | Command::Unsubscribe(_) | Command::PSubscribe(_) | Command::PUnsubscribe(_) => {
                // Pub/Sub 命令在 server.rs 中直接处理，不应到达此处
                Err(AppError::Command("pub/sub 命令应在连接层处理".to_string()))
            }
            Command::Publish(channel, message) => {
                // PUBLISH 需要 PubSubManager，但当前执行器未持有它。
                // 为保持兼容，返回提示性错误（实际在 server.rs 中处理）
                Err(AppError::Command(format!(
                    "PUBLISH 命令应在连接层处理 (channel={}, message={})",
                    channel,
                    String::from_utf8_lossy(&message),
                )))
            }
            Command::Multi | Command::Exec | Command::Discard | Command::Watch(_) => {
                // 事务命令在 server.rs 中直接处理，不应到达此处
                Err(AppError::Command("事务命令应在连接层处理".to_string()))
            }
            Command::Unknown(cmd_name) => {
                Ok(RespValue::Error(format!(
                    "ERR unknown command '{}'",
                    cmd_name,
                )))
            }
        }
    }
}

/// 提取命令的名称和参数列表（用于慢查询日志）
pub(crate) fn extract_cmd_info(cmd: &Command) -> (String, Vec<String>) {
    match cmd {
        Command::Get(key) => ("GET".to_string(), vec![key.clone()]),
        Command::Set(key, _, _) => ("SET".to_string(), vec![key.clone()]),
        Command::Del(keys) => ("DEL".to_string(), keys.clone()),
        Command::Exists(keys) => ("EXISTS".to_string(), keys.clone()),
        Command::Ping(msg) => ("PING".to_string(), msg.clone().into_iter().collect()),
        Command::FlushAll => ("FLUSHALL".to_string(), vec![]),
        Command::Expire(key, _) => ("EXPIRE".to_string(), vec![key.clone()]),
        Command::Ttl(key) => ("TTL".to_string(), vec![key.clone()]),
        Command::ConfigGet(key) => ("CONFIG".to_string(), vec!["GET".to_string(), key.clone()]),
        Command::ConfigSet(key, value) => ("CONFIG".to_string(), vec!["SET".to_string(), key.clone(), value.clone()]),
        Command::ConfigRewrite => ("CONFIG".to_string(), vec!["REWRITE".to_string()]),
        Command::ConfigResetStat => ("CONFIG".to_string(), vec!["RESETSTAT".to_string()]),
        Command::MemoryUsage(key, _) => ("MEMORY USAGE".to_string(), vec![key.clone()]),
        Command::MemoryDoctor => ("MEMORY DOCTOR".to_string(), vec![]),
        Command::LatencyLatest => ("LATENCY LATEST".to_string(), vec![]),
        Command::LatencyHistory(event) => ("LATENCY HISTORY".to_string(), vec![event.clone()]),
        Command::LatencyReset(events) => ("LATENCY RESET".to_string(), events.clone()),
        Command::Reset => ("RESET".to_string(), vec![]),
        Command::Hello(_, _, _) => ("HELLO".to_string(), vec![]),
        Command::Monitor => ("MONITOR".to_string(), vec![]),
        Command::CommandInfo => ("COMMAND".to_string(), vec![]),
        Command::MGet(keys) => ("MGET".to_string(), keys.clone()),
        Command::MSet(pairs) => ("MSET".to_string(), pairs.iter().map(|(k, _)| k.clone()).collect()),
        Command::Incr(key) => ("INCR".to_string(), vec![key.clone()]),
        Command::Decr(key) => ("DECR".to_string(), vec![key.clone()]),
        Command::IncrBy(key, _) => ("INCRBY".to_string(), vec![key.clone()]),
        Command::DecrBy(key, _) => ("DECRBY".to_string(), vec![key.clone()]),
        Command::Append(key, _) => ("APPEND".to_string(), vec![key.clone()]),
        Command::SetNx(key, _) => ("SETNX".to_string(), vec![key.clone()]),
        Command::SetExCmd(key, _, _) => ("SETEX".to_string(), vec![key.clone()]),
        Command::PSetEx(key, _, _) => ("PSETEX".to_string(), vec![key.clone()]),
        Command::GetSet(key, _) => ("GETSET".to_string(), vec![key.clone()]),
        Command::GetDel(key) => ("GETDEL".to_string(), vec![key.clone()]),
        Command::GetEx(key, _) => ("GETEX".to_string(), vec![key.clone()]),
        Command::MSetNx(pairs) => ("MSETNX".to_string(), pairs.iter().map(|(k, _)| k.clone()).collect()),
        Command::IncrByFloat(key, _) => ("INCRBYFLOAT".to_string(), vec![key.clone()]),
        Command::SetRange(key, _, _) => ("SETRANGE".to_string(), vec![key.clone()]),
        Command::GetRange(key, _, _) => ("GETRANGE".to_string(), vec![key.clone()]),
        Command::StrLen(key) => ("STRLEN".to_string(), vec![key.clone()]),
        Command::LPush(key, _) => ("LPUSH".to_string(), vec![key.clone()]),
        Command::RPush(key, _) => ("RPUSH".to_string(), vec![key.clone()]),
        Command::LPop(key) => ("LPOP".to_string(), vec![key.clone()]),
        Command::RPop(key) => ("RPOP".to_string(), vec![key.clone()]),
        Command::LLen(key) => ("LLEN".to_string(), vec![key.clone()]),
        Command::LRange(key, _, _) => ("LRANGE".to_string(), vec![key.clone()]),
        Command::LIndex(key, _) => ("LINDEX".to_string(), vec![key.clone()]),
        Command::LSet(key, _, _) => ("LSET".to_string(), vec![key.clone()]),
        Command::LInsert(key, _, _, _) => ("LINSERT".to_string(), vec![key.clone()]),
        Command::LRem(key, _, _) => ("LREM".to_string(), vec![key.clone()]),
        Command::LTrim(key, _, _) => ("LTRIM".to_string(), vec![key.clone()]),
        Command::LPos(key, _, _, _, _) => ("LPOS".to_string(), vec![key.clone()]),
        Command::BLPop(keys, _) => ("BLPOP".to_string(), keys.clone()),
        Command::BRPop(keys, _) => ("BRPOP".to_string(), keys.clone()),
        Command::HSet(key, _) => ("HSET".to_string(), vec![key.clone()]),
        Command::HGet(key, field) => ("HGET".to_string(), vec![key.clone(), field.clone()]),
        Command::HDel(key, fields) => ("HDEL".to_string(), vec![key.clone(), fields.join(" ")]),
        Command::HExists(key, field) => ("HEXISTS".to_string(), vec![key.clone(), field.clone()]),
        Command::HGetAll(key) => ("HGETALL".to_string(), vec![key.clone()]),
        Command::HKeys(key) => ("HKEYS".to_string(), vec![key.clone()]),
        Command::HVals(key) => ("HVALS".to_string(), vec![key.clone()]),
        Command::HLen(key) => ("HLEN".to_string(), vec![key.clone()]),
        Command::HMGet(key, fields) => ("HMGET".to_string(), vec![key.clone(), fields.join(" ")]),
        Command::HMSet(key, _) => ("HMSET".to_string(), vec![key.clone()]),
        Command::HIncrBy(key, field, _) => ("HINCRBY".to_string(), vec![key.clone(), field.clone()]),
        Command::HIncrByFloat(key, field, _) => ("HINCRBYFLOAT".to_string(), vec![key.clone(), field.clone()]),
        Command::HSetNx(key, field, _) => ("HSETNX".to_string(), vec![key.clone(), field.clone()]),
        Command::SAdd(key, _) => ("SADD".to_string(), vec![key.clone()]),
        Command::SRem(key, _) => ("SREM".to_string(), vec![key.clone()]),
        Command::SIsMember(key, _) => ("SISMEMBER".to_string(), vec![key.clone()]),
        Command::SCard(key) => ("SCARD".to_string(), vec![key.clone()]),
        Command::SMembers(key) => ("SMEMBERS".to_string(), vec![key.clone()]),
        Command::SUnion(keys) => ("SUNION".to_string(), keys.clone()),
        Command::SInter(keys) => ("SINTER".to_string(), keys.clone()),
        Command::SDiff(keys) => ("SDIFF".to_string(), keys.clone()),
        Command::SPop(key, _) => ("SPOP".to_string(), vec![key.clone()]),
        Command::SRandMember(key, _) => ("SRANDMEMBER".to_string(), vec![key.clone()]),
        Command::SMove(source, dest, _) => ("SMOVE".to_string(), vec![source.clone(), dest.clone()]),
        Command::SUnionStore(dest, keys) => ("SUNIONSTORE".to_string(), vec![dest.clone(), keys.join(" ")]),
        Command::SInterStore(dest, keys) => ("SINTERSTORE".to_string(), vec![dest.clone(), keys.join(" ")]),
        Command::SDiffStore(dest, keys) => ("SDIFFSTORE".to_string(), vec![dest.clone(), keys.join(" ")]),
        Command::SScan(key, _, _, _) => ("SSCAN".to_string(), vec![key.clone()]),
        Command::ZAdd(key, _) => ("ZADD".to_string(), vec![key.clone()]),
        Command::ZRem(key, members) => ("ZREM".to_string(), vec![key.clone(), members.join(" ")]),
        Command::ZCard(key) => ("ZCARD".to_string(), vec![key.clone()]),
        Command::ZRange(key, _, _, _) => ("ZRANGE".to_string(), vec![key.clone()]),
        Command::ZRevRange(key, _, _, _) => ("ZREVRANGE".to_string(), vec![key.clone()]),
        Command::ZRank(key, member) => ("ZRANK".to_string(), vec![key.clone(), member.clone()]),
        Command::ZRevRank(key, member) => ("ZREVRANK".to_string(), vec![key.clone(), member.clone()]),
        Command::ZScore(key, member) => ("ZSCORE".to_string(), vec![key.clone(), member.clone()]),
        Command::ZIncrBy(key, _, member) => ("ZINCRBY".to_string(), vec![key.clone(), member.clone()]),
        Command::ZPopMin(key, _) => ("ZPOPMIN".to_string(), vec![key.clone()]),
        Command::ZPopMax(key, _) => ("ZPOPMAX".to_string(), vec![key.clone()]),
        Command::ZUnionStore(dest, _, _, _) => ("ZUNIONSTORE".to_string(), vec![dest.clone()]),
        Command::ZInterStore(dest, _, _, _) => ("ZINTERSTORE".to_string(), vec![dest.clone()]),
        Command::ZCount(key, _, _) => ("ZCOUNT".to_string(), vec![key.clone()]),
        Command::ZRangeByScore(key, _, _, _) => ("ZRANGEBYSCORE".to_string(), vec![key.clone()]),
        Command::ZRangeByLex(key, _, _) => ("ZRANGEBYLEX".to_string(), vec![key.clone()]),
        Command::ZScan(key, _, _, _) => ("ZSCAN".to_string(), vec![key.clone()]),
        Command::SInterCard(keys, _) => ("SINTERCARD".to_string(), keys.clone()),
        Command::SMisMember(key, _) => ("SMISMEMBER".to_string(), vec![key.clone()]),
        Command::ZRandMember(key, _, _) => ("ZRANDMEMBER".to_string(), vec![key.clone()]),
        Command::ZDiff(keys, _) => ("ZDIFF".to_string(), keys.clone()),
        Command::ZDiffStore(dest, _) => ("ZDIFFSTORE".to_string(), vec![dest.clone()]),
        Command::ZInter(keys, _, _, _) => ("ZINTER".to_string(), keys.clone()),
        Command::ZUnion(keys, _, _, _) => ("ZUNION".to_string(), keys.clone()),
        Command::ZRangeStore(dst, src, _, _, _, _, _, _, _) => ("ZRANGESTORE".to_string(), vec![dst.clone(), src.clone()]),
        Command::ZMpop(keys, _, _) => ("ZMPOP".to_string(), keys.clone()),
        Command::BZMpop(_, keys, _, _) => ("BZMPOP".to_string(), keys.clone()),
        Command::BZPopMin(keys, _) => ("BZPOPMIN".to_string(), keys.clone()),
        Command::BZPopMax(keys, _) => ("BZPOPMAX".to_string(), keys.clone()),
        Command::ZRevRangeByScore(key, _, _, _, _, _) => ("ZREVRANGEBYSCORE".to_string(), vec![key.clone()]),
        Command::ZRevRangeByLex(key, _, _, _, _) => ("ZREVRANGEBYLEX".to_string(), vec![key.clone()]),
        Command::ZMScore(key, _) => ("ZMSCORE".to_string(), vec![key.clone()]),
        Command::ZLexCount(key, _, _) => ("ZLEXCOUNT".to_string(), vec![key.clone()]),
        Command::ZRangeUnified(key, _, _, _, _, _, _, _, _) => ("ZRANGE".to_string(), vec![key.clone()]),
        Command::Keys(_) => ("KEYS".to_string(), vec![]),
        Command::Scan(_, _, _) => ("SCAN".to_string(), vec![]),
        Command::Rename(src, dest) => ("RENAME".to_string(), vec![src.clone(), dest.clone()]),
        Command::Type(key) => ("TYPE".to_string(), vec![key.clone()]),
        Command::Persist(key) => ("PERSIST".to_string(), vec![key.clone()]),
        Command::PExpire(key, _) => ("PEXPIRE".to_string(), vec![key.clone()]),
        Command::PTtl(key) => ("PTTL".to_string(), vec![key.clone()]),
        Command::DbSize => ("DBSIZE".to_string(), vec![]),
        Command::Info(_) => ("INFO".to_string(), vec![]),
        Command::SetBit(key, _, _) => ("SETBIT".to_string(), vec![key.clone()]),
        Command::GetBit(key, _) => ("GETBIT".to_string(), vec![key.clone()]),
        Command::BitCount(key, _, _, _) => ("BITCOUNT".to_string(), vec![key.clone()]),
        Command::BitOp(op, dest, keys) => ("BITOP".to_string(), vec![op.clone(), dest.clone(), keys.join(" ")]),
        Command::BitPos(key, _, _, _, _) => ("BITPOS".to_string(), vec![key.clone()]),
        Command::BitField(key, _) => ("BITFIELD".to_string(), vec![key.clone()]),
        Command::BitFieldRo(key, _) => ("BITFIELD_RO".to_string(), vec![key.clone()]),
        Command::XAdd(key, _, _, _, _, _) => ("XADD".to_string(), vec![key.clone()]),
        Command::XLen(key) => ("XLEN".to_string(), vec![key.clone()]),
        Command::XRange(key, _, _, _) => ("XRANGE".to_string(), vec![key.clone()]),
        Command::XRevRange(key, _, _, _) => ("XREVRANGE".to_string(), vec![key.clone()]),
        Command::XTrim(key, _, _) => ("XTRIM".to_string(), vec![key.clone()]),
        Command::XDel(key, _) => ("XDEL".to_string(), vec![key.clone()]),
        Command::XRead(keys, _, _) => ("XREAD".to_string(), keys.clone()),
        Command::XSetId(key, _) => ("XSETID".to_string(), vec![key.clone()]),
        Command::XGroupCreate(key, _, _, _) => ("XGROUP".to_string(), vec![key.clone()]),
        Command::XGroupDestroy(key, _) => ("XGROUP".to_string(), vec![key.clone()]),
        Command::XGroupSetId(key, _, _) => ("XGROUP".to_string(), vec![key.clone()]),
        Command::XGroupDelConsumer(key, _, _) => ("XGROUP".to_string(), vec![key.clone()]),
        Command::XGroupCreateConsumer(key, _, _) => ("XGROUP".to_string(), vec![key.clone()]),
        Command::XReadGroup(_, _, keys, _, _, _) => ("XREADGROUP".to_string(), keys.clone()),
        Command::XAck(key, _, _) => ("XACK".to_string(), vec![key.clone()]),
        Command::XClaim(key, _, _, _, _, _) => ("XCLAIM".to_string(), vec![key.clone()]),
        Command::XAutoClaim(key, _, _, _, _, _, _) => ("XAUTOCLAIM".to_string(), vec![key.clone()]),
        Command::XPending(key, _, _, _, _, _) => ("XPENDING".to_string(), vec![key.clone()]),
        Command::XInfoStream(key, _) => ("XINFO".to_string(), vec![key.clone()]),
        Command::XInfoGroups(key) => ("XINFO".to_string(), vec![key.clone()]),
        Command::XInfoConsumers(key, _) => ("XINFO".to_string(), vec![key.clone()]),
        Command::PfAdd(key, _) => ("PFADD".to_string(), vec![key.clone()]),
        Command::PfCount(keys) => ("PFCOUNT".to_string(), keys.clone()),
        Command::PfMerge(dest, _) => ("PFMERGE".to_string(), vec![dest.clone()]),
        Command::GeoAdd(key, _) => ("GEOADD".to_string(), vec![key.clone()]),
        Command::GeoDist(key, _, _, _) => ("GEODIST".to_string(), vec![key.clone()]),
        Command::GeoHash(key, _) => ("GEOHASH".to_string(), vec![key.clone()]),
        Command::GeoPos(key, _) => ("GEOPOS".to_string(), vec![key.clone()]),
        Command::GeoSearch(key, _, _, _, _, _, _, _, _, _) => ("GEOSEARCH".to_string(), vec![key.clone()]),
        Command::GeoSearchStore(dest, _, _, _, _, _, _, _, _) => ("GEOSEARCHSTORE".to_string(), vec![dest.clone()]),
        Command::Select(db) => ("SELECT".to_string(), vec![db.to_string()]),
        Command::Auth(_, _) => ("AUTH".to_string(), vec![]),
        Command::AclSetUser(username, _) => ("ACL SETUSER".to_string(), vec![username.clone()]),
        Command::AclGetUser(username) => ("ACL GETUSER".to_string(), vec![username.clone()]),
        Command::AclDelUser(names) => ("ACL DELUSER".to_string(), names.clone()),
        Command::AclList => ("ACL LIST".to_string(), vec![]),
        Command::AclCat(_) => ("ACL CAT".to_string(), vec![]),
        Command::AclWhoAmI => ("ACL WHOAMI".to_string(), vec![]),
        Command::AclLog(_) => ("ACL LOG".to_string(), vec![]),
        Command::AclGenPass(_) => ("ACL GENPASS".to_string(), vec![]),
        Command::ClientSetName(name) => ("CLIENT".to_string(), vec!["SETNAME".to_string(), name.clone()]),
        Command::ClientGetName => ("CLIENT".to_string(), vec!["GETNAME".to_string()]),
        Command::ClientList => ("CLIENT".to_string(), vec!["LIST".to_string()]),
        Command::ClientId => ("CLIENT".to_string(), vec!["ID".to_string()]),
        Command::ClientInfo => ("CLIENT".to_string(), vec!["INFO".to_string()]),
        Command::ClientKill { .. } => ("CLIENT".to_string(), vec!["KILL".to_string()]),
        Command::ClientPause(_, _) => ("CLIENT".to_string(), vec!["PAUSE".to_string()]),
        Command::ClientUnpause => ("CLIENT".to_string(), vec!["UNPAUSE".to_string()]),
        Command::ClientNoEvict(_) => ("CLIENT".to_string(), vec!["NO-EVICT".to_string()]),
        Command::ClientNoTouch(_) => ("CLIENT".to_string(), vec!["NO-TOUCH".to_string()]),
        Command::ClientReply(_) => ("CLIENT".to_string(), vec!["REPLY".to_string()]),
        Command::ClientUnblock(_, _) => ("CLIENT".to_string(), vec!["UNBLOCK".to_string()]),
        Command::Sort(key, _, _, _, _, _, _, _) => ("SORT".to_string(), vec![key.clone()]),
        Command::Unlink(keys) => ("UNLINK".to_string(), keys.clone()),
        Command::Copy(source, dest, _) => ("COPY".to_string(), vec![source.clone(), dest.clone()]),
        Command::Dump(key) => ("DUMP".to_string(), vec![key.clone()]),
        Command::Restore(key, _, _, _) => ("RESTORE".to_string(), vec![key.clone()]),
        Command::Eval(_, _, _) => ("EVAL".to_string(), vec![]),
        Command::EvalSha(_, _, _) => ("EVALSHA".to_string(), vec![]),
        Command::ScriptLoad(_) => ("SCRIPT".to_string(), vec!["LOAD".to_string()]),
        Command::ScriptExists(_) => ("SCRIPT".to_string(), vec!["EXISTS".to_string()]),
        Command::ScriptFlush => ("SCRIPT".to_string(), vec!["FLUSH".to_string()]),
        Command::FunctionLoad(_, _) => ("FUNCTION LOAD".to_string(), vec![]),
        Command::FunctionDelete(lib) => ("FUNCTION DELETE".to_string(), vec![lib.clone()]),
        Command::FunctionList(_, _) => ("FUNCTION LIST".to_string(), vec![]),
        Command::FunctionDump => ("FUNCTION DUMP".to_string(), vec![]),
        Command::FunctionRestore(_, _) => ("FUNCTION RESTORE".to_string(), vec![]),
        Command::FunctionStats => ("FUNCTION STATS".to_string(), vec![]),
        Command::FunctionFlush(_) => ("FUNCTION FLUSH".to_string(), vec![]),
        Command::FCall(name, keys, _) => ("FCALL".to_string(), vec![name.clone()]),
        Command::FCallRO(name, keys, _) => ("FCALL_RO".to_string(), vec![name.clone()]),
        Command::EvalRO(_, keys, _) => ("EVAL_RO".to_string(), keys.clone()),
        Command::EvalShaRO(_, keys, _) => ("EVALSHA_RO".to_string(), keys.clone()),
        Command::Save => ("SAVE".to_string(), vec![]),
        Command::BgSave => ("BGSAVE".to_string(), vec![]),
        Command::SlowLogGet(count) => ("SLOWLOG".to_string(), vec!["GET".to_string(), count.to_string()]),
        Command::SlowLogLen => ("SLOWLOG".to_string(), vec!["LEN".to_string()]),
        Command::SlowLogReset => ("SLOWLOG".to_string(), vec!["RESET".to_string()]),
        Command::ObjectEncoding(key) => ("OBJECT".to_string(), vec!["ENCODING".to_string(), key.clone()]),
        Command::ObjectRefCount(key) => ("OBJECT".to_string(), vec!["REFCOUNT".to_string(), key.clone()]),
        Command::ObjectIdleTime(key) => ("OBJECT".to_string(), vec!["IDLETIME".to_string(), key.clone()]),
        Command::ObjectHelp => ("OBJECT".to_string(), vec!["HELP".to_string()]),
        Command::DebugSetActiveExpire(flag) => ("DEBUG".to_string(), vec!["SET-ACTIVE-EXPIRE".to_string(), if *flag { "1".to_string() } else { "0".to_string() }]),
        Command::DebugSleep(seconds) => ("DEBUG".to_string(), vec!["SLEEP".to_string(), seconds.to_string()]),
        Command::DebugObject(key) => ("DEBUG".to_string(), vec!["OBJECT".to_string(), key.clone()]),
        Command::Echo(msg) => ("ECHO".to_string(), vec![msg.clone()]),
        Command::Time => ("TIME".to_string(), vec![]),
        Command::RandomKey => ("RANDOMKEY".to_string(), vec![]),
        Command::Touch(keys) => ("TOUCH".to_string(), keys.clone()),
        Command::ExpireAt(key, ts) => ("EXPIREAT".to_string(), vec![key.clone(), ts.to_string()]),
        Command::PExpireAt(key, ts) => ("PEXPIREAT".to_string(), vec![key.clone(), ts.to_string()]),
        Command::ExpireTime(key) => ("EXPIRETIME".to_string(), vec![key.clone()]),
        Command::PExpireTime(key) => ("PEXPIRETIME".to_string(), vec![key.clone()]),
        Command::RenameNx(key, newkey) => ("RENAMENX".to_string(), vec![key.clone(), newkey.clone()]),
        Command::SwapDb(idx1, idx2) => ("SWAPDB".to_string(), vec![idx1.to_string(), idx2.to_string()]),
        Command::FlushDb => ("FLUSHDB".to_string(), vec![]),
        Command::Shutdown(_) => ("SHUTDOWN".to_string(), vec![]),
        Command::LastSave => ("LASTSAVE".to_string(), vec![]),
        Command::SubStr(key, _, _) => ("SUBSTR".to_string(), vec![key.clone()]),
        Command::Lcs(key1, key2, _, _, _, _) => ("LCS".to_string(), vec![key1.clone(), key2.clone()]),
        Command::Lmove(source, dest, _, _) => ("LMOVE".to_string(), vec![source.clone(), dest.clone()]),
        Command::Rpoplpush(source, dest) => ("RPOPLPUSH".to_string(), vec![source.clone(), dest.clone()]),
        Command::Lmpop(keys, _, _) => ("LMPOP".to_string(), keys.clone()),
        Command::BLmove(source, dest, _, _, _) => ("BLMOVE".to_string(), vec![source.clone(), dest.clone()]),
        Command::BLmpop(keys, _, _, _) => ("BLMPOP".to_string(), keys.clone()),
        Command::BRpoplpush(source, dest, _) => ("BRPOPLPUSH".to_string(), vec![source.clone(), dest.clone()]),
        Command::Multi | Command::Exec | Command::Discard | Command::Watch(_) => ("MULTI".to_string(), vec![]),
        Command::Quit => ("QUIT".to_string(), vec![]),
        Command::SetEx(key, _, _) => ("SETEX".to_string(), vec![key.clone()]),
        Command::HRandField(key, _, _) => ("HRANDFIELD".to_string(), vec![key.clone()]),
        Command::HScan(key, _, _, _) => ("HSCAN".to_string(), vec![key.clone()]),
        Command::HExpire(key, fields, _) => ("HEXPIRE".to_string(), vec![key.clone(), fields.join(" ")]),
        Command::HPExpire(key, fields, _) => ("HPEXPIRE".to_string(), vec![key.clone(), fields.join(" ")]),
        Command::HExpireAt(key, fields, _) => ("HEXPIREAT".to_string(), vec![key.clone(), fields.join(" ")]),
        Command::HPExpireAt(key, fields, _) => ("HPEXPIREAT".to_string(), vec![key.clone(), fields.join(" ")]),
        Command::HTtl(key, fields) => ("HTTL".to_string(), vec![key.clone(), fields.join(" ")]),
        Command::HPTtl(key, fields) => ("HPTTL".to_string(), vec![key.clone(), fields.join(" ")]),
        Command::HExpireTime(key, fields) => ("HEXPIRETIME".to_string(), vec![key.clone(), fields.join(" ")]),
        Command::HPExpireTime(key, fields) => ("HPEXPIRETIME".to_string(), vec![key.clone(), fields.join(" ")]),
        Command::HPersist(key, fields) => ("HPERSIST".to_string(), vec![key.clone(), fields.join(" ")]),
        Command::Subscribe(channels) => ("SUBSCRIBE".to_string(), channels.clone()),
        Command::Unsubscribe(channels) => ("UNSUBSCRIBE".to_string(), channels.clone()),
        Command::PSubscribe(patterns) => ("PSUBSCRIBE".to_string(), patterns.clone()),
        Command::PUnsubscribe(patterns) => ("PUNSUBSCRIBE".to_string(), patterns.clone()),
        Command::Publish(channel, _) => ("PUBLISH".to_string(), vec![channel.clone()]),
        Command::BgRewriteAof => ("BGREWRITEAOF".to_string(), vec![]),
        Command::Unknown(name) => (name.clone(), vec![]),
    }
}

impl std::fmt::Debug for CommandExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandExecutor")
            .field("storage", &self.storage)
            .field("aof_enabled", &self.aof.is_some())
            .finish()
    }
}

impl Clone for CommandExecutor {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            aof: self.aof.clone(),
            script_engine: self.script_engine.clone(),
            slowlog: self.slowlog.clone(),
            acl: self.acl.clone(),
            latency: self.latency.clone(),
            keyspace_notifier: self.keyspace_notifier.clone(),
            aof_use_rdb_preamble: self.aof_use_rdb_preamble.clone(),
        }
    }
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
