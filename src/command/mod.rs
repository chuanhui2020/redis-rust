// 命令解析与分发模块

use std::sync::atomic::Ordering;

use bytes::Bytes;

use crate::error::AppError;
use crate::protocol::RespValue;
use crate::slowlog::SlowLog;
#[allow(unused_imports)]
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
pub mod resp_admin;
pub mod resp_bitmap;
pub mod resp_geo;
pub mod resp_hash;
pub mod resp_hll;
pub mod resp_list;
pub mod resp_set;
pub mod resp_stream;
pub mod resp_string;
pub mod resp_zset;
pub mod parser_key;
pub mod parser_pubsub;
pub mod parser_server;
pub mod parser;
pub mod executor;
pub mod executor_admin;
pub mod executor_bitmap;
pub mod executor_geo;
pub mod executor_hash;
pub mod executor_hll;
pub mod executor_list;
pub mod executor_set;
pub mod executor_stream;
pub mod executor_string;
pub mod executor_zset;
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
mod tests;
