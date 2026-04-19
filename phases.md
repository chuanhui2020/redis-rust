# Redis-Rust 分阶段执行方案

## 原则
- Claude（我）：拆任务、下指令、验收、纠偏
- Kimi：写代码、写测试、写文档
- 每阶段用 `kimi -p "..." -y -C` 执行，`-C` 续接上下文
- 每阶段完成后 Claude 做轻量验收（编译/测试/读关键文件）

---

## 阶段 1：项目骨架
**指令要点：** 创建 Cargo 项目，确定依赖（tokio, bytes），搭好模块结构
**验收：** cargo build 通过，目录结构合理

## 阶段 2：TCP Server + RESP 协议
**指令要点：** 实现 async TCP server，RESP 协议解析器（支持 Simple String, Error, Integer, Bulk String, Array）
**验收：** cargo build 通过，能 telnet 连上，发 PING 返回 PONG

## 阶段 3：核心命令（SET/GET/DEL/EXISTS）
**指令要点：** 内存存储引擎 + 命令分发 + 实现 PING/SET/GET/DEL/EXISTS
**验收：** cargo test 通过，redis-cli 可交互

## 阶段 4：TTL（EXPIRE/TTL 命令）
**指令要点：** 实现过期机制（惰性删除 + 定期清理），EXPIRE/TTL 命令
**验收：** TTL 测试通过，过期 key 不可读

## 阶段 5：持久化（AOF）
**指令要点：** 实现 AOF 日志，启动时重放恢复
**验收：** 写入数据 → 重启 → 数据恢复，测试通过

## 阶段 6：测试补全
**指令要点：** 补充集成测试、边界测试、并发测试
**验收：** cargo test 全部通过

## 阶段 7：Benchmark + 优化
**指令要点：** 用 redis-benchmark 对比测试，针对瓶颈优化
**验收：** 性能达标（GET/SET ≥ Redis 70%）

## 阶段 8：文档 ✅
**指令要点：** 中文 README，架构说明，benchmark 结果记录
**验收：** 文档完整

---

# 第二期：扩展功能

## 阶段 9：字符串扩展命令
**指令要点：** MSET/MGET、INCR/DECR/INCRBY/DECRBY、APPEND、SETNX、GETRANGE/STRLEN
**验收：** cargo test 通过，redis-cli 验证每个命令

## 阶段 10：List 数据结构
**指令要点：** 新建 VecDeque 存储，LPUSH/RPUSH/LPOP/RPOP/LLEN/LRANGE/LINDEX
**验收：** cargo test 通过，redis-cli 验证

## 阶段 11：Hash 数据结构
**指令要点：** HashMap 嵌套存储，HSET/HGET/HDEL/HEXISTS/HGETALL/HLEN/HMSET/HMGET
**验收：** cargo test 通过，redis-cli 验证

## 阶段 12：Set 数据结构
**指令要点：** HashSet 存储，SADD/SREM/SMEMBERS/SISMEMBER/SCARD/SINTER/SUNION/SDIFF
**验收：** cargo test 通过，redis-cli 验证

## 阶段 13：Sorted Set 数据结构
**指令要点：** BTreeMap+HashMap 存储，ZADD/ZREM/ZSCORE/ZRANK/ZRANGE/ZRANGEBYSCORE/ZCARD
**验收：** cargo test 通过，redis-cli 验证

## 阶段 14：键管理 + 服务器命令
**指令要点：** KEYS/SCAN、RENAME、TYPE、PERSIST、PEXPIRE/PTTL、DBSIZE、INFO、SELECT
**验收：** cargo test 通过

## 阶段 15：发布订阅
**指令要点：** SUBSCRIBE/UNSUBSCRIBE/PUBLISH/PSUBSCRIBE，用 broadcast channel 实现
**验收：** 多客户端订阅/发布测试通过

## 阶段 16：事务
**指令要点：** MULTI/EXEC/DISCARD/WATCH，命令队列 + 乐观锁
**验收：** 事务原子性测试通过

## 阶段 17：内存管理 + AOF 重写
**指令要点：** maxmemory 配置、LRU 淘汰策略、AOF rewrite 压缩
**验收：** 内存限制生效，AOF 重写后文件变小

## 阶段 18：Benchmark ✅
**验收：** 性能全面达标

## 阶段 19：文档更新 ✅
**验收：** README 完整

---

# 第三期：单机功能补全

## 阶段 20：字符串长尾命令 ✅
**指令要点：** SETEX/PSETEX、GETSET/GETDEL/GETEX、MSETNX、INCRBYFLOAT、SETRANGE
**验收：** cargo test 通过，201 个测试全部通过

## 阶段 21：List 长尾命令 ✅
**指令要点：** LSET、LINSERT、LREM、LTRIM、LPOS、BLPOP/BRPOP（阻塞弹出，用 tokio::sync::Notify 实现）
**验收：** cargo test 通过，222 个测试全部通过，BLPOP 阻塞/超时行为正确

## 阶段 22：Hash 长尾命令 ✅
**指令要点：** HINCRBY、HINCRBYFLOAT、HKEYS、HVALS、HSETNX、HRANDFIELD、HSCAN
**验收：** cargo test 通过，237 个测试全部通过

## 阶段 23：Set 长尾命令 ✅
**指令要点：** SPOP、SRANDMEMBER、SMOVE、SINTERSTORE/SUNIONSTORE/SDIFFSTORE、SSCAN
**验收：** cargo test 通过，251 个测试全部通过

## 阶段 24：Sorted Set 长尾命令 ✅
**指令要点：** ZREVRANGE、ZREVRANK、ZINCRBY、ZCOUNT、ZPOPMIN/ZPOPMAX、ZUNIONSTORE/ZINTERSTORE、ZSCAN、ZRANGEBYLEX
**验收：** cargo test 272 个测试全部通过（239 单元 + 33 集成）

## 阶段 25：Bitmap ✅
**指令要点：** 在 String 值上实现位操作，SETBIT/GETBIT/BITCOUNT/BITOP/BITPOS
**验收：** cargo test 285 个测试全部通过（251 单元 + 34 集成），BITOP AND/OR/XOR/NOT 正确

## 阶段 26：HyperLogLog ✅
**指令要点：** PFADD/PFCOUNT/PFMERGE，使用标准 HLL 算法（16384 个寄存器）
**验收：** cargo test 295 个测试全部通过（260 单元 + 35 集成），10000 元素估算误差 < 5%

## 阶段 27：Geo ✅
**指令要点：** 基于 Sorted Set + GeoHash 编码，GEOADD/GEODIST/GEOHASH/GEOPOS/GEOSEARCH/GEOSEARCHSTORE
**验收：** cargo test 307 个测试全部通过（271 单元 + 36 集成），距离计算精度 < 2%

## 阶段 28：多数据库 + 认证
**指令要点：** SELECT（16 个 db）、AUTH 密码认证、CLIENT SETNAME/GETNAME/LIST/KILL
**验收：** cargo test 通过，SELECT 切换 db 隔离正确

## 阶段 29：SORT + UNLINK + COPY/DUMP/RESTORE
**指令要点：** SORT key [BY/GET/LIMIT/ASC|DESC/ALPHA/STORE]、UNLINK（异步删除）、COPY/DUMP/RESTORE 键序列化
**验收：** cargo test 通过

## 阶段 30：Lua 脚本
**指令要点：** 集成 rlua 或 mlua crate，EVAL/EVALSHA/SCRIPT LOAD/EXISTS/FLUSH，脚本中可调用 redis.call/redis.pcall
**验收：** cargo test 通过，脚本内 SET/GET/HSET 等命令可用

## 阶段 31：RDB 快照持久化
**指令要点：** SAVE/BGSAVE，实现 RDB 二进制格式写入（String/List/Hash/Set/ZSet），启动时优先加载 RDB
**验收：** 写入 → 重启 → 数据恢复，cargo test 通过

## 阶段 32：多淘汰策略 + SLOWLOG
**指令要点：** LFU/random/volatile-lru/volatile-ttl/volatile-random/noeviction 策略、SLOWLOG GET/LEN/RESET
**验收：** cargo test 通过，各策略淘汰行为正确

## 阶段 33：OBJECT + DEBUG + 内部编码优化
**指令要点：** OBJECT ENCODING/REFCOUNT/IDLETIME/HELP、小数据量时使用紧凑编码（ziplist 思路的 Vec<u8> 替代 HashMap）
**验收：** cargo test 通过，小 Hash/Set 内存占用降低

## 阶段 34：Benchmark + 文档 ✅

---

# 第四期：Redis 7 完整覆盖

## 阶段 35：低复杂度零散命令（第一批）
**指令要点：** ECHO/TIME/RANDOMKEY/TOUCH/EXPIREAT/PEXPIREAT/EXPIRETIME/PEXPIRETIME/RENAMENX/SWAPDB/FLUSHDB/SHUTDOWN/LASTSAVE/SUBSTR
**验收：** cargo test 通过

## 阶段 36：SET 扩展选项 + 字符串补全
**指令要点：** SET NX/XX/GET/KEEPTTL/EXAT/PXAT 选项、LCS 命令
**验收：** cargo test 通过

## 阶段 37：List 补全
**指令要点：** LMOVE/BLMOVE/RPOPLPUSH/BRPOPLPUSH/LMPOP/BLMPOP
**验收：** cargo test 通过

## 阶段 38：Hash 补全 ✅
**指令要点：** HEXPIRE/HPEXPIRE/HTTL/HPTTL/HPERSIST/HEXPIREAT/HPEXPIREAT/HEXPIRETIME/HPEXPIRETIME
**验收：** cargo test 通过（459 测试：419 单元 + 40 集成）

## 阶段 39：Set + Sorted Set 补全 ✅
**指令要点：** SINTERCARD/SMISMEMBER + ZRANDMEMBER/ZDIFF/ZDIFFSTORE/ZINTER/ZUNION/ZRANGESTORE/ZMPOP/BZMPOP/BZPOPMIN/BZPOPMAX/ZREVRANGEBYSCORE/ZREVRANGEBYLEX/ZMSCORE/ZLEXCOUNT/ZRANGE统一语法
**验收：** cargo test 通过（473 测试：433 单元 + 40 集成）

## 阶段 40：Bitmap 补全 ✅
**指令要点：** BITFIELD/BITFIELD_RO（GET/SET/INCRBY/OVERFLOW）
**验收：** cargo test 通过（491 测试：451 单元 + 40 集成）

## 阶段 41：Stream（基础） ✅
**指令要点：** XADD/XLEN/XRANGE/XREVRANGE/XTRIM/XDEL/XREAD/XSETID，Stream 数据结构用 BTreeMap<StreamId, Vec<(String,String)>>
**验收：** cargo test 通过（507 测试：467 单元 + 40 集成）

## 阶段 42：Stream（消费者组）
**指令要点：** XGROUP CREATE/DESTROY/SETID/DELCONSUMER/CREATECONSUMER、XREADGROUP/XACK/XCLAIM/XAUTOCLAIM/XPENDING/XINFO
**验收：** cargo test 通过

## 阶段 43：ACL 权限系统
**指令要点：** ACL SETUSER/GETUSER/DELUSER/LIST/CAT/WHOAMI/LOG/SAVE/LOAD/GENPASS，用户权限模型
**验收：** cargo test 通过

## 阶段 44：Function 系统
**指令要点：** FUNCTION LOAD/DELETE/LIST/DUMP/RESTORE/STATS/FLUSH、FCALL/FCALL_RO、EVAL_RO/EVALSHA_RO
**验收：** cargo test 通过

## 阶段 45：客户端命令补全
**指令要点：** CLIENT KILL/PAUSE/UNPAUSE/INFO/NO-EVICT/NO-TOUCH/REPLY/UNBLOCK
**验收：** cargo test 通过

## 阶段 46：服务器命令补全 + MONITOR
**指令要点：** MEMORY USAGE/DOCTOR、LATENCY LATEST/HISTORY/RESET、CONFIG REWRITE/RESETSTAT、MONITOR、RESET、HELLO（简化版）
**验收：** cargo test 通过

## 阶段 47：Keyspace 通知 + 混合持久化
**指令要点：** CONFIG SET notify-keyspace-events + Pub/Sub 推送、RDB+AOF 混合持久化格式
**验收：** cargo test 通过

## 阶段 48：最终 Benchmark + 文档
**指令要点：** 全命令 benchmark、更新 README、覆盖率统计
**验收：** 性能达标，文档完整
