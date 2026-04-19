# redis-rust

用 Rust 从零实现的 Redis 兼容缓存系统。

## 项目简介

redis-rust 是一个使用 Rust 语言从头编写的 Redis 协议兼容服务器，支持 RESP 协议、丰富的数据类型、持久化、事务、发布订阅、Lua 脚本等核心功能。项目采用异步 I/O（tokio）和多线程分片锁架构，在保持代码简洁的同时提供接近原生 Redis 的读性能。

## 功能特性

### 基础命令
- `PING`、`SET`、`GET`、`DEL`、`EXISTS`、`EXPIRE`、`TTL`、`KEYS`、`SCAN`、`RENAME`、`TYPE`、`OBJECT`
- `FLUSHDB`、`FLUSHALL`、`DBSIZE`、`INFO`、`CONFIG`

### 字符串操作
- `MSET`、`MGET`、`GETSET`
- `INCR`、`DECR`、`INCRBY`、`DECRBY`、`INCRBYFLOAT`
- `APPEND`、`SETNX`、`GETRANGE`、`SETRANGE`、`STRLEN`
- `SETEX`、`PSETEX`、`LCS`

### 列表操作
- `LPUSH`、`RPUSH`、`LPOP`、`RPOP`、`LLEN`
- `LRANGE`、`LINDEX`、`LSET`、`LINSERT`、`LREM`、`LTRIM`
- `BLPOP`、`BRPOP`、`LMOVE`、`BLMOVE`

### 哈希操作
- `HSET`、`HGET`、`HDEL`、`HMSET`、`HMGET`、`HGETALL`
- `HLEN`、`HINCRBY`、`HINCRBYFLOAT`
- `HEXPIRE`、`HTTL`、`HPERSIST`

### 集合操作
- `SADD`、`SREM`、`SMEMBERS`、`SISMEMBER`、`SCARD`
- `SINTER`、`SUNION`、`SDIFF` 及对应的 `*STORE` 版本
- `SRANDMEMBER`、`SPOP`

### 有序集合
- `ZADD`、`ZREM`、`ZSCORE`、`ZRANK`、`ZREVRANK`
- `ZRANGE`、`ZREVRANGE`、`ZRANGEBYSCORE`、`ZREVRANGEBYSCORE`
- `ZPOPMIN`、`ZPOPMAX`、`BZPOPMIN`、`BZPOPMAX`
- `ZINTERSTORE`、`ZUNIONSTORE`、`ZDIFFSTORE`、`ZRANGESTORE`

### Bitmap
- `SETBIT`、`GETBIT`、`BITCOUNT`、`BITOP`、`BITPOS`、`BITFIELD`

### HyperLogLog
- `PFADD`、`PFCOUNT`、`PFMERGE`

### 地理空间
- `GEOADD`、`GEODIST`、`GEOHASH`、`GEOPOS`、`GEOSEARCH`

### Stream（流）
- `XADD`、`XLEN`、`XRANGE`、`XREVRANGE`、`XREAD`、`XDEL`、`XTRIM`
- 消费者组：`XGROUP CREATE`、`XREADGROUP`、`XACK`、`XCLAIM`、`XAUTOCLAIM`、`XPENDING`
- 管理命令：`XGROUP DESTROY`、`XGROUP DELCONSUMER`、`XGROUP SETID`、`XGROUP CREATECONSUMER`
- `XINFO STREAM`、`XINFO GROUPS`、`XINFO CONSUMERS`

### 发布订阅
- `SUBSCRIBE`、`UNSUBSCRIBE`、`PUBLISH`
- `PSUBSCRIBE`、`PUNSUBSCRIBE`

### 事务
- `MULTI`、`EXEC`、`DISCARD`、`WATCH`、`UNWATCH`

### Lua 脚本
- `EVAL`、`EVALSHA`
- `FUNCTION LOAD`、`FUNCTION LIST`、`FUNCTION CALL`、`FUNCTION DELETE`、`FUNCTION FLUSH`

### 其他高级特性
- **ACL 权限系统**：`ACL LIST`、`ACL SETUSER`、`ACL DELUSER`、`ACL CAT`、`ACL WHOAMI`
- **持久化**：AOF 日志、RDB 快照、混合持久化
- **内存管理**：`maxmemory` 限制 + LRU / LFU / Random / TTL 淘汰策略
- **多数据库**：`SELECT` 支持 16 个独立数据库（db0~db15）
- **Keyspace 通知**：键过期、删除事件通知
- **SLOWLOG**：慢查询日志
- **MONITOR**：实时命令监控

## 快速开始

```bash
# 编译
cargo build --release

# 启动（默认端口 6379）
./target/release/redis-rust

# 指定端口
./target/release/redis-rust --port 6380

# 不启用 AOF（提升写入性能）
./target/release/redis-rust --no-aof
```

然后使用 `redis-cli` 连接：

```bash
redis-cli -p 6379
> SET hello world
OK
> GET hello
"world"
```

## Benchmark

对比 Redis 8.6.2（macOS ARM64，100K 请求，50 并发连接）：

| 测试项目 | redis-rust | Redis | 性能比 |
|---------|-----------|-------|--------|
| PING | 175K rps | 187K rps | 93.8% |
| SET | 121K rps | 189K rps | 64% |
| GET | 176K rps | 186K rps | 94.4% |
| 混合（PING+SET+GET） | 163K rps | 183K rps | 89.2% |

**说明**：SET 性能差距主要源于架构差异——Redis 使用单线程事件循环（完全无锁），redis-rust 使用多线程 + 分片锁。读操作（GET/PING）因分片读锁可并发，性能接近 Redis。

运行基准测试：

```bash
./bench.sh
```

## 架构说明

- **异步 TCP 服务器**：基于 tokio，每连接一个异步任务
- **RESP 协议解析**：完整支持 RESP2/RESP3 协议
- **分片锁存储引擎**：64 分片 `RwLock<HashMap>`，按 key hash 路由，降低锁竞争
- **持久化**：
  - AOF：命令追加日志，支持重放恢复
  - RDB：二进制快照，支持 SAVE/BGSAVE
- **过期键清理**：被动过期（访问时检查）+ 主动后台清理

## 测试

```bash
# 运行全部测试（602 个：539 单元测试 + 63 集成测试）
cargo test

# 仅运行单元测试
cargo test --lib

# 仅运行集成测试
cargo test --test integration_test
```

## 技术栈

- **Rust** stable
- **tokio** — 异步运行时
- **bytes** — 高效字节缓冲区
- **ordered-float** — 有序浮点数（有序集合）

## 许可证

MIT
