# redis-rust

用 Rust 从零实现的 Redis 7 兼容服务器。

## 项目简介

redis-rust 是一个使用 Rust 语言从头编写的 Redis 协议兼容服务器，实现了 282 个命令，覆盖 Redis 7.x 全部核心功能：RESP2/RESP3 协议、9 种数据类型、持久化（RDB + AOF）、事务、发布订阅、Lua 脚本、ACL、主从复制、Sentinel 哨兵、Cluster 集群。项目采用异步 I/O（tokio）和 64 分片锁架构，在保持代码简洁的同时提供接近原生 Redis 的读性能。

## 功能特性

### 数据类型（282 个命令，100% 覆盖）

| 分类 | 命令数 | 示例 |
|------|--------|------|
| String | 21 | SET/GET/MGET/INCR/APPEND/LCS |
| List | 18 | LPUSH/RPUSH/LPOP/LRANGE/BLPOP/LMOVE |
| Hash | 23 | HSET/HGET/HMGET/HGETALL/HEXPIRE/HTTL |
| Set | 17 | SADD/SMEMBERS/SINTER/SUNION/SDIFF/SPOP |
| Sorted Set | 35 | ZADD/ZRANGE/ZRANGEBYSCORE/ZPOPMIN/BZPOPMAX |
| Stream | 24 | XADD/XREAD/XREADGROUP/XACK/XCLAIM/XAUTOCLAIM |
| Bitmap | 7 | SETBIT/GETBIT/BITCOUNT/BITOP/BITFIELD |
| HyperLogLog | 3 | PFADD/PFCOUNT/PFMERGE |
| Geo | 6 | GEOADD/GEODIST/GEOSEARCH/GEOPOS |

### 服务器功能

| 功能 | 说明 |
|------|------|
| RESP2/RESP3 协议 | 完整支持，含 HELLO 协商 |
| 事务 | MULTI/EXEC/DISCARD/WATCH/UNWATCH |
| Lua 脚本 | EVAL/EVALSHA + FUNCTION LOAD/CALL/LIST/DELETE |
| ACL 权限 | SETUSER/DELUSER/LIST/CAT/WHOAMI/DRYRUN/SAVE/LOAD |
| Pub/Sub | SUBSCRIBE/PUBLISH + PSUBSCRIBE + Sharded Pub/Sub |
| Client Tracking | CLIENT TRACKING/CACHING/GETREDIR/TRACKINGINFO |
| 持久化 | RDB 快照 + AOF 日志 + 混合持久化 + appendfsync 策略 |
| 内存管理 | maxmemory + LRU/LFU/Random/TTL 淘汰策略 |
| 多数据库 | SELECT 支持 16 个独立数据库 |
| Keyspace 通知 | 键过期、删除、修改事件通知 |
| SLOWLOG | 慢查询日志 |
| MONITOR | 实时命令监控 |
| 优雅关闭 | SHUTDOWN [SAVE\|NOSAVE] + Ctrl+C 信号处理 |

### 分布式功能

| 功能 | 说明 |
|------|------|
| 主从复制 | 全量/增量同步 + 写传播 + 心跳 + 断线重连 + RDB 持久化 replid |
| Sentinel | SDOWN/ODOWN 检测 + epoch 选举 + 自动故障转移 + 配置持久化 |
| Cluster | slot 分片 + MOVED/ASK 重定向 + Gossip 协议 + 自动故障转移 + READONLY 副本读取 |

## 快速开始

```bash
# 编译
cargo build --release

# 启动（默认端口 6379）
./target/release/redis-rust

# 指定端口 + 禁用 AOF
./target/release/redis-rust --port 6380 --no-aof

# 指定绑定地址 + 内存限制
./target/release/redis-rust --bind 0.0.0.0 --maxmemory 1073741824 --maxmemory-policy allkeys-lru

# Cluster 模式
./target/release/redis-rust --port 7000 --cluster-enabled yes
```

使用 `redis-cli` 连接：

```bash
redis-cli -p 6379
> SET hello world
OK
> GET hello
"world"
```

### CLI 参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--port <port>` | 监听端口 | 6379 |
| `--bind <ip>` | 绑定地址 | 127.0.0.1 |
| `--no-aof` | 禁用 AOF 持久化 | — |
| `--appendfsync <always\|everysec\|no>` | AOF 刷盘策略 | everysec |
| `--maxmemory <bytes>` | 最大内存限制 | 无限制 |
| `--maxmemory-policy <policy>` | 淘汰策略 | noeviction |
| `--sentinel` | Sentinel 模式 | — |
| `--cluster-enabled <yes\|no>` | Cluster 模式 | no |
| `--cluster-announce-ip <ip>` | 集群通告 IP | 同 --bind |
| `--cluster-announce-port <port>` | 集群通告端口 | 同 --port |
| `--cluster-announce-bus-port <port>` | 集群总线端口 | port + 10000 |

### 集群部署

一键部署 3 主 3 从本地集群：

```powershell
cd scripts/cluster
.\deploy-cluster.ps1    # 部署
.\check-cluster.ps1     # 健康检查
.\stop-cluster.ps1      # 停止
```

## Benchmark

对比 Redis 7.x（macOS ARM64，100K 请求，50 并发连接）：

| 测试项目 | redis-rust | Redis 7 | 性能比 |
|---------|-----------|---------|--------|
| PING | 175K rps | 187K rps | 93.8% |
| SET | 121K rps | 189K rps | 64% |
| GET | 176K rps | 186K rps | 94.4% |
| 混合（PING+SET+GET） | 163K rps | 183K rps | 89.2% |

**说明**：SET 性能差距主要源于架构差异——Redis 使用单线程事件循环（完全无锁），redis-rust 使用多线程 + 分片锁。读操作（GET/PING）因分片读锁可并发，性能接近 Redis。

运行基准测试：

```bash
./scripts/bench/bench.sh
```

## 架构说明

- **异步 TCP 服务器**：基于 tokio，每连接一个异步任务
- **RESP 协议解析**：完整支持 RESP2/RESP3 协议，含 HELLO 版本协商
- **分片锁存储引擎**：64 分片 `RwLock<HashMap>`，按 key hash（ahash）路由，降低锁竞争
- **命令系统**：每种数据类型三层架构 — parser（解析）→ executor（执行）→ resp（响应构建）
- **持久化**：RDB 快照优先加载，AOF 日志重放叠加，支持 appendfsync always/everysec/no
- **Pipeline 优化**：批量延迟 side effects（AOF/复制/keyspace 通知），pipeline 结束后一次性刷新
- **过期键清理**：被动过期（访问时检查）+ 主动后台清理

## 测试

```bash
# 运行全部测试（876 个：575 单元测试 + 291 集成测试 + 10 压力测试）
cargo test

# 仅运行单元测试
cargo test --lib

# 仅运行集成测试
cargo test --test integration_test
```

## 技术栈

- **Rust** stable (Edition 2024)
- **tokio** — 异步运行时
- **bytes** — 高效字节缓冲区
- **crossbeam** — 无锁 channel（AOF 异步写入）
- **ahash** — 高性能哈希（分片路由）
- **mlua** — Lua 5.4 脚本引擎
- **ordered-float** — 有序浮点数（有序集合）

## 许可证

MIT
