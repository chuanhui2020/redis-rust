# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Redis server implementation in Rust (Edition 2024), supporting 100+ commands with RESP2/RESP3 protocol, persistence (RDB + AOF), Lua scripting, Pub/Sub, ACL, and keyspace notifications.

## Common Commands

```bash
# Build
cargo build
cargo build --release

# Run server (default port 6379)
cargo run -- --port 6380
cargo run -- --no-aof          # disable AOF persistence

# Run all tests
cargo test

# Run specific test categories
cargo test --lib                          # unit tests only
cargo test --test integration_test        # integration tests only
cargo test --test integration_test test_ping  # single integration test

# Benchmark (requires redis-server and redis-benchmark installed)
bash scripts/bench/bench.sh
```

## Build Environment

**macOS**
- 直接 `cargo build` 即可，无需额外配置
- 需要 Xcode Command Line Tools（`xcode-select --install`）
- mlua 依赖的 LuaJIT 会自动编译

**Windows**
- 需要安装 Visual Studio Build Tools 2022（VCTools workload + Windows SDK）
- Git Bash 环境下 `link.exe` 会解析到 Git 自带的 Unix link 而非 MSVC 链接器，必须通过 `.cargo/config.toml` 显式指定 MSVC 链接器和库路径：
  ```toml
  [target.x86_64-pc-windows-msvc]
  linker = "C:\\Program Files (x86)\\Microsoft Visual Studio\\2022\\BuildTools\\VC\\Tools\\MSVC\\<version>\\bin\\Hostx64\\x64\\link.exe"
  rustflags = [
      "-Lnative=C:\\Program Files (x86)\\Microsoft Visual Studio\\2022\\BuildTools\\VC\\Tools\\MSVC\\<version>\\lib\\x64",
      "-Lnative=C:\\Program Files (x86)\\Windows Kits\\10\\Lib\\<sdk_version>\\um\\x64",
      "-Lnative=C:\\Program Files (x86)\\Windows Kits\\10\\Lib\\<sdk_version>\\ucrt\\x64",
  ]
  ```
- `.cargo/config.toml` 包含 Windows 本地路径，不要提交到 git（已在 `.gitignore` 中）
- cargo 不在默认 PATH 中时，需手动加：`export PATH="/c/Users/Administrator/.cargo/bin:$PATH"`

## Architecture

**Request pipeline:** TCP connection → `RespParser` (RESP protocol) → `CommandParser` → `Command` enum → `CommandExecutor` → `StorageEngine` → encode response → write to socket.

**Storage engine** (`src/storage/`): Uses a 64-shard `RwLock<HashMap>` (`ShardedMap`) for concurrent access. Each data type (string, list, hash, set, zset, stream, bitmap, HLL, geo) has its own `*_ops.rs` file for operations.

**Command system** (`src/command/`): Split into three layers per data type — `parser_*.rs` (parse RESP into Command), `executor_*.rs` (execute against storage), `resp_*.rs` (build RESP response).

**Server** (`src/server/`): Tokio-based async TCP server. `connection.rs` handles the per-client event loop, `transaction.rs` handles MULTI/EXEC/WATCH, `pubsub.rs` handles subscription state.

**Persistence**: RDB snapshots (`src/rdb.rs`) loaded first, then AOF log (`src/aof.rs`) replayed on top. Files: `dump.rdb`, `appendonly.aof`.

**Scripting** (`src/scripting.rs`): Lua 5.4 via mlua with `redis.call`/`redis.pcall` bindings.

## Development Workflow (Master-Agent / Sub-Agent)

本项目采用双 Agent 协作模式开发：

**角色分工**
- **Claude Code (Master Agent)**: 任务规划、拆解、并行分配，以及结果验收。不做具体编码，token 消耗应尽量少。
- **Kimi CLI (Sub Agent)**: 执行具体编码、重构、新增功能等工作。大部分 token 消耗在 Kimi 侧。Claude 可同时调用多个 Kimi CLI 实例并行处理无依赖的子任务。

**工作流程**

1. **规划阶段 (Claude)**
   - 分析用户需求，拆解为具体的、可独立验证的子任务
   - 识别子任务间的依赖关系，无依赖的任务并行分发给多个 Kimi 实例
   - 为每个子任务编写清晰的 prompt，包含：要修改的文件、预期行为、边界条件、验收标准
   - 通过 `kimi` CLI 将任务分发给 Kimi 执行

2. **编码阶段 (Kimi)**
   - Kimi 根据 prompt 完成编码工作
   - Claude 不参与编码细节，不重复 Kimi 已完成的工作
   - 多个 Kimi 实例可并行工作于不同子任务

3. **验收阶段 (Claude 亲自执行)** — 所有验证命令必须由 Claude 自己运行，不得委托给 Kimi。Claude 在每个子任务完成后按以下标准严格验证：

   **编译检查**
   ```bash
   cargo build 2>&1          # 必须零 error
   cargo clippy 2>&1         # 必须零 warning（或仅已知的）
   ```

   **测试验证**
   ```bash
   cargo test 2>&1           # 全部测试必须通过，不允许跳过或忽略失败
   ```

   **Redis 7 兼容性验证** — 所有行为对标 Redis 7.x，启动服务器后用 redis-cli 验证：
   ```bash
   # 启动 redis-rust
   cargo run --release -- --port 6399 --no-aof &
   # 用 redis-cli 执行目标命令，对比官方 Redis 7 的输出
   redis-cli -p 6399 <具体命令>
   ```

   **性能基准验证** — 对性能敏感的改动必须跑 benchmark，对标 Redis 7：
   ```bash
   # 单项 benchmark（示例：SET 命令）
   redis-benchmark -p 6399 -t set -n 100000 -c 50
   # 完整 benchmark 对比（对标 Redis 7）
   bash scripts/bench/bench.sh
   ```
   性能验收标准（对标 Redis 7）：
   - GET/PING 类读操作：不低于 Redis 7 的 90%
   - SET 类写操作：不低于 Redis 7 的 60%
   - 新功能不得导致已有命令性能回退超过 5%

   **验收不通过时**：Claude 定位具体失败原因（编译错误、测试失败、兼容性偏差、性能回退），将失败信息和修复方向编写为新的 prompt 重新分发给 Kimi 返工。此循环必须持续进行，直到上述全部验收标准通过为止。Claude 不得在任何标准未满足时放行。

   **验收通过后**：Claude 立即 commit 并 push 代码到 GitHub，提交信息遵循 conventional commits 规范（中文描述）。

## Conventions

- Code comments and documentation are in Chinese.
- Commit messages use conventional commits with Chinese descriptions (e.g., `feat: 添加 pipeline 支持`).
- Integration tests spawn a real TCP server on `127.0.0.1:0` (ephemeral port) and communicate via raw RESP protocol.
- No CI/CD pipeline — all validation is local.
- 所有功能对标 Redis 7.x，兼容性和性能均以 Redis 7 为基准。
- `docs/` 下的 HTML 文件（`commands.html`、`server-lifecycle-*.html`）是面向浏览器的可视化文档，必须保留并随功能变更同步更新。

## Redis 7 Implementation Progress

> 每次开发完成后必须更新此章节，保持进度与代码同步。

**已实现（282 个命令）：**

| 分类 | 数量 | 覆盖率 | 状态 |
|------|------|--------|------|
| String | 21 | 100% | 完成 |
| List | 18 | 100% | 完成 |
| Hash | 23（含字段级过期） | 100% | 完成 |
| Set | 17 | 100% | 完成 |
| Sorted Set | 35 | 100% | 完成 |
| Bitmap | 7 | 100% | 完成 |
| HyperLogLog | 3 | 100% | 完成 |
| Geo | 6 | 100% | 完成 |
| Stream | 24（含消费者组） | 100% | 完成 |
| Key 管理 | 20 | 100% | 完成 |
| Sort | 1 | 100% | 完成 |
| Transaction | 5（含 UNWATCH） | 100% | 完成 |
| Scripting | 12（含 SCRIPT DEBUG/HELP） | 86% | 完成 |
| ACL | 11（含 SAVE/LOAD/DRYRUN） | 92% | 完成 |
| Client | 16（含 TRACKING/CACHING/GETREDIR/TRACKINGINFO） | 94% | 完成 |
| Pub/Sub | 13（含 Sharded Pub/Sub） | 100% | 完成 |
| Server/Admin | 36（含 COMMAND COUNT/LIST/DOCS/GETKEYS） | 90% | 完成 |
| 持久化（RDB+AOF） | — | — | 完成 |
| Lua 脚本 + Functions | — | — | 完成 |
| Replication | 8（REPLICAOF/ROLE/PSYNC/REPLCONF/WAIT/SYNC/FAILOVER） | — | 生产级（全量/增量同步+写传播+心跳+断线重连+RDB持久化replid+多节点验证通过） |
| Sentinel | 13（含 FAILOVER/RESET/CKQUORUM） | — | 生产级（SDOWN/ODOWN+epoch选举+自动故障转移+配置持久化+hello_subscriber发现+多节点验证通过） |
| Cluster | 26（含 READONLY/READWRITE + LINKS/FLUSHSLOTS/SAVECONFIG） | — | 生产级（slot分片+MOVED/ASK重定向+Gossip slot传播+数据复制集成+自动故障转移+READONLY replica读取+六节点验证通过） |

**未实现：无（MODULE 为兼容性 stub，不支持真正的模块加载）**

**总覆盖率：100%（282/282），含 Replication + Sentinel + Cluster 全功能维度。**

## 待优化项

### P0 — 正确性 / 安全性

| 项目 | 说明 | 涉及文件 | 工作量 |
|------|------|----------|--------|
| RwLock unwrap 统一处理 | `pubsub.rs`、`replication.rs`、`cluster/state.rs`、`connection.rs` 中大量对 RwLock/Mutex 直接 `.unwrap()`，lock poisoning 会导致连锁崩溃。统一改为 `.unwrap_or_else(\|e\| e.into_inner())` 或封装 helper | pubsub.rs, replication.rs, cluster/state.rs, server/connection.rs | 中 |
| ACL 密码 SHA1→SHA256 | `acl.rs:604-608` 用裸 SHA1 无盐哈希，Redis 7 用 SHA256。需对齐 Redis 7 行为 | acl.rs | 小 |
| AOF fsync 错误传播 | `aof.rs:150,175,189,210` — `sync_data()` 失败只 log 不上报，`appendfsync always` 模式下客户端收到 OK 但数据未持久化。应传播错误或至少在 INFO 中统计失败次数 | aof.rs | 小 |
| RDB 写入加 fsync | `rdb.rs:169` — `writer.flush()` 后无 `fsync()`，OS 崩溃时 RDB 可能不完整。加 `file.sync_all()` | rdb.rs | 极小 |
| RENAME 跨 shard 死锁 | `key_ops.rs:91-118` 两个并发 RENAME 分别锁 shard A→B 和 B→A 时可死锁。应按 shard index 排序后统一获取锁 | storage/key_ops.rs | 小 |

### P1 — 性能热路径优化

| 项目 | 说明 | 涉及文件 | 工作量 |
|------|------|----------|--------|
| 命令解析零分配 | `parser.rs:35-36` 每条命令 `String::from_utf8_lossy().to_ascii_uppercase()` 产生堆分配。改为栈上 `[u8; 32]` 缓冲区 + 字节级大小写转换，消除最热路径分配，预估 pipeline 场景 5-10% 吞吐提升 | command/parser.rs | 小 |
| RESP buffer 回滚优化 | `protocol.rs:235` 解析 incomplete array 时 `buf.clone()` 做快照。改为记录偏移量做 checkpoint，避免整个 BytesMut clone | protocol.rs | 小 |
| Lua VM 池化 | 当前每次 EVAL/EVALSHA 创建全新 Lua 实例（含内存分配、标准库注册、redis.call 绑定）。改为 per-tokio-worker VM pool，执行完 reset 全局状态复用 | scripting.rs | 中 |
| Entry 元数据压缩 | `storage/mod.rs:86-92` 每 Entry 带 `last_access: Instant`(8B) + `access_count: u64`(8B)，百万 key = 16MB 纯开销。`access_count` 改 `u32`，`last_access` 改相对启动时间的 `u32` 秒数，每 Entry 省 8B | storage/mod.rs | 小 |
| SCAN 全量收集优化 | `key_ops.rs:55-88` 当前 SCAN 先收集所有 shard 全部 key 到 Vec 再排序过滤，百万 key 场景每次 SCAN 产生巨大分配。改为 cursor 编码 shard_index + shard_offset 的惰性迭代 | storage/key_ops.rs | 中 |
| Glob 匹配算法优化 | `key_ops.rs:24-50` 每次 glob_match 分配 O(n*m) DP 表，SCAN/KEYS 对每个 key 都调用。改为 O(n) 线性双指针算法（对标 Redis 官方实现） | storage/key_ops.rs | 小 |
| 读操作写锁降级 | `key_ops.rs:121-149`（key_type）、`key_ops.rs:231-249`（dbsize）为清理过期 key 获取写锁，但绝大多数 key 未过期。改为先读锁检查，仅过期时升级写锁 | storage/key_ops.rs | 小 |
| RDB 保存零拷贝 | `rdb.rs:118-130` 保存时 clone 整个 StorageValue（List/Hash/ZSet 可能很大），大数据集下内存翻倍。改为持有读锁直接序列化，避免深拷贝 | rdb.rs | 中 |
| Keyspace 通知 config 免 clone | `keyspace.rs:149,154` 每次写操作触发通知时 `config.read().unwrap().clone()`，高 QPS 下大量无意义分配。改为 `Arc<Config>` + atomic swap | keyspace.rs | 小 |
| is_expired() 重复 syscall | `storage/mod.rs:115-118` `is_expired()` 每次调用 `SystemTime::now()`，KEYS/SCAN 扫描百万 key 时产生百万次 syscall。应在命令执行入口缓存当前时间戳，通过参数传递 | storage/mod.rs | 小 |
| TCP_NODELAY 未设置 | `connection.rs` 未对 TCP socket 设置 `set_nodelay(true)`，Nagle 算法导致小响应延迟 40ms。对 Redis 这类低延迟服务影响显著 | server/connection.rs | 极小 |
| WATCH spin loop 浪费 CPU | `storage/mod.rs:820-832` `get_version()` 用 100 次 try_read() 自旋等待锁，无退避策略。高并发 WATCH 场景下浪费 CPU | storage/mod.rs | 小 |
| ACL 权限检查每命令加锁 | `acl.rs:495-510` 每条命令执行前 `check_command()` 获取 RwLock 读锁 + glob 匹配 key pattern。高 QPS 下锁竞争明显。改为 `ArcSwap` 或缓存编译后的 pattern | acl.rs | 中 |
| Replication backlog drain O(n) | `replication.rs:104-124` backlog 满时 `VecDeque::drain(..to_drop)` 是 O(n) 操作，频繁写入时开销大。改为环形缓冲区实现 O(1) 淘汰 | replication.rs | 中 |
| Eviction policy RwLock 热路径 | `storage/mod.rs:191,309` eviction_policy 用 `Arc<RwLock<EvictionPolicy>>` 保护，每次写操作都获取读锁检查策略。策略极少变更，改为 `AtomicU8` 编码或 `ArcSwap` | storage/mod.rs | 极小 |

### P2 — 架构改进

| 项目 | 说明 | 涉及文件 | 工作量 |
|------|------|----------|--------|
| ZSet SkipList | `ZSetData` 仍用 `BTreeMap<(OrderedFloat, String)>`，换自定义 SkipList 可提升大规模 ZSet 插入/删除性能 | storage/zset_ops.rs | 大 |
| 官方 RDB 兼容 | 当前用自定义 `REDIS-RUST` magic header，无法与官方 Redis 交换 RDB 文件。目标：支持读取官方 RDB 格式 | rdb.rs | 大 |
| Pub/Sub bounded channel | `connection.rs:684` 用 `mpsc::unbounded_channel()` 给订阅客户端，慢消费者可无限堆积导致 OOM。改为 bounded channel + 超限断开（对标 Redis 7 `client-output-buffer-limit pubsub`） | server/connection.rs, pubsub.rs | 中 |
| Cluster quorum failover | `cluster/failover.rs` 单节点即可 PFAIL→FAIL 并触发故障转移，Redis Cluster 要求多数 master 同意。当前实现网络分区时可能 split-brain | cluster/failover.rs | 大 |
| blocking_waiters per-shard | `storage/mod.rs:159` BLPOP/BRPOP 等待者注册表用全局 RwLock，高并发阻塞操作时成为瓶颈。改为 per-shard waiters 或 DashMap | storage/mod.rs | 中 |
| hash_field_expirations 分片 | `storage/mod.rs:159` 单一全局 `RwLock<HashMap>` 管理所有 hash field TTL，所有操作竞争同一把锁。应按 key hash 分片，与主存储对齐 | storage/mod.rs | 中 |
| Client 注册表 DashMap 化 | `connection.rs:615-616` clients HashMap 用 RwLock 保护，CLIENT 命令和连接建立/断开都竞争写锁。改为 DashMap 或按 client_id 分片 | server/connection.rs | 小 |
| 空闲连接超时 | 当前无 idle connection 检测机制，僵尸连接永不释放。应实现 `timeout` 配置（对标 Redis 7 `timeout` 参数），定期清理空闲连接 | server/connection.rs | 中 |
| 内存使用量实时追踪 | 当前仅存储 maxmemory 限制值，无实际内存使用量统计。eviction 无法精确触发。应实现 per-shard 内存计数器，写操作时增量更新 | storage/mod.rs | 中 |
| Hash field 过期时间索引 | `storage/mod.rs:738-760` 清理过期 hash field 时遍历所有 field 找过期项，O(n) 复杂度。改为 BTreeMap 按过期时间索引，O(log n) 查找 | storage/mod.rs | 中 |

### P3 — 代码质量 / 可维护性

| 项目 | 说明 | 涉及文件 | 工作量 |
|------|------|----------|--------|
| 静态响应预编码 | `OK`/`PONG`/`NULL`/`0`/`1` 等高频响应仍通过 `RespValue` 构造后编码，可预编码为静态字节减少运行时开销 | server/handler.rs, command/resp*.rs | 小 |
| scripting panic→error | `scripting.rs:1090,1139` Lua 返回非预期类型时直接 `panic!()`，用户脚本可 crash 服务器。改为返回 Redis error response | scripting.rs | 小 |
| connection.rs UTF-8 安全 | `connection.rs:3188` 对客户端数据 `from_utf8().unwrap()`，恶意客户端发非 UTF-8 即可 crash。改为 `from_utf8_lossy()` 或返回错误 | server/connection.rs | 极小 |
| 大文件拆分 | `connection.rs`(3467行)、`executor.rs`(1654行) 过大。connection.rs 可拆分 cluster redirect、blocking ops、pub/sub 到独立模块 | server/connection.rs, command/executor.rs | 中 |
| Hash/ZSet 热路径冗余 clone | `hash_ops.rs:103,234,295` hgetall 对所有 kv clone 再 collect；`zset_ops.rs:25-31` add 对 member 做 2-3 次 clone。应尽量用引用或 Cow 减少中间分配 | storage/hash_ops.rs, storage/zset_ops.rs | 小 |
| PubSub channel 名重复 clone | `pubsub.rs` subscribe/publish 路径上 channel 名被 clone 3-4 次。改为 `Arc<str>` 共享所有权，消除重复分配 | pubsub.rs | 小 |
| Replication read-clone 模式 | `replication.rs:239,255,782` master_replid/master_host 等不常变字段每次读取都 `read().unwrap().clone()`。改为 `ArcSwap` 实现无锁读 | replication.rs | 小 |
| 连接上下文打包 | `server/mod.rs:262-298` 每个新连接 clone 20+ 个 Arc。打包为单一 `ConnectionContext` struct 只 clone 一次，减少代码冗余 | server/mod.rs | 小 |
| Slowlog 双锁合并 | `slowlog.rs:72-96` `record()` 方法顺序获取两把 Mutex（entries + next_id），每条慢命令都触发。合并为单一 `Mutex<(VecDeque, u64)>` 减少 50% 锁获取 | slowlog.rs | 极小 |
| Gossip 消息序列化优化 | `cluster/gossip.rs:91-102` 每次 PING/PONG 用 `format!()` 拼接字符串 + `split_whitespace()` 解析。集群 100 节点时每秒 100 次分配。改为预分配缓冲区或二进制协议 | cluster/gossip.rs | 中 |
| 错误构造 format!() 开销 | `replication.rs`、`cluster/` 等多处错误路径用 `format!()` 构造错误消息，即使错误不被展示也分配 String。改为静态错误消息或延迟格式化 | 多文件 | 小 |
| Inline 命令解析栈分配 | `protocol.rs:415-438` `parse_inline()` 每次分配 Vec 存储分词结果。对 redis-benchmark 等简单客户端影响大。改为栈上 `[&str; 8]` 小数组优化常见场景 | protocol.rs | 小 |

### P4 — 架构级改动（性能跳跃）

> 以下改动可带来 15-30% 的性能跳跃，但工作量巨大且涉及核心架构替换，需根据实际部署场景评估 ROI。

| 项目 | 说明 | 预估提升 | 工作量 |
|------|------|----------|--------|
| io_uring 替代 epoll | 当前 tokio 底层用 epoll，每次 I/O 至少 1 次 syscall（~100-200ns 上下文切换）。io_uring 通过共享内存环形队列实现批量提交 + 零 syscall 轮询，高 QPS 下显著减少内核态开销。可选方案：tokio-uring 或 monoio（字节 thread-per-core runtime）。仅 Linux 5.1+ | 15-30%（I/O 密集场景） | 巨大 |
| 紧凑数据结构 | Redis 对小对象使用 ziplist/listpack 紧凑编码（连续内存、无指针开销），当前实现全部用标准库 VecDeque/HashMap/BTreeMap。引入 listpack 编码可大幅降低小对象内存占用和 cache miss | 内存降 30-50%（小对象场景），间接提升吞吐 | 巨大 |
| thread-per-core 模型 | 当前 tokio work-stealing 模型存在跨线程任务窃取和缓存失效开销。改为 thread-per-core（每线程独立 reactor + 独立分片数据），消除跨线程同步。可参考 monoio/glommio。需重新设计连接分配和数据分片策略 | 10-20%（高并发场景） | 巨大 |
