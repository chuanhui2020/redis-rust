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

> **优化进度**：已完成 45 项，剩余 7 项（均为大/巨大工作量架构级改动）。

### 已完成

| 项目 | 级别 | 完成方式 |
|------|------|----------|
| ~~RENAME 跨 shard 死锁~~ | P0 | 按 shard index 排序加锁（commit 089cb17） |
| ~~Glob 匹配算法优化~~ | P1 | O(n) 线性双指针算法（commit 089cb17） |
| ~~TCP_NODELAY 未设置~~ | P1 | `stream.set_nodelay(true)` in server/mod.rs（commit 089cb17） |
| ~~静态响应预编码~~ | P3 | RESP_OK/PONG/NULL/ZERO/ONE 预编码为静态字节（server/handler.rs） |
| ~~Pub/Sub bounded channel~~ | P2 | 已使用 `broadcast::channel(256)` 有界通道 |
| ~~RDB 写入加 fsync~~ | P0 | `writer.get_ref().sync_all()?` in rdb.rs save() |
| ~~connection.rs UTF-8 安全~~ | P3 | `if let Ok()` 安全处理替代 unwrap |
| ~~Slowlog 双锁合并~~ | P3 | 合并为单一 `Mutex<SlowLogInner>` |
| ~~Eviction policy 改 AtomicU8~~ | P1 | `AtomicU8` 编码 + `Relaxed` 读写 |
| ~~Hash 读操作改 read_shard~~ | P1 | 8 个只读操作改用 `read_shard()` + 惰性清理 |
| ~~Instant::now() 按需调用~~ | P1 | NoEviction 时跳过 LRU 更新 |
| ~~Tokio features 精简~~ | P3 | 精简为 7 个实际需要的 feature |
| ~~Keyspace 通知 config 免 clone~~ | P1 | 零分配便捷方法，不再 clone 整个 Config |
| ~~命令解析零分配~~ | P1 | 栈上 `[u8; 32]` 缓冲区 + 字节级大小写转换 |
| ~~RESP buffer 回滚优化~~ | P1 | 偏移量追踪替代 `buf.clone()` 快照 |
| ~~ACL 密码 SHA1→SHA256~~ | P0 | `sha2::Sha256` 替代 `sha1::Sha1` |
| ~~is_expired() 缓存时间戳~~ | P1 | 批量遍历时外部传入时间戳 |
| ~~Inline 命令解析栈分配~~ | P3 | `[Option<&str>; 8]` 栈数组 + fallback |
| ~~Hash/Set/Key 去除无意义 sort()~~ | P1 | 删除 4 处无意义排序 |
| ~~cleanup_hash_expired_fields 条件化~~ | P1 | 先检查 has_hash_field_expirations |
| ~~check_and_remove_expired 优化~~ | P1 | Entry 添加 `has_expiry` 标记跳过无 TTL key |
| ~~List 操作热路径 clone 优化~~ | P3 | with_capacity 预分配 |
| ~~ahash 全面应用~~ | P2 | ShardedMap/Hash/Stream/ZSet 全部改为 AHashMap |
| ~~Lua VM 池化~~ | P1 | per-worker VM pool，执行完 reset 复用 |
| ~~SCAN cursor 惰性迭代~~ | P1 | cursor 编码 shard_index(高16位) + offset(低48位) |
| ~~RwLock unwrap 统一处理~~ | P0 | PoisonRecover trait，151 处统一替换 |
| ~~blocking_waiters per-shard~~ | P2 | 64 shard 分片，与 ShardedMap 对齐 |
| ~~Replication backlog 环形缓冲区~~ | P1 | 固定大小环形缓冲区 O(1) 淘汰 |
| ~~ACL 权限检查 ArcSwap~~ | P1 | ArcSwap + glob::Pattern 预编译 |
| ~~Entry 元数据压缩~~ | P1 | last_access 改 u32 秒偏移 + access_count 改 u32 |
| ~~WATCH spin loop 优化~~ | P1 | 改为阻塞 read_or_recover() |
| ~~读操作写锁降级~~ | P1 | key_type/dbsize 先读锁检查，过期时才升级写锁 |
| ~~PubSub channel 名 Arc<str>~~ | P3 | subscribe/publish 路径改为 Arc<str> 共享 |
| ~~RDB 保存零拷贝~~ | P1 | 持有读锁直接序列化，避免深拷贝 |
| ~~AOF fsync 错误传播~~ | P0 | fsync_failures 计数器 + always 模式传播错误 |
| ~~Replication read-clone ArcSwap~~ | P3 | master_replid/host 改为 ArcSwap 无锁读 |
| ~~Client 注册表分片~~ | P2 | ShardedClients 16 分片替代全局 RwLock |
| ~~hash_field_expirations 分片~~ | P2 | 64 shard 分片，与主存储对齐 |
| ~~Hash field 过期时间索引~~ | P2 | BTreeMap 反向索引 O(log n) 查找 |
| ~~Hash/ZSet 热路径 clone 优化~~ | P3 | hgetall with_capacity + zadd get_mut 原地更新 |
| ~~连接上下文打包~~ | P3 | ConnectionContext struct 统一 clone |
| ~~错误构造 format!() 优化~~ | P3 | AppError 改为 Cow<'static, str> 零分配静态错误 |
| ~~Gossip 消息序列化优化~~ | P3 | with_capacity 预分配 + split_whitespace 迭代器 |
| ~~内存使用量实时追踪~~ | P2 | per-shard AtomicUsize 计数器，O(1) 查询 |
| ~~空闲连接超时~~ | P2 | tokio::time::timeout + CONFIG SET timeout 动态配置 |

### 剩余（大/巨大工作量，需评估 ROI）

| 项目 | 级别 | 说明 | 工作量 |
|------|------|------|--------|
| ZSet SkipList | P2 | `ZSetData` 仍用 `BTreeMap<(OrderedFloat, String)>`，换自定义 SkipList 可提升大规模 ZSet 插入/删除性能 | 大 |
| 官方 RDB 兼容 | P2 | 当前用自定义 `REDIS-RUST` magic header，无法与官方 Redis 交换 RDB 文件。目标：支持读取官方 RDB 格式 | 大 |
| Cluster quorum failover | P2 | `cluster/failover.rs` 单节点即可 PFAIL→FAIL 并触发故障转移，Redis Cluster 要求多数 master 同意。当前实现网络分区时可能 split-brain | 大 |
| 大文件拆分 | P3 | `connection.rs`(3467行)、`executor.rs`(1654行) 过大。connection.rs 可拆分 cluster redirect、blocking ops、pub/sub 到独立模块 | 中 |
| io_uring 替代 epoll | P4 | tokio 底层用 epoll，io_uring 通过共享内存环形队列实现批量提交 + 零 syscall 轮询。仅 Linux 5.1+。预估提升 15-30% | 巨大 |
| 紧凑数据结构 | P4 | 引入 ziplist/listpack 紧凑编码，大幅降低小对象内存占用和 cache miss。预估内存降 30-50% | 巨大 |
| thread-per-core 模型 | P4 | 改为 thread-per-core（每线程独立 reactor + 独立分片数据），消除跨线程同步。预估提升 10-20% | 巨大 |
