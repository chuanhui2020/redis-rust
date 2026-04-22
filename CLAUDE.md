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
bash bench.sh
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
   bash bench.sh
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

## Redis 7 Implementation Progress

> 每次开发完成后必须更新此章节，保持进度与代码同步。

**已实现（209 个命令）：**

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
| Scripting | 10（EVAL + Functions） | 71% | 缺 SCRIPT DEBUG/HELP |
| ACL | 8 | 67% | 缺 SAVE/LOAD/DRYRUN |
| Client | 12 | 71% | 缺 TRACKING/CACHING |
| Pub/Sub | 8（含 PUBSUB CHANNELS/NUMSUB/NUMPAT） | 67% | 缺 Sharded Pub/Sub |
| Server/Admin | 32 | 80% | 缺 COMMAND DOCS/LIST/COUNT |
| 持久化（RDB+AOF） | — | — | 完成 |
| Lua 脚本 + Functions | — | — | 完成 |
| Replication | 4（REPLICAOF/ROLE/PSYNC/REPLCONF） | — | 基础完成（全量同步+增量同步+写传播+心跳） |

**未实现（~73 个命令）：**

| 分类 | 缺失内容 | 优先级 |
|------|----------|--------|
| Sharded Pub/Sub | SPUBLISH/SSUBSCRIBE/SUNSUBSCRIBE | 中 |
| ACL 持久化 | ACL SAVE/LOAD | 中 |
| ACL DRYRUN | 权限测试 | 低 |
| CLIENT TRACKING | 服务端辅助客户端缓存 | 中 |
| COMMAND 内省 | DOCS/LIST/COUNT/GETKEYS | 低 |
| Cluster | 全部 30+ 命令 | 暂不实现（单机架构） |
| Replication 补全 | SYNC（旧协议）、WAIT、FAILOVER | 低 |
| Sentinel | 全部 10+ 命令 | 暂不实现 |
| Module | MODULE LOAD/UNLOAD/LIST | 暂不实现 |

**总覆盖率：~74%（209/282），单机功能维度 >95%。**
