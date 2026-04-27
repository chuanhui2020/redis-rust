# Redis-Rust 性能优化记录

## 已完成的优化

### Phase 1 — P0 关键瓶颈（已完成）

| 项目 | 描述 | 关键改动 | 状态 |
|------|------|----------|------|
| WATCH/EXEC 死锁修复 | `bump_version()` 递归获取 `RwLock` 导致死锁 | 改为从调用方传入 `&mut HashMap`；修复 50+ 调用点 | ✅ |
| 淘汰随机采样 | `evict_lru`/`evict_lfu`/`evict_ttl` 原 O(N) 全表扫描 | 引入 Redis 风格随机采样（16 候选） | ✅ |
| AOF 异步写入 | `Arc<Mutex<AofWriter>>` 全局锁竞争 | `crossbeam::channel` + 独立后台线程；`append()` 非阻塞 | ✅ |

### Phase 2 — P1 中高优先级（已完成）

| 项目 | 描述 | 关键改动 | 状态 |
|------|------|----------|------|
| Pipeline 缓冲区复用 | 每条命令分配 `Vec::with_capacity(64)` | 连接循环内复用 `encode_buf`（`write_resp_buf`） | ✅ |
| RESP3 协议支持 | 扩展协议解析/编码 | 新增 `Null`/`Bool`/`Double`/`Map`/`Set`/`Push`；解析+编码+Lua桥接 | ✅ |

---

## 剩余优化项

### P1 — 中高优先级

#### 1. RESP3 `HELLO` 协商
- **问题**：目前解析器能识别 RESP3 类型，但连接建立后没有协议版本协商逻辑
- **目标**：实现 `HELLO [2|3]` 命令，客户端可选择 RESP2（默认）或 RESP3 模式
- **涉及文件**：`src/server/connection.rs`、`src/server/handler.rs`、`src/command/executor.rs`
- **工作量**：中等（需维护连接级别的协议版本状态，并切换响应格式）

### P2 — 长期改进

#### 2. ZSet SkipList
- **问题**：`ZSetData` 仍使用 `BTreeMap<OrderedFloat, String> + HashMap<String, OrderedFloat>`
- **目标**：实现自定义 SkipList，降低插入/删除复杂度，提升 ZSet 大规模数据性能
- **涉及文件**：`src/storage/zset_ops.rs` 及相关 ZSet 模块
- **工作量**：大（需重写 ZSet 核心数据结构及所有操作）

#### 3. 官方 RDB 兼容
- **问题**：当前 RDB 文件使用自定义 magic header `REDIS-RUST`，无法与官方 Redis 交换 RDB
- **目标**：读取官方 Redis RDB 格式（保留写入自定义格式的可选能力）
- **涉及文件**：`src/rdb.rs`
- **工作量**：大（需完整实现官方 RDB 版本解析，兼容多个 Redis 版本）

### 其他

#### 4. 编译警告清理
- `src/aof.rs:417`：`Mutex` 未使用导入（测试代码中）
- `src/server/handler.rs:15`：`write_resp_bytes` 未使用
- `src/storage/mod.rs`：`touch_entry`、`bump_version_in_db` 未使用
- **工作量**：小

#### 5. 静态响应热路径
- 部分高频响应（如 `OK`、`PONG`、`0`、`1`、`NULL`）已有预编码静态字节，但尚未在所有路径上替换
- **工作量**：小（需梳理各命令返回路径，统一使用 `write_resp_bytes`）
