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
| MGET 读锁优化 | `mget()` 原使用 `write()` 锁，阻塞并发读 | 改为 `read()` 锁；移除读取时的主动过期删除（与 Redis 一致） | ✅ |
| AOF 序列化减分配 | `append()` 每次创建 `RespParser` + `encode().to_vec()` | 直接使用 `encode_append()` 到 Vec，避免中间 `Bytes` 分配 | ✅ |
| 复制传播序列化减分配 | `serialize_command_to_resp()` 使用 `parser.encode()` | 改用 `encode_append()` 到 Vec 再转 `Bytes` | ✅ |
| SET 路径 `key.clone()` 消除 | `set_plain`/`set_with_options`/`set_with_ttl`/`setnx` 中 `key.clone()` | 在 entry 构造阶段直接设置 version，避免 insert 后再 bump | ✅ |
| 条件 bump_version | `set_plain`/`set_with_options` 无条件调用 `bump_version()`（`fetch_add SeqCst`） | 仅在 `watch_count > 0` 时执行原子递增 | ✅ |
| evict_if_needed 前置检查 | `set_plain`/`set_with_options`/`set_with_ttl`/`setnx` 直接调用 `evict_if_needed()` | 添加 `has_maxmem` 判断，maxmemory=0 时跳过全部淘汰检查 | ✅ |
| 编译警告清理 | `write_resp_bytes`、`touch_entry`、`bump_version_in_db` 等未使用 | 移除死代码；简化 transaction.rs 导入 | ✅ |
| 响应路径 inline | `write_resp_buf`、`send_reply`、`is_write_command` 未内联 | 添加 `#[inline]` 属性减少函数调用开销 | ✅ |

---

## 剩余优化项

### P1 — 中高优先级

#### ~~1. RESP3 `HELLO` 协商~~ ✅ 已完成
- 已实现 `HELLO [2|3]` 命令，支持连接级协议版本协商

#### ~~2. Pipeline 批量 Side Effects~~ ✅ 已完成
- Pipeline 内循环延迟 AOF/keyspace/复制写入，结束后通过 `flush_batch()` 一次性批量处理

### P2 — 长期改进

#### 3. ZSet SkipList
- **问题**：`ZSetData` 仍使用 `BTreeMap<OrderedFloat, String> + HashMap<String, OrderedFloat>`
- **目标**：实现自定义 SkipList，降低插入/删除复杂度，提升 ZSet 大规模数据性能
- **涉及文件**：`src/storage/zset_ops.rs` 及相关 ZSet 模块
- **工作量**：大（需重写 ZSet 核心数据结构及所有操作）

#### 4. 官方 RDB 兼容
- **问题**：当前 RDB 文件使用自定义 magic header `REDIS-RUST`，无法与官方 Redis 交换 RDB
- **目标**：读取官方 Redis RDB 格式（保留写入自定义格式的可选能力）
- **涉及文件**：`src/rdb.rs`
- **工作量**：大（需完整实现官方 RDB 版本解析，兼容多个 Redis 版本）

### 其他

#### 5. 静态响应热路径补全
- `OK`、`PONG`、`0`、`1`、`NULL` 预编码字节已覆盖 `write_resp_buf`，但部分命令仍通过 `RespValue` 构造后编码
- **工作量**：小（需梳理各命令返回路径，直接返回预编码字节或简化 `RespValue` 构造）
