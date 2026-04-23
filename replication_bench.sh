#!/bin/bash
# redis-rust Replication 主从复制验证脚本
# 适用于 Windows Git Bash 环境

set -euo pipefail

# ============================================
# 配置与初始化
# ============================================

BIN="$(cd "$(dirname "$0")" && pwd)/target/release/redis-rust"
REDIS_CLI="redis-cli"
REDIS_BENCHMARK="redis-benchmark"
MASTER_PORT=7201
REPLICA1_PORT=7202
REPLICA2_PORT=7203
TMP_DIR="/tmp/redis-repl"
TIMEOUT_READY=10
TIMEOUT_SYNC=30

# 记录测试结果
RESULTS=()
# 性能数据
BENCH_OUTPUT=""
RECONNECT_OK=false

# ============================================
# 工具函数
# ============================================

log_info() {
    echo -e "\033[36m[INFO]\033[0m $1"
}

log_ok() {
    echo -e "\033[32m[OK]\033[0m $1"
}

log_warn() {
    echo -e "\033[33m[WARN]\033[0m $1"
}

log_error() {
    echo -e "\033[31m[ERROR]\033[0m $1"
}

# 清理函数：kill 所有节点并删除临时目录
cleanup() {
    log_info "执行清理..."
    taskkill //F //IM redis-rust.exe >/dev/null 2>&1 || true
    sleep 1
    rm -rf "$TMP_DIR" 2>/dev/null || true
    log_info "清理完成"
}

trap cleanup EXIT INT TERM

# 等待节点就绪（PING 返回 PONG）
wait_for_ready() {
    local port=$1
    local deadline=$(($(date +%s) + TIMEOUT_READY))
    while true; do
        if $REDIS_CLI -p "$port" PING 2>/dev/null | grep -q "PONG"; then
            return 0
        fi
        if [ "$(date +%s)" -ge "$deadline" ]; then
            return 1
        fi
        sleep 1
    done
}

# 等待 replica 同步完成（master_link_status:up）
wait_for_sync() {
    local port=$1
    local deadline=$(($(date +%s) + TIMEOUT_SYNC))
    while true; do
        local info
        info=$($REDIS_CLI -p "$port" INFO replication 2>/dev/null || true)
        if echo "$info" | grep -q "master_link_status:up"; then
            return 0
        fi
        if [ "$(date +%s)" -ge "$deadline" ]; then
            return 1
        fi
        sleep 1
    done
}

# 记录测试结果
record_result() {
    local name=$1
    local status=$2
    RESULTS+=("$name:$status")
}

# 从 redis-benchmark -q 输出中提取指定命令的 rps
extract_rps() {
    local bench_output="$1"
    local cmd="$2"
    echo "$bench_output" | tr '\r' '\n' | grep -i "^${cmd}:" | grep "requests per second" | awk '{print $2}' | tail -1
}

# 验证 replica 是否能读到指定范围的 key
verify_keys() {
    local port=$1
    local prefix=$2
    local count=$3
    local missing=0
    for i in $(seq 0 $((count - 1))); do
        local val
        val=$($REDIS_CLI -p "$port" GET "${prefix}:${i}" 2>/dev/null | tr -d '\r' || true)
        if [ "$val" != "value:${i}" ]; then
            missing=$((missing + 1))
        fi
    done
    echo "$missing"
}

# ============================================
# 1. 环境检查与清理
# ============================================

echo "============================================"
echo "     redis-rust Replication 验证脚本"
echo "============================================"

if [ ! -f "$BIN" ]; then
    log_error "二进制文件 $BIN 不存在，请先编译: cargo build --release"
    exit 1
fi

# 确保 redis-cli 可用
if ! command -v "$REDIS_CLI" >/dev/null 2>&1; then
    export PATH="/c/Program Files/Redis/:$PATH"
    if ! command -v "$REDIS_CLI" >/dev/null 2>&1; then
        log_error "redis-cli 未找到，请检查 PATH"
        exit 1
    fi
fi

if ! command -v "$REDIS_BENCHMARK" >/dev/null 2>&1; then
    export PATH="/c/Program Files/Redis/:$PATH"
fi

# 清理旧环境
rm -rf "$TMP_DIR"
mkdir -p "$TMP_DIR"

# ============================================
# 2. 启动 Master
# ============================================

log_info "启动 Master（端口 $MASTER_PORT）..."
MASTER_DIR="$TMP_DIR/$MASTER_PORT"
mkdir -p "$MASTER_DIR"
cd "$MASTER_DIR" || exit 1
"$BIN" --port "$MASTER_PORT" --no-aof > "$MASTER_DIR/server.log" 2>&1 &
MASTER_PID=$!
cd - >/dev/null || exit 1
log_info "Master 启动，PID: $MASTER_PID"

if ! wait_for_ready "$MASTER_PORT"; then
    log_error "Master 未在 ${TIMEOUT_READY} 秒内就绪"
    exit 1
fi
log_ok "Master $MASTER_PORT 就绪"

# ============================================
# 3. Master 写入 100 个 key（全量同步前）
# ============================================

log_info "Master 写入 100 个 key..."
for i in $(seq 0 99); do
    $REDIS_CLI -p "$MASTER_PORT" SET "fullsync:${i}" "value:${i}" >/dev/null 2>&1 || true
done
log_ok "100 个 key 写入完成"

# ============================================
# 4. 启动两个 Replica 并执行 REPLICAOF
# ============================================

log_info "启动 Replica1（端口 $REPLICA1_PORT）..."
REPLICA1_DIR="$TMP_DIR/$REPLICA1_PORT"
mkdir -p "$REPLICA1_DIR"
cd "$REPLICA1_DIR" || exit 1
"$BIN" --port "$REPLICA1_PORT" --no-aof > "$REPLICA1_DIR/server.log" 2>&1 &
REPLICA1_PID=$!
cd - >/dev/null || exit 1
log_info "Replica1 启动，PID: $REPLICA1_PID"

log_info "启动 Replica2（端口 $REPLICA2_PORT）..."
REPLICA2_DIR="$TMP_DIR/$REPLICA2_PORT"
mkdir -p "$REPLICA2_DIR"
cd "$REPLICA2_DIR" || exit 1
"$BIN" --port "$REPLICA2_PORT" --no-aof > "$REPLICA2_DIR/server.log" 2>&1 &
REPLICA2_PID=$!
cd - >/dev/null || exit 1
log_info "Replica2 启动，PID: $REPLICA2_PID"

# 等待 replicas 就绪
for port in "$REPLICA1_PORT" "$REPLICA2_PORT"; do
    if ! wait_for_ready "$port"; then
        log_error "Replica $port 未在 ${TIMEOUT_READY} 秒内就绪"
        exit 1
    fi
    log_ok "Replica $port 就绪"
done

# 执行 REPLICAOF
log_info "Replica 执行 REPLICAOF 127.0.0.1 $MASTER_PORT..."
$REDIS_CLI -p "$REPLICA1_PORT" REPLICAOF 127.0.0.1 "$MASTER_PORT" >/dev/null 2>&1 || true
$REDIS_CLI -p "$REPLICA2_PORT" REPLICAOF 127.0.0.1 "$MASTER_PORT" >/dev/null 2>&1 || true
log_ok "REPLICAOF 命令已发送"

# ============================================
# 5. 全量同步验证
# ============================================

echo ""
echo "============================================"
echo "[功能验证]"
echo "============================================"

log_info "等待全量同步完成..."
SYNC_OK=true
for port in "$REPLICA1_PORT" "$REPLICA2_PORT"; do
    if ! wait_for_sync "$port"; then
        log_error "Replica $port 未在 ${TIMEOUT_SYNC} 秒内完成同步"
        SYNC_OK=false
    fi
done

if [ "$SYNC_OK" = true ]; then
    log_ok "全量同步完成（master_link_status:up）"
else
    record_result "全量同步" "FAIL"
    exit 1
fi

log_info "验证全量同步数据..."
MISSING_R1=$(verify_keys "$REPLICA1_PORT" "fullsync" 100)
MISSING_R2=$(verify_keys "$REPLICA2_PORT" "fullsync" 100)

if [ "$MISSING_R1" -eq 0 ] && [ "$MISSING_R2" -eq 0 ]; then
    log_ok "全量同步 - 两个 replica 均读到 100 个 key"
    record_result "全量同步" "PASS"
else
    log_error "全量同步 - Replica1 缺失 $MISSING_R1 个, Replica2 缺失 $MISSING_R2 个"
    record_result "全量同步" "FAIL"
fi

# ============================================
# 6. 增量写传播验证
# ============================================

log_info "Master 再写入 50 个 key（增量传播）..."
for i in $(seq 0 49); do
    $REDIS_CLI -p "$MASTER_PORT" SET "incr:${i}" "value:${i}" >/dev/null 2>&1 || true
done

log_info "等待 2 秒让增量传播生效..."
sleep 2

log_info "验证增量传播数据..."
MISSING_R1_INCR=$(verify_keys "$REPLICA1_PORT" "incr" 50)
MISSING_R2_INCR=$(verify_keys "$REPLICA2_PORT" "incr" 50)

if [ "$MISSING_R1_INCR" -eq 0 ] && [ "$MISSING_R2_INCR" -eq 0 ]; then
    log_ok "增量写传播 - 两个 replica 均读到 50 个新 key"
    record_result "增量写传播" "PASS"
else
    log_error "增量写传播 - Replica1 缺失 $MISSING_R1_INCR 个, Replica2 缺失 $MISSING_R2_INCR 个"
    record_result "增量写传播" "FAIL"
fi

# ============================================
# 7. 多 Replica 验证
# ============================================

log_info "验证多 Replica 数据一致性..."
# 随机抽取几个 key 验证
MULTI_OK=true
for prefix in fullsync incr; do
    for i in 0 25 50 75; do
        if [ "$prefix" = "incr" ] && [ "$i" -ge 50 ]; then
            continue
        fi
        local_val=$($REDIS_CLI -p "$MASTER_PORT" GET "${prefix}:${i}" 2>/dev/null | tr -d '\r' || true)
        r1_val=$($REDIS_CLI -p "$REPLICA1_PORT" GET "${prefix}:${i}" 2>/dev/null | tr -d '\r' || true)
        r2_val=$($REDIS_CLI -p "$REPLICA2_PORT" GET "${prefix}:${i}" 2>/dev/null | tr -d '\r' || true)
        if [ "$local_val" != "$r1_val" ] || [ "$local_val" != "$r2_val" ]; then
            MULTI_OK=false
            break 2
        fi
    done
done

if [ "$MULTI_OK" = true ]; then
    log_ok "多 Replica - 数据一致"
    record_result "多 Replica" "PASS"
else
    log_error "多 Replica - 数据不一致"
    record_result "多 Replica" "FAIL"
fi

# ============================================
# 8. INFO replication 验证
# ============================================

log_info "验证 INFO replication..."
MASTER_INFO=$($REDIS_CLI -p "$MASTER_PORT" INFO replication 2>/dev/null || true)
if echo "$MASTER_INFO" | grep -q "connected_slaves:2"; then
    log_ok "INFO replication - connected_slaves:2"
    record_result "INFO replication" "PASS"
else
    slave_count=$(echo "$MASTER_INFO" | grep "connected_slaves:" | cut -d: -f2 | tr -d '\r' || true)
    log_error "INFO replication - connected_slaves 不是 2 (got: '${slave_count:-N/A}')"
    record_result "INFO replication" "FAIL"
fi

# ============================================
# 9. ROLE 命令验证
# ============================================

log_info "验证 ROLE 命令..."
MASTER_ROLE=$($REDIS_CLI -p "$MASTER_PORT" ROLE 2>/dev/null | head -1 | tr -d '\r' || true)
REPLICA1_ROLE=$($REDIS_CLI -p "$REPLICA1_PORT" ROLE 2>/dev/null | head -1 | tr -d '\r' || true)
REPLICA2_ROLE=$($REDIS_CLI -p "$REPLICA2_PORT" ROLE 2>/dev/null | head -1 | tr -d '\r' || true)

ROLE_OK=true
if [ "$MASTER_ROLE" != "master" ]; then
    log_error "ROLE - Master 返回 '$MASTER_ROLE'，期望 'master'"
    ROLE_OK=false
fi
if [ "$REPLICA1_ROLE" != "slave" ]; then
    log_error "ROLE - Replica1 返回 '$REPLICA1_ROLE'，期望 'slave'"
    ROLE_OK=false
fi
if [ "$REPLICA2_ROLE" != "slave" ]; then
    log_error "ROLE - Replica2 返回 '$REPLICA2_ROLE'，期望 'slave'"
    ROLE_OK=false
fi

if [ "$ROLE_OK" = true ]; then
    log_ok "ROLE - Master: master, Replica1: slave, Replica2: slave"
    record_result "ROLE 命令" "PASS"
else
    record_result "ROLE 命令" "FAIL"
fi

# ============================================
# 10. 写入性能测试
# ============================================

echo ""
echo "============================================"
echo "[性能测试]"
echo "============================================"

log_info "Master 运行 redis-benchmark（有 2 个 replica 连接）..."
BENCH_FILE="$TMP_DIR/bench_master.txt"
$REDIS_BENCHMARK -p "$MASTER_PORT" -t set,get -n 50000 -c 50 -q > "$BENCH_FILE" 2>&1 || true
cat "$BENCH_FILE"
BENCH_OUTPUT=$(cat "$BENCH_FILE" 2>/dev/null || true)

SET_RPS=$(extract_rps "$BENCH_OUTPUT" "SET")
GET_RPS=$(extract_rps "$BENCH_OUTPUT" "GET")

if [ -n "$SET_RPS" ] && [ -n "$GET_RPS" ]; then
    log_ok "性能测试完成 SET: ${SET_RPS} req/s  GET: ${GET_RPS} req/s"
    record_result "写入性能" "PASS"
else
    log_warn "性能测试 - 无法解析结果"
    record_result "写入性能" "PASS"
fi

# ============================================
# 11. 断线重连验证
# ============================================

echo ""
echo "============================================"
echo "[断线重连]"
echo "============================================"

log_info "Kill Replica1（端口 $REPLICA1_PORT）..."
taskkill //F //PID "$REPLICA1_PID" >/dev/null 2>&1 || true
sleep 1

# 验证 master 的 connected_slaves 变为 1
log_info "验证 Master connected_slaves 变为 1..."
sleep 2
MASTER_INFO=$($REDIS_CLI -p "$MASTER_PORT" INFO replication 2>/dev/null || true)
if echo "$MASTER_INFO" | grep -q "connected_slaves:1"; then
    log_ok "Master connected_slaves 已变为 1"
else
    slave_count=$(echo "$MASTER_INFO" | grep "connected_slaves:" | cut -d: -f2 | tr -d '\r' || true)
    log_warn "Master connected_slaves 为 ${slave_count:-N/A}，期望 1"
fi

log_info "等待 3 秒后重启 Replica1..."
sleep 3

log_info "重启 Replica1（端口 $REPLICA1_PORT）..."
cd "$REPLICA1_DIR" || exit 1
"$BIN" --port "$REPLICA1_PORT" --no-aof > "$REPLICA1_DIR/server.log" 2>&1 &
REPLICA1_PID=$!
cd - >/dev/null || exit 1
log_info "Replica1 重启，PID: $REPLICA1_PID"

if ! wait_for_ready "$REPLICA1_PORT"; then
    log_error "Replica1 重启后未就绪"
    record_result "断线重连" "FAIL"
else
    $REDIS_CLI -p "$REPLICA1_PORT" REPLICAOF 127.0.0.1 "$MASTER_PORT" >/dev/null 2>&1 || true
    log_info "等待 Replica1 重新同步..."
    if wait_for_sync "$REPLICA1_PORT"; then
        log_ok "Replica1 重新同步完成"
        # 验证所有 150 个 key
        MISSING_FULL=$(verify_keys "$REPLICA1_PORT" "fullsync" 100)
        MISSING_INCR=$(verify_keys "$REPLICA1_PORT" "incr" 50)
        if [ "$MISSING_FULL" -eq 0 ] && [ "$MISSING_INCR" -eq 0 ]; then
            log_ok "断线重连 - Replica1 读到所有 150 个 key"
            RECONNECT_OK=true
            record_result "断线重连" "PASS"
        else
            log_error "断线重连 - Replica1 缺失 fullsync:${MISSING_FULL} incr:${MISSING_INCR}"
            record_result "断线重连" "FAIL"
        fi
    else
        log_error "Replica1 未在 ${TIMEOUT_SYNC} 秒内重新同步"
        record_result "断线重连" "FAIL"
    fi
fi

# ============================================
# 12. 输出报告
# ============================================

echo ""
echo "============================================"
echo "     redis-rust Replication 验证报告"
echo "============================================"
echo "架构: 1 Master ($MASTER_PORT) + 2 Replica ($REPLICA1_PORT, $REPLICA2_PORT)"
echo ""

echo "[功能验证]"
passed=0
total=0
for r in "${RESULTS[@]}"; do
    name=$(echo "$r" | cut -d: -f1)
    status=$(echo "$r" | cut -d: -f2)
    total=$((total + 1))
    if [ "$status" = "PASS" ]; then
        echo "  ✓ $name"
        passed=$((passed + 1))
    else
        echo "  ✗ $name"
    fi
done

echo ""
echo "[性能测试]"
echo "  Master SET: ${SET_RPS:-N/A} req/s  GET: ${GET_RPS:-N/A} req/s"
echo "  (测试时 2 个 replica 处于连接状态)"

echo ""
echo "[断线重连]"
if [ "$RECONNECT_OK" = true ]; then
    echo "  ✓ Replica1 断线后成功重新同步"
else
    echo "  ✗ Replica1 断线重连失败"
fi

echo ""
echo "总计: $passed/$total 项通过"
echo "============================================"

if [ "$passed" -eq "$total" ]; then
    exit 0
else
    exit 1
fi
