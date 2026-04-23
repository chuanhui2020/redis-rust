#!/bin/bash
# redis-rust Sentinel 三节点验证脚本
# 适用于 Windows Git Bash 环境

set -euo pipefail

# ============================================
# 配置与初始化
# ============================================

BIN="$(cd "$(dirname "$0")" && pwd)/target/release/redis-rust"
REDIS_CLI="redis-cli"
TMP_DIR="/tmp/redis-sentinel"
TIMEOUT_READY=10
TIMEOUT_FAILOVER=30

MASTER_PORT=7101
REPLICA_PORT=7102
SENTINEL_PORTS=(27101 27102 27103)

# 记录测试结果
RESULTS=()
# 故障转移数据
FAILOVER_TIME=""
FAILOVER_OK=false
NEW_MASTER_PORT=""

# ============================================
# 工具函数
# ============================================

# 带颜色的输出
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

# 通过端口号找到 Windows PID 并杀掉
kill_by_port() {
    local port=$1
    local pid
    pid=$(netstat -ano 2>/dev/null | grep "LISTENING" | grep ":${port} " | awk '{print $NF}' | head -1)
    if [ -n "$pid" ] && [ "$pid" != "0" ]; then
        taskkill //F //PID "$pid" >/dev/null 2>&1 || true
        return 0
    fi
    return 1
}

# 记录测试结果
record_result() {
    local name=$1
    local status=$2
    RESULTS+=("$name:$status")
}

# ============================================
# 1. 环境检查与清理
# ============================================

echo "============================================"
echo "     redis-rust Sentinel 验证脚本"
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

# 清理旧环境
rm -rf "$TMP_DIR"
mkdir -p "$TMP_DIR"

# ============================================
# 2. 启动 5 个进程
# ============================================

OLDPWD=$(pwd)

# 启动 Master
log_info "启动 Master（端口 $MASTER_PORT）..."
mkdir -p "$TMP_DIR/$MASTER_PORT"
cd "$TMP_DIR/$MASTER_PORT"
"$BIN" --port "$MASTER_PORT" --no-aof > "$TMP_DIR/$MASTER_PORT/server.log" 2>&1 &
cd "$OLDPWD" || exit 1
log_info "Master 启动，PID: $!"

# 启动 Replica
log_info "启动 Replica（端口 $REPLICA_PORT）..."
mkdir -p "$TMP_DIR/$REPLICA_PORT"
cd "$TMP_DIR/$REPLICA_PORT"
"$BIN" --port "$REPLICA_PORT" --no-aof > "$TMP_DIR/$REPLICA_PORT/server.log" 2>&1 &
cd "$OLDPWD" || exit 1
log_info "Replica 启动，PID: $!"

# 启动 3 个 Sentinel
for port in "${SENTINEL_PORTS[@]}"; do
    log_info "启动 Sentinel（端口 $port）..."
    mkdir -p "$TMP_DIR/$port"
    cd "$TMP_DIR/$port"
    "$BIN" --port "$port" --sentinel > "$TMP_DIR/$port/server.log" 2>&1 &
    cd "$OLDPWD" || exit 1
    log_info "Sentinel $port 启动，PID: $!"
done

# 等待所有节点就绪
log_info "等待所有节点就绪..."
if ! wait_for_ready "$MASTER_PORT"; then
    log_error "Master $MASTER_PORT 未在 ${TIMEOUT_READY} 秒内就绪"
    exit 1
fi
log_ok "Master $MASTER_PORT 就绪"

if ! wait_for_ready "$REPLICA_PORT"; then
    log_error "Replica $REPLICA_PORT 未在 ${TIMEOUT_READY} 秒内就绪"
    exit 1
fi
log_ok "Replica $REPLICA_PORT 就绪"

for port in "${SENTINEL_PORTS[@]}"; do
    if ! wait_for_ready "$port"; then
        log_error "Sentinel $port 未在 ${TIMEOUT_READY} 秒内就绪"
        exit 1
    fi
    log_ok "Sentinel $port 就绪"
done

# ============================================
# 3. 配置拓扑
# ============================================

log_info "配置 Replica $REPLICA_PORT 跟随 Master $MASTER_PORT..."
$REDIS_CLI -p "$REPLICA_PORT" REPLICAOF 127.0.0.1 "$MASTER_PORT" >/dev/null 2>&1 || true
sleep 1

log_info "配置 Sentinel 监控..."
for port in "${SENTINEL_PORTS[@]}"; do
    $REDIS_CLI -p "$port" SENTINEL MONITOR mymaster 127.0.0.1 "$MASTER_PORT" 1 >/dev/null 2>&1 || true
    $REDIS_CLI -p "$port" SENTINEL SET mymaster down-after-milliseconds 5000 >/dev/null 2>&1 || true
    $REDIS_CLI -p "$port" SENTINEL SET mymaster failover-timeout 10000 >/dev/null 2>&1 || true
    log_info "Sentinel $port 配置完成"
done

# Sentinel 每 10 秒通过 INFO replication 发现 replica，等待足够时间
log_info "等待 Sentinel 发现拓扑 (15 秒)..."
sleep 15

# ============================================
# 4. 功能验证
# ============================================

echo ""
echo "============================================"
echo "[功能验证]"
echo "============================================"

# 4.1 SENTINEL MASTERS
log_info "测试: SENTINEL MASTERS..."
resp=$($REDIS_CLI -p 27101 SENTINEL MASTERS 2>/dev/null || true)
if echo "$resp" | grep -q "mymaster"; then
    log_ok "SENTINEL MASTERS"
    record_result "SENTINEL MASTERS" "PASS"
else
    log_error "SENTINEL MASTERS - 未找到 mymaster"
    record_result "SENTINEL MASTERS" "FAIL"
fi

# 4.2 SENTINEL MASTER mymaster
log_info "测试: SENTINEL MASTER mymaster..."
resp=$($REDIS_CLI -p 27101 SENTINEL MASTER mymaster 2>/dev/null || true)
if echo "$resp" | grep -q "127.0.0.1" && echo "$resp" | grep -q "$MASTER_PORT"; then
    log_ok "SENTINEL MASTER mymaster"
    record_result "SENTINEL MASTER mymaster" "PASS"
else
    log_error "SENTINEL MASTER mymaster - 返回异常"
    record_result "SENTINEL MASTER mymaster" "FAIL"
fi

# 4.3 SENTINEL REPLICAS mymaster
log_info "测试: SENTINEL REPLICAS mymaster..."
resp=$($REDIS_CLI -p 27101 SENTINEL REPLICAS mymaster 2>/dev/null || true)
replica_count=$(echo "$resp" | grep -c "slave" || true)
if [ -n "$replica_count" ] && [ "$replica_count" -ge 1 ]; then
    log_ok "SENTINEL REPLICAS mymaster ($replica_count 个 replica)"
    record_result "SENTINEL REPLICAS mymaster" "PASS"
else
    log_error "SENTINEL REPLICAS mymaster - 未找到 replica"
    record_result "SENTINEL REPLICAS mymaster" "FAIL"
fi

# 4.4 SENTINEL SENTINELS mymaster
log_info "测试: SENTINEL SENTINELS mymaster..."
resp=$($REDIS_CLI -p 27101 SENTINEL SENTINELS mymaster 2>/dev/null || true)
sentinel_count=$(echo "$resp" | grep -c "runid" || true)
if [ -n "$sentinel_count" ] && [ "$sentinel_count" -ge 2 ]; then
    log_ok "SENTINEL SENTINELS mymaster ($sentinel_count 个其他 sentinel)"
    record_result "SENTINEL SENTINELS mymaster" "PASS"
else
    log_error "SENTINEL SENTINELS mymaster - 发现 $sentinel_count 个 sentinel，期望 2"
    record_result "SENTINEL SENTINELS mymaster" "FAIL"
fi

# 4.5 SENTINEL GET-MASTER-ADDR-BY-NAME mymaster
log_info "测试: SENTINEL GET-MASTER-ADDR-BY-NAME mymaster..."
resp=$($REDIS_CLI -p 27101 SENTINEL GET-MASTER-ADDR-BY-NAME mymaster 2>/dev/null || true)
if echo "$resp" | grep -q "127.0.0.1" && echo "$resp" | grep -q "$MASTER_PORT"; then
    log_ok "SENTINEL GET-MASTER-ADDR-BY-NAME mymaster"
    record_result "SENTINEL GET-MASTER-ADDR-BY-NAME mymaster" "PASS"
else
    log_error "SENTINEL GET-MASTER-ADDR-BY-NAME mymaster - 返回异常: $resp"
    record_result "SENTINEL GET-MASTER-ADDR-BY-NAME mymaster" "FAIL"
fi

# 4.6 数据复制验证
log_info "测试: 数据复制验证..."
$REDIS_CLI -p "$MASTER_PORT" SET sentinel_test_key "hello_sentinel" >/dev/null 2>&1 || true
sleep 2
replica_val=$($REDIS_CLI -p "$REPLICA_PORT" GET sentinel_test_key 2>/dev/null | tr -d '\r' || true)
if [ "$replica_val" = "hello_sentinel" ]; then
    log_ok "数据复制验证"
    record_result "数据复制验证" "PASS"
else
    log_error "数据复制验证 - replica 读取值不匹配 (got: '$replica_val')"
    record_result "数据复制验证" "FAIL"
fi

# ============================================
# 5. 故障转移测试
# ============================================

echo ""
echo "============================================"
echo "[故障转移]"
echo "============================================"

# 在 kill master 前先写入测试数据
log_info "向 Master 写入故障转移测试数据..."
$REDIS_CLI -p "$MASTER_PORT" SET failover_key "failover_value" >/dev/null 2>&1 || true
sleep 1

log_info "Kill 掉 Master $MASTER_PORT..."
kill_by_port "$MASTER_PORT"

FAILOVER_START=$(date +%s)
deadline=$(($(date +%s) + TIMEOUT_FAILOVER))
FAILOVER_OK=false
NEW_MASTER_PORT=""

log_info "等待 Sentinel 选举新 master（最多 ${TIMEOUT_FAILOVER} 秒）..."
while [ "$(date +%s)" -lt "$deadline" ]; do
    resp=$($REDIS_CLI -p 27101 SENTINEL GET-MASTER-ADDR-BY-NAME mymaster 2>/dev/null || true)
    new_port=$(echo "$resp" | grep -oE '[0-9]+' | tail -1 || true)
    if [ -n "$new_port" ] && [ "$new_port" != "$MASTER_PORT" ]; then
        NEW_MASTER_PORT="$new_port"
        FAILOVER_OK=true
        break
    fi
    sleep 2
done

FAILOVER_END=$(date +%s)
FAILOVER_TIME=$((FAILOVER_END - FAILOVER_START))

if [ "$FAILOVER_OK" = true ]; then
    log_ok "故障转移完成，新 master 端口: $NEW_MASTER_PORT (耗时: ${FAILOVER_TIME}秒)"
    record_result "自动故障转移" "PASS"
else
    log_error "故障转移失败 - 未在 ${TIMEOUT_FAILOVER} 秒内选举新 master"
    record_result "自动故障转移" "FAIL"
fi

# ============================================
# 6. 故障转移后数据完整性
# ============================================

log_info "测试: 故障转移后数据完整性..."
if [ "$FAILOVER_OK" = true ] && [ -n "$NEW_MASTER_PORT" ]; then
    # 等待新 master 就绪
    sleep 2
    val=$($REDIS_CLI -p "$NEW_MASTER_PORT" GET failover_key 2>/dev/null | tr -d '\r' || true)
    if [ "$val" = "failover_value" ]; then
        log_ok "故障转移后数据完整性"
        record_result "故障转移后数据完整性" "PASS"
    else
        log_error "故障转移后数据完整性 - 值不匹配 (got: '$val')"
        record_result "故障转移后数据完整性" "FAIL"
    fi
else
    log_warn "故障转移后数据完整性 - 跳过（故障转移未成功）"
    record_result "故障转移后数据完整性" "FAIL"
fi

# ============================================
# 7. 输出报告
# ============================================

echo ""
echo "============================================"
echo "     redis-rust Sentinel 验证报告"
echo "============================================"
echo "架构: 1 Master + 1 Replica + 3 Sentinel"
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
echo "[故障转移]"
if [ "$FAILOVER_OK" = true ]; then
    echo "  ✓ Master $MASTER_PORT 宕机后端口 $NEW_MASTER_PORT 提升为 master"
    echo "  故障转移耗时: ${FAILOVER_TIME} 秒"
else
    echo "  ✗ 故障转移失败"
fi

echo ""
echo "总计: $passed/$total 项通过"
echo "============================================"

if [ "$passed" -eq "$total" ]; then
    exit 0
else
    exit 1
fi
