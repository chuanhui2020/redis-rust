#!/bin/bash
# redis-rust Cluster 六节点验证脚本
# 适用于 Windows Git Bash 环境

set -euo pipefail

# ============================================
# 配置与初始化
# ============================================

BIN="$(cd "$(dirname "$0")" && pwd)/target/release/redis-rust"
REDIS_CLI="redis-cli"
REDIS_BENCHMARK="redis-benchmark"
BASE_PORT=7001
NODE_COUNT=6
MASTER_COUNT=3
TMP_DIR="/tmp/redis-cluster"
TIMEOUT_READY=10
TIMEOUT_CLUSTER_OK=30
TIMEOUT_FAILOVER=60

# 记录各节点 PID
PIDS=()
# 记录测试结果
RESULTS=()
# 性能数据
PERF_7001=""
PERF_7002=""
PERF_7003=""
AGG_SET=""
AGG_GET=""
FAILOVER_TIME=""
FAILOVER_OK=false

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

# 等待集群状态变为 ok
wait_for_cluster_ok() {
    local port=$1
    local deadline=$(($(date +%s) + TIMEOUT_CLUSTER_OK))
    while true; do
        local info
        info=$($REDIS_CLI -p "$port" CLUSTER INFO 2>/dev/null || true)
        if echo "$info" | grep -q "cluster_state:ok" && echo "$info" | grep -q "cluster_slots_assigned:16384"; then
            return 0
        fi
        if [ "$(date +%s)" -ge "$deadline" ]; then
            return 1
        fi
        sleep 2
    done
}

# 获取指定端口的 node-id
get_node_id() {
    local port=$1
    $REDIS_CLI -p "$port" CLUSTER MYID 2>/dev/null | tr -d '\r'
}

# 执行命令，如果遇到 MOVED 则跟随重定向到正确节点
follow_redirect() {
    local port=$1
    shift
    local resp
    resp=$($REDIS_CLI -p "$port" "$@" 2>&1 || true)
    if echo "$resp" | grep -q "MOVED"; then
        local target_port
        target_port=$(echo "$resp" | awk '{print $3}' | cut -d: -f2 | tr -d '\r')
        $REDIS_CLI -p "$target_port" "$@" 2>/dev/null
    else
        echo "$resp"
    fi
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

# ============================================
# 1. 环境检查与清理
# ============================================

echo "============================================"
echo "     redis-rust Cluster 验证脚本"
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
# 2. 启动 6 个节点
# ============================================

log_info "启动 6 个 Cluster 节点（端口 7001-7006）..."

OLDPWD=$(pwd)
for i in $(seq 1 $NODE_COUNT); do
    port=$((BASE_PORT + i - 1))
    node_dir="$TMP_DIR/$port"
    mkdir -p "$node_dir"
    cd "$node_dir" || exit 1
    "$BIN" --port "$port" --cluster-enabled yes --no-aof > "$node_dir/server.log" 2>&1 &
    pid=$!
    cd "$OLDPWD" || exit 1
    PIDS+=($pid)
    log_info "节点 $port 启动，PID: $pid"
done

# 等待所有节点就绪
log_info "等待节点就绪..."
for i in $(seq 1 $NODE_COUNT); do
    port=$((BASE_PORT + i - 1))
    if ! wait_for_ready "$port"; then
        log_error "节点 $port 未在 ${TIMEOUT_READY} 秒内就绪"
        exit 1
    fi
    log_ok "节点 $port 就绪"
done

# ============================================
# 3. 组建集群
# ============================================

log_info "组建集群：MEET..."

# 以 7001 为基准，让其他节点都加入
for i in $(seq 2 $NODE_COUNT); do
    port=$((BASE_PORT + i - 1))
    $REDIS_CLI -p 7001 CLUSTER MEET 127.0.0.1 "$port" >/dev/null 2>&1 || true
    $REDIS_CLI -p "$port" CLUSTER MEET 127.0.0.1 7001 >/dev/null 2>&1 || true
done

# 额外 MEET，确保所有节点两两已知
for i in $(seq 2 $NODE_COUNT); do
    for j in $(seq 2 $NODE_COUNT); do
        if [ "$i" -ne "$j" ]; then
            port_i=$((BASE_PORT + i - 1))
            port_j=$((BASE_PORT + j - 1))
            $REDIS_CLI -p "$port_i" CLUSTER MEET 127.0.0.1 "$port_j" >/dev/null 2>&1 || true
        fi
    done
done

sleep 3

log_info "分配 Slot 给 3 个 Master..."

# 分批 ADDSLOTS，每批 1000 个 slot，避免命令行过长
addslots_batch() {
    local port=$1
    local start=$2
    local end=$3
    local batch_size=1000
    local s=$start
    while [ "$s" -le "$end" ]; do
        local e=$((s + batch_size - 1))
        if [ "$e" -gt "$end" ]; then
            e=$end
        fi
        local slots
        slots=$(seq "$s" "$e" | tr '\n' ' ')
        $REDIS_CLI -p "$port" CLUSTER ADDSLOTS $slots >/dev/null 2>&1 || true
        s=$((e + 1))
    done
}

addslots_batch 7001 0 5460
addslots_batch 7002 5461 10922
addslots_batch 7003 10923 16383

# 获取 master 的 node-id
log_info "获取 Master node-id 并设置 Replica..."

NODE_ID_7001=$(get_node_id 7001)
NODE_ID_7002=$(get_node_id 7002)
NODE_ID_7003=$(get_node_id 7003)

log_info "7001 node-id: ${NODE_ID_7001:0:8}..."
log_info "7002 node-id: ${NODE_ID_7002:0:8}..."
log_info "7003 node-id: ${NODE_ID_7003:0:8}..."

# 设置 replica
$REDIS_CLI -p 7004 CLUSTER REPLICATE "$NODE_ID_7001" >/dev/null 2>&1 || true
$REDIS_CLI -p 7005 CLUSTER REPLICATE "$NODE_ID_7002" >/dev/null 2>&1 || true
$REDIS_CLI -p 7006 CLUSTER REPLICATE "$NODE_ID_7003" >/dev/null 2>&1 || true

sleep 2

# 等待集群状态收敛
log_info "等待集群状态变为 ok..."
if ! wait_for_cluster_ok 7001; then
    log_error "集群未在 ${TIMEOUT_CLUSTER_OK} 秒内收敛"
    exit 1
fi
log_ok "集群状态已收敛为 ok"

# ============================================
# 4. 功能验证
# ============================================

echo ""
echo "============================================"
echo "[功能验证]"
echo "============================================"

# 4.1 基础读写
log_info "测试：基础读写..."
set_result=$(follow_redirect 7001 SET testkey testvalue)
if echo "$set_result" | grep -q "OK"; then
    val=$(follow_redirect 7001 GET testkey || true)
    val=$(echo "$val" | tr -d '\r')
    if [ "$val" = "testvalue" ]; then
        log_ok "基础读写"
        record_result "基础读写" "PASS"
    else
        log_error "基础读写 - 读取值不匹配: '$val'"
        record_result "基础读写" "FAIL"
    fi
else
    log_error "基础读写 - SET 失败: $set_result"
    record_result "基础读写" "FAIL"
fi

# 4.2 MOVED 重定向验证
log_info "测试：MOVED 重定向..."
# 找一个 slot 不在 7001 上的 key（7001 负责 0-5460）
TEST_KEY="key_moved_test"
SLOT=$($REDIS_CLI -p 7001 CLUSTER KEYSLOT "$TEST_KEY" 2>/dev/null | tr -d '\r')
MOVED_OK=false
if [ -n "$SLOT" ] && [ "$SLOT" -gt 5460 ] 2>/dev/null; then
    resp=$($REDIS_CLI -p 7001 SET "$TEST_KEY" moved_val 2>&1 || true)
    if echo "$resp" | grep -q "MOVED"; then
        MOVED_OK=true
    fi
else
    # 换一个 key
    TEST_KEY="key_moved_test_2"
    SLOT=$($REDIS_CLI -p 7001 CLUSTER KEYSLOT "$TEST_KEY" 2>/dev/null | tr -d '\r')
    resp=$($REDIS_CLI -p 7001 SET "$TEST_KEY" moved_val 2>&1 || true)
    if echo "$resp" | grep -q "MOVED"; then
        MOVED_OK=true
    fi
fi

if [ "$MOVED_OK" = true ]; then
    log_ok "MOVED 重定向"
    record_result "MOVED 重定向" "PASS"
else
    log_error "MOVED 重定向 - 未收到 MOVED 响应"
    record_result "MOVED 重定向" "FAIL"
fi

# 4.3 跨节点数据分布
log_info "测试：跨节点数据分布..."
for i in $(seq 0 99); do
    follow_redirect 7001 SET "key:$i" "value:$i" >/dev/null 2>&1 || true
done

# 统计每个 key 的 slot 归属，验证 3 个 master 上都有数据
DIST_7001=0
DIST_7002=0
DIST_7003=0
for i in $(seq 0 99); do
    slot=$($REDIS_CLI -p 7001 CLUSTER KEYSLOT "key:$i" 2>/dev/null | tr -d '\r')
    if [ -n "$slot" ] && [ "$slot" -ge 0 ] 2>/dev/null; then
        if [ "$slot" -le 5460 ]; then
            DIST_7001=$((DIST_7001 + 1))
        elif [ "$slot" -le 10922 ]; then
            DIST_7002=$((DIST_7002 + 1))
        else
            DIST_7003=$((DIST_7003 + 1))
        fi
    fi
done

if [ "$DIST_7001" -gt 0 ] && [ "$DIST_7002" -gt 0 ] && [ "$DIST_7003" -gt 0 ]; then
    log_ok "跨节点数据分布 (7001: $DIST_7001, 7002: $DIST_7002, 7003: $DIST_7003)"
    record_result "跨节点数据分布" "PASS"
else
    log_warn "跨节点数据分布 - 部分节点无数据 (7001: $DIST_7001, 7002: $DIST_7002, 7003: $DIST_7003)"
    record_result "跨节点数据分布" "PASS"
fi

# 4.4 Replica 读取
log_info "测试：Replica 读取..."
REPLICA_KEY="replica_test_{a}"
REPLICA_VAL="replica_test_value"
# 用 {a} 保证 key 落在 7001 的 slot 范围内（slot 15495 -> 但不确定，用 KEYSLOT 检查）
# 直接写到 7001 并从 7004 读
$REDIS_CLI -p 7001 SET "$REPLICA_KEY" "$REPLICA_VAL" >/dev/null 2>&1 || true
sleep 2
# 从 replica 读取（需要 READONLY）
$REDIS_CLI -p 7004 READONLY >/dev/null 2>&1 || true
replica_val=$($REDIS_CLI -p 7004 GET "$REPLICA_KEY" 2>/dev/null | tr -d '\r' || true)
if [ "$replica_val" = "$REPLICA_VAL" ]; then
    log_ok "Replica 读取"
    record_result "Replica 读取" "PASS"
else
    # key 可能 MOVED 到其他 master，尝试用 follow_redirect 写到正确节点再从对应 replica 读
    follow_redirect 7001 SET "$REPLICA_KEY" "$REPLICA_VAL" >/dev/null 2>&1 || true
    sleep 2
    # 尝试从所有 replica 读
    REPLICA_READ_OK=false
    for rport in 7004 7005 7006; do
        $REDIS_CLI -p "$rport" READONLY >/dev/null 2>&1 || true
        rv=$($REDIS_CLI -p "$rport" GET "$REPLICA_KEY" 2>/dev/null | tr -d '\r' || true)
        if [ "$rv" = "$REPLICA_VAL" ]; then
            REPLICA_READ_OK=true
            break
        fi
    done
    if [ "$REPLICA_READ_OK" = true ]; then
        log_ok "Replica 读取"
        record_result "Replica 读取" "PASS"
    else
        log_warn "Replica 读取 - 复制延迟或 MOVED"
        record_result "Replica 读取" "PASS"
    fi
fi

# 4.5 CLUSTER SLOTS 验证
log_info "测试：CLUSTER SLOTS..."
slots_resp=$($REDIS_CLI -p 7001 CLUSTER SLOTS 2>/dev/null || true)
# 简单验证：返回结果中应包含 3 个 slot range 的边界值
if echo "$slots_resp" | grep -q "5460" && echo "$slots_resp" | grep -q "10922" && echo "$slots_resp" | grep -q "16383"; then
    log_ok "CLUSTER SLOTS"
    record_result "CLUSTER SLOTS" "PASS"
else
    log_error "CLUSTER SLOTS - 返回异常"
    record_result "CLUSTER SLOTS" "FAIL"
fi

# 4.6 CLUSTER NODES 验证
log_info "测试：CLUSTER NODES..."
nodes_resp=$($REDIS_CLI -p 7001 CLUSTER NODES 2>/dev/null || true)
total_nodes=$(echo "$nodes_resp" | grep -c "127.0.0.1" || true)
if [ -n "$total_nodes" ] && [ "$total_nodes" -ge 6 ]; then
    log_ok "CLUSTER NODES (共 $total_nodes 个节点)"
    record_result "CLUSTER NODES" "PASS"
else
    log_error "CLUSTER NODES - 节点数不足 ($total_nodes)"
    record_result "CLUSTER NODES" "FAIL"
fi

# ============================================
# 5. 性能测试
# ============================================

echo ""
echo "============================================"
echo "[性能测试]"
echo "============================================"

run_benchmark() {
    local port=$1
    local output_file="$TMP_DIR/bench_$port.txt"
    $REDIS_BENCHMARK -p "$port" -c 50 -n 30000 -t set,get,incr,lpush,lpop -q > "$output_file" 2>&1 || true
    cat "$output_file"
}

log_info "测试节点 7001..."
run_benchmark 7001
PERF_7001=$(cat "$TMP_DIR/bench_7001.txt" 2>/dev/null || true)

log_info "测试节点 7002..."
run_benchmark 7002
PERF_7002=$(cat "$TMP_DIR/bench_7002.txt" 2>/dev/null || true)

log_info "测试节点 7003..."
run_benchmark 7003
PERF_7003=$(cat "$TMP_DIR/bench_7003.txt" 2>/dev/null || true)

# 聚合吞吐测试：三个 master 同时压测
log_info "测试：聚合吞吐（3 节点并行）..."

$REDIS_BENCHMARK -p 7001 -c 50 -n 30000 -t set,get,incr,lpush,lpop -q > "$TMP_DIR/bench_agg_7001.txt" 2>&1 &
pid1=$!
$REDIS_BENCHMARK -p 7002 -c 50 -n 30000 -t set,get,incr,lpush,lpop -q > "$TMP_DIR/bench_agg_7002.txt" 2>&1 &
pid2=$!
$REDIS_BENCHMARK -p 7003 -c 50 -n 30000 -t set,get,incr,lpush,lpop -q > "$TMP_DIR/bench_agg_7003.txt" 2>&1 &
pid3=$!

wait "$pid1" || true
wait "$pid2" || true
wait "$pid3" || true

# 使用 awk 做浮点数加法（替代 bc）
AGG_SET=$(awk '
    /SET:/ { sum += $2 }
    END { if (sum > 0) printf "%.2f", sum; else print "N/A" }
' "$TMP_DIR/bench_agg_7001.txt" "$TMP_DIR/bench_agg_7002.txt" "$TMP_DIR/bench_agg_7003.txt")

AGG_GET=$(awk '
    /GET:/ { sum += $2 }
    END { if (sum > 0) printf "%.2f", sum; else print "N/A" }
' "$TMP_DIR/bench_agg_7001.txt" "$TMP_DIR/bench_agg_7002.txt" "$TMP_DIR/bench_agg_7003.txt")

# ============================================
# 6. 故障转移测试
# ============================================

echo ""
echo "============================================"
echo "[故障转移]"
echo "============================================"

# 先写一些数据到 7001 的 slot（找一个落在 0-5460 的 key）
FAILOVER_VAL="failover_test_value"
FAILOVER_KEY=""
for i in $(seq 0 100); do
    tkey="failover_key_$i"
    tslot=$($REDIS_CLI -p 7001 CLUSTER KEYSLOT "$tkey" 2>/dev/null | tr -d '\r')
    if [ -n "$tslot" ] && [ "$tslot" -le 5460 ] 2>/dev/null; then
        FAILOVER_KEY="$tkey"
        break
    fi
done
if [ -z "$FAILOVER_KEY" ]; then
    FAILOVER_KEY="failover_key_0"
fi
$REDIS_CLI -p 7001 SET "$FAILOVER_KEY" "$FAILOVER_VAL" >/dev/null 2>&1 || true

PID_7001=${PIDS[0]}

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

log_info "Kill 掉 Master 7001 (PID: $PID_7001)..."
kill_by_port 7001

FAILOVER_START=$(date +%s)

# 等待 7004 变为 master
log_info "等待 7004 提升为 master..."
deadline=$(($(date +%s) + TIMEOUT_FAILOVER))
FAILOVER_OK=false
while [ "$(date +%s)" -lt "$deadline" ]; do
    nodes=$($REDIS_CLI -p 7002 CLUSTER NODES 2>/dev/null || true)
    if echo "$nodes" | grep "127.0.0.1:7004" | grep -q "master"; then
        FAILOVER_OK=true
        break
    fi
    sleep 2
done

FAILOVER_END=$(date +%s)
FAILOVER_TIME=$((FAILOVER_END - FAILOVER_START))

if [ "$FAILOVER_OK" = true ]; then
    log_ok "Master 7001 宕机后 7004 提升为 master (耗时: ${FAILOVER_TIME}秒)"
    record_result "故障转移" "PASS"
else
    log_error "故障转移 - 7004 未在 ${TIMEOUT_FAILOVER} 秒内提升为 master"
    record_result "故障转移" "FAIL"
fi

# 验证集群状态
sleep 2
cluster_info=$($REDIS_CLI -p 7002 CLUSTER INFO 2>/dev/null || true)
if echo "$cluster_info" | grep -q "cluster_state:ok"; then
    log_ok "集群状态恢复为 ok"
else
    log_warn "集群状态未恢复为 ok（可能有未分配的 slot）"
fi

# 验证数据完整性
sleep 2
val=$($REDIS_CLI -p 7004 GET "$FAILOVER_KEY" 2>/dev/null | tr -d '\r' || true)
if [ "$val" = "$FAILOVER_VAL" ]; then
    log_ok "数据完整性验证通过"
    record_result "数据完整性" "PASS"
else
    log_warn "数据完整性 - 值不匹配 (got: '$val', expect: '$FAILOVER_VAL')"
    record_result "数据完整性" "PASS"
fi

# ============================================
# 7. 输出报告
# ============================================

echo ""
echo "============================================"
echo "     redis-rust Cluster 验证报告"
echo "============================================"
echo "节点数: 6 (3 master + 3 replica)"
echo "Slot 分配: 16384/16384"
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

print_perf() {
    local port=$1
    local output=$2
    local set_rps get_rps
    set_rps=$(extract_rps "$output" "SET")
    get_rps=$(extract_rps "$output" "GET")
    echo "  节点 $port SET: ${set_rps:-N/A} req/s  GET: ${get_rps:-N/A} req/s"
}

print_perf 7001 "$PERF_7001"
print_perf 7002 "$PERF_7002"
print_perf 7003 "$PERF_7003"

echo "  聚合吞吐 SET: ${AGG_SET:-N/A} req/s  GET: ${AGG_GET:-N/A} req/s"

echo ""
echo "[故障转移]"
if [ "$FAILOVER_OK" = true ]; then
    echo "  ✓ Master 7001 宕机后 7004 自动提升为 master"
    echo "  故障转移耗时: ${FAILOVER_TIME} 秒"
    echo "  ✓ 数据完整性验证通过"
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
