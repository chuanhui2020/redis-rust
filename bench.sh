#!/bin/bash

# 基准测试对比脚本：redis-rust vs redis-server

set -euo pipefail

REDIS_RUST_PORT=6399
REDIS_SERVER_PORT=6380
REDIS_RUST_BIN="./target/release/redis-rust"
REDIS_RUST_PID=""
REDIS_SERVER_PID=""

# 清理函数：退出时关闭所有启动的服务器
cleanup() {
    echo ""
    echo "[清理] 正在关闭测试进程..."
    if [[ -n "$REDIS_RUST_PID" ]] && kill -0 "$REDIS_RUST_PID" 2>/dev/null; then
        kill "$REDIS_RUST_PID" 2>/dev/null || true
        wait "$REDIS_RUST_PID" 2>/dev/null || true
    fi
    if [[ -n "$REDIS_SERVER_PID" ]] && kill -0 "$REDIS_SERVER_PID" 2>/dev/null; then
        kill "$REDIS_SERVER_PID" 2>/dev/null || true
        wait "$REDIS_SERVER_PID" 2>/dev/null || true
    fi
    echo "[清理] 完成"
}
trap cleanup EXIT INT TERM

# 等待服务器就绪（通过 PING 检测）
wait_for_server() {
    local host=$1
    local port=$2
    local name=$3
    local retries=30
    local delay=0.1

    for ((i = 1; i <= retries; i++)); do
        if redis-cli -h "$host" -p "$port" PING 2>/dev/null | grep -q "PONG"; then
            echo "[$name] 就绪 (端口 $port)"
            return 0
        fi
        sleep "$delay"
    done

    echo "[$name] 等待超时，端口 $port 未就绪"
    return 1
}

# 运行 benchmark 并提取 requests per second
run_benchmark() {
    local port=$1
    local tests=$2

    redis-benchmark -h 127.0.0.1 -p "$port" -t "$tests" -n 100000 -c 50 -q 2>/dev/null | tr '\r' '\n' | grep "requests per second" | grep -v "rps="
}

# 运行 benchmark 并提取 requests per second（支持额外参数）
run_benchmark_ex() {
    local port=$1
    local tests=$2
    shift 2
    redis-benchmark -h 127.0.0.1 -p "$port" -t "$tests" -n 100000 -q "$@" 2>/dev/null | tr '\r' '\n' | grep "requests per second" | grep -v "rps="
}

# 从 benchmark 输出中提取平均 rps
extract_rps() {
    local output="$1"
    echo "$output" | sed 's/^[[:space:]]*//' | awk -F': ' '{print $2}' | awk '{s+=$1; n++} END {if(n>0) printf "%.2f", s/n; else print "0"}'
}

# 检查可执行文件和依赖
echo "[检查] 验证环境..."

if [[ ! -x "$REDIS_RUST_BIN" ]]; then
    echo "错误：找不到 $REDIS_RUST_BIN，请先执行 cargo build --release"
    exit 1
fi

if ! command -v redis-server &>/dev/null; then
    echo "错误：找不到 redis-server，请先安装 Redis"
    exit 1
fi

if ! command -v redis-benchmark &>/dev/null; then
    echo "错误：找不到 redis-benchmark，请先安装 Redis"
    exit 1
fi

if ! command -v redis-cli &>/dev/null; then
    echo "错误：找不到 redis-cli，请先安装 Redis"
    exit 1
fi

echo "[检查] 环境正常"
echo ""

# 启动 redis-rust
echo "[启动] redis-rust (端口 $REDIS_RUST_PORT)..."
$REDIS_RUST_BIN --port "$REDIS_RUST_PORT" --no-aof &
REDIS_RUST_PID=$!

# 启动 redis-server
echo "[启动] redis-server (端口 $REDIS_SERVER_PORT)..."
redis-server --port "$REDIS_SERVER_PORT" --daemonize no &
REDIS_SERVER_PID=$!

# 等待两个服务器就绪
wait_for_server 127.0.0.1 "$REDIS_RUST_PORT" "redis-rust"
wait_for_server 127.0.0.1 "$REDIS_SERVER_PORT" "redis-server"

# 存储所有结果
declare -a ALL_NAMES=()
declare -a ALL_RUST_RPS=()
declare -a ALL_REDIS_RPS=()
declare -a ALL_PCTS=()

# 运行单个测试并记录结果
run_single_test() {
    local name=$1
    local rust_port=$2
    local redis_port=$3
    local tests=$4
    shift 4

    echo "[测试] $name ..."

    rust_output=$(run_benchmark_ex "$rust_port" "$tests" "$@")
    rust_val=$(extract_rps "$rust_output")
    if [[ -z "$rust_val" ]]; then
        rust_val=0
    fi

    redis_output=$(run_benchmark_ex "$redis_port" "$tests" "$@")
    redis_val=$(extract_rps "$redis_output")
    if [[ -z "$redis_val" ]]; then
        redis_val=0
    fi

    if [[ "$(echo "$redis_val > 0" | bc -l)" -eq 1 ]]; then
        pct=$(echo "scale=4; ($rust_val / $redis_val) * 100" | bc -l | awk '{printf "%.1f", $1}')
    else
        pct="0.0"
    fi

    ALL_NAMES+=("$name")
    ALL_RUST_RPS+=("$rust_val")
    ALL_REDIS_RPS+=("$redis_val")
    ALL_PCTS+=("$pct")
}

# ============ 基础测试 (50 并发) ============
echo ""
echo "============================================"
echo "           基础测试 (50 并发)"
echo "============================================"
echo ""

run_single_test "PING" "$REDIS_RUST_PORT" "$REDIS_SERVER_PORT" "ping" -c 50
run_single_test "SET" "$REDIS_RUST_PORT" "$REDIS_SERVER_PORT" "set" -c 50
run_single_test "GET" "$REDIS_RUST_PORT" "$REDIS_SERVER_PORT" "get" -c 50
run_single_test "混合(PING+SET+GET)" "$REDIS_RUST_PORT" "$REDIS_SERVER_PORT" "ping,set,get" -c 50
run_single_test "INCR" "$REDIS_RUST_PORT" "$REDIS_SERVER_PORT" "incr" -c 50
run_single_test "LPUSH" "$REDIS_RUST_PORT" "$REDIS_SERVER_PORT" "lpush" -c 50
run_single_test "LPOP" "$REDIS_RUST_PORT" "$REDIS_SERVER_PORT" "lpop" -c 50
run_single_test "HSET" "$REDIS_RUST_PORT" "$REDIS_SERVER_PORT" "hset" -c 50

# ============ Pipeline 测试 (-P 16, 50 并发) ============
echo ""
echo "============================================"
echo "         Pipeline 测试 (-P 16, 50 并发)"
echo "============================================"
echo ""

run_single_test "PING pipeline=16" "$REDIS_RUST_PORT" "$REDIS_SERVER_PORT" "ping" -c 50 -P 16
run_single_test "SET pipeline=16" "$REDIS_RUST_PORT" "$REDIS_SERVER_PORT" "set" -c 50 -P 16
run_single_test "GET pipeline=16" "$REDIS_RUST_PORT" "$REDIS_SERVER_PORT" "get" -c 50 -P 16

# ============ 并发度对比测试 (SET) ============
echo ""
echo "============================================"
echo "         并发度对比测试 (SET)"
echo "============================================"
echo ""

run_single_test "SET (c=1)" "$REDIS_RUST_PORT" "$REDIS_SERVER_PORT" "set" -c 1
run_single_test "SET (c=50)" "$REDIS_RUST_PORT" "$REDIS_SERVER_PORT" "set" -c 50
run_single_test "SET (c=200)" "$REDIS_RUST_PORT" "$REDIS_SERVER_PORT" "set" -c 200

# ============ 并发度对比测试 (GET) ============
echo ""
echo "============================================"
echo "         并发度对比测试 (GET)"
echo "============================================"
echo ""

run_single_test "GET (c=1)" "$REDIS_RUST_PORT" "$REDIS_SERVER_PORT" "get" -c 1
run_single_test "GET (c=50)" "$REDIS_RUST_PORT" "$REDIS_SERVER_PORT" "get" -c 50
run_single_test "GET (c=200)" "$REDIS_RUST_PORT" "$REDIS_SERVER_PORT" "get" -c 200

# ============ 输出结果 ============
{
    echo ""
    echo "============================================"
    echo "         基准测试报告"
    echo "============================================"
    echo "生成时间: $(date)"
    echo ""
    echo "============================================"
    echo "              测试结果对比"
    echo "============================================"
    printf "%-30s %15s %15s %10s\n" "测试项目" "redis-rust" "redis-server" "性能比"
    printf "%-30s %15s %15s %10s\n" "------------------------------" "---------------" "---------------" "----------"

    for i in "${!ALL_NAMES[@]}"; do
        printf "%-30s %15.0f %15.0f %9s%%\n" \
            "${ALL_NAMES[$i]}" \
            "${ALL_RUST_RPS[$i]}" \
            "${ALL_REDIS_RPS[$i]}" \
            "${ALL_PCTS[$i]}"
    done

    echo "============================================"
    echo ""

    # 总结
    THRESHOLD=80.0
    echo "============================================"
    echo "                 总结"
    echo "============================================"
    echo "达标标准: 性能比 >= ${THRESHOLD}%"
    echo ""

    echo "[达标]"
    passed_count=0
    for i in "${!ALL_NAMES[@]}"; do
        if [[ "$(echo "${ALL_PCTS[$i]} >= $THRESHOLD" | bc -l)" -eq 1 ]]; then
            echo "  ✓ ${ALL_NAMES[$i]} (${ALL_PCTS[$i]}%)"
            ((passed_count++))
        fi
    done

    echo ""
    echo "[未达标]"
    failed_count=0
    for i in "${!ALL_NAMES[@]}"; do
        if [[ "$(echo "${ALL_PCTS[$i]} < $THRESHOLD" | bc -l)" -eq 1 ]]; then
            echo "  ✗ ${ALL_NAMES[$i]} (${ALL_PCTS[$i]}%)"
            ((failed_count++))
        fi
    done

    echo ""
    echo "总计: ${passed_count} 项达标, ${failed_count} 项未达标 (共 ${#ALL_NAMES[@]} 项)"
    echo "============================================"
    echo ""
} | tee benchmark_results.txt
