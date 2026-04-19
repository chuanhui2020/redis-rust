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

echo ""
echo "============================================"
echo "        基准测试开始 (100K 请求, 50 并发)"
echo "============================================"
echo ""

# 定义测试项目
declare -a TEST_NAMES=("PING" "SET" "GET" "混合(PING+SET+GET)")
declare -a TEST_CMDS=("ping" "set" "get" "ping,set,get")

declare -a RUST_RPS=()
declare -a REDIS_RPS=()
declare -a PERCENTAGES=()

# 执行各项测试
for i in "${!TEST_NAMES[@]}"; do
    name="${TEST_NAMES[$i]}"
    cmd="${TEST_CMDS[$i]}"

    echo "[测试] $name ..."

    # 测试 redis-rust
    rust_output=$(run_benchmark "$REDIS_RUST_PORT" "$cmd")
    rust_val=$(extract_rps "$rust_output")
    if [[ -z "$rust_val" ]]; then
        rust_val=0
    fi

    # 测试 redis-server
    redis_output=$(run_benchmark "$REDIS_SERVER_PORT" "$cmd")
    redis_val=$(extract_rps "$redis_output")
    if [[ -z "$redis_val" ]]; then
        redis_val=0
    fi

    # 计算百分比
    if [[ "$(echo "$redis_val > 0" | bc -l)" -eq 1 ]]; then
        pct=$(echo "scale=4; ($rust_val / $redis_val) * 100" | bc -l | awk '{printf "%.1f", $1}')
    else
        pct="0.0"
    fi

    RUST_RPS+=("$rust_val")
    REDIS_RPS+=("$redis_val")
    PERCENTAGES+=("$pct")

done

echo ""
echo "============================================"
echo "              测试结果对比"
echo "============================================"
printf "%-20s %15s %15s %10s\n" "测试项目" "redis-rust" "redis-server" "性能比"
printf "%-20s %15s %15s %10s\n" "--------------------" "---------------" "---------------" "----------"

for i in "${!TEST_NAMES[@]}"; do
    printf "%-20s %15.0f %15.0f %9s%%\n" \
        "${TEST_NAMES[$i]}" \
        "${RUST_RPS[$i]}" \
        "${REDIS_RPS[$i]}" \
        "${PERCENTAGES[$i]}"
done

echo "============================================"
echo ""
