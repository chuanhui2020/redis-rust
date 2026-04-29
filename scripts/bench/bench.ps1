# 基准测试脚本：redis-rust vs redis-server (PowerShell)
$REDIS_RUST_PORT = 6399
$REDIS_SERVER_PORT = 6380
$REDIS_RUST_BIN = "$PSScriptRoot\..\..\target\release\redis-rust.exe"
$REDIS_SERVER_BIN = "C:/Program Files/Redis/redis-server.exe"
$REDIS_BENCHMARK_BIN = "C:/Program Files/Redis/redis-benchmark.exe"
$REDIS_CLI_BIN = "C:/Program Files/Redis/redis-cli.exe"

function Wait-ForServer($port, $name) {
    for ($i = 0; $i -lt 30; $i++) {
        try {
            $out = & $REDIS_CLI_BIN -h 127.0.0.1 -p $port PING 2>$null
            if ($out -eq "PONG") {
                Write-Host ("[{0}] 就绪 (端口 {1})" -f $name, $port)
                return $true
            }
        } catch {}
        Start-Sleep -Milliseconds 100
    }
    Write-Host ("[{0}] 等待超时" -f $name)
    return $false
}

function Run-Benchmark($port, $tests, $extraArgs) {
    $allArgs = @("-h", "127.0.0.1", "-p", "$port", "-t", "$tests", "-n", "100000", "-q") + $extraArgs
    $output = & $REDIS_BENCHMARK_BIN @allArgs 2>$null
    $rpsValues = $output | Select-String "requests per second" | ForEach-Object {
        if ($_ -match "([0-9,.]+) requests per second") { [double]($Matches[1] -replace ',','') }
    }
    if ($rpsValues) { ($rpsValues | Measure-Object -Average).Average } else { 0 }
}

function Kill-RedisProcesses {
    Get-Process | Where-Object {
        $_.Path -and ($_.Path -like "*redis-rust*" -or $_.Path -like "*redis-server*")
    } | Stop-Process -Force -ErrorAction SilentlyContinue
}

Kill-RedisProcesses
Start-Sleep -Seconds 1

if (-not (Test-Path $REDIS_RUST_BIN)) {
    Write-Error "找不到 $REDIS_RUST_BIN"
    exit 1
}

$testList = @(
    @("PING", "ping", @("-c","50")),
    @("SET", "set", @("-c","50")),
    @("GET", "get", @("-c","50")),
    @("INCR", "incr", @("-c","50")),
    @("LPUSH", "lpush", @("-c","50")),
    @("HSET", "hset", @("-c","50")),
    @("PING pipeline=16", "ping", @("-c","50","-P","16")),
    @("SET pipeline=16", "set", @("-c","50","-P","16")),
    @("GET pipeline=16", "get", @("-c","50","-P","16")),
    @("SET (c=1)", "set", @("-c","1")),
    @("SET (c=50)", "set", @("-c","50")),
    @("SET (c=200)", "set", @("-c","200")),
    @("GET (c=1)", "get", @("-c","1")),
    @("GET (c=50)", "get", @("-c","50")),
    @("GET (c=200)", "get", @("-c","200"))
)

$rounds = 5
$results = @{}

for ($r = 1; $r -le $rounds; $r++) {
    Write-Host ""
    Write-Host "========================================"
    Write-Host ("  Round {0} / {1}" -f $r, $rounds)
    Write-Host "========================================"

    Write-Host "[启动] redis-rust port=$REDIS_RUST_PORT"
    $rustProc = Start-Process -FilePath $REDIS_RUST_BIN -ArgumentList "--port", "$REDIS_RUST_PORT", "--no-aof" -PassThru -WindowStyle Hidden

    Write-Host "[启动] redis-server port=$REDIS_SERVER_PORT"
    $serverProc = Start-Process -FilePath $REDIS_SERVER_BIN -ArgumentList "--port", "$REDIS_SERVER_PORT" -PassThru -WindowStyle Hidden

    if (-not (Wait-ForServer $REDIS_RUST_PORT "redis-rust")) { break }
    if (-not (Wait-ForServer $REDIS_SERVER_PORT "redis-server")) { break }
    Start-Sleep -Seconds 1

    foreach ($t in $testList) {
        $name = $t[0]
        $tests = $t[1]
        $args = $t[2]
        Write-Host ("[测试] {0} ..." -f $name)
        $rustRps = Run-Benchmark $REDIS_RUST_PORT $tests $args
        $redisRps = Run-Benchmark $REDIS_SERVER_PORT $tests $args
        if (-not $results.ContainsKey($name)) {
            $results[$name] = @( @(), @() )
        }
        $results[$name][0] += $rustRps
        $results[$name][1] += $redisRps
        Write-Host ("  rust: {0:N0} | redis: {1:N0}" -f $rustRps, $redisRps)
    }

    Write-Host "[清理] 关闭服务器..."
    Stop-Process -Id $rustProc.Id -Force -ErrorAction SilentlyContinue
    Stop-Process -Id $serverProc.Id -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
}

Write-Host ""
Write-Host "========================================"
Write-Host ("  Benchmark Report ({0} rounds avg)" -f $rounds)
Write-Host "========================================"
Write-Host ""
Write-Host "Test                           rust-rps    redis-rps   ratio"
Write-Host "------------------------------ ----------- ----------- ------"
foreach ($t in $testList) {
    $name = $t[0]
    $avgRust = ($results[$name][0] | Measure-Object -Average).Average
    $avgRedis = ($results[$name][1] | Measure-Object -Average).Average
    $pct = if ($avgRedis -gt 0) { ($avgRust / $avgRedis) * 100 } else { 0 }
    Write-Host ("{0,-30} {1,11:N0} {2,11:N0} {3,6:N1}%" -f $name, $avgRust, $avgRedis, $pct)
}
Write-Host "========================================"
