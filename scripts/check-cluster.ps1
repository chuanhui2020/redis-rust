#Requires -Version 5.1
<#
.SYNOPSIS
    Check local redis-rust cluster health
#>

$Cli   = "C:\Program Files\Redis\redis-cli.exe"
$Ports = 7000, 7001, 7002, 7003, 7004, 7005

function Invoke-Redis($port, $argsArr) {
    $allArgs = @("-h", "127.0.0.1", "-p", $port) + $argsArr
    & $Cli @allArgs 2>$null
}

Write-Host "========================================"
Write-Host "      redis-rust cluster health check"
Write-Host "========================================"

# Check node liveness
Write-Host ""
Write-Host "--- Node liveness check ---"
$alive = 0
foreach ($port in $Ports) {
    try {
        $out = Invoke-Redis $port @("PING")
        if ($out -eq "PONG") {
            Write-Host "  [OK]   127.0.0.1:$port"
            $alive++
        } else {
            Write-Host "  [FAIL] 127.0.0.1:$port  response: $out"
        }
    } catch {
        Write-Host "  [FAIL] 127.0.0.1:$port  connection refused"
    }
}
Write-Host "  Alive: $alive / $($Ports.Count)"

if ($alive -eq 0) {
    Write-Error "All nodes are down. Please run .\deploy-cluster.ps1 first"
}

# CLUSTER INFO
Write-Host ""
Write-Host "--- CLUSTER INFO (from :7000) ---"
Invoke-Redis 7000 @("CLUSTER", "INFO") | ForEach-Object { Write-Host "  $_" }

# CLUSTER NODES
Write-Host ""
Write-Host "--- CLUSTER NODES ---"
$nodesInfo = Invoke-Redis 7000 @("CLUSTER", "NODES")
$masters = 0
$slaves  = 0
$fail    = 0
foreach ($line in $nodesInfo -split "`r?`n") {
    if (-not $line.Trim()) { continue }
    Write-Host "  $line"
    if ($line -match "\smaster\s") { $masters++ }
    if ($line -match "\sslave\s")  { $slaves++ }
    if ($line -match "\sfail\s")   { $fail++ }
}

Write-Host ""
Write-Host "========================================"
Write-Host "  Masters: $masters   Slaves: $slaves   Failed: $fail"
Write-Host "========================================"
