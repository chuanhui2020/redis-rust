#Requires -Version 5.1
<#
.SYNOPSIS
    涓€閿儴缃?redis-rust 3涓?浠庨泦缇?(鏈湴 127.0.0.1)
.DESCRIPTION
    鍚姩 6 涓妭鐐癸紝寤虹珛鍏ㄨ繛鎺ワ紝鍒嗛厤 16384 涓?slot锛岀粦瀹氫粠鑺傜偣銆?    姣忎釜鑺傜偣浣跨敤鐙珛宸ヤ綔鐩綍锛岄伩鍏?nodes.conf 鍐茬獊銆?#>

$ErrorActionPreference = "Stop"

# ---------- 閰嶇疆 ----------
$RustBin = "..\target\release\redis-rust.exe"
$Cli     = "C:\Program Files\Redis\redis-cli.exe"
$BaseDir = "$PSScriptRoot\cluster-data"

$Nodes = @(
    @{ Port = 7000; Role = "master"; Name = "master-1"; Slots = @(0..5460)   }
    @{ Port = 7001; Role = "master"; Name = "master-2"; Slots = @(5461..10922) }
    @{ Port = 7002; Role = "master"; Name = "master-3"; Slots = @(10923..16383) }
    @{ Port = 7003; Role = "slave";  Name = "slave-1";  MasterPort = 7000 }
    @{ Port = 7004; Role = "slave";  Name = "slave-2";  MasterPort = 7001 }
    @{ Port = 7005; Role = "slave";  Name = "slave-3";  MasterPort = 7002 }
)

# ---------- 杈呭姪鍑芥暟 ----------
function Test-Command($cmd) {
    try { & $cmd --version 2>$null; return $true } catch { return $false }
}

function Wait-NodeReady($port, $maxSec = 15) {
    for ($i = 0; $i -lt ($maxSec * 10); $i++) {
        try {
            $out = & $Cli -h 127.0.0.1 -p $port PING 2>$null
            if ($out -eq "PONG") { return $true }
        } catch {}
        Start-Sleep -Milliseconds 100
    }
    return $false
}

function Invoke-Redis($port, $argsArr) {
    $allArgs = @("-h", "127.0.0.1", "-p", $port) + $argsArr
    & $Cli @allArgs 2>$null
}

# ---------- 鍓嶇疆妫€鏌?----------
if (-not (Test-Path $RustBin)) {
    Write-Error "鎵句笉鍒?redis-rust.exe: $RustBin`n璇峰厛鎵ц: cargo build --release"
}
$RustBin = Resolve-Path $RustBin

if (-not (Test-Path $Cli)) {
    Write-Error "鎵句笉鍒?redis-cli.exe: $Cli`n璇峰畨瑁?Redis for Windows 鎴栦慨鏀硅剼鏈腑鐨勮矾寰?
}
$Cli = Resolve-Path $Cli

# ---------- 娓呯悊鏃ф暟鎹?----------
Write-Host "=== 娓呯悊鏃ч泦缇ゆ暟鎹?===" -ForegroundColor Cyan
Get-Process | Where-Object {
    $_.Path -and ($_.Path -like "*redis-rust*")
} | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 1

if (Test-Path $BaseDir) {
    Remove-Item -Recurse -Force $BaseDir
}

# ---------- 鍒涘缓鐩綍骞跺惎鍔ㄨ妭鐐?----------
Write-Host "=== 鍚姩 6 涓妭鐐?===" -ForegroundColor Cyan
$procs = @()
foreach ($node in $Nodes) {
    $dir = "$BaseDir\node-$($node.Port)"
    New-Item -ItemType Directory -Force -Path $dir | Out-Null

    $proc = Start-Process -FilePath $RustBin `
        -ArgumentList "--port", $node.Port, "--cluster-enabled", "yes" `
        -WorkingDirectory $dir `
        -PassThru -WindowStyle Hidden

    $procs += [PSCustomObject]@{
        Port    = $node.Port
        Role    = $node.Role
        Name    = $node.Name
        Process = $proc
    }
    Write-Host "  [$($node.Name)] port=$($node.Port) bus=$($node.Port + 10000) pid=$($proc.Id)"
}

# ---------- 绛夊緟鎵€鏈夎妭鐐瑰氨缁?----------
Write-Host "=== 绛夊緟鑺傜偣灏辩华 ===" -ForegroundColor Cyan
foreach ($p in $procs) {
    if (Wait-NodeReady $p.Port) {
        Write-Host "  [OK] $($p.Name) on port $($p.Port)"
    } else {
        Write-Error "  [FAIL] $($p.Name) on port $($p.Port) 鍚姩瓒呮椂"
    }
}
Start-Sleep -Seconds 1

# ---------- 鍏ㄨ繛鎺?MEET ----------
Write-Host "=== 寤虹珛鑺傜偣闂村叏杩炴帴 (CLUSTER MEET) ===" -ForegroundColor Cyan
$ports = $Nodes | ForEach-Object { $_.Port }
$meetCount = 0
for ($i = 0; $i -lt $ports.Count; $i++) {
    for ($j = $i + 1; $j -lt $ports.Count; $j++) {
        $src = $ports[$i]
        $dst = $ports[$j]
        $null = Invoke-Redis $src @("CLUSTER", "MEET", "127.0.0.1", $dst)
        $meetCount++
    }
}
Write-Host "  宸插彂閫?$meetCount 娆?MEET 鍛戒护"

# 绛夊緟 gossip 鍚屾
Write-Host "  绛夊緟 3 绉掕 gossip 瀹屾垚鎷撴墤鍚屾..."
Start-Sleep -Seconds 3

# ---------- 鍒嗛厤 Slot ----------
Write-Host "=== 鍒嗛厤 Slot 缁欎富鑺傜偣 ===" -ForegroundColor Cyan
foreach ($node in $Nodes | Where-Object { $_.Role -eq "master" }) {
    $port   = $node.Port
    $slots  = $node.Slots
    $batch  = 200
    $sent   = 0
    for ($i = 0; $i -lt $slots.Count; $i += $batch) {
        $end  = [Math]::Min($i + $batch - 1, $slots.Count - 1)
        $chunk = $slots[$i..$end]
        $args = @("CLUSTER", "ADDSLOTS") + $chunk
        $null = Invoke-Redis $port $args
        $sent += $chunk.Count
    }
    Write-Host "  [$($node.Name)] port=$port assigned $sent slots"
}

# 鍐嶇瓑寰呬竴涓嬭 slot 淇℃伅浼犳挱
Start-Sleep -Seconds 2

# ---------- 缁戝畾浠庤妭鐐?----------
Write-Host "=== 缁戝畾浠庤妭鐐?===" -ForegroundColor Cyan
# 鍏堣幏鍙栨墍鏈夎妭鐐?ID
$nodeInfo = Invoke-Redis 7000 @("CLUSTER", "NODES")
if (-not $nodeInfo) {
    Write-Error "鏃犳硶鑾峰彇 CLUSTER NODES 淇℃伅"
}

# 瑙ｆ瀽鑺傜偣 ID
$nodeMap = @{}
foreach ($line in $nodeInfo -split "`r?`n") {
    if ($line -match "^(\w+)\s+127\.0\.0\.1:(\d+)@") {
        $nodeMap[[int]$Matches[2]] = $Matches[1]
    }
}

foreach ($node in $Nodes | Where-Object { $_.Role -eq "slave" }) {
    $slavePort  = $node.Port
    $masterPort = $node.MasterPort
    $masterId   = $nodeMap[$masterPort]
    if (-not $masterId) {
        Write-Warning "  鎵句笉鍒?master port=$masterPort 鐨?node ID锛岃烦杩?
        continue
    }
    $null = Invoke-Redis $slavePort @("CLUSTER", "REPLICATE", $masterId)
    Write-Host "  [$($node.Name)] port=$slavePort -> master port=$masterPort id=$($masterId.Substring(0,8))..."
}

# 绛夊緟澶嶅埗鎻℃墜鍜屾嫇鎵戜紶鎾?Write-Host "  绛夊緟 3 绉掕澶嶅埗鍜屾嫇鎵戝悓姝?.."
Start-Sleep -Seconds 3

# ---------- 楠岃瘉 ----------
Write-Host "=== 闆嗙兢鐘舵€侀獙璇?===" -ForegroundColor Green
Write-Host ""
Write-Host "--- CLUSTER INFO ---"
Invoke-Redis 7000 @("CLUSTER", "INFO") | Write-Host

Write-Host "--- CLUSTER SLOTS (鍓?3 娈? ---"
Invoke-Redis 7000 @("CLUSTER", "SLOTS") | Write-Host

Write-Host "--- CLUSTER NODES ---"
Invoke-Redis 7000 @("CLUSTER", "NODES") | ForEach-Object { Write-Host "  $_" }

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "  3涓?浠庨泦缇ら儴缃插畬鎴?" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "鑺傜偣鍦板潃:"
foreach ($p in $procs) {
    Write-Host "  $($p.Name.PadRight(10)) 127.0.0.1:$($p.Port)  (bus: $($p.Port + 10000))"
}
Write-Host ""
Write-Host "浣跨敤绀轰緥:"
Write-Host "  $Cli -p 7000 SET foo bar"
Write-Host "  $Cli -p 7001 GET foo"
Write-Host ""
Write-Host "鍏抽棴闆嗙兢:"
Write-Host "  .\stop-cluster.ps1"
