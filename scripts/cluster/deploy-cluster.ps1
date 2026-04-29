#Requires -Version 5.1
<#
.SYNOPSIS
    Deploy redis-rust 3-master 3-slave cluster
.DESCRIPTION
    Start 6 nodes, establish full mesh, assign 16384 slots, attach slaves.
    Each node uses an independent working directory to avoid nodes.conf conflicts.
    For cross-datacenter deployment, use -Bind 0.0.0.0 and -ClusterAnnounceIp.
.PARAMETER Bind
    IP address to bind (default: 127.0.0.1). Use 0.0.0.0 for cross-machine.
.PARAMETER ClusterAnnounceIp
    IP address announced to other cluster nodes (default: same as Bind).
.PARAMETER MaxMemoryMB
    Per-node memory limit in MB (default: 256). Total cluster = 6 * MaxMemoryMB.
#>

param(
    [string]$Bind = "127.0.0.1",
    [string]$ClusterAnnounceIp = "",
    [int]$MaxMemoryMB = 256
)

$ErrorActionPreference = "Stop"

if (-not $ClusterAnnounceIp) {
    $ClusterAnnounceIp = $Bind
}

# ---------- Config ----------
$RustBin = "$PSScriptRoot\..\..\target\release\redis-rust.exe"
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

# ---------- Helpers ----------
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

# ---------- Pre-check ----------
if (-not (Test-Path $RustBin)) {
    Write-Error "redis-rust.exe not found: $RustBin`nPlease run: cargo build --release"
}
$RustBin = Resolve-Path $RustBin

if (-not (Test-Path $Cli)) {
    Write-Error "redis-cli.exe not found: $Cli`nPlease install Redis for Windows or update the path in this script"
}
$Cli = Resolve-Path $Cli

# ---------- Cleanup old data ----------
Write-Host "=== Cleaning up old cluster data ==="
Get-Process | Where-Object {
    $_.Path -and ($_.Path -like "*redis-rust*")
} | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 1

if (Test-Path $BaseDir) {
    Remove-Item -Recurse -Force $BaseDir
}

# ---------- Create dirs and start nodes ----------
Write-Host "=== Starting 6 nodes ==="
$procs = @()
foreach ($node in $Nodes) {
    $dir = "$BaseDir\node-$($node.Port)"
    New-Item -ItemType Directory -Force -Path $dir | Out-Null

    $maxMemoryBytes = $MaxMemoryMB * 1024 * 1024
    $argList = @(
        "--port", $node.Port,
        "--bind", $Bind,
        "--cluster-enabled", "yes",
        "--maxmemory", $maxMemoryBytes,
        "--maxmemory-policy", "allkeys-lru"
    )
    if ($ClusterAnnounceIp -ne $Bind) {
        $argList += @("--cluster-announce-ip", $ClusterAnnounceIp)
    }
    $proc = Start-Process -FilePath $RustBin `
        -ArgumentList $argList `
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

# ---------- Wait for all nodes ready ----------
Write-Host "=== Waiting for nodes to be ready ==="
foreach ($p in $procs) {
    if (Wait-NodeReady $p.Port) {
        Write-Host "  [OK] $($p.Name) on port $($p.Port)"
    } else {
        Write-Error "  [FAIL] $($p.Name) on port $($p.Port) startup timeout"
    }
}
Start-Sleep -Seconds 1

# ---------- Full mesh MEET ----------
Write-Host "=== Establishing full mesh (CLUSTER MEET) ==="
$ports = $Nodes | ForEach-Object { $_.Port }
$meetCount = 0
for ($i = 0; $i -lt $ports.Count; $i++) {
    for ($j = $i + 1; $j -lt $ports.Count; $j++) {
        $src = $ports[$i]
        $dst = $ports[$j]
        $null = Invoke-Redis $src @("CLUSTER", "MEET", $ClusterAnnounceIp, $dst)
        $meetCount++
    }
}
Write-Host "  Sent $meetCount MEET commands"

Write-Host "  Waiting 3s for gossip topology sync..."
Start-Sleep -Seconds 3

# ---------- Assign Slots ----------
Write-Host "=== Assigning slots to masters ==="
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

Start-Sleep -Seconds 2

# ---------- Attach slaves ----------
Write-Host "=== Attaching slaves ==="
$nodeInfo = Invoke-Redis 7000 @("CLUSTER", "NODES")
if (-not $nodeInfo) {
    Write-Error "Failed to get CLUSTER NODES info"
}

$nodeMap = @{}
foreach ($line in $nodeInfo -split "`r?`n") {
    if ($line -match "^([0-9a-f]+)\s+127\.0\.0\.1:(\d+)@") {
        $nodeMap[[int]$Matches[2]] = $Matches[1]
    }
}

foreach ($node in $Nodes | Where-Object { $_.Role -eq "slave" }) {
    $slavePort  = $node.Port
    $masterPort = $node.MasterPort
    $masterId   = $nodeMap[$masterPort]
    if (-not $masterId) {
        Write-Warning "  Master port=$masterPort node ID not found, skipping"
        continue
    }
    $null = Invoke-Redis $slavePort @("CLUSTER", "REPLICATE", $masterId)
    Write-Host "  [$($node.Name)] port=$slavePort -> master port=$masterPort id=$($masterId.Substring(0,8))..."
}

Write-Host "  Waiting 3s for replication and topology sync..."
Start-Sleep -Seconds 3

# ---------- Verification ----------
Write-Host "=== Cluster verification ==="
Write-Host ""
Write-Host "--- CLUSTER INFO ---"
Invoke-Redis 7000 @("CLUSTER", "INFO") | Write-Host

Write-Host "--- CLUSTER SLOTS (first 3 shards) ---"
Invoke-Redis 7000 @("CLUSTER", "SLOTS") | Write-Host

Write-Host "--- CLUSTER NODES ---"
Invoke-Redis 7000 @("CLUSTER", "NODES") | ForEach-Object { Write-Host "  $_" }

Write-Host ""
Write-Host "--- Memory Limits (CONFIG GET maxmemory) ---"
foreach ($node in $Nodes) {
    $mem = Invoke-Redis $node.Port @("CONFIG", "GET", "maxmemory")
    $policy = Invoke-Redis $node.Port @("CONFIG", "GET", "maxmemory-policy")
    Write-Host "  [$($node.Name)] port=$($node.Port)  $mem  $policy"
}

Write-Host ""
Write-Host "========================================"
Write-Host "  3-master 3-slave cluster deployed!"
Write-Host "========================================"
Write-Host ""
Write-Host "Node addresses:"
foreach ($p in $procs) {
    Write-Host "  $($p.Name.PadRight(10)) 127.0.0.1:$($p.Port)  (bus: $($p.Port + 10000))"
}
Write-Host ""
Write-Host "Usage example:"
Write-Host "  $Cli -p 7000 SET foo bar"
Write-Host "  $Cli -p 7001 GET foo"
Write-Host ""
Write-Host "Stop cluster:"
Write-Host "  .\stop-cluster.ps1"
