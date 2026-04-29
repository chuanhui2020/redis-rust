#Requires -Version 5.1
<#
.SYNOPSIS
    Stop all local redis-rust cluster nodes
#>

$ErrorActionPreference = "SilentlyContinue"

Write-Host "=== Stopping redis-rust cluster nodes ==="

$killed = 0
Get-Process | Where-Object {
    $_.Path -and ($_.Path -like "*redis-rust*")
} | ForEach-Object {
    Write-Host "  Killing PID $($_.Id)  $($_.Path)"
    $_.Kill()
    $killed++
}

if ($killed -eq 0) {
    Write-Host "  No running redis-rust processes found"
} else {
    Write-Host "  Killed $killed processes"
}

$BaseDir = "$PSScriptRoot\cluster-data"
if (Test-Path $BaseDir) {
    Write-Host "  Cleaning data dir: $BaseDir"
    Remove-Item -Recurse -Force $BaseDir
}

Write-Host "=== Cluster stopped ==="
