$path = "deploy-cluster-bom.ps1"
$err = $null
$tokens = [System.Management.Automation.PSParser]::Tokenize((Get-Content -Path $path -Raw), [ref]$err)
Write-Host ("Errors: " + $err.Count)
foreach ($e in $err) {
    Write-Host ("Line {0}, Col {1}: {2}" -f $e.Token.StartLine, $e.Token.StartColumn, $e.Message)
}
