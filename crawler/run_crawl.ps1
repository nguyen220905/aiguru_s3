# Runs the crawl in passes. Each pass skips what is already stored, so later passes only
# retry transient errors and groups that were paused/disabled. Safe to stop and start again.
#   run_crawl.ps1 -Tag big  -Hosts www.cnkang.com,www.120ask.com
#   run_crawl.ps1 -Tag rest -ExcludeHosts www.cnkang.com,www.120ask.com
param([string]$Tag = "main", [string]$Hosts = "", [string]$ExcludeHosts = "", [int]$Passes = 3, [int]$GapMinutes = 30)

$env:PYTHONWARNINGS = "ignore"
$env:PYTHONIOENCODING = "utf-8"
Set-Location $PSScriptRoot
$logs = "D:\project_r2ai\crawl\logs"

$argList = @("crawl.py", "--include-blocked", "--tag", "_$Tag")
if ($Hosts) { $argList += @("--hosts", $Hosts) }
if ($ExcludeHosts) { $argList += @("--exclude-hosts", $ExcludeHosts) }

for ($i = 1; $i -le $Passes; $i++) {
    "$(Get-Date -Format s) [$Tag] pass $i/$Passes start: $($argList -join ' ')" | Add-Content "$logs\runner.log"
    & python @argList *>> "$logs\stdout_$Tag.log"
    "$(Get-Date -Format s) [$Tag] pass $i/$Passes exit code $LASTEXITCODE" | Add-Content "$logs\runner.log"
    if ($i -lt $Passes) { Start-Sleep -Seconds ($GapMinutes * 60) }
}
