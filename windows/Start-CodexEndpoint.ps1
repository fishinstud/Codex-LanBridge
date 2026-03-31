param(
    [switch]$Foreground,
    [switch]$ForceRestart,
    [string]$ConfigFile = "",
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$PassthruArgs
)

$Root = Split-Path -Parent $PSScriptRoot
$EndpointRunner = Join-Path $Root "run_endpoint.py"
if (-not (Test-Path $EndpointRunner)) {
    throw "Missing endpoint runner: $EndpointRunner"
}

$PythonLauncher = (Get-Command py -ErrorAction Stop).Source
$StateDir = Join-Path $Root "state"
$null = New-Item -ItemType Directory -Force -Path $StateDir
$StdoutLog = Join-Path $StateDir "windows-endpoint.stdout.log"
$StderrLog = Join-Path $StateDir "windows-endpoint.stderr.log"
$RunnerPattern = [regex]::Escape($EndpointRunner)

$Existing = @(Get-CimInstance Win32_Process |
    Where-Object {
        $_.CommandLine -and
        $_.CommandLine -match $RunnerPattern
    })

if ($ForceRestart -and $Existing.Count -gt 0) {
    $Existing | ForEach-Object {
        Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 1
    $Existing = @(Get-CimInstance Win32_Process |
        Where-Object {
            $_.CommandLine -and
            $_.CommandLine -match $RunnerPattern
        })
}

$ArgumentList = @($EndpointRunner)
if ($ConfigFile) {
    $ArgumentList += @("--config-file", $ConfigFile)
}
if ($PassthruArgs) {
    $ArgumentList += $PassthruArgs
}

if ($Foreground) {
    & $PythonLauncher @ArgumentList
    exit $LASTEXITCODE
}

if ($Existing.Count -gt 0) {
    $Pids = ($Existing | ForEach-Object { $_.ProcessId }) -join ", "
    Write-Output "Codex endpoint already running (PID $Pids)."
    Write-Output "stdout: $StdoutLog"
    Write-Output "stderr: $StderrLog"
    exit 0
}

$Process = Start-Process `
    -FilePath $PythonLauncher `
    -ArgumentList $ArgumentList `
    -WorkingDirectory $Root `
    -RedirectStandardOutput $StdoutLog `
    -RedirectStandardError $StderrLog `
    -WindowStyle Hidden `
    -PassThru

Write-Output "Started Codex endpoint in background (PID $($Process.Id))."
Write-Output "stdout: $StdoutLog"
Write-Output "stderr: $StderrLog"
