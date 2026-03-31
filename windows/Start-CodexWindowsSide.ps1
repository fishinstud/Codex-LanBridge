$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$PreferredConfigPath = Join-Path $Root "config.windows.local.json"
$FallbackConfigPath = Join-Path $Root "config.local.json"
$StateDir = Join-Path $Root "state"
$StdoutLogPath = Join-Path $StateDir "windows-endpoint.stdout.log"
$StderrLogPath = Join-Path $StateDir "windows-endpoint.stderr.log"
$PidPath = Join-Path $StateDir "windows-endpoint.pid"
$NoRestartMarkerPath = Join-Path $StateDir "disable-endpoint-restart.txt"
function Resolve-Python {
    $candidates = @(
        "C:\Users\C\AppData\Local\Python\pythoncore-3.14-64\python.exe",
        "C:\Users\C\AppData\Local\Python\bin\python.exe"
    )
    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) {
            return $candidate
        }
    }
    $PyLauncher = (Get-Command py.exe -ErrorAction SilentlyContinue | Select-Object -First 1 -ExpandProperty Source)
    if ($PyLauncher) {
        return $PyLauncher
    }
    return "py"
}

function Convert-ToBoolean {
    param(
        $Value,
        [bool]$Default = $false
    )

    if ($null -eq $Value) {
        return $Default
    }
    if ($Value -is [bool]) {
        return [bool]$Value
    }

    $text = [string]$Value
    if ([string]::IsNullOrWhiteSpace($text)) {
        return $Default
    }

    switch -Regex ($text.Trim().ToLowerInvariant()) {
        "^(1|true|yes|on)$" { return $true }
        "^(0|false|no|off)$" { return $false }
        default { return $Default }
    }
}

function Normalize-DriveLetter {
    param([Parameter(Mandatory = $true)][string]$Letter)

    $value = $Letter.Trim().TrimEnd(":")
    if ($value -notmatch "^[A-Za-z]$") {
        throw "Invalid drive letter in network_drives: $Letter"
    }
    return $value.ToUpperInvariant()
}

function Get-NetworkDriveCredentialFallback {
    param(
        [Parameter(Mandatory = $true)][string]$Letter
    )

    $drivePrefix = "BF6TOOLS_{0}DRIVE" -f $Letter.ToUpperInvariant()
    $user = [Environment]::GetEnvironmentVariable("${drivePrefix}_USER", "Process")
    if (-not $user) {
        $user = [Environment]::GetEnvironmentVariable("${drivePrefix}_USER", "User")
    }

    $password = [Environment]::GetEnvironmentVariable("${drivePrefix}_PASSWORD", "Process")
    if (-not $password) {
        $password = [Environment]::GetEnvironmentVariable("${drivePrefix}_PASSWORD", "User")
    }

    if (-not $user) {
        $user = [Environment]::GetEnvironmentVariable("BF6TOOLS_SHARE_USER", "Process")
    }
    if (-not $user) {
        $user = [Environment]::GetEnvironmentVariable("BF6TOOLS_SHARE_USER", "User")
    }

    if (-not $password) {
        $password = [Environment]::GetEnvironmentVariable("BF6TOOLS_SHARE_PASSWORD", "Process")
    }
    if (-not $password) {
        $password = [Environment]::GetEnvironmentVariable("BF6TOOLS_SHARE_PASSWORD", "User")
    }

    return @{
        UserName = $user
        Password = $password
    }
}

function Get-NetworkLogicalDrive {
    param(
        [Parameter(Mandatory = $true)][string]$Letter
    )

    return Get-CimInstance Win32_LogicalDisk -Filter ("DeviceID='{0}:'" -f $Letter) -ErrorAction SilentlyContinue
}

function Remove-StaleNetworkConnections {
    param(
        [Parameter(Mandatory = $true)][string]$UncPath,
        [string]$CredentialTarget = ""
    )

    & net.exe use $UncPath /delete /y | Out-Null
    if ($CredentialTarget) {
        & net.exe use "\\$CredentialTarget\IPC$" /delete /y | Out-Null
    }
}

function Ensure-NetworkDrive {
    param([Parameter(Mandatory = $true)]$DriveSpec)

    $letter = Normalize-DriveLetter ([string]$DriveSpec.letter)
    $uncPath = [string]$DriveSpec.path
    if ([string]::IsNullOrWhiteSpace($uncPath)) {
        throw "network_drives entry for $letter`: is missing path."
    }
    $uncPath = $uncPath.Trim()
    if ($uncPath -notmatch "^\\\\") {
        throw "network_drives entry for $letter`: must use a UNC path: $uncPath"
    }

    $persistent = $true
    if ($DriveSpec.PSObject.Properties.Name -contains "persistent") {
        $persistent = Convert-ToBoolean -Value $DriveSpec.persistent -Default $true
    }

    $username = ""
    if ($DriveSpec.PSObject.Properties.Name -contains "username" -and $DriveSpec.username) {
        $username = [string]$DriveSpec.username
    }

    $password = ""
    if ($DriveSpec.PSObject.Properties.Name -contains "password" -and $DriveSpec.password) {
        $password = [string]$DriveSpec.password
    }

    if (-not $username -or -not $password) {
        $fallbackCredential = Get-NetworkDriveCredentialFallback -Letter $letter
        if (-not $username -and $fallbackCredential.UserName) {
            $username = [string]$fallbackCredential.UserName
        }
        if (-not $password -and $fallbackCredential.Password) {
            $password = [string]$fallbackCredential.Password
        }
    }

    $credentialTarget = ""
    if ($DriveSpec.PSObject.Properties.Name -contains "credential_target" -and $DriveSpec.credential_target) {
        $credentialTarget = [string]$DriveSpec.credential_target
    } elseif ($uncPath -match "^\\\\([^\\]+)\\") {
        $credentialTarget = $Matches[1]
    }

    $existingDrive = Get-NetworkLogicalDrive -Letter $letter
    $alreadyMapped = ($null -ne $existingDrive -and -not [string]::IsNullOrWhiteSpace($existingDrive.ProviderName))
    $needsRemap = $true
    if ($alreadyMapped -and $existingDrive.ProviderName -eq $uncPath) {
        & cmd.exe /d /c "dir ${letter}:\ >nul 2>&1"
        $needsRemap = ($LASTEXITCODE -ne 0)
    }

    if (-not $needsRemap) {
        Write-Host "Verified network drive ${letter}: -> $uncPath"
        return
    }

    if ($alreadyMapped) {
        & net.exe use "${letter}:" /delete /y | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to remove existing mapping for ${letter}:"
        }
    }

    if ($username -and $password -and $credentialTarget) {
        & cmdkey.exe "/add:$credentialTarget" "/user:$username" "/pass:$password" | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to store credentials for $credentialTarget."
        }
    }

    $mapArgs = @("use", "${letter}:", $uncPath)
    if ($username) {
        $mapArgs += "/user:$username"
        if ($password) {
            $mapArgs += $password
        }
    }
    if ($persistent) {
        $mapArgs += "/persistent:yes"
    } else {
        $mapArgs += "/persistent:no"
    }

    $mapOutput = & net.exe @mapArgs 2>&1
    if ($LASTEXITCODE -ne 0 -and (($mapOutput | Out-String) -match "System error 1219|Multiple connections")) {
        Remove-StaleNetworkConnections -UncPath $uncPath -CredentialTarget $credentialTarget
        $mapOutput = & net.exe @mapArgs 2>&1
    }
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to map ${letter}: to $uncPath. $((($mapOutput | Out-String).Trim()))"
    }

    $mappedDrive = Get-NetworkLogicalDrive -Letter $letter
    if ($null -eq $mappedDrive -or $mappedDrive.ProviderName -ne $uncPath) {
        throw "Mapped ${letter}: but Windows did not report the expected target $uncPath."
    }

    & cmd.exe /d /c "dir ${letter}:\ >nul 2>&1"
    if ($LASTEXITCODE -ne 0) {
        throw "Mapped ${letter}: to $uncPath but the drive is not accessible."
    }

    Write-Host "Mapped network drive ${letter}: -> $uncPath"
}

$BootTime = (Get-Date).ToString("o")
try {
    $BootTime = (Get-CimInstance Win32_OperatingSystem).LastBootUpTime.ToString("o")
} catch {
    $BootTime = (Get-Date).ToString("o")
}
if (Test-Path $NoRestartMarkerPath) {
    $MarkerBoot = Get-Content -Path $NoRestartMarkerPath -Raw
    if ($MarkerBoot.Trim() -eq $BootTime) {
        Write-Host "Startup suppressed: endpoint auto-restart disabled for this boot (marker exists at $NoRestartMarkerPath)."
        exit 0
    }
    Remove-Item -Path $NoRestartMarkerPath -Force -ErrorAction SilentlyContinue
}

if (Test-Path $PreferredConfigPath) {
    $ConfigPath = $PreferredConfigPath
} elseif (Test-Path $FallbackConfigPath) {
    $ConfigPath = $FallbackConfigPath
} else {
    throw "No bridge config was found. Expected $PreferredConfigPath or $FallbackConfigPath."
}

$null = New-Item -ItemType Directory -Force -Path $StateDir
$Config = Get-Content $ConfigPath -Raw | ConvertFrom-Json
$AgentId = if ($Config.endpoint.agent_id) { [string]$Config.endpoint.agent_id } else { "windows-codex" }

$NetworkDrives = @()
if ($Config.PSObject.Properties.Name -contains "network_drives" -and $null -ne $Config.network_drives) {
    $NetworkDrives = @($Config.network_drives)
}
foreach ($DriveSpec in $NetworkDrives) {
    Ensure-NetworkDrive -DriveSpec $DriveSpec
}

$ExistingProcess = Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" | Where-Object {
    $_.CommandLine -like "*agent_endpoint.py*" -and $_.CommandLine -like "*--agent-id $AgentId*"
} | Select-Object -First 1

if ($ExistingProcess) {
    Set-Content -Path $PidPath -Value $ExistingProcess.ProcessId -Encoding ascii
    Write-Host "$AgentId is already running (PID $($ExistingProcess.ProcessId))."
    exit 0
}

$RunEndpointPath = Join-Path $Root "run_endpoint.py"
$PythonExe = Resolve-Python
$Process = Start-Process `
    -FilePath $PythonExe `
    -ArgumentList @($RunEndpointPath, "--config-file", $ConfigPath, "--max-concurrency", "1", "--session-idle-seconds", "900", "--max-sessions", "1", "--warm-clients", "0") `
    -WorkingDirectory $Root `
    -WindowStyle Hidden `
    -RedirectStandardOutput $StdoutLogPath `
    -RedirectStandardError $StderrLogPath `
    -PassThru

Set-Content -Path $PidPath -Value $Process.Id -Encoding ascii

Start-Sleep -Seconds 2

if ($Process.HasExited) {
    $RecoveredProcess = Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" | Where-Object {
        $_.CommandLine -like "*agent_endpoint.py*" -and $_.CommandLine -like "*--agent-id $AgentId*"
    } | Select-Object -First 1
    if ($RecoveredProcess) {
        Set-Content -Path $PidPath -Value $RecoveredProcess.ProcessId -Encoding ascii
        Write-Host "Started $AgentId (PID $($RecoveredProcess.ProcessId))."
        Write-Host "Logs: $StdoutLogPath"
        exit 0
    } else {
        Set-Content -Path $NoRestartMarkerPath -Value $BootTime -Encoding utf8
        $ErrorText = ""
        if (Test-Path $StderrLogPath) {
            $ErrorText = ((Get-Content $StderrLogPath -Tail 20) -join [Environment]::NewLine).Trim()
        }
        if (-not $ErrorText) {
            $ErrorText = "The endpoint exited immediately. Check $StdoutLogPath and $StderrLogPath."
        }
        throw $ErrorText
    }
}

Write-Host "Started $AgentId (PID $($Process.Id))."
Write-Host "Logs: $StdoutLogPath"
