$ErrorActionPreference = "Stop"

$TaskName = "CodexLanBridgeWindowsEndpoint"
$CurrentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$LauncherPath = Join-Path $PSScriptRoot "Start-CodexWindowsSide.ps1"
$PowerShellPath = Join-Path $env:SystemRoot "System32\WindowsPowerShell\v1.0\powershell.exe"
$TaskDescription = "Starts the Codex LAN Bridge Windows endpoint from the live checkout."

if (-not (Test-Path $LauncherPath)) {
    throw "Missing launcher script: $LauncherPath"
}

$Action = New-ScheduledTaskAction `
    -Execute $PowerShellPath `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$LauncherPath`""

$Triggers = @(
    (New-ScheduledTaskTrigger -AtStartup),
    (New-ScheduledTaskTrigger -AtLogOn -User $CurrentUser)
)

$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit ([TimeSpan]::Zero)

$Principal = New-ScheduledTaskPrincipal `
    -UserId $CurrentUser `
    -LogonType Interactive `
    -RunLevel Limited

try {
    Register-ScheduledTask `
        -TaskName $TaskName `
        -Description $TaskDescription `
        -Action $Action `
        -Trigger $Triggers `
        -Settings $Settings `
        -Principal $Principal `
        -Force | Out-Null

    $RegisteredTask = Get-ScheduledTask -TaskName $TaskName
    [pscustomobject]@{
        Mode = "ScheduledTask"
        TaskName = $RegisteredTask.TaskName
        TaskPath = $RegisteredTask.TaskPath
        State = $RegisteredTask.State
        UserId = $RegisteredTask.Principal.UserId
    } | Format-List
} catch {
    if ($_.Exception.Message -notmatch "Access is denied") {
        throw
    }

    $StartupDir = [Environment]::GetFolderPath("Startup")
    $ShortcutPath = Join-Path $StartupDir "$TaskName.lnk"
    $Shell = New-Object -ComObject WScript.Shell
    $Shortcut = $Shell.CreateShortcut($ShortcutPath)
    $Shortcut.TargetPath = $PowerShellPath
    $Shortcut.Arguments = "-NoProfile -ExecutionPolicy Bypass -File `"$LauncherPath`""
    $Shortcut.WorkingDirectory = $PSScriptRoot
    $Shortcut.IconLocation = "$PowerShellPath,0"
    $Shortcut.Description = $TaskDescription
    $Shortcut.Save()

    [pscustomobject]@{
        Mode = "StartupShortcut"
        ShortcutPath = $ShortcutPath
        TargetPath = $Shortcut.TargetPath
        Arguments = $Shortcut.Arguments
    } | Format-List
}
