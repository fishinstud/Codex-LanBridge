param(
    [Parameter(Mandatory = $true)]
    [string]$BrokerUrl,

    [Parameter(Mandatory = $true)]
    [string]$Token
)

$Root = Split-Path -Parent $PSScriptRoot

py (Join-Path $Root "init_config.py") `
  --role windows `
  --broker-url $BrokerUrl `
  --token $Token
