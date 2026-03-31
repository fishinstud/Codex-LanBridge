$Root = Split-Path -Parent $PSScriptRoot
py (Join-Path $Root "doctor.py") @args
