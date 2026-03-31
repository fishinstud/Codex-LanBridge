$Root = Split-Path -Parent $PSScriptRoot
py (Join-Path $Root "install_mcp.py")
