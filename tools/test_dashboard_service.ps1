param(
    [string]$BaseUrl = "http://127.0.0.1:8010"
)

Write-Host "Dashboard health:" -ForegroundColor Cyan
Invoke-RestMethod -Uri "$BaseUrl/api/health" -Method Get

Write-Host ""
Write-Host "Dashboard samples:" -ForegroundColor Cyan
Invoke-RestMethod -Uri "$BaseUrl/api/samples" -Method Get

