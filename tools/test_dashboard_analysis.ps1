param(
    [string]$BaseUrl = "http://127.0.0.1:8010",
    [string]$SampleName = "ob_cpu_reco_light_001__traces_0001__active.pt",
    [string]$Preset = "suspicious",
    [switch]$ForceRca
)

$body = @{
    sample_name = $SampleName
    preset = $Preset
    run_rca_on_any_input = [bool]$ForceRca
} | ConvertTo-Json

Invoke-RestMethod -Uri "$BaseUrl/api/demo/analyze" -Method Post -ContentType "application/json" -Body $body
