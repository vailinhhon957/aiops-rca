param(
    [string]$Namespace = "default",
    [string]$JaegerUrl = "http://127.0.0.1:16686",
    [string]$Mode = "all",
    [string]$RunIds = "",
    [string]$SplitTags = "",
    [int]$WarmupSeconds = 60,
    [int]$CooldownSeconds = 45,
    [int]$FaultDurationSeconds = 60,
    [int]$ReplicaDropTo = 0,
    [int]$QueryLimit = 500
)

$scriptRoot = $PSScriptRoot
$pythonScript = Join-Path $scriptRoot "scripts\13_run_batch_dataset.py"
$metadataFile = Join-Path $scriptRoot "templates\batch1_fill_only.csv"

python $pythonScript `
  --metadata-file $metadataFile `
  --namespace $Namespace `
  --jaeger-url $JaegerUrl `
  --mode $Mode `
  --run-ids $RunIds `
  --split-tags $SplitTags `
  --warmup-seconds $WarmupSeconds `
  --cooldown-seconds $CooldownSeconds `
  --fault-duration-seconds $FaultDurationSeconds `
  --replica-drop-to $ReplicaDropTo `
  --query-limit $QueryLimit
