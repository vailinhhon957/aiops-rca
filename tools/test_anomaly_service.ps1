param(
    [string]$BaseUrl = "http://127.0.0.1:8000"
)

$body = @{
    features = @{
        trace_count = 100
        service_count = 6
        service_role_count = 5
        avg_trace_duration_ms = 120
        p95_trace_duration_ms = 350
        max_trace_duration_ms = 800
        error_trace_ratio = 0.01
        error_span_ratio = 0.001
        request_fanout_mean = 4
        critical_path_mean_ms = 110
        latency_cv = 1.2
        mean_span_count = 18
        call_type_diversity = 3
        entrypoint_trace_ratio = 1
        stateful_trace_ratio = 0
    }
    metadata = @{
        run_id = "demo_run"
        window_id = "demo_window"
    }
} | ConvertTo-Json -Depth 5

Invoke-RestMethod -Uri "$BaseUrl/predict/window" -Method Post -ContentType "application/json" -Body $body
