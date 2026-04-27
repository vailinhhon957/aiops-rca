const config = window.DASHBOARD_CONFIG || {};

const healthCards = document.getElementById("healthCards");
const sampleSelect = document.getElementById("sampleSelect");
const presetSelect = document.getElementById("presetSelect");
const forceRcaToggle = document.getElementById("forceRcaToggle");
const refreshHealthBtn = document.getElementById("refreshHealthBtn");
const runHealthyBtn = document.getElementById("runHealthyBtn");
const runIncidentBtn = document.getElementById("runIncidentBtn");
const runLiveBtn = document.getElementById("runLiveBtn");
const runDemoBtn = document.getElementById("runDemoBtn");
const metadataBox = document.getElementById("metadataBox");
const pipelineState = document.getElementById("pipelineState");
const anomalyScore = document.getElementById("anomalyScore");
const anomalyThreshold = document.getElementById("anomalyThreshold");
const anomalyDecision = document.getElementById("anomalyDecision");
const anomalyModel = document.getElementById("anomalyModel");
const anomalyBar = document.getElementById("anomalyBar");
const top1Service = document.getElementById("top1Service");
const top1Score = document.getElementById("top1Score");
const topkList = document.getElementById("topkList");
const recoveryCard = document.getElementById("recoveryCard");
const recoveryLog = document.getElementById("recoveryLog");
const recoveryHistoryList = document.getElementById("recoveryHistoryList");
const restartPodBtn = document.getElementById("restartPodBtn");
const scaleServiceBtn = document.getElementById("scaleServiceBtn");
const alertOnlyBtn = document.getElementById("alertOnlyBtn");
const liveSnapshot = document.getElementById("liveSnapshot");

let latestAnalysis = null;

document.getElementById("anomalyUrl").textContent = config.anomalyBaseUrl || "-";
document.getElementById("rcaUrl").textContent = config.rcaBaseUrl || "-";
document.getElementById("orchUrl").textContent = config.orchestratorBaseUrl || "-";
document.getElementById("jaegerUrl").textContent = config.jaegerUrl || "-";
document.getElementById("promUrl").textContent = config.prometheusUrl || "-";

function badgeClass(status) {
  if (status === "ok") return "ok";
  if (status === "down") return "bad";
  return "warn";
}

function setPipelineState(text, klass = "muted") {
  pipelineState.textContent = text;
  pipelineState.className = `state-pill ${klass}`;
}

function renderHealth(data) {
  const items = [
    { label: "Dashboard", payload: data.dashboard },
    { label: "Anomaly Service", payload: data.anomaly },
    { label: "RCA Service", payload: data.rca },
    { label: "Orchestrator", payload: data.orchestrator?.orchestrator || data.orchestrator },
  ];
  healthCards.innerHTML = items.map((item) => {
    const status = item.payload?.status || "unknown";
    const meta = Object.entries(item.payload || {})
      .filter(([key]) => key !== "status")
      .slice(0, 2)
      .map(([key, value]) => `<div><small>${key}: ${String(value)}</small></div>`)
      .join("");
    return `
      <article class="health-card">
        <span class="label">${item.label}</span>
        <span class="status-pill ${badgeClass(status)}">${status}</span>
        <div style="margin-top:10px">${meta}</div>
      </article>
    `;
  }).join("");
}

function renderMetadata(data) {
  metadataBox.textContent = JSON.stringify({
    presets: data.window_presets,
    sample_count: data.samples?.length || 0,
    live_defaults: data.live_defaults,
    recovery: data.recovery,
    anomaly_model: data.anomaly?.inference_config?.model_name,
    rca_model: data.rca?.inference_config?.model_name,
    orchestrator: data.orchestrator,
  }, null, 2);
}

function renderSamples(items) {
  sampleSelect.innerHTML = "";
  for (const item of items) {
    const option = document.createElement("option");
    option.value = item.name;
    option.textContent = item.name;
    sampleSelect.appendChild(option);
  }
  if (items.length > 0) {
    sampleSelect.value = items[0].name;
  }
}

function renderLiveSnapshot(result) {
  const live = result.live_context || null;
  if (!live) return;

  const trace = live.trace_snapshot || {};
  const metrics = live.metrics_snapshot || {};
  const metricText = metrics.values && Object.keys(metrics.values).length
    ? Object.entries(metrics.values).map(([key, value]) => `${key}=${Number(value.value).toFixed(3)}`).join(" | ")
    : "No compatible Prometheus metric found";

  liveSnapshot.innerHTML = `
    <div><strong>Source:</strong> ${live.source_service}</div>
    <div><strong>Trace count:</strong> ${trace.trace_count ?? "-"}</div>
    <div><strong>Span count:</strong> ${trace.span_count ?? "-"}</div>
    <div><strong>Service count:</strong> ${trace.service_count ?? "-"}</div>
    <div><strong>Prometheus:</strong> ${metrics.status || "-"}</div>
    <div><strong>Metrics:</strong> ${metricText}</div>
  `;
}

function renderRecoveryHistory(items) {
  if (!Array.isArray(items) || items.length === 0) {
    recoveryHistoryList.innerHTML = `<div class="history-empty">No recovery action recorded yet.</div>`;
    return;
  }

  recoveryHistoryList.innerHTML = items.map((item) => `
    <article class="history-item">
      <div class="history-item-top">
        <span class="history-action">${item.action_label}</span>
        <span class="history-status ${item.status === "executed" ? "ok" : "warn"}">${item.status}</span>
      </div>
      <div class="history-meta">
        <span>service=${item.service_name}</span>
        <span>severity=${item.severity}</span>
        <span>mode=${item.mode || "-"}</span>
      </div>
      <div class="history-note">${item.notes || "-"}</div>
      <div class="history-time">${item.timestamp || "-"}</div>
    </article>
  `).join("");
}

async function refreshRecoveryHistory() {
  try {
    const history = await fetchJson("/api/recovery/history");
    renderRecoveryHistory(history.items || []);
  } catch (err) {
    recoveryHistoryList.innerHTML = `<div class="history-empty">Failed to load recovery history: ${err.message}</div>`;
  }
}

function renderAnalysis(result) {
  latestAnalysis = result;
  const anomaly = result.anomaly || {};
  const rca = result.rca || null;
  const rec = result.recommendation || {};

  anomalyScore.textContent = Number(anomaly.anomaly_score || 0).toFixed(4);
  anomalyThreshold.textContent = `threshold: ${Number(anomaly.threshold || 0).toFixed(2)}`;
  anomalyDecision.textContent = anomaly.is_anomaly ? "Anomaly" : "Normal";
  anomalyModel.textContent = `model: ${anomaly.model_name || "-"}`;
  anomalyBar.style.width = `${Math.max(2, Math.min(100, (anomaly.anomaly_score || 0) * 100))}%`;
  setPipelineState(result.pipeline_state || "unknown", anomaly.is_anomaly ? "warn" : "ok");
  anomalyDecision.style.color = anomaly.is_anomaly ? "#f85149" : "#3fb950";

  if (rca && rca.top1) {
    top1Service.textContent = rca.top1.service_name;
    top1Score.textContent = `score: ${Number(rca.top1.score || 0).toFixed(3)}`;
    topkList.innerHTML = (rca.topk || []).map((item, idx) => `
      <li>
        <span><strong>#${idx + 1}</strong> <span class="rank-service">${item.service_name}</span></span>
        <span>${Number(item.score || 0).toFixed(3)}</span>
      </li>
    `).join("");
  } else {
    top1Service.textContent = "Skipped";
    top1Score.textContent = "score: -";
    topkList.innerHTML = `<li><span>RCA was not triggered for this run.</span><span>-</span></li>`;
  }

  recoveryCard.innerHTML = `
    <p><strong>Status:</strong> ${rec.status || "-"}</p>
    <p><strong>Severity:</strong> ${rec.severity || "-"}</p>
    <p><strong>Primary action:</strong> ${rec.primary_action || "-"}</p>
    <p><strong>Secondary action:</strong> ${rec.secondary_action || "-"}</p>
    <p>${rec.notes || "No recommendation yet."}</p>
  `;

  renderLiveSnapshot(result);
}

function currentRecoveryContext() {
  const recommendation = latestAnalysis?.recommendation || {};
  const rca = latestAnalysis?.rca || {};
  return {
    service_name: recommendation.predicted_service || rca?.top1?.service_name || "unknown",
    severity: recommendation.severity || "unknown",
    context: {
      pipeline_state: latestAnalysis?.pipeline_state || "unknown",
      sample_name: latestAnalysis?.demo_context?.sample_name || latestAnalysis?.live_context?.source_service || sampleSelect.value,
      preset: latestAnalysis?.demo_context?.preset || presetSelect.value,
      anomaly_score: latestAnalysis?.anomaly?.anomaly_score ?? null,
    },
  };
}

async function executeRecovery(action) {
  if (!latestAnalysis) {
    alert("Run an analysis first so the dashboard has a target service.");
    return;
  }

  const context = currentRecoveryContext();
  try {
    const result = await fetchJson("/api/recovery/execute", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        action,
        service_name: context.service_name,
        severity: context.severity,
        source: "dashboard_demo",
        context: context.context,
      }),
    });
    recoveryLog.innerHTML = `
      <strong>${result.action_label}</strong> accepted for <strong>${result.service_name}</strong><br/>
      <span>Status: ${result.status} | Severity: ${result.severity} | Mode: ${result.mode || "-"}</span><br/>
      <span>${result.notes}</span><br/>
      <small>${result.timestamp}</small>
    `;
    await refreshRecoveryHistory();
  } catch (err) {
    alert(err.message);
  }
}

async function fetchJson(url, options = {}) {
  const resp = await fetch(url, options);
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(text || `Request failed with ${resp.status}`);
  }
  return resp.json();
}

async function runAnalysis({ preset, runRcaOnAnyInput }) {
  if (!sampleSelect.value) {
    alert("Please choose a graph sample first.");
    setPipelineState("Sample required", "bad");
    return;
  }
  setPipelineState("Analyzing", "warn");
  try {
    const result = await fetchJson("/api/demo/analyze", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        sample_name: sampleSelect.value,
        preset,
        run_rca_on_any_input: runRcaOnAnyInput,
      }),
    });
    renderAnalysis(result);
  } catch (err) {
    setPipelineState("Analysis failed", "bad");
    alert(err.message);
  }
}

async function runLiveAnalysis() {
  setPipelineState("Collecting live data", "warn");
  try {
    const result = await fetchJson("/api/live/analyze", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        run_rca_on_any_input: false,
      }),
    });
    renderAnalysis(result);
  } catch (err) {
    setPipelineState("Live analyze failed", "bad");
    alert(err.message);
  }
}

async function boot() {
  setPipelineState("Loading", "muted");
  const [health, metadata, samples, history] = await Promise.all([
    fetchJson("/api/health"),
    fetchJson("/api/metadata"),
    fetchJson("/api/samples"),
    fetchJson("/api/recovery/history"),
  ]);
  renderHealth(health);
  renderMetadata(metadata);
  renderSamples(samples.items || []);
  renderRecoveryHistory(history.items || []);
  setPipelineState("Ready", "ok");
}

refreshHealthBtn.addEventListener("click", async () => {
  setPipelineState("Refreshing health", "muted");
  try {
    const health = await fetchJson("/api/health");
    renderHealth(health);
    setPipelineState("Ready", "ok");
  } catch (err) {
    setPipelineState("Health error", "bad");
    alert(err.message);
  }
});

runDemoBtn.addEventListener("click", async () => {
  runAnalysis({
    preset: presetSelect.value,
    runRcaOnAnyInput: forceRcaToggle.checked,
  });
});

runHealthyBtn.addEventListener("click", async () => {
  presetSelect.value = "healthy";
  forceRcaToggle.checked = false;
  await runAnalysis({ preset: "healthy", runRcaOnAnyInput: false });
});

runIncidentBtn.addEventListener("click", async () => {
  presetSelect.value = "suspicious";
  forceRcaToggle.checked = false;
  await runAnalysis({ preset: "suspicious", runRcaOnAnyInput: false });
});

runLiveBtn.addEventListener("click", async () => {
  await runLiveAnalysis();
});

restartPodBtn.addEventListener("click", async () => {
  await executeRecovery("restart_pod");
});

scaleServiceBtn.addEventListener("click", async () => {
  await executeRecovery("scale_service");
});

alertOnlyBtn.addEventListener("click", async () => {
  await executeRecovery("alert_only");
});

boot().catch((err) => {
  setPipelineState("Startup failed", "bad");
  metadataBox.textContent = err.message;
});
