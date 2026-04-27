from __future__ import annotations

from typing import Any


SERVICE_ACTIONS: dict[str, dict[str, Any]] = {
    "checkoutservice": {
        "severity": "high",
        "primary_action": "Restart Pod",
        "secondary_action": "Scale Service",
        "notes": "Checkout faults often impact the user journey immediately, so restart first and scale if errors persist.",
    },
    "paymentservice": {
        "severity": "high",
        "primary_action": "Alert / Approval",
        "secondary_action": "Rollback / Config Update",
        "notes": "Payment faults are high risk. Prefer human approval before rollback or traffic changes.",
    },
    "currencyservice": {
        "severity": "medium",
        "primary_action": "Restart Pod",
        "secondary_action": "Alert / Approval",
        "notes": "Currency faults can recover after restart, but alert if the anomaly remains across multiple windows.",
    },
    "recommendationservice": {
        "severity": "medium",
        "primary_action": "Scale Service",
        "secondary_action": "Restart Pod",
        "notes": "Recommendation latency often responds well to additional replicas before harder interventions.",
    },
    "productcatalogservice": {
        "severity": "medium",
        "primary_action": "Scale Service",
        "secondary_action": "Rollback / Config Update",
        "notes": "Catalog issues can be traffic-sensitive. Scale first, then inspect configuration drift.",
    },
    "cartservice": {
        "severity": "medium",
        "primary_action": "Restart Pod",
        "secondary_action": "Scale Service",
        "notes": "Cart faults often correlate with transient state or memory pressure; restart is a safe first action.",
    },
    "frontend": {
        "severity": "high",
        "primary_action": "Rollback / Config Update",
        "secondary_action": "Alert / Approval",
        "notes": "Frontend faults are user-visible immediately. Rollback is often safer than repeated restarts.",
    },
}


def recommend_actions(anomaly_result: dict[str, Any], rca_result: dict[str, Any] | None) -> dict[str, Any]:
    if not anomaly_result.get("is_anomaly", False):
        return {
            "status": "monitor",
            "severity": "low",
            "primary_action": "Wait & Observe",
            "secondary_action": "No action",
            "notes": "The current window is below the anomaly threshold. Continue monitoring and compare the next window.",
        }

    if not rca_result or not rca_result.get("top1"):
        return {
            "status": "needs_triage",
            "severity": "medium",
            "primary_action": "Alert / Approval",
            "secondary_action": "Collect More Evidence",
            "notes": "An anomaly is present but RCA did not return a usable root cause. Escalate for manual review.",
        }

    service_name = rca_result["top1"].get("service_name", "")
    recommendation = SERVICE_ACTIONS.get(
        service_name,
        {
            "severity": "medium",
            "primary_action": "Alert / Approval",
            "secondary_action": "Restart Pod",
            "notes": "Fallback recommendation because the predicted service is outside the tuned policy set.",
        },
    )
    return {
        "status": "actionable",
        "predicted_service": service_name,
        **recommendation,
    }
