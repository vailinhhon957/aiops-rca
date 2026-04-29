from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


STATE_DIR = Path(os.environ.get("AIOPS_DASHBOARD_STATE_DIR", "/tmp/aiops-dashboard"))
DB_PATH = Path(os.environ.get("AIOPS_DASHBOARD_DB_PATH", str(STATE_DIR / "aiops_dashboard.sqlite3")))
JSON_PATH = Path(os.environ.get("AIOPS_DASHBOARD_JSON_STORE_PATH", str(STATE_DIR / "aiops_dashboard_store.json")))
_FALLBACK_TO_JSON = False


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_dump(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)


def _json_load(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _empty_state() -> dict[str, Any]:
    return {
        "monitoring_events": [],
        "feedback": [],
        "audit_logs": [],
        "next_ids": {"monitoring_events": 1, "feedback": 1, "audit_logs": 1},
    }


def _read_state() -> dict[str, Any]:
    JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not JSON_PATH.exists():
        return _empty_state()
    return json.loads(JSON_PATH.read_text(encoding="utf-8") or "{}") or _empty_state()


def _write_state(state: dict[str, Any]) -> None:
    JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    JSON_PATH.write_text(json.dumps(state, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def _next_id(state: dict[str, Any], key: str) -> int:
    next_ids = state.setdefault("next_ids", {})
    value = int(next_ids.get(key, 1))
    next_ids[key] = value + 1
    return value


def init_store() -> None:
    global _FALLBACK_TO_JSON
    if _FALLBACK_TO_JSON:
        _write_state(_read_state())
        return
    try:
        with _connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS monitoring_events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  created_at TEXT NOT NULL,
                  system_id TEXT NOT NULL,
                  source_service TEXT NOT NULL,
                  status TEXT NOT NULL,
                  is_anomaly INTEGER NOT NULL,
                  anomaly_score REAL,
                  anomaly_model TEXT,
                  rca_top1_service TEXT,
                  rca_top1_score REAL,
                  recommendation_action TEXT,
                  payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS feedback (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  event_id INTEGER NOT NULL,
                  created_at TEXT NOT NULL,
                  feedback TEXT NOT NULL,
                  actor TEXT NOT NULL,
                  notes TEXT NOT NULL DEFAULT '',
                  payload_json TEXT NOT NULL,
                  FOREIGN KEY(event_id) REFERENCES monitoring_events(id)
                );

                CREATE TABLE IF NOT EXISTS audit_logs (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  created_at TEXT NOT NULL,
                  actor TEXT NOT NULL,
                  action TEXT NOT NULL,
                  target_type TEXT NOT NULL,
                  target_id TEXT NOT NULL,
                  payload_json TEXT NOT NULL
                );
                """
            )
    except sqlite3.Error:
        _FALLBACK_TO_JSON = True
        _write_state(_read_state())


def create_monitoring_event(result: dict[str, Any]) -> dict[str, Any]:
    init_store()
    live = result.get("live_context") or {}
    anomaly = result.get("anomaly") or {}
    rca = result.get("rca") or {}
    top1 = rca.get("top1") or {}
    recommendation = result.get("recommendation") or {}
    created_at = utc_now()
    system_id = str(live.get("system_id") or "unknown")
    source_service = str(live.get("source_service") or "unknown")
    is_anomaly = bool(anomaly.get("is_anomaly", False))
    status = "anomaly" if is_anomaly else "normal"
    if _FALLBACK_TO_JSON:
        state = _read_state()
        event_id = _next_id(state, "monitoring_events")
        state["monitoring_events"].append(
            {
                "id": event_id,
                "created_at": created_at,
                "system_id": system_id,
                "source_service": source_service,
                "status": status,
                "is_anomaly": is_anomaly,
                "anomaly_score": anomaly.get("anomaly_score"),
                "anomaly_model": anomaly.get("model_name"),
                "rca_top1_service": top1.get("service_name"),
                "rca_top1_score": top1.get("score"),
                "recommendation_action": recommendation.get("primary_action"),
                "payload": result,
            }
        )
        _write_state(state)
        return {
            "id": event_id,
            "created_at": created_at,
            "system_id": system_id,
            "source_service": source_service,
            "status": status,
            "is_anomaly": is_anomaly,
        }

    with _connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO monitoring_events (
              created_at, system_id, source_service, status, is_anomaly,
              anomaly_score, anomaly_model, rca_top1_service, rca_top1_score,
              recommendation_action, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                created_at,
                system_id,
                source_service,
                status,
                1 if is_anomaly else 0,
                anomaly.get("anomaly_score"),
                anomaly.get("model_name"),
                top1.get("service_name"),
                top1.get("score"),
                recommendation.get("primary_action"),
                _json_dump(result),
            ),
        )
        event_id = int(cursor.lastrowid)
    return {
        "id": event_id,
        "created_at": created_at,
        "system_id": system_id,
        "source_service": source_service,
        "status": status,
        "is_anomaly": is_anomaly,
    }


def _event_from_row(row: sqlite3.Row, include_payload: bool = False) -> dict[str, Any]:
    payload = {
        "id": int(row["id"]),
        "created_at": row["created_at"],
        "system_id": row["system_id"],
        "source_service": row["source_service"],
        "status": row["status"],
        "is_anomaly": bool(row["is_anomaly"]),
        "anomaly_score": row["anomaly_score"],
        "anomaly_model": row["anomaly_model"],
        "rca_top1_service": row["rca_top1_service"],
        "rca_top1_score": row["rca_top1_score"],
        "recommendation_action": row["recommendation_action"],
    }
    if include_payload:
        payload["payload"] = _json_load(row["payload_json"], {})
    return payload


def list_monitoring_events(limit: int = 50, system_id: str | None = None) -> list[dict[str, Any]]:
    init_store()
    limit = max(1, min(int(limit), 200))
    if _FALLBACK_TO_JSON:
        items = list(reversed(_read_state().get("monitoring_events", [])))
        if system_id:
            items = [item for item in items if item.get("system_id") == system_id]
        return [{key: value for key, value in item.items() if key != "payload"} for item in items[:limit]]
    with _connect() as conn:
        if system_id:
            rows = conn.execute(
                "SELECT * FROM monitoring_events WHERE system_id = ? ORDER BY id DESC LIMIT ?",
                (system_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM monitoring_events ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
    return [_event_from_row(row) for row in rows]


def get_monitoring_event(event_id: int) -> dict[str, Any] | None:
    init_store()
    if _FALLBACK_TO_JSON:
        for item in _read_state().get("monitoring_events", []):
            if int(item["id"]) == int(event_id):
                payload = {key: value for key, value in item.items() if key != "payload"}
                payload["payload"] = item.get("payload", {})
                return payload
        return None
    with _connect() as conn:
        row = conn.execute("SELECT * FROM monitoring_events WHERE id = ?", (int(event_id),)).fetchone()
    return _event_from_row(row, include_payload=True) if row else None


def write_audit_log(
    *,
    action: str,
    target_type: str,
    target_id: str,
    actor: str = "dashboard",
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    init_store()
    created_at = utc_now()
    if _FALLBACK_TO_JSON:
        state = _read_state()
        audit_id = _next_id(state, "audit_logs")
        item = {
            "id": audit_id,
            "created_at": created_at,
            "actor": actor,
            "action": action,
            "target_type": target_type,
            "target_id": target_id,
            "payload": payload or {},
        }
        state["audit_logs"].append(item)
        _write_state(state)
        return {key: item[key] for key in ["id", "created_at", "actor", "action", "target_type", "target_id"]}
    with _connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO audit_logs (created_at, actor, action, target_type, target_id, payload_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (created_at, actor, action, target_type, target_id, _json_dump(payload or {})),
        )
        audit_id = int(cursor.lastrowid)
    return {
        "id": audit_id,
        "created_at": created_at,
        "actor": actor,
        "action": action,
        "target_type": target_type,
        "target_id": target_id,
    }


def save_feedback(
    *,
    event_id: int,
    feedback: str,
    actor: str = "operator",
    notes: str = "",
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    init_store()
    created_at = utc_now()
    if _FALLBACK_TO_JSON:
        state = _read_state()
        if not any(int(item["id"]) == int(event_id) for item in state.get("monitoring_events", [])):
            raise KeyError(f"Monitoring event not found: {event_id}")
        feedback_id = _next_id(state, "feedback")
        state["feedback"].append(
            {
                "id": feedback_id,
                "event_id": int(event_id),
                "created_at": created_at,
                "feedback": feedback,
                "actor": actor,
                "notes": notes,
                "payload": payload or {},
            }
        )
        _write_state(state)
        write_audit_log(
            action=f"feedback.{feedback}",
            target_type="monitoring_event",
            target_id=str(event_id),
            actor=actor,
            payload={"notes": notes, **(payload or {})},
        )
        return {
            "id": feedback_id,
            "event_id": int(event_id),
            "created_at": created_at,
            "feedback": feedback,
            "actor": actor,
            "notes": notes,
        }
    with _connect() as conn:
        event = conn.execute("SELECT id FROM monitoring_events WHERE id = ?", (int(event_id),)).fetchone()
        if event is None:
            raise KeyError(f"Monitoring event not found: {event_id}")
        cursor = conn.execute(
            """
            INSERT INTO feedback (event_id, created_at, feedback, actor, notes, payload_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (int(event_id), created_at, feedback, actor, notes, _json_dump(payload or {})),
        )
        feedback_id = int(cursor.lastrowid)
    write_audit_log(
        action=f"feedback.{feedback}",
        target_type="monitoring_event",
        target_id=str(event_id),
        actor=actor,
        payload={"notes": notes, **(payload or {})},
    )
    return {
        "id": feedback_id,
        "event_id": int(event_id),
        "created_at": created_at,
        "feedback": feedback,
        "actor": actor,
        "notes": notes,
    }


def list_audit_logs(limit: int = 50) -> list[dict[str, Any]]:
    init_store()
    limit = max(1, min(int(limit), 200))
    if _FALLBACK_TO_JSON:
        return list(reversed(_read_state().get("audit_logs", [])))[:limit]
    with _connect() as conn:
        rows = conn.execute("SELECT * FROM audit_logs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return [
        {
            "id": int(row["id"]),
            "created_at": row["created_at"],
            "actor": row["actor"],
            "action": row["action"],
            "target_type": row["target_type"],
            "target_id": row["target_id"],
            "payload": _json_load(row["payload_json"], {}),
        }
        for row in rows
    ]
