"""
alerts.py

Alert delivery for detected anomalies. Deliberately mocked: this module
prints to the console and simulates a webhook POST by logging the payload
that *would* be sent -- it never contacts a real endpoint. Swapping the
mock for a real Slack/PagerDuty/webhook integration is a matter of
replacing the body of `send_mock_webhook` with an actual HTTP client call
and reading the URL from an environment variable / secrets manager instead
of hardcoding it.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class AlertPayload:
    alert_id: int
    triggered_at: str
    metric_name: str
    event_id: int
    value: float
    rolling_mean: float
    rolling_std: float
    z_score: float
    severity: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


_alert_counter = {"n": 0}


def _next_alert_id() -> int:
    _alert_counter["n"] += 1
    return _alert_counter["n"]


def build_alert_payload(event, detection_result) -> AlertPayload:
    z = detection_result.z_score if isinstance(detection_result.z_score, (int, float)) else 0.0
    severity = "critical" if abs(z) > 5 else "warning"
    return AlertPayload(
        alert_id=_next_alert_id(),
        triggered_at=datetime.now().isoformat(timespec="seconds"),
        metric_name=event.metric_name,
        event_id=event.event_id,
        value=event.value,
        rolling_mean=detection_result.rolling_mean,
        rolling_std=detection_result.rolling_std,
        z_score=z,
        severity=severity,
    )


def send_mock_webhook(payload: AlertPayload, webhook_url: str = "https://example.invalid/mock-webhook") -> None:
    """Stub for what would be `requests.post(webhook_url, json=payload.to_dict())`
    in a real integration. Intentionally never makes a network call --
    `example.invalid` is a reserved, non-resolvable TLD (RFC 2606) used here
    purely to make clear this is illustrative, not a live endpoint."""
    print(f"[mock webhook] POST {webhook_url}")
    print(f"[mock webhook] body: {json.dumps(payload.to_dict())}")


def mock_alert(event, detection_result) -> AlertPayload:
    """Fire an alert for a detected anomaly: print a human-readable console
    alert and simulate a webhook delivery. Returns the payload so callers
    (e.g. tests, or a pipeline summary) can inspect what was 'sent'."""
    payload = build_alert_payload(event, detection_result)

    print(
        f"\n*** ANOMALY DETECTED [{payload.severity.upper()}] "
        f"alert #{payload.alert_id} ***\n"
        f"  metric:       {payload.metric_name}\n"
        f"  event_id:     {payload.event_id}\n"
        f"  value:        {payload.value}\n"
        f"  rolling_mean: {payload.rolling_mean}\n"
        f"  rolling_std:  {payload.rolling_std}\n"
        f"  z_score:      {payload.z_score}\n"
        f"  reason:       {detection_result.reason}\n"
    )
    send_mock_webhook(payload)
    return payload
