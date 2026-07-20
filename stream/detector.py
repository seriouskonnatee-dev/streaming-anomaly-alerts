"""
detector.py

Incremental rolling z-score anomaly detector, designed to be updated one
value at a time (or one micro-batch at a time) without ever recomputing
mean/variance from scratch over the whole window -- O(1) per update instead
of O(window_size).

This is the same core idea used by streaming anomaly detectors in
production systems (e.g. a Flink/Spark stateful operator, or a simple
in-memory detector inside a Kafka consumer): maintain running sufficient
statistics (sum, sum of squares) over a fixed-size sliding window, update
them incrementally as new points arrive and old points fall out of the
window, and derive mean/std from those statistics on demand.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class DetectionResult:
    value: float
    rolling_mean: Optional[float]
    rolling_std: Optional[float]
    z_score: Optional[float]
    is_anomaly: bool
    reason: str


class RollingZScoreDetector:
    """Maintains a fixed-size sliding window and flags a new value as
    anomalous if its z-score against the window's mean/std exceeds
    `z_threshold`.

    Update is O(1): we keep a running sum and sum-of-squares for the
    window and adjust them as values enter/leave, rather than recomputing
    over the whole deque on every call.
    """

    def __init__(self, window_size: int = 30, z_threshold: float = 3.0, min_samples: int = 10):
        self.window_size = window_size
        self.z_threshold = z_threshold
        self.min_samples = min_samples

        self._window: deque[float] = deque(maxlen=window_size)
        self._sum: float = 0.0
        self._sum_sq: float = 0.0

    def _mean(self) -> float:
        return self._sum / len(self._window)

    def _std(self) -> float:
        n = len(self._window)
        mean = self._mean()
        variance = max(0.0, (self._sum_sq / n) - mean**2)  # population variance
        return variance**0.5

    def update(self, value: float) -> DetectionResult:
        """Score `value` against the *current* window (before adding it),
        then add it to the window for future calls. Scoring before adding
        avoids a point being compared against a window that already
        contains itself, which would suppress its own anomaly signal."""
        n = len(self._window)

        if n < self.min_samples:
            rolling_mean = self._mean() if n else None
            rolling_std = self._std() if n else None
            self._add(value)
            return DetectionResult(
                value=value,
                rolling_mean=rolling_mean,
                rolling_std=rolling_std,
                z_score=None,
                is_anomaly=False,
                reason=f"warm-up ({n}/{self.min_samples} samples)",
            )

        rolling_mean = self._mean()
        rolling_std = self._std()

        if rolling_std == 0:
            # Degenerate case: a perfectly flat window. Any deviation at all
            # is anomalous; treat std as a tiny epsilon to avoid /0.
            z_score = float("inf") if value != rolling_mean else 0.0
        else:
            z_score = (value - rolling_mean) / rolling_std

        is_anomaly = abs(z_score) > self.z_threshold
        reason = (
            f"|z|={abs(z_score):.2f} exceeds threshold {self.z_threshold}"
            if is_anomaly
            else "within normal range"
        )

        self._add(value)

        return DetectionResult(
            value=value,
            rolling_mean=round(rolling_mean, 4),
            rolling_std=round(rolling_std, 4),
            z_score=round(z_score, 4) if z_score not in (float("inf"), float("-inf")) else z_score,
            is_anomaly=is_anomaly,
            reason=reason,
        )

    def _add(self, value: float) -> None:
        if len(self._window) == self.window_size:
            oldest = self._window[0]
            self._sum -= oldest
            self._sum_sq -= oldest**2
        self._window.append(value)
        self._sum += value
        self._sum_sq += value**2
