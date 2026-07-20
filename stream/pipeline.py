"""
pipeline.py

Wires the pieces together: event stream -> micro-batches -> incremental
detector -> mock alert. Runnable as a script for a live-feeling console
demo, or importable for tests / notebooks.

Usage:
    python -m stream.pipeline
    python -m stream.pipeline --events 300 --sleep 0.02 --batch-size 10
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass

from .alerts import mock_alert
from .detector import DetectionResult, RollingZScoreDetector
from .event_generator import Event, batched, generate_event_stream


@dataclass
class PipelineSummary:
    total_events: int
    total_batches: int
    alerts_fired: int
    true_anomalies_injected: int
    true_positives: int
    false_positives: int
    false_negatives: int


def run_pipeline(
    n_events: int = 200,
    batch_size: int = 5,
    window_size: int = 30,
    z_threshold: float = 3.0,
    min_samples: int = 10,
    sleep_seconds: float = 0.05,
    seed: int | None = 7,
    verbose: bool = True,
) -> PipelineSummary:
    detector = RollingZScoreDetector(window_size=window_size, z_threshold=z_threshold, min_samples=min_samples)

    stream = generate_event_stream(n_events=n_events, sleep_seconds=sleep_seconds, seed=seed)

    total_events = 0
    alerts_fired = 0
    true_anomalies_injected = 0
    true_positives = 0
    false_positives = 0
    batch_count = 0

    for batch in batched(stream, batch_size=batch_size):
        batch_count += 1
        if verbose:
            print(f"\n--- micro-batch {batch_count} ({len(batch)} events) ---")
        for event in batch:
            total_events += 1
            if event.is_injected_anomaly:
                true_anomalies_injected += 1

            result: DetectionResult = detector.update(event.value)

            if verbose:
                tag = "ANOMALY" if result.is_anomaly else "ok"
                print(
                    f"  event {event.event_id:>4} value={event.value:>8.2f} "
                    f"z={result.z_score!s:>8} [{tag}]"
                )

            if result.is_anomaly:
                alerts_fired += 1
                if event.is_injected_anomaly:
                    true_positives += 1
                else:
                    false_positives += 1
                mock_alert(event, result)

    false_negatives = true_anomalies_injected - true_positives

    summary = PipelineSummary(
        total_events=total_events,
        total_batches=batch_count,
        alerts_fired=alerts_fired,
        true_anomalies_injected=true_anomalies_injected,
        true_positives=true_positives,
        false_positives=false_positives,
        false_negatives=false_negatives,
    )

    if verbose:
        print("\n=== Pipeline summary ===")
        print(f"Events processed:        {summary.total_events}")
        print(f"Micro-batches:           {summary.total_batches}")
        print(f"Alerts fired:            {summary.alerts_fired}")
        print(f"Injected anomalies:      {summary.true_anomalies_injected}")
        print(f"True positives:          {summary.true_positives}")
        print(f"False positives:         {summary.false_positives}")
        print(f"False negatives:         {summary.false_negatives}")

    return summary


def main():
    parser = argparse.ArgumentParser(description="Run the streaming anomaly-detection demo pipeline.")
    parser.add_argument("--events", type=int, default=200, help="Number of synthetic events to generate.")
    parser.add_argument("--batch-size", type=int, default=5, help="Micro-batch size.")
    parser.add_argument("--window-size", type=int, default=30, help="Rolling window size for the detector.")
    parser.add_argument("--z-threshold", type=float, default=3.0, help="Z-score threshold to flag an anomaly.")
    parser.add_argument("--sleep", type=float, default=0.05, help="Seconds to sleep between events (0 = fast).")
    parser.add_argument("--seed", type=int, default=7, help="Random seed for reproducibility.")
    args = parser.parse_args()

    run_pipeline(
        n_events=args.events,
        batch_size=args.batch_size,
        window_size=args.window_size,
        z_threshold=args.z_threshold,
        sleep_seconds=args.sleep,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
