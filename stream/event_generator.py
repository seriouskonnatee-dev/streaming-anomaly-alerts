"""
event_generator.py

A Python generator that simulates a real-time event stream — standing in for
a Kafka topic / Pub-Sub subscription / message queue consumer. Each event is
a transaction-value reading (e.g. order amount from a payment processor).

Because this is a generator, downstream code consumes it exactly the way it
would consume a real streaming client: pull one event at a time, no need to
materialize the whole stream in memory.
"""
from __future__ import annotations

import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterator


@dataclass
class Event:
    event_id: int
    timestamp: datetime
    metric_name: str
    value: float
    is_injected_anomaly: bool = False  # ground truth, for demo/evaluation only


def generate_event_stream(
    n_events: int = 200,
    metric_name: str = "order_value_usd",
    base_mean: float = 45.0,
    base_std: float = 8.0,
    anomaly_rate: float = 0.04,
    anomaly_multiplier_range: tuple[float, float] = (3.5, 6.0),
    sleep_seconds: float = 0.05,
    seed: int | None = None,
) -> Iterator[Event]:
    """Yield synthetic events one at a time, sleeping `sleep_seconds` between
    each to simulate real-time arrival. A small fraction of events are
    deliberately spiked (or dropped toward zero) to simulate real anomalies
    -- e.g. a pricing bug, a fraud spike, or a broken sensor.

    Set sleep_seconds=0 for fast, deterministic use in tests.
    """
    rng = random.Random(seed)
    start = datetime.now()

    for i in range(1, n_events + 1):
        is_anomaly = rng.random() < anomaly_rate
        if is_anomaly:
            multiplier = rng.uniform(*anomaly_multiplier_range)
            # Half the anomalies spike up, half crash toward zero.
            value = base_mean * multiplier if rng.random() < 0.7 else max(0.5, base_mean / multiplier)
        else:
            value = max(0.5, rng.gauss(base_mean, base_std))

        event = Event(
            event_id=i,
            timestamp=start + timedelta(seconds=i),
            metric_name=metric_name,
            value=round(value, 2),
            is_injected_anomaly=is_anomaly,
        )
        yield event

        if sleep_seconds:
            time.sleep(sleep_seconds)


def batched(stream: Iterator[Event], batch_size: int = 5) -> Iterator[list[Event]]:
    """Group a stream of events into micro-batches, the way a real streaming
    job (Spark Structured Streaming, Kafka consumer with manual commit,
    etc.) would pull a bounded chunk of records per trigger interval."""
    batch: list[Event] = []
    for event in stream:
        batch.append(event)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch
