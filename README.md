# Streaming Anomaly Alerts

A simulated real-time event stream, consumed in micro-batches, scored by an
incremental rolling z-score anomaly detector, with a mocked alert function
firing when something looks wrong. Built to demonstrate the core pattern
behind production streaming-anomaly systems (Kafka/Pub-Sub -> stream
processor -> stateful detector -> alerting) without needing any of that
actual infrastructure to run.

## Why this exists

A lot of "anomaly detection" portfolio projects are just `df.describe()` +
a static threshold run once over a CSV. That's not what makes streaming
detection hard. The interesting parts are: scoring each point against
*only what's been seen so far* (no peeking at future data), doing it in
O(1) per point instead of recomputing statistics over a growing window, and
processing data in bounded micro-batches instead of loading everything into
memory. This repo implements all three.

## Architecture

```
                     ┌────────────────────────┐
                     │   event_generator.py    │
                     │  (mock Kafka / Pub-Sub) │
                     │  yields Event one at a  │
                     │  time, ~4% injected     │
                     │  anomalies              │
                     └───────────┬─────────────┘
                                 │ Iterator[Event]
                                 ▼
                     ┌────────────────────────┐
                     │      batched()          │
                     │  groups events into     │
                     │  micro-batches of N     │
                     │  (simulates a streaming │
                     │  trigger interval)      │
                     └───────────┬─────────────┘
                                 │ Iterator[list[Event]]
                                 ▼
              ┌──────────────────────────────────────┐
              │        RollingZScoreDetector          │
              │  stateful, O(1) per-point update:     │
              │  - fixed-size sliding window          │
              │  - running sum / sum-of-squares       │
              │  - z = (x - rolling_mean) / rolling_std│
              │  - warm-up period before scoring      │
              └───────────────────┬────────────────────┘
                                  │ DetectionResult
                                  ▼
                     ┌────────────────────────┐
                     │   is_anomaly == True?   │──No──▶ (continue stream)
                     └───────────┬─────────────┘
                                 │ Yes
                                 ▼
                     ┌────────────────────────┐
                     │      mock_alert()       │
                     │  console alert +        │
                     │  send_mock_webhook()    │
                     │  (no real endpoint --   │
                     │  logs the payload that  │
                     │  would be POSTed)        │
                     └────────────────────────┘
```

`stream/pipeline.py` wires all of the above together and also tracks
true/false positive counts against the generator's injected ground truth,
so you can see detector precision/recall directly in the console summary.

## How the detector works

`RollingZScoreDetector` keeps a fixed-size sliding window (a `deque`) plus
a running `sum` and `sum_of_squares` for that window. On each `update(value)`:

1. Score `value` against the *current* window's mean/std (before adding it
   -- otherwise a spike would dilute its own z-score).
2. Flag as anomalous if `|z| > z_threshold` (default 3.0).
3. Add `value` to the window, evicting the oldest point if the window is
   full, adjusting `sum`/`sum_of_squares` incrementally -- no re-scan of
   the window needed.
4. Skip scoring during a warm-up period (`min_samples`) so early alerts
   aren't fired against too little data.

This is the same sliding-window sufficient-statistics trick used inside
real stateful stream processors (Flink/Spark Structured Streaming state
stores, or a hand-rolled Kafka consumer keeping per-key state in Redis).

## Run it

```bash
pip install -r requirements.txt

# Fast, no artificial delay
python -m stream.pipeline --events 200 --sleep 0

# "Live" feeling demo with visible pacing between events
python examples/run_stream_demo.py

# Instant version of the same demo
python examples/run_stream_demo.py --fast
```

Sample captured output: [`examples/sample_run_output.txt`](examples/sample_run_output.txt).

## Example output

```
--- micro-batch 23 (5 events) ---
  event  111 value=   53.15 z=  0.1406 [ok]
  event  112 value=  266.73 z=  9.8397 [ANOMALY]

*** ANOMALY DETECTED [CRITICAL] alert #4 ***
  metric:       order_value_usd
  event_id:     112
  value:        266.73
  rolling_mean: 50.2837
  rolling_std:  21.9972
  z_score:      9.8397
  reason:       |z|=9.84 exceeds threshold 3.0

[mock webhook] POST https://example.invalid/mock-webhook
[mock webhook] body: {"alert_id": 4, "triggered_at": "...", "metric_name": "order_value_usd", "event_id": 112, "value": 266.73, "rolling_mean": 50.2837, "rolling_std": 21.9972, "z_score": 9.8397, "severity": "critical"}
```

Full 200-event run summary:

```
=== Pipeline summary ===
Events processed:        200
Micro-batches:           40
Alerts fired:            9
Injected anomalies:      11
True positives:          9
False positives:         0
False negatives:         2
```

The 2 false negatives are anomalies injected close together in time, before
the rolling window had fully "forgotten" the first spike -- a realistic
failure mode for fixed-window detectors, and a good illustration of the
precision/recall trade-off between window size and sensitivity.

## Note on the webhook

`stream/alerts.py`'s `send_mock_webhook()` never makes a real network call.
It posts to `https://example.invalid` (a reserved, non-resolvable TLD per
RFC 2606) purely to show the intended call shape, and logs the JSON payload
that would be sent. Wiring up a real Slack/PagerDuty/webhook integration is
a one-line change: swap the `print()` calls for `requests.post(webhook_url,
json=payload.to_dict())`, and read `webhook_url` from an environment
variable instead of hardcoding it.

## Tests

```bash
pytest tests/ -v
```

7 unit tests cover the detector's warm-up behavior, spike detection,
window eviction, and the zero-variance edge case.

## Project structure

```
stream/
  event_generator.py   # synthetic event stream + micro-batching
  detector.py            # RollingZScoreDetector (incremental, O(1) update)
  alerts.py               # AlertPayload + mock_alert() + send_mock_webhook()
  pipeline.py              # wires stream -> detector -> alerts, CLI entry
tests/
  test_detector.py          # unit tests for the detector
examples/
  run_stream_demo.py         # "live" console demo
  sample_run_output.txt       # committed sample output
```

## Skills demonstrated

Streaming/real-time data processing concepts, stateful incremental
algorithms, Python generators and iterators, anomaly detection, and
designing systems around a mockable external side effect (alerting)
so the core logic stays testable without live infrastructure.
