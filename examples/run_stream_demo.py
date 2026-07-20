#!/usr/bin/env python3
"""
run_stream_demo.py

A "live-feeling" console demo: runs a shorter stream with a small sleep
between events so the anomaly alerts appear to arrive in real time, the way
they would when tailing the logs of a real streaming job.

Usage:
    python examples/run_stream_demo.py            # ~15s, visible pacing
    python examples/run_stream_demo.py --fast      # instant, sleep=0
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from stream.pipeline import run_pipeline


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fast", action="store_true", help="Run with no sleep between events.")
    parser.add_argument("--events", type=int, default=120)
    args = parser.parse_args()

    run_pipeline(
        n_events=args.events,
        batch_size=5,
        window_size=30,
        z_threshold=3.0,
        sleep_seconds=0.0 if args.fast else 0.05,
        seed=7,
        verbose=True,
    )


if __name__ == "__main__":
    main()
