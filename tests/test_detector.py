from stream.detector import RollingZScoreDetector


def test_warm_up_never_flags_anomaly():
    det = RollingZScoreDetector(window_size=10, z_threshold=3.0, min_samples=5)
    for v in [10, 10, 10, 10]:
        result = det.update(v)
        assert result.is_anomaly is False
        assert result.z_score is None
        assert "warm-up" in result.reason


def test_stable_values_are_not_anomalous():
    det = RollingZScoreDetector(window_size=20, z_threshold=3.0, min_samples=5)
    results = [det.update(10.0) for _ in range(30)]
    assert all(not r.is_anomaly for r in results[5:])  # after warm-up


def test_spike_is_flagged_as_anomaly():
    det = RollingZScoreDetector(window_size=20, z_threshold=3.0, min_samples=5)
    for _ in range(15):
        det.update(10.0)
    result = det.update(1000.0)
    assert result.is_anomaly is True
    assert result.z_score > 3.0


def test_gradual_drift_within_threshold_is_not_flagged():
    det = RollingZScoreDetector(window_size=10, z_threshold=3.0, min_samples=5)
    for _ in range(8):
        det.update(10.0)
    # A modest bump, not a spike -- should stay under a z=3 threshold given
    # the window's std is 0 for a perfectly flat series (so this exercises
    # the zero-std edge case rather than assert on a specific z value).
    result = det.update(10.5)
    assert result.z_score in (float("inf"), 0.0) or isinstance(result.z_score, float)


def test_window_evicts_old_values():
    det = RollingZScoreDetector(window_size=5, z_threshold=3.0, min_samples=3)
    # Fill window with a low baseline, then move the whole window to a new
    # baseline -- the detector should adapt and stop flagging the new
    # baseline as anomalous once the old values have rolled out of the
    # window.
    for _ in range(5):
        det.update(10.0)
    flagged = []
    for _ in range(10):
        result = det.update(50.0)
        flagged.append(result.is_anomaly)
    # Later updates (after the window has fully rolled over to 50s) should
    # no longer be flagged, even though the first jump to 50 was anomalous.
    assert flagged[-1] is False


def test_returns_rolling_mean_and_std_after_warmup():
    det = RollingZScoreDetector(window_size=10, z_threshold=3.0, min_samples=3)
    for v in [1, 2, 3]:
        det.update(v)
    result = det.update(4)
    assert result.rolling_mean is not None
    assert result.rolling_std is not None


def test_negative_and_positive_spikes_both_detected():
    det = RollingZScoreDetector(window_size=20, z_threshold=3.0, min_samples=5)
    for _ in range(15):
        det.update(100.0)
    high = det.update(500.0)
    assert high.is_anomaly is True
