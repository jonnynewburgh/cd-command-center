"""
Coarse perf baselines so refactors that silently regress hot paths fail
loudly. Thresholds are deliberately generous (3-5x current measurement)
so they only fire on real regressions, not normal jitter.

Run with: pytest tests/test_perf_baseline.py -v
"""

import time

import pytest


def _time_call(fn, *args, **kwargs):
    t0 = time.perf_counter()
    out = fn(*args, **kwargs)
    return out, (time.perf_counter() - t0) * 1000  # ms


def test_nearby_warm_under_500ms(client):
    """get_nearby_facilities at downtown Atlanta. Warm path is ~10ms on
    Postgres after the bbox prefilter (P0 #3); 500ms is a 50x cushion
    that still flags a return-to-old-behavior regression."""
    # Warmup
    client.get("/search/nearby", params={"lat": 33.7490, "lon": -84.3880, "radius": 5})

    samples = []
    for _ in range(3):
        r = client.get("/search/nearby", params={"lat": 33.7490, "lon": -84.3880, "radius": 5})
        assert r.status_code == 200
        # Parse but don't include parsing in the budget — we want to measure
        # the API path, not httpx + json
        samples.append(r.elapsed.total_seconds() * 1000)

    median = sorted(samples)[len(samples) // 2]
    assert median < 500, f"nearby search median latency {median:.1f}ms exceeds 500ms budget"


def test_schools_list_warm_under_2s(client):
    """Default-size /schools page should return well under 2s even with the
    full schools join (CTE + lea_accountability + census_tracts)."""
    # Warmup
    client.get("/schools", params={"limit": 100})

    samples = []
    for _ in range(3):
        r = client.get("/schools", params={"limit": 100})
        assert r.status_code == 200
        samples.append(r.elapsed.total_seconds() * 1000)

    median = sorted(samples)[len(samples) // 2]
    assert median < 2000, f"/schools median latency {median:.1f}ms exceeds 2s budget"


def test_batch_upsert_beats_per_row():
    """Reproduces the P1 #6 win in test form: the batched path should be
    at least an order of magnitude faster than the per-row loop on a
    100-row schools sample. Generous threshold (5x) keeps it from
    flapping on slow CI."""
    import db

    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM schools LIMIT 100")
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.close()

    if len(rows) < 100:
        pytest.skip("need >=100 schools to run this benchmark")

    for r in rows:
        r.pop("id", None)
        r.pop("updated_at", None)

    _, batched_ms = _time_call(
        db.upsert_rows, "schools", rows,
        unique_cols=["nces_id"], touch_cols=["updated_at"],
    )
    _, per_row_ms = _time_call(
        lambda: [db.upsert_school(r) for r in rows]
    )

    speedup = per_row_ms / batched_ms
    assert speedup >= 5, (
        f"batched upsert was only {speedup:.1f}x faster than per-row "
        f"({batched_ms:.0f}ms vs {per_row_ms:.0f}ms); P1 #6 regression?"
    )
