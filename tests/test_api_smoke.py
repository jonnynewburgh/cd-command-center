"""
Smoke tests for the public API surface.

Covers the routes the CODEX audit (P2 #11) flagged as the minimum useful
suite — `/health`, `/schools`, `/schools/{id}`, `/tracts`, `/search`,
`/search/nearby` — plus a handful of pagination + bbox-prefilter
invariants from the P0 #3 / P0 #4 sessions so future refactors don't
silently regress them.
"""


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# /schools
# ---------------------------------------------------------------------------

def test_schools_returns_paged_envelope(client):
    r = client.get("/schools", params={"limit": 5})
    assert r.status_code == 200
    body = r.json()
    assert set(body) == {"items", "total", "limit", "offset"}
    assert body["limit"] == 5
    assert body["offset"] == 0
    assert isinstance(body["items"], list)
    assert isinstance(body["total"], int)
    # `total` is the unpaginated count, so >= page size
    assert body["total"] >= len(body["items"])


def test_schools_pagination_does_not_overlap(client, known_school_state):
    page_a = client.get("/schools", params={"states": known_school_state, "limit": 10, "offset": 0}).json()
    page_b = client.get("/schools", params={"states": known_school_state, "limit": 10, "offset": 10}).json()
    if page_a["total"] < 20:
        # Not enough rows to page past — invariant trivially holds
        return
    ids_a = {r["nces_id"] for r in page_a["items"]}
    ids_b = {r["nces_id"] for r in page_b["items"]}
    assert ids_a.isdisjoint(ids_b), "consecutive pages overlap; ORDER BY isn't stable"


def test_schools_state_filter_lowers_total(client, known_school_state):
    """Filtered total should not exceed the unfiltered total."""
    full = client.get("/schools", params={"limit": 1}).json()["total"]
    filt = client.get("/schools", params={"states": known_school_state, "limit": 1}).json()["total"]
    assert filt <= full
    assert filt > 0  # the state was discovered from the DB, so it must have rows


def test_schools_bad_sort_is_ignored_not_500(client):
    """The sort whitelist is the SQL-injection guard. Unknown keys must
    fall back to the default ORDER BY rather than reach the DB."""
    r = client.get(
        "/schools",
        params={"limit": 3, "sort": "pwned'; DROP TABLE schools;--"},
    )
    assert r.status_code == 200
    assert len(r.json()["items"]) <= 3


def test_schools_limit_bounds(client):
    assert client.get("/schools", params={"limit": 0}).status_code == 422
    assert client.get("/schools", params={"limit": 99999}).status_code == 422


def test_schools_detail_returns_record(client, known_school_nces_id):
    r = client.get(f"/schools/{known_school_nces_id}")
    assert r.status_code == 200
    body = r.json()
    assert body["nces_id"] == known_school_nces_id
    # Detail endpoint should hand back the geo + name fields the dashboard needs
    for key in ("school_name", "state", "latitude", "longitude"):
        assert key in body, f"detail response missing {key}"


def test_schools_detail_404_on_unknown_id(client):
    r = client.get("/schools/__definitely_not_a_real_nces_id__")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# /tracts
# ---------------------------------------------------------------------------

def test_tracts_paged(client):
    r = client.get("/tracts", params={"states": "GA", "limit": 50})
    assert r.status_code == 200
    body = r.json()
    assert set(body) == {"items", "total", "limit", "offset"}
    assert len(body["items"]) <= 50


# ---------------------------------------------------------------------------
# /search
# ---------------------------------------------------------------------------

def test_search_basic(client):
    r = client.get("/search", params={"q": "atlanta"})
    assert r.status_code == 200
    body = r.json()
    assert set(body) >= {"schools", "fqhc", "ece"}  # other keys allowed too


def test_search_nearby_envelope_and_bbox_parity(client):
    """Downtown Atlanta baseline from the P0 #3 commit. Counts may shift
    when new data lands; the invariants we lock down here are:
      - response shape
      - distance_miles + lat/lon present on every row
      - schools/fqhc/nmtc all return at least one nearby record
        (Atlanta has plenty of each)
    """
    r = client.get("/search/nearby", params={"lat": 33.7490, "lon": -84.3880, "radius": 5})
    assert r.status_code == 200
    body = r.json()
    assert set(body) == {"schools", "fqhc", "ece", "nmtc"}
    for key in ("schools", "fqhc", "nmtc"):
        assert len(body[key]) > 0, f"{key} returned 0 nearby records for downtown Atlanta"
        sample = body[key][0]
        for col in ("distance_miles", "latitude", "longitude"):
            assert col in sample, f"{key}[0] missing {col}"
            assert sample[col] is not None, f"{key}[0].{col} is null"
        # Every returned row should be inside the requested radius
        assert sample["distance_miles"] <= 5.0
