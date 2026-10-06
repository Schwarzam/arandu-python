import pandas as pd
import pytest

from arandu import AranduClient


class Result:
    data = [{"dia_object_id": 12}]


class FakeADSSClient:
    def __init__(self):
        self.calls = []

    def query_and_wait(self, sql, mode):
        self.calls.append((sql, mode))
        return Result()

    def login(self, username, password):
        self.calls.append(("login", username, password))

    def listen_alerts(self, **kwargs):
        self.calls.append(("listen_alerts", kwargs))
        return iter([{"alert_id": 1}])


def test_sources_queries_the_object_in_time_order():
    backend = FakeADSSClient()
    client = AranduClient(adss_client=backend)

    frame = client.sources(12, limit=50)

    assert isinstance(frame, pd.DataFrame)
    sql, mode = backend.calls[0]
    assert "WHERE (dia_object_id = 12)" in sql
    assert "ORDER BY midpoint_mjd_tai" in sql
    assert mode == "astroql"


def test_sources_allows_an_unbounded_history_request():
    backend = FakeADSSClient()

    AranduClient(adss_client=backend).sources(12, limit=None)

    sql, _ = backend.calls[0]
    assert "SELECT TOP" not in sql
    assert "ORDER BY midpoint_mjd_tai" in sql


@pytest.mark.parametrize("value", [0, -1, True, "12"])
def test_identifiers_must_be_positive_integers(value):
    with pytest.raises(ValueError):
        AranduClient(adss_client=FakeADSSClient()).object(value)


def test_spatial_and_date_helpers_generate_astroql():
    backend = FakeADSSClient()
    client = AranduClient(adss_client=backend)

    client.cone_search("arandu.dia_object", ra=10, dec=-2, radius_deg=0.1, limit=1)
    client.polygon_search(
        "arandu.dia_object", [(10, -2), (11, -2), (11, -1)], limit=1
    )
    client.sources_by_date(60000, 60001, limit=1)

    queries = [call[0] for call in backend.calls]
    assert "cone(ra, dec, 10.0, -2.0, 0.1)" in queries[0]
    assert "polygon(ra, dec, 10.0, -2.0, 11.0, -2.0, 11.0, -1.0)" in queries[1]
    assert "midpoint_mjd_tai >= 60000.0 AND midpoint_mjd_tai <= 60001.0" in queries[2]
    assert all("SELECT TOP 1" in query for query in queries)


def test_date_batches_use_non_overlapping_date_predicates():
    backend = FakeADSSClient()
    client = AranduClient(adss_client=backend)

    batches = list(client.sources_by_date_batches(60000, 60002, interval_days=1))

    assert len(batches) == 2
    queries = [call[0] for call in backend.calls]
    assert "midpoint_mjd_tai >= 60000.0 AND midpoint_mjd_tai < 60001.0" in queries[0]
    assert "midpoint_mjd_tai >= 60001.0 AND midpoint_mjd_tai < 60002.0" in queries[1]


def test_alert_listener_forwards_stream_options():
    backend = FakeADSSClient()
    alerts = list(
        AranduClient(adss_client=backend).listen_alerts(
            ["rapid-high-confidence"], replay="latest", follow=False
        )
    )

    assert alerts == [{"alert_id": 1}]
    assert backend.calls[0] == (
        "listen_alerts",
        {
            "categories": ["rapid-high-confidence"],
            "replay": "latest",
            "limit": None,
            "follow": False,
            "include_control_events": False,
        },
    )


def test_raw_query_uses_native_sql_mode():
    backend = FakeADSSClient()

    AranduClient(adss_client=backend).raw_query("SELECT 1")

    assert backend.calls == [("SELECT 1", "sql")]


def test_classification_query_always_filters_classification_and_dates():
    backend = FakeADSSClient()

    AranduClient(adss_client=backend).enrichments_by_classification(
        "candidate's choice",
        "2026-09-07T00:00:00+00:00",
        "2026-09-08T00:00:00+00:00",
        enricher_name="simple-transient-classifier",
        limit=25,
    )

    sql, mode = backend.calls[0]
    assert mode == "sql"
    assert "COALESCE(e.value->>'label', e.value->>'class') = 'candidate''s choice'" in sql
    assert "e.enriched_at >= '2026-09-07T00:00:00+00:00'" in sql
    assert "e.enriched_at < '2026-09-08T00:00:00+00:00'" in sql
    assert "e.enricher_name = 'simple-transient-classifier'" in sql
    assert "LIMIT 25" in sql


@pytest.mark.parametrize("kwargs", [
    {"classification": "", "start": "2026-09-07", "end": "2026-09-08"},
    {"classification": "usable", "start": "", "end": "2026-09-08"},
    {"classification": "usable", "start": "2026-09-07", "end": ""},
])
def test_classification_query_requires_all_filters(kwargs):
    with pytest.raises(ValueError):
        AranduClient(adss_client=FakeADSSClient()).enrichments_by_classification(**kwargs)


def test_download_filters_support_flux_and_custom_predicates():
    backend = FakeADSSClient()
    client = AranduClient(adss_client=backend)

    client.cone_search(
        "arandu.dia_source",
        ra=10,
        dec=-2,
        radius_deg=0.1,
        filters={"snr": (">=", 5), "band": ["F146", "F184"]},
        min_flux=100,
        max_flux=1_000,
    )

    sql, mode = backend.calls[0]
    assert mode == "astroql"
    assert "snr >= 5.0" in sql
    assert "band IN ('F146', 'F184')" in sql
    assert "psf_flux >= 100.0" in sql
    assert "psf_flux <= 1000.0" in sql


def test_cone_search_uses_native_spatial_filter_with_enrichment_selection():
    backend = FakeADSSClient()

    AranduClient(adss_client=backend).cone_search(
        "arandu.dia_source",
        ra=10,
        dec=-2,
        radius_deg=0.1,
        classification="usable",
        enrichment_start="2026-09-07T00:00:00+00:00",
        enrichment_end="2026-09-08T00:00:00+00:00",
    )

    sql, mode = backend.calls[0]
    assert mode == "sql"
    assert "q3c_radial_query(ra, dec, 10.0, -2.0, 0.1)" in sql
    assert "EXISTS (SELECT 1 FROM arandu.enrichment AS e" in sql


def test_client_refreshes_credentials_before_a_request_when_due():
    backend = FakeADSSClient()
    client = AranduClient(
        adss_client=backend,
        username="user",
        password="secret",
        refresh_interval_seconds=1,
    )
    client._last_refresh -= 2

    client.query("SELECT 1")

    assert backend.calls[0] == ("login", "user", "secret")
    assert backend.calls[1] == ("SELECT 1", "astroql")


def test_date_batches_can_group_matching_rows_by_object_id():
    class ObjectBatchBackend(FakeADSSClient):
        def query_and_wait(self, sql, mode):
            self.calls.append((sql, mode))
            result = Result()
            if "FROM arandu.dia_object AS o" not in sql:
                result.data = []
            elif "OFFSET 3" in sql:
                result.data = []
            elif "OFFSET 2" in sql:
                result.data = [{"dia_object_id": 13}]
            else:
                result.data = [{"dia_object_id": 11}, {"dia_object_id": 12}]
            return result

    backend = ObjectBatchBackend()
    batches = list(
        AranduClient(adss_client=backend).sources_by_date_batches(
            60000, 60001, objects_per_batch=2
        )
    )

    assert len(batches) == 2
    queries = [call[0] for call in backend.calls]
    assert "SELECT TOP 2 o.dia_object_id FROM arandu.dia_object AS o" in queries[0]
    assert "DISTINCT" not in queries[0]
    assert "OFFSET 0" in queries[0]
    assert "dia_object_id IN (11, 12)" in queries[1]
    assert "dia_object_id IN (13)" in queries[3]
