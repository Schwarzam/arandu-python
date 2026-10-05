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


def test_sources_queries_the_object_in_time_order():
    backend = FakeADSSClient()
    client = AranduClient(adss_client=backend)

    frame = client.sources(12, limit=50)

    assert isinstance(frame, pd.DataFrame)
    sql, mode = backend.calls[0]
    assert "WHERE s.dia_object_id = 12" in sql
    assert "ORDER BY s.midpoint_mjd_tai" in sql
    assert mode == "astroql"


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
