# arandu

`arandu` is a compact Python interface to the Arandu broker data served by
ADSS. It returns query results as pandas DataFrames and keeps raw SQL
available when needed.

## Install

```bash
pip install -e '.[cutouts]'
```

## Quick start

```python
from arandu import AranduClient

client = AranduClient.from_env()
objects = client.objects(limit=100)
sources = client.sources(dia_object_id=12345)
```

Set `ARANDU_USERNAME` and `ARANDU_PASSWORD` before using `from_env()`. The
optional `ARANDU_ADSS_BASE_URL` defaults to `https://ai-scope.cbpf.br`.

```python
from arandu import download_cutout

image = download_cutout(alert, "science")
```

Cutouts require the `cutouts` extra. `alert` may contain a published URL at
`payload.cutouts`; otherwise the standard broker cutout URL is used.

## Download selections

All spatial coordinates and radii are in degrees. Date selections use the
Roman source table's `midpoint_mjd_tai` (MJD TAI).

```python
# One DataFrame.
sources = client.download("arandu.dia_source", limit=100_000)

# For a large result, split the indexed MJD range into daily requests.
# Each interval is [start, end), so no rows are duplicated.
for batch in client.sources_by_date_batches(60000, 60010, interval_days=1):
    print(len(batch))

cone = client.cone_search(
    "arandu.dia_object", ra=277.48425, dec=13.5576, radius_deg=5 / 60
)

polygon = client.polygon_search(
    "arandu.dia_object",
    [(277.4, 13.5), (277.6, 13.5), (277.6, 13.7), (277.4, 13.7)],
)

sources = client.sources_by_date(60000, 60001)
```

The spatial helpers generate AstroQL `cone(...)` and `polygon(...)` clauses.
Date batches constrain `midpoint_mjd_tai` directly and do not use `OFFSET` or
an expensive global `ORDER BY`.
