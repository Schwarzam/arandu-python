# Arandu tutorial

## Connect

The normal interactive client is simply:

```python
from arandu import AranduClient

client = AranduClient()
```

It asks for ADSS credentials once. The client re-authenticates automatically
before a request after six hours. To use environment credentials instead, set
`ARANDU_USERNAME` and `ARANDU_PASSWORD` and use `AranduClient.from_env()`.
Set `refresh_interval_seconds=None` to disable automatic refresh, or call
`client.refresh()` to refresh immediately.

## Spatial searches

Coordinates and radii are degrees.

```python
objects = client.cone_search(
    "arandu.dia_object",
    ra=277.48425, dec=13.5576, radius_deg=180 / 3600,
    limit=1_000,
)

polygon_objects = client.polygon_search(
    "arandu.dia_object",
    [(277.40, 13.50), (277.60, 13.50), (277.60, 13.70), (277.40, 13.70)],
    limit=1_000,
)
```

## Custom and flux filters

Every selection helper accepts `filters`: a scalar is equality, a list is
`IN`, and an `(operator, value)` tuple is a comparison. `min_flux` and
`max_flux` use `psf_flux` by default; set `flux_column` to another source
flux field such as `ap_flux`, `trail_flux`, `science_flux`, `template_flux`,
or `dipole_mean_flux`.

```python
bright_sources = client.cone_search(
    "arandu.dia_source",
    ra=277.48425, dec=13.5576, radius_deg=180 / 3600,
    filters={"snr": (">=", 5), "band": ["F146", "F184"]},
    min_flux=1_000,
)
```

For an advanced, trusted predicate, pass `where="reliability >= 0.9"`.

## Date selections

Source dates are MJD TAI. For a large range, batches are half-open (`[start,
end)`), so adjacent requests do not duplicate rows.

```python
sources = client.sources_by_date(61318, 61319, limit=50_000)

for batch in client.sources_by_date_batches(61318, 61320, interval_days=1):
    print(f"Received {len(batch):,} rows")
```

To keep each yielded source frame to a fixed number of distinct DIA objects,
use `objects_per_batch`. The client pages `dia_object` primary keys with `TOP`
and `OFFSET` using a matching-source `EXISTS` condition, then fetches every
source for each group—so no source rows are silently truncated.

```python
for batch in client.sources_by_date_batches(
    61318, 61320,
    interval_days=1,
    objects_per_batch=500,
):
    print(batch["dia_object_id"].nunique(), len(batch))
```

## Enrichment classification and date

Classification filters always require a classification and an enrichment UTC
time window (`[start, end)`). They inspect JSONB `value.label` and fall back to
`value.class`. They work with `download`, spatial searches, date helpers,
`objects`, and `sources` for `arandu.dia_source` and `arandu.dia_object`.

```python
classified = client.enrichments_by_classification(
    "high_confidence_transient",
    "2026-09-07T00:00:00+00:00",
    "2026-09-08T00:00:00+00:00",
    enricher_name="simple-transient-classifier",
    limit=1_000,
)

sources = client.sources_by_date(
    61318, 61319,
    classification="usable",
    enrichment_start="2026-09-07T00:00:00+00:00",
    enrichment_end="2026-09-08T00:00:00+00:00",
    enricher_name="detection-quality",
)
```

These filters use native SQL for JSONB. When combined with cone or polygon
searches, the client uses the equivalent q3c spatial predicate automatically.

## Light curves

Fetch the complete source history for an object with `limit=None`; the
`dia_source` fields `midpoint_mjd_tai`, `psf_flux`, and `psf_flux_err` make a
basic band-separated light curve:

```python
import matplotlib.pyplot as plt

object_id = int(sources["dia_object_id"].dropna().iloc[0])
object_sources = client.sources(object_id, limit=None)
for band, points in object_sources.groupby("band"):
    points = points.sort_values("midpoint_mjd_tai")
    plt.errorbar(
        points["midpoint_mjd_tai"], points["psf_flux"],
        yerr=points["psf_flux_err"], fmt="o", capsize=2, label=band,
    )
plt.xlabel("MJD TAI")
plt.ylabel("PSF flux")
plt.legend(title="Band")
plt.show()
```

## Native SQL

Use `raw_query` for PostgreSQL features such as JSONB. Only interpolate trusted
or validated values: ADSS does not offer parameter binding through this method.

```python
usable = client.raw_query("""
    SELECT enrichment_id, dia_source_id, enricher_name, value, enriched_at
    FROM arandu.enrichment
    WHERE value->>'label' = 'usable'
    ORDER BY enriched_at DESC
    LIMIT 100
""")
```

## Alert cutouts

Install the optional dependency first: `pip install -e '.[cutouts]'`.

```python
import matplotlib.pyplot as plt
from arandu import CUTOUT_KINDS, download_cutout

alert = {"alert_id": 9514770633}
fig, axes = plt.subplots(1, len(CUTOUT_KINDS), figsize=(12, 4))
for axis, kind in zip(axes, CUTOUT_KINDS):
    axis.imshow(download_cutout(alert, kind), origin="lower", cmap="gray")
    axis.set_title(kind)
    axis.set_axis_off()
plt.tight_layout()
```

## Live alerts

Inspect the categories available to the account, then start a stream. This
example stops after five events; remove the `break` in a long-running worker.

```python
client.alert_categories()

for number, alert in enumerate(
    client.listen_alerts(categories=["rapid-high-confidence"], replay="latest"),
    start=1,
):
    print(alert)
    if number == 5:
        break
```
