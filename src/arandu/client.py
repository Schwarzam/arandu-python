"""ADSS-backed access to the Arandu catalog."""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping, Sequence
from datetime import datetime
from getpass import getpass
from math import isfinite
from time import monotonic
from typing import Any

import adss
import pandas as pd

DEFAULT_ADSS_BASE_URL = "https://ai-scope.cbpf.br"
DEFAULT_REFRESH_INTERVAL_SECONDS = 6 * 60 * 60


class AranduClient:
    """Query Arandu tables through an :class:`adss.ADSSClient`.

    Parameters are passed straight to ADSS. Use :meth:`from_env` for the
    conventional ``ARANDU_*`` environment variables.
    """

    def __init__(
        self,
        base_url: str = DEFAULT_ADSS_BASE_URL,
        *,
        username: str | None = None,
        password: str | None = None,
        adss_client: Any | None = None,
        refresh_interval_seconds: float | None = DEFAULT_REFRESH_INTERVAL_SECONDS,
    ) -> None:
        """Create a client and re-authenticate it every six hours by default.

        With no credentials, the constructor prompts once, just like
        ``adss.ADSSClient``. Set ``refresh_interval_seconds=None`` to disable
        automatic refresh.
        """
        if refresh_interval_seconds is not None and refresh_interval_seconds <= 0:
            raise ValueError("refresh_interval_seconds must be positive or None")
        if adss_client is None and not username:
            username = input("Username: ").strip()
        if adss_client is None and not password:
            password = getpass("Password: ").strip()
        self._username = username
        self._password = password
        self._refresh_interval_seconds = refresh_interval_seconds
        self._last_refresh = monotonic()
        self._client = adss_client or adss.ADSSClient(
            base_url=base_url, username=username, password=password
        )

    @classmethod
    def from_env(cls) -> "AranduClient":
        """Create a client from ``ARANDU_ADSS_BASE_URL``, username and password."""
        return cls(
            os.getenv("ARANDU_ADSS_BASE_URL", DEFAULT_ADSS_BASE_URL),
            username=os.getenv("ARANDU_USERNAME"),
            password=os.getenv("ARANDU_PASSWORD"),
        )

    def query(self, sql: str, *, mode: str = "astroql") -> pd.DataFrame:
        """Run SQL and return its data as a new DataFrame."""
        self._refresh_if_due()
        result = self._client.query_and_wait(sql, mode=mode)
        data = result.data
        return data.copy() if isinstance(data, pd.DataFrame) else pd.DataFrame(data)

    def raw_query(self, sql: str) -> pd.DataFrame:
        """Run a native SQL query, for example one using PostgreSQL JSONB operators.

        Unlike :meth:`query`, this sends ``mode='sql'`` to ADSS.  Treat ``sql``
        as trusted input: ADSS does not expose parameter binding through this
        client method.
        """
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError("sql must be a non-empty string")
        return self.query(sql, mode="sql")

    def alert_categories(self) -> list[dict[str, Any]]:
        """Return the alert categories currently available from ADSS."""
        self._refresh_if_due()
        return self._client.get_alert_categories()

    def listen_alerts(
        self,
        categories: str | Sequence[str] = "all",
        *,
        replay: str | None = None,
        limit: int | None = None,
        follow: bool = True,
        include_control_events: bool = False,
    ) -> Iterator[dict[str, Any]]:
        """Yield broker alerts from the ADSS Server-Sent Events stream.

        Set ``replay='latest'`` to start from the latest available event, and
        ``follow=False`` to consume only the replayed alerts.
        """
        self._refresh_if_due()
        yield from self._client.listen_alerts(
            categories=categories,
            replay=replay,
            limit=limit,
            follow=follow,
            include_control_events=include_control_events,
        )

    def refresh(self) -> None:
        """Immediately authenticate again with the configured ADSS credentials."""
        if not self._username or not self._password:
            raise RuntimeError("ADSS refresh requires a username and password")
        self._client.login(self._username, self._password)
        self._last_refresh = monotonic()

    def _refresh_if_due(self) -> None:
        if (
            self._refresh_interval_seconds is not None
            and monotonic() - self._last_refresh >= self._refresh_interval_seconds
        ):
            self.refresh()

    def objects(self, *, limit: int = 1_000, **kwargs: Any) -> pd.DataFrame:
        """Return DIA objects, optionally constrained with :meth:`download` filters."""
        return self.download("arandu.dia_object", limit=limit, **kwargs)

    def download(
        self,
        table: str,
        *,
        columns: str = "*",
        where: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        order_by: str | None = None,
        filters: Mapping[str, Any] | None = None,
        classification: str | None = None,
        enrichment_start: str | None = None,
        enrichment_end: str | None = None,
        enricher_name: str | None = None,
        min_flux: float | None = None,
        max_flux: float | None = None,
        flux_column: str = "psf_flux",
    ) -> pd.DataFrame:
        """Download a table selection with optional custom, flux, and enrichment filters.

        ``filters`` maps a column to a value (equality), a list (``IN``), or
        an ``(operator, value)`` pair, for example ``{"snr": (">=", 5)}``.
        Classification filters require all of ``classification``,
        ``enrichment_start``, and ``enrichment_end`` and use an ``[start, end)``
        window on ``enrichment.enriched_at``.
        """
        _table_name(table)
        row_limit = _positive_int(limit, "limit") if limit is not None else None
        row_offset = _nonnegative_int(offset, "offset") if offset is not None else None
        predicates = [where] if where else []
        predicates.extend(_filter_predicates(filters))
        predicates.extend(_flux_predicates(min_flux, max_flux, flux_column))
        enrichment_predicate = _enrichment_predicate(
            table, classification, enrichment_start, enrichment_end, enricher_name
        )
        if enrichment_predicate:
            predicates.append(enrichment_predicate)
        top = f"TOP {row_limit} " if row_limit is not None and not enrichment_predicate else ""
        sql = f"SELECT {top}{columns} FROM {table}"
        if predicates:
            sql = f"{sql} WHERE {' AND '.join(f'({predicate})' for predicate in predicates)}"
        if order_by is not None:
            sql = f"{sql} ORDER BY {_column_name(order_by)}"
        if row_limit is not None and enrichment_predicate:
            sql = f"{sql} LIMIT {row_limit}"
        if row_offset is not None:
            sql = f"{sql} OFFSET {row_offset}"
        return self.raw_query(sql) if enrichment_predicate else self.query(sql)

    def download_date_batches(
        self,
        table: str,
        start: float,
        end: float,
        *,
        interval_days: float,
        date_column: str,
        columns: str = "*",
        where: str | None = None,
        filters: Mapping[str, Any] | None = None,
        classification: str | None = None,
        enrichment_start: str | None = None,
        enrichment_end: str | None = None,
        enricher_name: str | None = None,
        min_flux: float | None = None,
        max_flux: float | None = None,
        flux_column: str = "psf_flux",
        objects_per_batch: int | None = None,
    ) -> Iterator[pd.DataFrame]:
        """Yield consecutive half-open date intervals without ``OFFSET``.

        This keeps every request constrained by ``date_column`` (typically an
        indexed MJD column), rather than sorting and skipping prior rows.
        Set ``objects_per_batch`` to split each date interval into frames with
        at most that many distinct ``dia_object_id`` values.
        """
        _table_name(table)
        _column_name(date_column)
        start = _finite_number(start, "start")
        end = _finite_number(end, "end")
        interval = _finite_number(interval_days, "interval_days")
        if start >= end:
            raise ValueError("start must be earlier than end")
        if interval <= 0:
            raise ValueError("interval_days must be positive")
        if objects_per_batch is not None:
            objects_per_batch = _positive_int(objects_per_batch, "objects_per_batch")

        batch_start = start
        while batch_start < end:
            batch_end = min(batch_start + interval, end)
            date_where = f"{date_column} >= {batch_start} AND {date_column} < {batch_end}"
            query_where = f"({where}) AND {date_where}" if where else date_where
            download_kwargs = {
                "filters": filters,
                "classification": classification,
                "enrichment_start": enrichment_start,
                "enrichment_end": enrichment_end,
                "enricher_name": enricher_name,
                "min_flux": min_flux,
                "max_flux": max_flux,
                "flux_column": flux_column,
            }
            if objects_per_batch is None:
                yield self.download(table, columns=columns, where=query_where, **download_kwargs)
            else:
                object_offset = 0
                while True:
                    object_ids = self._object_id_page(
                        table,
                        where=query_where,
                        limit=objects_per_batch,
                        offset=object_offset,
                        **download_kwargs,
                    )
                    object_ids_batch = _object_ids(object_ids)
                    if not object_ids_batch:
                        break
                    ids = ", ".join(str(object_id) for object_id in object_ids_batch)
                    yield self.download(
                        table,
                        columns=columns,
                        where=f"({query_where}) AND dia_object_id IN ({ids})",
                        **download_kwargs,
                    )
                    object_offset += len(object_ids_batch)
            batch_start = batch_end

    def _object_id_page(
        self,
        table: str,
        *,
        where: str,
        limit: int,
        offset: int,
        filters: Mapping[str, Any] | None,
        classification: str | None,
        enrichment_start: str | None,
        enrichment_end: str | None,
        enricher_name: str | None,
        min_flux: float | None,
        max_flux: float | None,
        flux_column: str,
    ) -> pd.DataFrame:
        """Select a page of object primary keys through matching source rows."""
        if table.rsplit(".", 1)[-1] != "dia_source":
            raise ValueError("objects_per_batch is supported only for arandu.dia_source")
        predicates = [where]
        predicates.extend(_filter_predicates(filters))
        predicates.extend(_flux_predicates(min_flux, max_flux, flux_column))
        enrichment_predicate = _enrichment_predicate(
            table, classification, enrichment_start, enrichment_end, enricher_name
        )
        if enrichment_predicate:
            predicates.append(enrichment_predicate)
        source_where = " AND ".join(f"({predicate})" for predicate in predicates)
        relation = (
            "EXISTS (SELECT 1 FROM arandu.dia_source "
            f"WHERE {source_where} "
            "AND arandu.dia_source.dia_object_id = o.dia_object_id)"
        )
        if enrichment_predicate:
            return self.raw_query(
                "SELECT o.dia_object_id FROM arandu.dia_object AS o "
                f"WHERE {relation} LIMIT {limit} OFFSET {offset}"
            )
        return self.query(
            "SELECT TOP "
            f"{limit} o.dia_object_id FROM arandu.dia_object AS o "
            f"WHERE {relation} OFFSET {offset}"
        )

    def cone_search(
        self,
        table: str,
        *,
        ra: float,
        dec: float,
        radius_deg: float,
        ra_column: str = "ra",
        dec_column: str = "dec",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Download rows inside an AstroQL ``cone`` search (all angles in degrees)."""
        center_ra, center_dec, radius = _coordinates(ra, dec, radius_deg)
        if _has_enrichment_filter(kwargs):
            where = f"q3c_radial_query({ra_column}, {dec_column}, {center_ra}, {center_dec}, {radius})"
        else:
            where = f"cone({ra_column}, {dec_column}, {center_ra}, {center_dec}, {radius})"
        return self.download(table, where=where, **kwargs)

    def polygon_search(
        self,
        table: str,
        vertices: Sequence[tuple[float, float]],
        *,
        ra_column: str = "ra",
        dec_column: str = "dec",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Download rows within an AstroQL polygon described by degree vertices."""
        if len(vertices) < 3:
            raise ValueError("polygon searches need at least three (ra, dec) vertices")
        numbers = []
        for vertex_ra, vertex_dec in vertices:
            numbers.extend(_coordinates(vertex_ra, vertex_dec, 1.0)[:2])
        points = ", ".join(str(value) for value in numbers)
        if _has_enrichment_filter(kwargs):
            where = f"q3c_poly_query({ra_column}, {dec_column}, ARRAY[{points}])"
        else:
            where = f"polygon({ra_column}, {dec_column}, {points})"
        return self.download(table, where=where, **kwargs)

    def sources_by_date(
        self,
        start_mjd: float,
        end_mjd: float,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Download DIA sources whose ``midpoint_mjd_tai`` is in an MJD range."""
        return self.download_by_date(
            "arandu.dia_source",
            start_mjd,
            end_mjd,
            date_column="midpoint_mjd_tai",
            **kwargs,
        )

    def sources_by_date_batches(
        self,
        start_mjd: float,
        end_mjd: float,
        *,
        interval_days: float = 1,
        **kwargs: Any,
    ) -> Iterator[pd.DataFrame]:
        """Yield DIA-source data in MJD intervals, suitable for large downloads."""
        return self.download_date_batches(
            "arandu.dia_source",
            start_mjd,
            end_mjd,
            interval_days=interval_days,
            date_column="midpoint_mjd_tai",
            **kwargs,
        )

    def download_by_date(
        self,
        table: str,
        start: float,
        end: float,
        *,
        date_column: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Download a numeric date range, such as an MJD interval, from a table."""
        _column_name(date_column)
        start = _finite_number(start, "start")
        end = _finite_number(end, "end")
        if start > end:
            raise ValueError("start must not be later than end")
        return self.download(table, where=f"{date_column} >= {start} AND {date_column} <= {end}", **kwargs)

    def object(self, dia_object_id: int) -> pd.DataFrame:
        """Return the DIA object with ``dia_object_id``."""
        object_id = _positive_int(dia_object_id, "dia_object_id")
        return self.query(
            "SELECT * FROM arandu.dia_object "
            f"WHERE dia_object_id = {object_id}"
        )

    def sources(
        self, dia_object_id: int, *, limit: int | None = 50_000, **kwargs: Any
    ) -> pd.DataFrame:
        """Return time-ordered DIA sources for an object.

        Pass ``limit=None`` to request the complete source history.
        """
        object_id = _positive_int(dia_object_id, "dia_object_id")
        row_limit = _positive_int(limit, "limit") if limit is not None else None
        frame = self.download(
            "arandu.dia_source",
            where=f"dia_object_id = {object_id}",
            limit=row_limit,
            order_by="midpoint_mjd_tai",
            **kwargs,
        )
        return frame

    def enrichment(self, dia_object_id: int) -> pd.DataFrame:
        """Return enrichment records joined to a DIA object's sources."""
        object_id = _positive_int(dia_object_id, "dia_object_id")
        return self.query(
            "SELECT e.*, s.dia_object_id, s.midpoint_mjd_tai, s.band, "
            "s.ra, s.dec, s.psf_flux, s.psf_flux_err, s.snr "
            "FROM arandu.enrichment AS e "
            "JOIN arandu.alert AS a ON e.dia_source_id = a.dia_source_id "
            "JOIN arandu.dia_source AS s ON a.dia_source_id = s.dia_source_id "
            f"WHERE s.dia_object_id = {object_id} "
            "ORDER BY s.midpoint_mjd_tai"
        )

    def enrichments_by_classification(
        self,
        classification: str,
        start: str,
        end: str,
        *,
        enricher_name: str | None = None,
        limit: int = 1_000,
    ) -> pd.DataFrame:
        """Return enrichments for one classification in an ``[start, end)`` UTC window.

        The date filter always applies to ``enrichment.enriched_at``.  A
        classification is read from the enricher JSONB value's ``label`` key,
        with ``class`` accepted for compatible enrichers.  This helper uses
        native SQL because JSONB operators are PostgreSQL syntax.
        """
        classification = _required_text(classification, "classification")
        start = _utc_timestamp(start, "start")
        end = _utc_timestamp(end, "end")
        if _parse_timestamp(start) >= _parse_timestamp(end):
            raise ValueError("start must be earlier than end")
        row_limit = _positive_int(limit, "limit")

        predicates = [
            f"COALESCE(e.value->>'label', e.value->>'class') = {_sql_literal(classification)}",
            f"e.enriched_at >= {_sql_literal(start)}",
            f"e.enriched_at < {_sql_literal(end)}",
        ]
        if enricher_name is not None:
            predicates.append(
                f"e.enricher_name = {_sql_literal(_required_text(enricher_name, 'enricher_name'))}"
            )

        return self.raw_query(
            f"SELECT e.*, s.dia_object_id, s.midpoint_mjd_tai, s.band, s.ra, s.dec, "
            f"a.ingested_at "
            f"FROM arandu.enrichment AS e "
            f"JOIN arandu.alert AS a ON a.dia_source_id = e.dia_source_id "
            f"JOIN arandu.dia_source AS s ON s.dia_source_id = e.dia_source_id "
            f"WHERE {' AND '.join(predicates)} "
            f"ORDER BY e.enriched_at DESC"
            f" LIMIT {row_limit}"
        )


def _positive_int(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _nonnegative_int(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _table_name(value: str) -> str:
    if not value or any(not part.isidentifier() for part in value.split(".")):
        raise ValueError("name must be a dotted identifier")
    return value


def _column_name(value: str) -> str:
    return _table_name(value)


def _coordinates(ra: float, dec: float, radius: float) -> tuple[float, float, float]:
    values = tuple(_finite_number(value, "coordinates") for value in (ra, dec, radius))
    if not -90 <= values[1] <= 90:
        raise ValueError("declination must be between -90 and 90 degrees")
    if values[2] <= 0:
        raise ValueError("radius must be positive")
    return values


def _finite_number(value: float, name: str) -> float:
    value = float(value)
    if not isfinite(value):
        raise ValueError(f"{name} must be a finite number")
    return value


def _required_text(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    if "\x00" in value:
        raise ValueError(f"{name} must not contain a null byte")
    return value


def _sql_literal(value: str) -> str:
    """Quote a trusted scalar for the SQL text accepted by ADSS."""
    return "'" + value.replace("'", "''") + "'"


def _utc_timestamp(value: str, name: str) -> str:
    value = _required_text(value, name)
    try:
        parsed = _parse_timestamp(value)
    except ValueError as error:
        raise ValueError(f"{name} must be an ISO 8601 timestamp") from error
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must include a UTC offset")
    return value


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _filter_predicates(filters: Mapping[str, Any] | None) -> list[str]:
    if filters is None:
        return []
    if not isinstance(filters, Mapping):
        raise ValueError("filters must be a mapping of columns to filter values")

    predicates = []
    for column, value in filters.items():
        column = _column_name(column)
        if isinstance(value, tuple):
            if (
                len(value) != 2
                or not isinstance(value[0], str)
                or value[0] not in {"=", "!=", "<", "<=", ">", ">=", "IN", "NOT IN"}
            ):
                raise ValueError("tuple filters must be (operator, value) using a supported operator")
            operator, operand = value
        elif isinstance(value, list):
            operator, operand = "IN", value
        elif value is None:
            predicates.append(f"{column} IS NULL")
            continue
        else:
            operator, operand = "=", value

        if operator in {"IN", "NOT IN"}:
            if not isinstance(operand, (list, tuple)) or not operand:
                raise ValueError(f"{operator} filters need a non-empty list or tuple")
            predicates.append(f"{column} {operator} ({', '.join(_sql_value(item) for item in operand)})")
        elif operand is None:
            if operator not in {"=", "!="}:
                raise ValueError("null filters only support = or !=")
            predicates.append(f"{column} {'IS' if operator == '=' else 'IS NOT'} NULL")
        else:
            predicates.append(f"{column} {operator} {_sql_value(operand)}")
    return predicates


def _flux_predicates(
    min_flux: float | None, max_flux: float | None, flux_column: str
) -> list[str]:
    if min_flux is None and max_flux is None:
        return []
    column = _column_name(flux_column)
    lower = _finite_number(min_flux, "min_flux") if min_flux is not None else None
    upper = _finite_number(max_flux, "max_flux") if max_flux is not None else None
    if lower is not None and upper is not None and lower > upper:
        raise ValueError("min_flux must not exceed max_flux")
    predicates = []
    if lower is not None:
        predicates.append(f"{column} >= {lower}")
    if upper is not None:
        predicates.append(f"{column} <= {upper}")
    return predicates


def _enrichment_predicate(
    table: str,
    classification: str | None,
    start: str | None,
    end: str | None,
    enricher_name: str | None,
) -> str | None:
    values = (classification, start, end, enricher_name)
    if not any(value is not None for value in values):
        return None
    if classification is None or start is None or end is None:
        raise ValueError("classification, enrichment_start, and enrichment_end must be provided together")
    classification = _required_text(classification, "classification")
    start = _utc_timestamp(start, "enrichment_start")
    end = _utc_timestamp(end, "enrichment_end")
    if _parse_timestamp(start) >= _parse_timestamp(end):
        raise ValueError("enrichment_start must be earlier than enrichment_end")
    conditions = [
        f"COALESCE(e.value->>'label', e.value->>'class') = {_sql_literal(classification)}",
        f"e.enriched_at >= {_sql_literal(start)}",
        f"e.enriched_at < {_sql_literal(end)}",
    ]
    if enricher_name is not None:
        conditions.append(f"e.enricher_name = {_sql_literal(_required_text(enricher_name, 'enricher_name'))}")
    conditions_sql = " AND ".join(conditions)
    table_name = table.rsplit(".", 1)[-1]
    if table_name == "dia_source":
        return (
            "EXISTS (SELECT 1 FROM arandu.enrichment AS e "
            f"WHERE e.dia_source_id = {table}.dia_source_id AND {conditions_sql})"
        )
    if table_name == "dia_object":
        return (
            "EXISTS (SELECT 1 FROM arandu.dia_source AS enrichment_source "
            "JOIN arandu.enrichment AS e ON e.dia_source_id = enrichment_source.dia_source_id "
            f"WHERE enrichment_source.dia_object_id = {table}.dia_object_id AND {conditions_sql})"
        )
    raise ValueError("enrichment filters are supported only for arandu.dia_source and arandu.dia_object")


def _has_enrichment_filter(kwargs: Mapping[str, Any]) -> bool:
    return any(kwargs.get(key) is not None for key in ("classification", "enrichment_start", "enrichment_end", "enricher_name"))


def _sql_value(value: Any) -> str:
    if isinstance(value, str):
        return _sql_literal(value)
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(_finite_number(value, "filter value"))
    raise ValueError("filter values must be strings, numbers, booleans, null, or lists of those values")


def _object_ids(frame: pd.DataFrame) -> list[int]:
    if frame.empty:
        return []
    if "dia_object_id" not in frame:
        raise ValueError("objects_per_batch requires a table with a dia_object_id column")
    object_ids = []
    for value in frame["dia_object_id"].dropna().unique():
        numeric_value = _finite_number(value, "dia_object_id")
        if not numeric_value.is_integer() or numeric_value < 1:
            raise ValueError("dia_object_id values must be positive integers")
        object_ids.append(int(numeric_value))
    return list(dict.fromkeys(object_ids))
