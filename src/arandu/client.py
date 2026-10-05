"""ADSS-backed access to the Arandu catalog."""

from __future__ import annotations

import os
from collections.abc import Iterator, Sequence
from math import isfinite
from typing import Any

import adss
import pandas as pd

DEFAULT_ADSS_BASE_URL = "https://ai-scope.cbpf.br"


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
    ) -> None:
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
        result = self._client.query_and_wait(sql, mode=mode)
        data = result.data
        return data.copy() if isinstance(data, pd.DataFrame) else pd.DataFrame(data)

    def alert_categories(self) -> list[dict[str, Any]]:
        """Return the alert categories currently available from ADSS."""
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
        yield from self._client.listen_alerts(
            categories=categories,
            replay=replay,
            limit=limit,
            follow=follow,
            include_control_events=include_control_events,
        )

    def objects(self, *, limit: int = 1_000) -> pd.DataFrame:
        """Return up to ``limit`` DIA objects."""
        return self.query(f"SELECT TOP {_positive_int(limit, 'limit')} * FROM arandu.dia_object")

    def download(
        self,
        table: str,
        *,
        columns: str = "*",
        where: str | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        """Download one AstroQL table selection as a DataFrame."""
        _table_name(table)
        top = f"TOP {_positive_int(limit, 'limit')} " if limit is not None else ""
        sql = f"SELECT {top}{columns} FROM {table}"
        return self.query(f"{sql} WHERE {where}" if where else sql)

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
    ) -> Iterator[pd.DataFrame]:
        """Yield consecutive half-open date intervals without ``OFFSET``.

        This keeps every request constrained by ``date_column`` (typically an
        indexed MJD column), rather than sorting and skipping prior rows.
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

        batch_start = start
        while batch_start < end:
            batch_end = min(batch_start + interval, end)
            date_where = f"{date_column} >= {batch_start} AND {date_column} < {batch_end}"
            query_where = f"({where}) AND {date_where}" if where else date_where
            yield self.download(table, columns=columns, where=query_where)
            batch_start = batch_end

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

    def sources(self, dia_object_id: int, *, limit: int = 50_000) -> pd.DataFrame:
        """Return time-ordered DIA sources for an object."""
        object_id = _positive_int(dia_object_id, "dia_object_id")
        row_limit = _positive_int(limit, "limit")
        return self.query(
            f"SELECT TOP {row_limit} s.* "
            "FROM arandu.dia_source AS s "
            f"WHERE s.dia_object_id = {object_id} "
            "ORDER BY s.midpoint_mjd_tai"
        )

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


def _positive_int(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
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
