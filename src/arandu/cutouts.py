"""Utilities for downloading alert FITS cutouts."""

from __future__ import annotations

from io import BytesIO
from typing import Any, Mapping

import requests

CUTOUT_KINDS = ("science", "template", "difference")


def cutout_url(
    alert: Mapping[str, Any],
    kind: str,
    *,
    base_url: str = "https://arandu-portal.cbpf.br/cutouts",
) -> str:
    """Get a cutout URL from an alert or construct the broker-standard URL."""
    if kind not in CUTOUT_KINDS:
        raise ValueError(f"kind must be one of {CUTOUT_KINDS}")
    payload = alert.get("payload") or {}
    published_urls = payload.get("cutouts") or {}
    if kind in published_urls:
        return str(published_urls[kind])

    alert_id = alert.get("alert_id") or alert.get("diaSourceId")
    if alert_id is None:
        raise KeyError("alert must contain alert_id or diaSourceId")
    return f"{base_url.rstrip('/')}/{alert_id}/{kind}.fits"


def download_cutout(
    alert: Mapping[str, Any],
    kind: str,
    *,
    base_url: str = "https://arandu-portal.cbpf.br/cutouts",
    timeout: float = 30,
) -> Any:
    """Download a FITS cutout and return a copy of its primary image array.

    Install the ``cutouts`` extra to use this function.
    """
    try:
        from astropy.io import fits
    except ImportError as error:  # pragma: no cover - depends on extras
        raise ImportError("Install arandu[cutouts] to download FITS cutouts") from error

    response = requests.get(cutout_url(alert, kind, base_url=base_url), timeout=timeout)
    response.raise_for_status()
    with fits.open(BytesIO(response.content)) as hdul:
        return hdul[0].data.copy()
