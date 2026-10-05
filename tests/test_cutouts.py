import pytest

from arandu import cutout_url


def test_cutout_url_prefers_the_url_published_in_an_alert():
    alert = {"alert_id": 12, "payload": {"cutouts": {"science": "https://example.test/a.fits"}}}
    assert cutout_url(alert, "science") == "https://example.test/a.fits"


def test_cutout_url_constructs_the_standard_url():
    assert cutout_url({"diaSourceId": 12}, "difference", base_url="https://broker/cutouts/") == (
        "https://broker/cutouts/12/difference.fits"
    )


def test_cutout_url_rejects_unknown_kinds():
    with pytest.raises(ValueError):
        cutout_url({"alert_id": 12}, "invalid")
