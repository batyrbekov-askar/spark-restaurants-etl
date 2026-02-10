from src.enrich_geocode import is_invalid_lat_lon


def test_invalid_when_null():
    assert is_invalid_lat_lon(None, 10) is True
    assert is_invalid_lat_lon(10, None) is True


def test_invalid_when_out_of_range():
    assert is_invalid_lat_lon(100, 10) is True
    assert is_invalid_lat_lon(10, 200) is True


def test_valid():
    assert is_invalid_lat_lon(45, 80) is False
