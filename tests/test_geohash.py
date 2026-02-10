from src.enrich_geohash import _geohash


def test_geohash_precision_4():
    gh = _geohash(43.238949, 76.889709, 4)  # Алматы примерно
    assert gh is not None
    assert len(gh) == 4
