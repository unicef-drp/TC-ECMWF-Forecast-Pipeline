"""
Tests for the storm surge formula (ecmwf_surge_envelope_extractor.bathtub_surge).

test_matches_climada checks that the pipeline's numpy re-implementation gives
the same surge heights as CLIMADA Petals TCSurgeBathtub.from_tc_winds. It needs
climada + climada_petals (dev only, not a pipeline dependency) and is skipped
when they are not installed.

    python -m pytest tests/test_surge_bathtub.py -v
"""

import importlib.util
import tempfile
import unittest
from pathlib import Path

import numpy as np

from ecmwf_surge_envelope_extractor import bathtub_surge

HAS_CLIMADA = all(importlib.util.find_spec(m) is not None for m in ("climada", "climada_petals"))


class TestBathtubSurge(unittest.TestCase):

    def test_hand_computed_values(self):
        # V = 50 m/s -> 0.1023 * (50 - 26.8224) + 1.8288 = 4.19987 m of water at the coast
        wind = np.array([50.0, 50.0, 50.0, 20.0, 0.0, 50.0, 50.0])
        elev = np.array([0.0, 1.0, 1.0, 0.0, 0.0, -0.5, 11.0])
        dist = np.array([0.0, 0.0, 5.0, 0.0, 0.0, 0.0, 0.0])
        expected = [4.19987, 3.19987, 2.19987,  # coast, 1 m high, 1 m high + 5 km inland
                    1.8288,                     # weak wind: base value only
                    0.0,                        # no wind
                    0.0, 0.0]                   # below 0 m and above 10 m: excluded (CLIMADA rules)
        np.testing.assert_allclose(bathtub_surge(wind, elev, dist), expected, atol=1e-4)

    def test_never_negative_and_vectorized_over_members(self):
        rng = np.random.default_rng(0)
        wind = rng.uniform(0, 80, size=(51, 1000))
        elev = rng.uniform(-2, 12, size=1000)
        dist = rng.uniform(0, 60, size=1000)
        surge = bathtub_surge(wind, elev, dist)
        self.assertEqual(surge.shape, (51, 1000))
        self.assertTrue((surge >= 0).all())
        self.assertTrue((surge[:, dist > 50] == 0).all())

    @unittest.skipUnless(HAS_CLIMADA, "climada / climada_petals not installed")
    def test_matches_climada(self):
        import rasterio
        from rasterio.transform import from_origin
        from scipy.sparse import csr_matrix
        from climada.hazard import Centroids, Hazard
        import climada_petals.hazard.tc_surge_bathtub as tsb

        rng = np.random.default_rng(42)
        ny, nx, res = 30, 40, 0.01
        west, north = -77.5, 18.3
        elev = rng.uniform(-1.5, 12.0, size=(ny, nx)).astype("float32")

        rows, cols = np.mgrid[0:ny, 0:nx]
        lat = (north - (rows + 0.5) * res).ravel()   # centroids exactly at DEM pixel centres
        lon = (west + (cols + 0.5) * res).ravel()
        dist_km = rng.uniform(0.0, 60.0, size=lat.size)

        wind = rng.uniform(0.0, 75.0, size=(51, lat.size))
        wind[wind < 17.49] = 0.0                     # as in the pipeline

        centroids = Centroids(lat=lat, lon=lon)
        # CLIMADA convention: negative distance on land, in metres
        centroids.get_dist_coast = (lambda signed=False, precomputed=True:
                                    -dist_km * 1000 if signed else dist_km * 1000)
        haz = Hazard(haz_type="TC", intensity=csr_matrix(wind), centroids=centroids, units="m/s",
                     event_id=np.arange(1, 52), event_name=[str(i) for i in range(1, 52)],
                     frequency=np.full(51, 1 / 51), date=np.ones(51), orig=np.ones(51, dtype=bool))

        original_fraction = tsb._fraction_on_land
        tsb._fraction_on_land = lambda c, t: np.ones(c.size)  # land fraction is not compared
        try:
            with tempfile.TemporaryDirectory() as tmp:
                dem = Path(tmp) / "dem.tif"
                with rasterio.open(dem, "w", driver="GTiff", width=nx, height=ny, count=1,
                                   dtype="float32", crs="EPSG:4326",
                                   transform=from_origin(west, north, res, res)) as dst:
                    dst.write(elev, 1)
                climada_surge = tsb.TCSurgeBathtub.from_tc_winds(haz, str(dem)).intensity.toarray()
        finally:
            tsb._fraction_on_land = original_fraction

        ours = bathtub_surge(wind, elev.ravel().astype(np.float64), dist_km)
        np.testing.assert_allclose(ours, climada_surge, atol=1e-5)
        self.assertGreater((ours > 0).sum(), 1000)  # really exercises non-zero surge


if __name__ == "__main__":
    unittest.main()
