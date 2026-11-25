"""
Tests for the storm surge step (ecmwf_surge_envelope_extractor.py).

TestBathtubSurge: the surge formula. test_matches_climada checks that the
pipeline's numpy re-implementation gives the same surge heights as CLIMADA
Petals TCSurgeBathtub.from_tc_winds. It needs climada + climada_petals (dev
only, not a pipeline dependency) and is skipped when they are not installed.

TestSurgeRaster: the output table, on a small synthetic elevation cache and
synthetic wind (no GRIB file, no download): one column per member, maximum over
lead times, only the flooded cells listed.

    python -m pytest tests/test_surge_bathtub.py -v
"""

import importlib.util
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import rasterio
import xarray as xr
from rasterio.transform import from_origin

import ecmwf_surge_envelope_extractor as surge
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


class TestSurgeRaster(unittest.TestCase):
    """End-to-end on synthetic inputs: elevation cache -> surge -> sparse Parquet table."""

    RES = 15 / 3600                      # 15" grid, as the real cache
    WEST, NORTH = -78.0, 18.5            # small 40 x 40 cell area
    N = 40
    FT = pd.Timestamp("2025-10-28 00:00")
    LEADS = (0, 3, 6)
    WIND_BY_LEAD = {0: 30.0, 3: 50.0, 6: 20.0}   # m/s, uniform; the maximum is at lead 3

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        tmp = Path(self.tmp.name)

        # Elevation cache: a 10 x 20 block of coastal cells, NaN elsewhere
        elev = np.full((self.N, self.N), np.nan, dtype=np.float32)
        dist = np.full((self.N, self.N), np.nan, dtype=np.float32)
        elev[10:20, 10:30] = np.linspace(0.0, 9.0, 20, dtype=np.float32)   # 0 to 9 m, west to east
        dist[10:20, 10:30] = 1.0
        self.dem = tmp / "surge_coastal_dem.tif"
        with rasterio.open(self.dem, "w", driver="GTiff", width=self.N, height=self.N, count=2,
                           dtype="float32", crs="EPSG:4326", nodata=np.nan,
                           transform=from_origin(self.WEST, self.NORTH, self.RES, self.RES)) as dst:
            dst.write(elev, 1)
            dst.write(dist, 2)
            dst.update_tags(elevation_source="synthetic")
        self.elev_block = elev[10:20, 10:30]

        # Track: 51 members around the area (only its extent is used here)
        rows = [dict(forecast_time=self.FT, valid_time=self.FT + pd.Timedelta(hours=lt), lead_time=lt,
                     ensemble_member=m, latitude=18.4, longitude=-77.9)
                for m in range(1, 52) for lt in self.LEADS]
        self.track = tmp / "TEST_transformed.csv"
        pd.DataFrame(rows).to_csv(self.track, index=False)
        self.wind_files = [Path(f"wind_ens_2025-10-28_r00_f{lt:03d}h_{t}.grib2")
                           for lt in self.LEADS for t in ("pf", "cf")]
        self.out = tmp / "out"
        self.out.mkdir()

        # Synthetic wind instead of the GRIB reader: uniform per lead time,
        # perturbed member k gets +0.1*k m/s, the control none
        lat = np.arange(25.0, 10.0, -0.25)
        lon = np.arange(-85.0, -70.0, 0.25)

        def fake_load(path, bbox):
            lead = int(Path(path).name.split("_f")[1][:3])
            base = self.WIND_BY_LEAD[lead]
            if path.endswith("_pf.grib2"):
                data = np.stack([np.full((lat.size, lon.size), base + 0.1 * k, dtype=np.float32)
                                 for k in range(1, 51)])
                da = xr.DataArray(data, dims=("number", "latitude", "longitude"),
                                  coords={"number": np.arange(1, 51), "latitude": lat, "longitude": lon})
            else:
                da = xr.DataArray(np.full((lat.size, lon.size), base, dtype=np.float32),
                                  dims=("latitude", "longitude"), coords={"latitude": lat, "longitude": lon})
            return [da.sel(latitude=slice(bbox["lat_max"], bbox["lat_min"]),
                           longitude=slice(bbox["lon_min"], bbox["lon_max"]))]

        self._original_load = surge.load_wind_data_all_members
        surge.load_wind_data_all_members = fake_load

    def tearDown(self):
        surge.load_wind_data_all_members = self._original_load
        self.tmp.cleanup()

    def _run(self):
        files = surge._process_storm("TEST", str(self.track), self.wind_files, self.dem, self.out, verbose=False)
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0].name, "TEST_20251028T00Z_surge.parquet")
        return files[0]

    def _expected_surge(self, wind):
        """Surge of one member over the 10 x 20 coastal block, as a (10, 20) array."""
        return bathtub_surge(np.full(self.elev_block.size, wind), self.elev_block.ravel().astype(float),
                             np.ones(self.elev_block.size)).reshape(self.elev_block.shape)

    def test_columns_members_and_metadata(self):
        path = self._run()
        df = pd.read_parquet(path)
        self.assertEqual(list(df.columns), ["cell_row", "cell_col", "longitude", "latitude"]
                         + [f"member_{m}" for m in range(1, 52)])
        self.assertTrue(all(df[f"member_{m}"].dtype == np.float32 for m in range(1, 52)))
        meta = {k.decode(): v.decode() for k, v in pq.read_schema(path).metadata.items()}
        self.assertEqual(meta["member_numbers"], ",".join(str(m) for m in range(1, 52)))
        self.assertEqual(meta["units"], "m")
        self.assertEqual(meta["lead_times_h"], "0-6")
        self.assertEqual(meta["grid_crs"], "EPSG:4326")
        self.assertEqual((meta["grid_height"], meta["grid_width"]), (str(self.N), str(self.N)))
        # Cell index and centre coordinates are on the cache grid
        row, col = df["cell_row"].to_numpy(), df["cell_col"].to_numpy()
        np.testing.assert_allclose(df["longitude"], self.WEST + (col + 0.5) * self.RES)
        np.testing.assert_allclose(df["latitude"], self.NORTH - (row + 0.5) * self.RES)

    def test_values_are_the_maximum_over_lead_times(self):
        df = pd.read_parquet(self._run())
        for member, wind in ((1, 50.1), (50, 55.0), (51, 50.0)):     # member 1, member 50, control
            expected = self._expected_surge(wind)
            got = np.zeros_like(expected)                            # cells not listed are 0 for this member
            got[df["cell_row"] - 10, df["cell_col"] - 10] = df[f"member_{member}"]
            np.testing.assert_allclose(got, expected, atol=1e-4)

    def test_only_flooded_cells_are_listed(self):
        df = pd.read_parquet(self._run())
        flooded_by_any = self._expected_surge(55.0) > 0              # member 50 has the highest wind
        self.assertEqual(len(df), int(flooded_by_any.sum()))
        self.assertTrue(0 < len(df) < self.elev_block.size)
        self.assertFalse((df["cell_col"] == 29).any())               # the 9 m cells stay dry: not listed
        self.assertTrue((df[[f"member_{m}" for m in range(1, 52)]].max(axis=1) > 0).all())

    def test_cell_removed_from_the_cache_is_not_listed(self):
        # Remove the lowest coastal cell (elevation 0 m, certainly flooded) from the cache
        with rasterio.open(self.dem, "r+") as dst:
            elev = dst.read(1)
            elev[10, 10] = np.nan
            dst.write(elev, 1)
        df = pd.read_parquet(self._run())
        self.assertFalse(((df["cell_row"] == 10) & (df["cell_col"] == 10)).any())
        self.assertTrue(((df["cell_row"] == 11) & (df["cell_col"] == 10)).any())


if __name__ == "__main__":
    unittest.main()
