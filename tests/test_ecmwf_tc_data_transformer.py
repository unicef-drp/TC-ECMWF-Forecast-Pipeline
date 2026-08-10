import contextlib
import io
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from ecmwf_tc_data_transformer import transform_tc_data


LEGACY_STANDARD_COLUMNS = [
    "forecast_time",
    "track_id",
    "ensemble_member",
    "valid_time",
    "lead_time",
    "latitude",
    "longitude",
    "pressure_hpa",
    "wind_speed_knots",
    "radius_of_maximum_winds_km",
    "radius_34_knot_winds_ne_km",
    "radius_34_knot_winds_se_km",
    "radius_34_knot_winds_sw_km",
    "radius_34_knot_winds_nw_km",
    "radius_50_knot_winds_ne_km",
    "radius_50_knot_winds_se_km",
    "radius_50_knot_winds_sw_km",
    "radius_50_knot_winds_nw_km",
    "radius_64_knot_winds_ne_km",
    "radius_64_knot_winds_se_km",
    "radius_64_knot_winds_sw_km",
    "radius_64_knot_winds_nw_km",
    "wind_field_polygon_34kt",
    "wind_field_polygon_50kt",
    "wind_field_polygon_64kt",
]


class TransformIdentityRetentionTests(unittest.TestCase):
    def test_retains_raw_identifier_strings_without_pandas_inference(self):
        for storm_identifier in ("007", "  07-L  ", "００７"):
            with self.subTest(storm_identifier=storm_identifier):
                raw_rows = []
                for quadrant in range(1, 5):
                    raw_rows.append(
                        {
                            "storm_id": "BERYL",
                            "storm_identifier": storm_identifier,
                            "long_storm_name": "BERYL",
                            "ensemble_member": 1,
                            "step": 0,
                            "datetime": "2026-08-10 00:00:00",
                            "latitude": 15.0,
                            "longitude": -55.0,
                            "pressure": 99000.0,
                            "wlatitude": 15.1,
                            "wlongitude": -55.1,
                            "wind": 30.0,
                            "wind_threshold": 18,
                            "quadrant": quadrant,
                            "wind_radius": 40000.0,
                        }
                    )

                with tempfile.TemporaryDirectory() as temp_dir:
                    raw_path = Path(temp_dir) / "raw.csv"
                    output_base = Path(temp_dir) / "result"
                    pd.DataFrame(raw_rows).to_csv(raw_path, index=False)

                    normal_output = io.StringIO()
                    with contextlib.redirect_stdout(normal_output):
                        result = transform_tc_data(
                            str(raw_path),
                            str(output_base),
                            storm_name="FILENAME-NAME",
                            verbose=False,
                        )

                    self.assertTrue(result["success"])
                    transformed = pd.read_csv(
                        result["csv_file"],
                        converters={"storm_identifier": lambda value: value},
                    )

                self.assertEqual(len(transformed), 1)
                self.assertEqual(transformed.loc[0, "track_id"], "FILENAME-NAME")
                self.assertEqual(transformed.loc[0, "storm_identifier"], storm_identifier)
                self.assertEqual(transformed.loc[0, "long_storm_name"], "BERYL")
                self.assertNotIn(storm_identifier, normal_output.getvalue())

    def test_accepts_legacy_raw_csv_without_retained_identity_columns(self):
        raw_rows = []
        for quadrant in range(1, 5):
            raw_rows.append(
                {
                    "storm_id": "BERYL",
                    "ensemble_member": 1,
                    "step": 0,
                    "datetime": "2026-08-10 00:00:00",
                    "latitude": 15.0,
                    "longitude": -55.0,
                    "pressure": 99000.0,
                    "wlatitude": 15.1,
                    "wlongitude": -55.1,
                    "wind": 30.0,
                    "wind_threshold": 18,
                    "quadrant": quadrant,
                    "wind_radius": 40000.0,
                }
            )

        with tempfile.TemporaryDirectory() as temp_dir:
            raw_path = Path(temp_dir) / "legacy.csv"
            output_base = Path(temp_dir) / "result"
            pd.DataFrame(raw_rows).to_csv(raw_path, index=False)

            result = transform_tc_data(
                str(raw_path),
                str(output_base),
                storm_name="FILENAME-NAME",
                verbose=False,
            )

            self.assertTrue(result["success"])
            transformed = pd.read_csv(result["csv_file"])

        legacy_output = transformed[LEGACY_STANDARD_COLUMNS]
        self.assertEqual(list(legacy_output.columns), LEGACY_STANDARD_COLUMNS)
        self.assertEqual(len(legacy_output), 1)

        row = legacy_output.iloc[0]
        self.assertEqual(row["forecast_time"], "2026-08-10 00:00:00")
        self.assertEqual(row["track_id"], "FILENAME-NAME")
        self.assertEqual(row["ensemble_member"], 1)
        self.assertEqual(row["valid_time"], "2026-08-10 00:00:00")
        self.assertEqual(row["lead_time"], 0)
        self.assertEqual(row["latitude"], 15.0)
        self.assertEqual(row["longitude"], -55.0)
        self.assertEqual(row["pressure_hpa"], 990.0)
        self.assertAlmostEqual(row["wind_speed_knots"], 58.32)
        self.assertAlmostEqual(row["radius_of_maximum_winds_km"], 15.45799622485502)
        for column in LEGACY_STANDARD_COLUMNS[10:14]:
            self.assertEqual(row[column], 40.0)
        for column in LEGACY_STANDARD_COLUMNS[14:22]:
            self.assertTrue(pd.isna(row[column]))
        self.assertEqual(
            row["wind_field_polygon_34kt"],
            "POLYGON ((-55.37307249744507 14.63963963963964, "
            "-54.62692750255493 14.63963963963964, "
            "-54.62692750255493 15.36036036036036, "
            "-55.37307249744507 15.36036036036036, "
            "-55.37307249744507 14.63963963963964))",
        )
        self.assertTrue(pd.isna(row["wind_field_polygon_50kt"]))
        self.assertTrue(pd.isna(row["wind_field_polygon_64kt"]))
        self.assertTrue(transformed["storm_identifier"].isna().all())
        self.assertTrue(transformed["long_storm_name"].isna().all())


if __name__ == "__main__":
    unittest.main()
