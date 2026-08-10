import unittest

from ecmwf_tc_lineage import (
    PRODUCER,
    build_storm_identity_fields,
    canonical_episode_key,
    normalize_storm_identifier,
)


class StormIdentityFieldsTests(unittest.TestCase):
    def test_retains_raw_identifier_separately_from_long_name(self):
        fields = build_storm_identity_fields("  raw-id  ", "  Beryl  ")

        self.assertEqual(fields["storm_identifier"], "  raw-id  ")
        self.assertEqual(fields["long_storm_name"], "Beryl")
        self.assertEqual(fields["storm_id"], "Beryl")

    def test_preserves_storm_id_fallback_to_raw_identifier(self):
        fields = build_storm_identity_fields("07L", "   ")

        self.assertEqual(fields["storm_identifier"], "07L")
        self.assertEqual(fields["long_storm_name"], "")
        self.assertEqual(fields["storm_id"], "07L")


class StormIdentifierNormalizationTests(unittest.TestCase):
    def test_nfkc_trims_and_uppercases_ascii(self):
        self.assertEqual(normalize_storm_identifier("  ０7-l  "), "07-L")

    def test_accepts_valid_identifier_shapes(self):
        for value in ("AL09", "07-L", "A1-B2-C3"):
            with self.subTest(value=value):
                self.assertEqual(normalize_storm_identifier(value), value)

    def test_rejects_missing_non_ascii_control_and_invalid_values(self):
        invalid = (
            None,
            "",
            "   ",
            "José",
            "AL\n09",
            "AL 09",
            "-AL09",
            "AL09-",
            "AL--09",
            "AL_09",
        )
        for value in invalid:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    normalize_storm_identifier(value)


class CanonicalEpisodeKeyTests(unittest.TestCase):
    def test_accepts_each_explicit_supported_basin(self):
        for basin in ("AL", "EP", "CP"):
            with self.subTest(basin=basin):
                key = canonical_episode_key(
                    basin=basin,
                    season=2026,
                    storm_identifier="07L",
                )
                self.assertEqual(key[1], basin)

    def test_builds_fixed_producer_tuple_without_name_or_forecast_time(self):
        key = canonical_episode_key(
            basin="AL",
            season=2026,
            storm_identifier="  07-l ",
        )

        self.assertEqual(key, (PRODUCER, "AL", 2026, "07-L"))
        self.assertEqual(len(key), 4)

    def test_requires_supported_explicit_basin(self):
        for basin in (None, "", "al", "WP"):
            with self.subTest(basin=basin):
                with self.assertRaises(ValueError):
                    canonical_episode_key(
                        basin=basin,
                        season=2026,
                        storm_identifier="07L",
                    )

    def test_requires_explicit_integer_season(self):
        for season in (None, "2026", 2026.0, True):
            with self.subTest(season=season):
                with self.assertRaises(ValueError):
                    canonical_episode_key(
                        basin="AL",
                        season=season,
                        storm_identifier="07L",
                    )

    def test_rejects_season_outside_supported_four_digit_range(self):
        for season in (1999, 10000):
            with self.subTest(season=season):
                with self.assertRaises(ValueError):
                    canonical_episode_key(
                        basin="AL",
                        season=season,
                        storm_identifier="07L",
                    )

    def test_accepts_supported_season_boundaries(self):
        for season in (2000, 9999):
            with self.subTest(season=season):
                key = canonical_episode_key(
                    basin="AL",
                    season=season,
                    storm_identifier="07L",
                )
                self.assertEqual(key[2], season)


if __name__ == "__main__":
    unittest.main()
