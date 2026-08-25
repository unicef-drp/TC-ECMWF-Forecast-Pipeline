"""ECMWF tropical-cyclone identity retention and lineage helpers."""

import re
import unicodedata
from typing import Any, Dict, Optional, Tuple


PRODUCER = "ecmwf-ifs-tc-track"
SUPPORTED_BASINS = frozenset({"AL", "EP", "CP"})
_STORM_IDENTIFIER_PATTERN = re.compile(r"^[A-Z0-9]+(?:-[A-Z0-9]+)*$")


def build_storm_identity_fields(
    storm_identifier: Any,
    long_storm_name: Optional[str],
) -> Dict[str, Any]:
    """Retain provider fields while preserving the existing storm_id fallback."""
    stripped_long_name = long_storm_name.strip() if long_storm_name else ""
    return {
        "storm_id": stripped_long_name or storm_identifier,
        "storm_identifier": storm_identifier,
        "long_storm_name": stripped_long_name,
    }


def normalize_storm_identifier(storm_identifier: Any) -> str:
    """Normalize a raw provider identifier for provisional lineage use."""
    if not isinstance(storm_identifier, str):
        raise ValueError("storm_identifier must be a non-missing string")

    normalized = unicodedata.normalize("NFKC", storm_identifier)
    if any(unicodedata.category(char) == "Cc" for char in normalized):
        raise ValueError("storm_identifier must not contain control characters")

    normalized = normalized.strip()
    if not normalized:
        raise ValueError("storm_identifier must not be empty")
    if not normalized.isascii():
        raise ValueError("storm_identifier must contain ASCII characters only")

    normalized = normalized.upper()
    if not _STORM_IDENTIFIER_PATTERN.fullmatch(normalized):
        raise ValueError("storm_identifier must match ^[A-Z0-9]+(?:-[A-Z0-9]+)*$")
    return normalized


def canonical_episode_key(
    *,
    basin: str,
    season: int,
    storm_identifier: Any,
) -> Tuple[str, str, int, str]:
    """Build the provisional producer lineage tuple, not an episode ID."""
    if basin not in SUPPORTED_BASINS:
        raise ValueError("basin must be explicitly provided as AL, EP, or CP")
    if type(season) is not int:
        raise ValueError("season must be explicitly provided as an int")
    if not 2000 <= season <= 9999:
        raise ValueError("season must be between 2000 and 9999")

    return (
        PRODUCER,
        basin,
        season,
        normalize_storm_identifier(storm_identifier),
    )
