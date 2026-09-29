"""Simple geo helpers shared by the simulator and the streaming job.

The simulated fleet operates inside a fixed bounding box (roughly the
Colombo metro area) which is carved into a 3x3 grid of named zones. This
keeps zone assignment trivial and deterministic on both the producer side
(for realistic movement) and the Spark side (for enrichment), without
needing a real reverse-geocoding service.
"""

CITY_BOUNDS = {
    "min_lat": 6.83,
    "max_lat": 6.96,
    "min_lon": 79.83,
    "max_lon": 79.93,
}

_ZONE_NAMES = [
    ["zone_NW", "zone_N", "zone_NE"],
    ["zone_W", "zone_CENTRAL", "zone_E"],
    ["zone_SW", "zone_S", "zone_SE"],
]


def zone_for(lat: float, lon: float) -> str:
    """Bucket a lat/lon pair into one of 9 grid zones. Points outside the
    bounding box are clamped to the nearest edge zone rather than dropped,
    since GPS noise can occasionally push a point just outside the box."""
    lat_frac = (lat - CITY_BOUNDS["min_lat"]) / (CITY_BOUNDS["max_lat"] - CITY_BOUNDS["min_lat"])
    lon_frac = (lon - CITY_BOUNDS["min_lon"]) / (CITY_BOUNDS["max_lon"] - CITY_BOUNDS["min_lon"])
    lat_frac = min(max(lat_frac, 0.0), 0.999999)
    lon_frac = min(max(lon_frac, 0.0), 0.999999)

    row = 2 - int(lat_frac * 3)  # higher latitude -> "north" -> row 0
    col = int(lon_frac * 3)
    return _ZONE_NAMES[row][col]
