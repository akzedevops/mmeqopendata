"""Population exposure estimation using WorldPop raster."""
import logging
import math
import os
from functools import lru_cache
from typing import Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_RASTER = os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "data", "worldpop", "mmr_pop_2020_1km.tif"
)
DEFAULT_DAMS = os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "data", "myanmar_dams.geojson"
)


@lru_cache(maxsize=1)
def _load_raster(raster_path: str) -> Optional[Tuple[np.ndarray, object]]:
    """Load raster once and cache. Returns (data, transform) or None."""
    try:
        import rasterio
        with rasterio.open(raster_path) as src:
            data = src.read(1).astype(float)
            nodata = src.nodata
            transform = src.transform
        if nodata is not None:
            data[data == nodata] = np.nan
        data[data < 0] = np.nan
        return data, transform
    except Exception as e:
        logger.warning("Could not load raster %s: %s", raster_path, e)
        return None


def population_within_radius(lat: float, lon: float, radius_km: float = 50.0, raster_path: str = None) -> float:
    """Sum population within radius_km of (lat, lon) using the WorldPop raster.
    Returns total population count. Uses rasterio to read the GeoTIFF.
    If raster not found, returns 0.0 with a warning.
    """
    path = os.path.normpath(raster_path or DEFAULT_RASTER)
    result = _load_raster(path)
    if result is None:
        return 0.0

    data, transform = result
    nrows, ncols = data.shape

    # Pixel size in degrees
    px_lon = transform.a  # width
    px_lat = abs(transform.e)  # height (negative in north-up rasters)

    # Bounding box in pixel coords for the radius
    deg_lat = radius_km / 111.0
    deg_lon = radius_km / (111.0 * math.cos(math.radians(lat)))

    # Origin (top-left corner) of raster
    origin_lon = transform.c
    origin_lat = transform.f

    # Row/col of target point
    col_c = (lon - origin_lon) / px_lon
    row_c = (lat - origin_lat) / (-px_lat)

    # Window bounds (clamped)
    row_min = max(0, int(row_c - deg_lat / px_lat) - 1)
    row_max = min(nrows - 1, int(row_c + deg_lat / px_lat) + 1)
    col_min = max(0, int(col_c - deg_lon / px_lon) - 1)
    col_max = min(ncols - 1, int(col_c + deg_lon / px_lon) + 1)

    if row_min > row_max or col_min > col_max:
        return 0.0

    # Pixel center coordinates for the window
    rows = np.arange(row_min, row_max + 1)
    cols = np.arange(col_min, col_max + 1)
    lats = origin_lat - (rows + 0.5) * px_lat
    lons = origin_lon + (cols + 0.5) * px_lon

    lon_grid, lat_grid = np.meshgrid(lons, lats)
    dlat = (lat_grid - lat) * 111.0
    dlon = (lon_grid - lon) * 111.0 * math.cos(math.radians(lat))
    dist = np.sqrt(dlat**2 + dlon**2)

    window = data[row_min:row_max + 1, col_min:col_max + 1]
    mask = (dist <= radius_km) & ~np.isnan(window)
    return float(np.sum(window[mask]))


def population_exposure_at_dams(dam_df: pd.DataFrame = None, radius_km: float = 50.0) -> pd.DataFrame:
    """Compute population exposure for all dams. Returns DataFrame with name, lat, lon, pop_50km.
    If dam_df not provided, loads from myanmar_dams.geojson.
    """
    if dam_df is None:
        import geopandas as gpd
        path = os.path.normpath(DEFAULT_DAMS)
        gdf = gpd.read_file(path)
        dam_df = pd.DataFrame({
            "name": gdf.get("Name", gdf.get("name", gdf.index.astype(str))),
            "lat": gdf.geometry.y,
            "lon": gdf.geometry.x,
        })

    records = []
    for _, row in dam_df.iterrows():
        pop = population_within_radius(row["lat"], row["lon"], radius_km)
        records.append({"name": row.get("name", ""), "lat": row["lat"], "lon": row["lon"], "pop_50km": pop})

    return pd.DataFrame(records)
