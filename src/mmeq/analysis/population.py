import logging
import math
import os
from typing import Optional

import numpy as np
import pandas as pd
import json

logger = logging.getLogger(__name__)


def estimate_population_exposure(
    eq_df: pd.DataFrame,
    dams_df: Optional[pd.DataFrame] = None,
    radius_km: float = 50,
) -> pd.DataFrame:
    pop_path = os.path.join(os.getcwd(), "myanmar_population.csv")
    if os.path.exists(pop_path):
        pop_df = pd.read_csv(pop_path)
        logger.info(f"Loaded population data: {len(pop_df)} points")
    else:
        logger.info("No population data file found, using city-based estimates")
        pop_df = _build_city_population()

    major = eq_df.nlargest(1, "mag").iloc[0]
    results = []

    if dams_df is not None and not dams_df.empty:
        targets = dams_df
    else:
        top_quakes = eq_df.nlargest(20, "mag")
        targets = top_quakes.rename(columns={"latitude": "latitude", "longitude": "longitude"})
        targets = targets.copy()
        targets["name"] = "M" + targets["mag"].astype(str)

    for _, target in targets.iterrows():
        tlat, tlon = target["latitude"], target["longitude"]
        pop_within = 0
        for _, pop in pop_df.iterrows():
            dlat = pop["latitude"] - tlat
            dlon = (pop["longitude"] - tlon) * math.cos(math.radians(tlat))
            dist = math.sqrt(dlat ** 2 + dlon ** 2) * 111.0
            if dist <= radius_km:
                pop_within += pop.get("population", 0)

        results.append({
            "name": target.get("name", "?"),
            "latitude": tlat,
            "longitude": tlon,
            f"population_within_{int(radius_km)}km": int(pop_within),
        })

    exposure_df = pd.DataFrame(results).sort_values(
        f"population_within_{int(radius_km)}km", ascending=False
    ).reset_index(drop=True)
    logger.info(f"Population exposure computed for {len(targets)} targets")
    return exposure_df


def _build_city_population() -> pd.DataFrame:
    cities = [
        {"city": "Yangon", "latitude": 16.866, "longitude": 96.195, "population": 5350000},
        {"city": "Mandalay", "latitude": 21.975, "longitude": 96.084, "population": 1460000},
        {"city": "Naypyidaw", "latitude": 19.763, "longitude": 96.078, "population": 1160000},
        {"city": "Mawlamyine", "latitude": 16.491, "longitude": 97.630, "population": 456000},
        {"city": "Bago", "latitude": 17.335, "longitude": 96.481, "population": 425000},
        {"city": "Pathein", "latitude": 16.779, "longitude": 94.732, "population": 315000},
        {"city": "Taunggyi", "latitude": 20.789, "longitude": 97.038, "population": 381000},
        {"city": "Sittwe", "latitude": 20.146, "longitude": 92.900, "population": 201000},
        {"city": "Myitkyina", "latitude": 25.383, "longitude": 97.397, "population": 306000},
        {"city": "Hpa-an", "latitude": 16.883, "longitude": 97.633, "population": 267000},
        {"city": "Monywa", "latitude": 22.109, "longitude": 95.133, "population": 312000},
        {"city": "Meiktila", "latitude": 20.879, "longitude": 95.855, "population": 211000},
        {"city": "Sagaing", "latitude": 21.979, "longitude": 95.979, "population": 307000},
        {"city": "Dawei", "latitude": 14.089, "longitude": 98.197, "population": 201000},
        {"city": "Magway", "latitude": 20.149, "longitude": 94.933, "population": 147000},
        {"city": "Pakokku", "latitude": 21.338, "longitude": 95.098, "population": 197000},
        {"city": "Lashio", "latitude": 22.972, "longitude": 97.749, "population": 173000},
        {"city": "Pyay", "latitude": 18.825, "longitude": 95.217, "population": 155000},
        {"city": "Hinthada", "latitude": 17.642, "longitude": 95.460, "population": 178000},
        {"city": "Myeik", "latitude": 12.437, "longitude": 98.595, "population": 126000},
        {"city": "Kalewa", "latitude": 23.187, "longitude": 94.288, "population": 32000},
        {"city": "Katha", "latitude": 24.168, "longitude": 96.316, "population": 45000},
        {"city": "Kengtung", "latitude": 21.300, "longitude": 99.600, "population": 95000},
        {"city": "Pyin Oo Lwin", "latitude": 22.037, "longitude": 96.468, "population": 166000},
        {"city": "Bhamo", "latitude": 24.266, "longitude": 97.234, "population": 59000},
        {"city": "Hakha", "latitude": 22.648, "longitude": 93.594, "population": 32000},
        {"city": "Loikaw", "latitude": 19.679, "longitude": 97.210, "population": 71000},
        {"city": "Putao", "latitude": 27.375, "longitude": 97.840, "population": 25000},
    ]
    return pd.DataFrame(cities)
