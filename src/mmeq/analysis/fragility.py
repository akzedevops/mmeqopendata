"""
Dam fragility curves for seismic risk assessment.

Implements log-normal fragility functions for different dam types and damage states,
following HAZUS (FEMA 2003) and Zhang et al. (2009) methodologies.

Damage states:
    DS0: None (no damage)
    DS1: Slight/Slight
    DS2: Moderate
    DS3: Extensive
    DS4: Complete (failure)

References:
    FEMA (2003). HAZUS-MH Technical Manual, Chapter 8: Earthquake Loss Estimation
    Zhang, L. M., Xu, Y., & Jia, J. S. (2009). Analysis of the risk of dam failure
    from earthquakes. Proc. ICOLD 23rd Congress.
    Tekie, P. B., & Ellingwood, B. R. (2003). Seismic fragility assessment of
    concrete gravity dams. Earthquake Eng. Struct. Dyn., 32(14), 2221-2240.
"""

import logging
import math
import os
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy.stats import norm

logger = logging.getLogger(__name__)

FRAGILITY_PARAMS = {
    "earthfill": {
        "DS1": {"median": 0.15, "beta": 0.60},
        "DS2": {"median": 0.35, "beta": 0.55},
        "DS3": {"median": 0.60, "beta": 0.50},
        "DS4": {"median": 1.00, "beta": 0.50},
    },
    "concrete": {
        "DS1": {"median": 0.20, "beta": 0.55},
        "DS2": {"median": 0.45, "beta": 0.50},
        "DS3": {"median": 0.75, "beta": 0.50},
        "DS4": {"median": 1.20, "beta": 0.50},
    },
    "rockfill": {
        "DS1": {"median": 0.18, "beta": 0.60},
        "DS2": {"median": 0.40, "beta": 0.55},
        "DS3": {"median": 0.70, "beta": 0.50},
        "DS4": {"median": 1.10, "beta": 0.50},
    },
    "default": {
        "DS1": {"median": 0.17, "beta": 0.60},
        "DS2": {"median": 0.38, "beta": 0.55},
        "DS3": {"median": 0.65, "beta": 0.50},
        "DS4": {"median": 1.05, "beta": 0.50},
    },
}

DAMAGE_STATE_LABELS = {
    "DS0": "None",
    "DS1": "Slight",
    "DS2": "Moderate",
    "DS3": "Extensive",
    "DS4": "Complete",
}

LOSS_RATIOS = {
    "DS0": 0.0,
    "DS1": 0.05,
    "DS2": 0.20,
    "DS3": 0.55,
    "DS4": 1.0,
}


def classify_dam_type(dam_props: dict) -> str:
    """
    Classify dam into fragility type category based on properties.
    """
    func = str(dam_props.get("function", "")).lower()
    name = str(dam_props.get("name", "")).lower()

    if "concrete" in func or "gravity" in func or "buttress" in func or "arch" in func:
        return "concrete"
    if "rockfill" in func or "rock" in func:
        return "rockfill"
    if "earth" in func or "embankment" in func or "fill" in func or "earthfill" in func:
        return "earthfill"

    return "earthfill"


def fragility_probability(pga_g: float, dam_type: str = "default") -> Dict[str, float]:
    """
    Compute probability of exceedance for each damage state given PGA.

    Uses log-normal fragility function: P(DS>=ds | PGA) = Phi(ln(PGA/median)/beta)

    Parameters
    ----------
    pga_g : float - Peak Ground Acceleration in g
    dam_type : str - Dam type category ('earthfill', 'concrete', 'rockfill', 'default')

    Returns
    -------
    dict with keys DS1-DS4, values are exceedance probabilities
    """
    if pga_g <= 0:
        return {"DS1": 0.0, "DS2": 0.0, "DS3": 0.0, "DS4": 0.0}

    params = FRAGILITY_PARAMS.get(dam_type, FRAGILITY_PARAMS["default"])

    probs = {}
    for ds in ["DS1", "DS2", "DS3", "DS4"]:
        median = params[ds]["median"]
        beta = params[ds]["beta"]
        if pga_g > 0 and median > 0:
            z = math.log(pga_g / median) / beta
            probs[ds] = float(norm.cdf(z))
        else:
            probs[ds] = 0.0

    return probs


def damage_state_probabilities(pga_g: float, dam_type: str = "default") -> Dict[str, float]:
    """
    Compute discrete probability of being in each damage state.

    P(DS=i) = P(DS>=i) - P(DS>=i+1)
    """
    exceed = fragility_probability(pga_g, dam_type)

    p = {"DS0": 1.0 - exceed["DS1"]}
    p["DS1"] = exceed["DS1"] - exceed["DS2"]
    p["DS2"] = exceed["DS2"] - exceed["DS3"]
    p["DS3"] = exceed["DS3"] - exceed["DS4"]
    p["DS4"] = exceed["DS4"]

    for key in p:
        p[key] = max(0.0, min(1.0, p[key]))

    return p


def expected_loss_ratio(pga_g: float, dam_type: str = "default") -> float:
    """
    Compute expected loss ratio given PGA and dam type.
    E[LR] = sum(P(DS=i) * LR(i))
    """
    probs = damage_state_probabilities(pga_g, dam_type)
    elr = sum(probs[ds] * LOSS_RATIOS[ds] for ds in ["DS0", "DS1", "DS2", "DS3", "DS4"])
    return elr


def most_likely_damage_state(pga_g: float, dam_type: str = "default") -> str:
    """Return the most likely damage state given PGA."""
    probs = damage_state_probabilities(pga_g, dam_type)
    return max(probs, key=probs.get)


def compute_dam_fragilities(
    dam_risk_df=None,
) -> List[dict]:
    """
    Compute fragility analysis for all dams.

    Parameters
    ----------
    dam_risk_df : optional DataFrame with dam risk data (uses dam_risk module if not provided)

    Returns
    -------
    list of dicts with fragility analysis results per dam
    """
    import json
    import pandas as pd

    if dam_risk_df is not None and hasattr(dam_risk_df, "iterrows"):
        pass
    else:
        risk_path = os.path.join(os.getcwd(), "report", "dam_risk_scores.csv")
        if os.path.exists(risk_path):
            dam_risk_df = pd.read_csv(risk_path)
        else:
            logger.warning("No dam risk data available for fragility analysis")
            return []

    results = []
    for _, row in dam_risk_df.iterrows():
        pga_g = float(row.get("pga_g", 0))
        dam_type = classify_dam_type(row.to_dict() if hasattr(row, "to_dict") else {})

        probs = damage_state_probabilities(pga_g, dam_type)
        elr = expected_loss_ratio(pga_g, dam_type)
        ml_ds = most_likely_damage_state(pga_g, dam_type)

        results.append({
            "name": row.get("name", "Unknown"),
            "latitude": row.get("latitude", 0),
            "longitude": row.get("longitude", 0),
            "dam_type": dam_type,
            "pga_g": round(pga_g, 4),
            "p_none": round(probs["DS0"], 4),
            "p_slight": round(probs["DS1"], 4),
            "p_moderate": round(probs["DS2"], 4),
            "p_extensive": round(probs["DS3"], 4),
            "p_complete": round(probs["DS4"], 4),
            "expected_loss_ratio": round(elr, 4),
            "most_likely_damage": DAMAGE_STATE_LABELS[ml_ds],
        })

    n_complete = sum(1 for r in results if r["most_likely_damage"] == "Complete")
    n_extensive = sum(1 for r in results if r["most_likely_damage"] == "Extensive")
    n_moderate = sum(1 for r in results if r["most_likely_damage"] == "Moderate")
    n_slight = sum(1 for r in results if r["most_likely_damage"] == "Slight")
    n_none = sum(1 for r in results if r["most_likely_damage"] == "None")

    logger.info(
        f"Fragility: {n_none} None, {n_slight} Slight, {n_moderate} Moderate, "
        f"{n_extensive} Extensive, {n_complete} Complete"
    )

    return results


def generate_fragility_curves(
    dam_type: str = "earthfill",
    pga_range: np.ndarray = None,
) -> pd.DataFrame:
    """
    Generate fragility curve data for plotting.

    Returns DataFrame with pga_g and exceedance probabilities for each damage state.
    """
    import pandas as pd

    if pga_range is None:
        pga_range = np.linspace(0.01, 2.0, 200)

    params = FRAGILITY_PARAMS.get(dam_type, FRAGILITY_PARAMS["default"])

    data = {"pga_g": pga_range}
    for ds in ["DS1", "DS2", "DS3", "DS4"]:
        median = params[ds]["median"]
        beta = params[ds]["beta"]
        probs = norm.cdf(np.log(pga_range / median) / beta)
        data[f"P({DAMAGE_STATE_LABELS[ds]})"] = probs

    return pd.DataFrame(data)
