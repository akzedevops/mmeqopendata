import json
from pathlib import Path

import pytest

from mmeq.analysis.dam_risk import estimate_pga_ask08

REFERENCE = json.loads((Path(__file__).parent / "fixtures/as08_openquake.json").read_text())


@pytest.mark.parametrize("case", REFERENCE["cases"])
def test_vertical_strike_slip_pga_matches_openquake(case):
    actual = estimate_pga_ask08(case["mag"], case["rrup_km"], vs30=case["vs30"])
    assert actual == pytest.approx(case["pga_g"], rel=1e-10)
