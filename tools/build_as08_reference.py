"""Regenerate the optional OpenQuake reference fixture in a separate environment."""
import argparse
from importlib.metadata import version
import itertools
import json
from pathlib import Path

import numpy as np


def main():
    from openquake.hazardlib.gsim.abrahamson_silva_2008 import AbrahamsonSilva2008
    from openquake.hazardlib.imt import PGA

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="tests/fixtures/as08_openquake.json")
    args = parser.parse_args()
    combinations = list(itertools.product(
        [5.0, 6.5, 7.7, 8.0], [1.0, 10.0, 50.0, 100.0, 200.0], [180.0, 310.0, 760.0, 1100.0],
    ))
    fields = ["mag", "rake", "dip", "ztor", "width", "rrup", "rjb", "rx", "vs30", "z1pt0"]
    context = np.zeros(len(combinations), dtype=[(name, float) for name in fields]
                       + [("vs30measured", bool)]).view(np.recarray)
    context.mag, context.rrup, context.vs30 = np.array(combinations).T
    context.rjb = context.rrup
    context.rake = 180
    context.dip = 90
    context.ztor = 0
    context.width = 15
    context.rx = -1
    context.z1pt0 = -999
    outputs = [np.zeros((1, len(combinations))) for _ in range(4)]
    with np.errstate(divide="ignore", invalid="ignore"):
        AbrahamsonSilva2008().compute(context, [PGA()], *outputs)
    rows = [dict(mag=mag, rrup_km=distance, vs30=velocity, pga_g=float(value))
            for (mag, distance, velocity), value in zip(combinations, np.exp(outputs[0][0]))]
    payload = {"openquake_version": version("openquake.engine"), "cases": rows}
    Path(args.output).write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
