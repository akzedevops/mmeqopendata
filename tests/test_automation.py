from datetime import datetime, timezone
import importlib.util
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location("check_automation", Path(__file__).parents[1] / "tools/check_automation.py")
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)


def test_heartbeat_does_not_depend_on_earthquake_occurrence():
    run = {"status": "completed", "conclusion": "success", "updated_at": "2026-11-11T06:00:00Z"}
    monitor.check_heartbeat([run], datetime(2026, 11, 12, tzinfo=timezone.utc))
    with pytest.raises(ValueError, match="freshness"):
        monitor.check_heartbeat([run], datetime(2026, 11, 14, tzinfo=timezone.utc))
    run["conclusion"] = "failure"
    with pytest.raises(ValueError, match="did not succeed"):
        monitor.check_heartbeat([run], datetime(2026, 11, 12, tzinfo=timezone.utc))
    with pytest.raises(ValueError):
        monitor.check_heartbeat([], datetime(2026, 11, 12, tzinfo=timezone.utc))
