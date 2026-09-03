from __future__ import annotations

from pathlib import Path

import pytest


from scripts.live_db_harness.models import HarnessSettings


MYSQL_LIVE_NODE_IDS = {
    "tests/test_live_backends/test_index_ops_live.py::TestMySQLLive::test_table_scoped_drop_requires_table",
    "tests/test_live_backends/test_index_ops_live.py::TestMySQLLive::test_emulated_if_not_exists_is_idempotent",
    "tests/test_live_backends/test_index_ops_live.py::TestMySQLLive::test_emulated_if_exists_drop_absent_is_noop",
}


def test_mysql_selector_collects_all_test_mysql_live_node_ids(
    capsys: pytest.CaptureFixture[str],
):
    settings = HarnessSettings(config_files=[Path("tests/config/live-db.toml")])
    selector = settings.backends["mysql"].selector
    result = pytest.main(
        [
            "--collect-only",
            "-q",
            "-k",
            selector,
            "tests/test_live_backends/test_index_ops_live.py",
        ]
    )

    assert result == pytest.ExitCode.OK
    output = capsys.readouterr().out
    collected_node_ids = {
        line.strip()
        for line in output.splitlines()
        if line.strip().startswith("tests/test_live_backends/test_index_ops_live.py::")
    }
    assert collected_node_ids == MYSQL_LIVE_NODE_IDS
