from __future__ import annotations

import ast
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parents[3]))

from scripts.live_db_harness.models import HarnessSettings


MYSQL_LIVE_NODE_IDS = {
    "tests/test_integration/test_index_ops_live.py::TestMySQLLive::test_table_scoped_drop_requires_table",
    "tests/test_integration/test_index_ops_live.py::TestMySQLLive::test_emulated_if_not_exists_is_idempotent",
    "tests/test_integration/test_index_ops_live.py::TestMySQLLive::test_emulated_if_exists_drop_absent_is_noop",
}


def test_mysql_selector_collects_all_test_mysql_live_node_ids():
    source = Path("tests/test_integration/test_index_ops_live.py").read_text()
    tree = ast.parse(source)
    test_class = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "TestMySQLLive"
    )
    node_ids = {
        "tests/test_integration/test_index_ops_live.py::TestMySQLLive::" + node.name
        for node in test_class.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name.startswith("test_")
    }
    assert MYSQL_LIVE_NODE_IDS <= node_ids

    settings = HarnessSettings(config_files=[Path("tests/config/live-db.toml")])
    assert settings.backends["mysql"].selector == "mysql"
