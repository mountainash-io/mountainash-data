from __future__ import annotations

from collections.abc import Iterable

from scripts.live_db_harness.process import CompletedCommand
from scripts.live_db_harness.transports import ComposeInspector, ServiceState


def test_service_state_distinguishes_stopped_existing_service() -> None:
    class StateRunner:
        def run(self, argv: Iterable[object], **kwargs: object) -> object:
            command = tuple(str(part) for part in argv)
            output = {
                ("docker", "compose", "ps", "--all", "--services", "postgres"): "postgres\n",
                ("docker", "compose", "ps", "--status", "running", "--services", "postgres"): "",
            }.get(command, "")
            return CompletedCommand(command, 0, output, "")

    inspector = ComposeInspector(StateRunner())
    assert inspector.service_state("postgres", "postgres") == ServiceState(exists=True, running=False)
