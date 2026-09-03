from __future__ import annotations


class RecordingCursor:
    def __init__(self) -> None:
        self.executed: list[object] = []

    def execute(self, statement: object, *args: object, **kwargs: object) -> None:
        self.executed.append(statement)


class RecordingRawConnection:
    def __init__(self) -> None:
        self.cursor_calls = 0

    def cursor(self) -> RecordingCursor:
        self.cursor_calls += 1
        return RecordingCursor()
