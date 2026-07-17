from mountainash_data.backends.ibis._raw import raw_execute, raw_fetch_scalar


class FakeHandleWithExecute:
    def __init__(self, result=None):
        self.calls = []
        self._result = result

    def execute(self, sql):
        self.calls.append(sql)
        return self._result


class RecordingCursor:
    def __init__(self, log, fetch_result=None):
        self.log = log
        self._fetch_result = fetch_result
        self.closed = False

    def execute(self, sql):
        self.log.append(("cur", sql))

    def fetchone(self):
        return self._fetch_result

    def close(self):
        self.closed = True
        self.log.append(("close", None))


class FakeHandleNoExecute:
    def __init__(self, fetch_result=None):
        self.log = []
        self._fetch_result = fetch_result
        self.cursor_obj = None

    def cursor(self):
        self.cursor_obj = RecordingCursor(self.log, self._fetch_result)
        return self.cursor_obj


class FakeResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


def test_raw_execute_direct_path_uses_handle_execute():
    h = FakeHandleWithExecute()
    raw_execute(h, "SELECT 1")
    assert h.calls == ["SELECT 1"]


def test_raw_execute_cursor_path_when_no_execute():
    h = FakeHandleNoExecute()
    raw_execute(h, "SELECT 1")
    assert ("cur", "SELECT 1") in h.log
    assert ("close", None) in h.log
    assert h.cursor_obj.closed is True


def test_raw_execute_hook_override_skips_handle_execute():
    calls = []
    h = FakeHandleWithExecute()

    def hook(handle, sql):
        calls.append(sql)

    raw_execute(h, "SELECT 1", hook=hook)
    assert calls == ["SELECT 1"]
    assert h.calls == []


def test_raw_fetch_scalar_direct_path_returns_scalar():
    h = FakeHandleWithExecute(result=FakeResult((42,)))
    assert raw_fetch_scalar(h, "SELECT 42") == 42


def test_raw_fetch_scalar_cursor_path_returns_scalar():
    h = FakeHandleNoExecute(fetch_result=(7,))
    assert raw_fetch_scalar(h, "SELECT 7") == 7


def test_raw_fetch_scalar_empty_result_returns_none():
    h = FakeHandleWithExecute(result=FakeResult(None))
    assert raw_fetch_scalar(h, "SELECT NULL") is None
