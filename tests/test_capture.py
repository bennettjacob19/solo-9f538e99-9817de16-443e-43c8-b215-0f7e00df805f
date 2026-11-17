"""CaptureWorker 反馈钳制逻辑单元测试（不依赖真实 PostgreSQL）。

    python3 -m unittest tests.test_capture -v
"""
import types
import unittest
from unittest.mock import MagicMock

from app.capture import CaptureWorker


def make_worker(last_confirmed: int) -> CaptureWorker:
    worker = object.__new__(CaptureWorker)  # 绕过 __init__，只测反馈逻辑
    worker._last_confirmed_lsn = last_confirmed
    worker._last_feedback_at = 0.0
    worker._settings = types.SimpleNamespace(status_interval_s=0)
    return worker


def make_msg(wal_end: int) -> MagicMock:
    msg = MagicMock()
    msg.wal_end = wal_end
    return msg


class KeepaliveClampTest(unittest.TestCase):
    def test_confirmed_within_wal_end_sent_as_is(self):
        worker = make_worker(50)
        msg = make_msg(wal_end=100)
        worker._maybe_reply_keepalive(msg)
        msg.cursor.send_feedback.assert_called_once_with(flush_lsn=50)

    def test_confirmed_beyond_wal_end_is_clamped(self):
        # 旧卷的本地记录值超前于源端 WAL 末端：钳制到源端实际末端
        worker = make_worker(200)
        msg = make_msg(wal_end=100)
        worker._maybe_reply_keepalive(msg)
        msg.cursor.send_feedback.assert_called_once_with(flush_lsn=100)

    def test_no_confirmed_no_reply(self):
        worker = make_worker(0)
        msg = make_msg(wal_end=100)
        worker._maybe_reply_keepalive(msg)
        msg.cursor.send_feedback.assert_not_called()

    def test_throttled_within_interval(self):
        worker = make_worker(50)
        worker._settings.status_interval_s = 3600
        worker._last_feedback_at = __import__("time").monotonic()
        msg = make_msg(wal_end=100)
        worker._maybe_reply_keepalive(msg)
        msg.cursor.send_feedback.assert_not_called()


if __name__ == "__main__":
    unittest.main()
