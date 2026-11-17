"""HistoryStore 单元测试：v1→v2 原地迁移、提交点序号、歧义与回看语义。

    python3 -m unittest tests.test_storage -v
"""
import os
import sqlite3
import tempfile
import unittest

from app.storage import HistoryStore, lsn_to_num

# 旧版（v1）表结构：txns/artifact_history 没有 seq、lsn_kind
V1_SCHEMA = """
CREATE TABLE capture_state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE txns (
    xid INTEGER PRIMARY KEY,
    commit_lsn TEXT NOT NULL,
    commit_lsn_num INTEGER NOT NULL,
    commit_ts TEXT
);
CREATE TABLE artifact_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    artifact_no INTEGER NOT NULL,
    change_type TEXT NOT NULL,
    service_name TEXT, version TEXT, sha256 TEXT,
    xid INTEGER, commit_lsn TEXT, commit_lsn_num INTEGER,
    captured_at TEXT NOT NULL
);
"""

BASE_LSN = "0/50000000"          # 旧基线点
SHARED_LSN = "0/60000000"        # 两笔旧事务共享的记录 LSN（旧 bug 的产物）
SHA = "a" * 64


def make_v1_db(path: str) -> None:
    """构造一个 v1 旧库：基线 1 行 + 两笔共享同一 LSN 的旧事务。"""
    conn = sqlite3.connect(path)
    conn.executescript(V1_SCHEMA)
    state = {
        "phase": "STREAMING",
        "slot_name": "artifacts_history_slot",
        "baseline_lsn": BASE_LSN,
        "baseline_lsn_num": str(lsn_to_num(BASE_LSN)),
        "baseline_captured_at": "2026-11-01T00:00:00.000000Z",
        "last_commit_lsn": SHARED_LSN,
        "last_commit_lsn_num": str(lsn_to_num(SHARED_LSN)),
        "last_xid": "900002",
    }
    conn.executemany(
        "INSERT INTO capture_state(key, value) VALUES(?, ?)", state.items()
    )
    shared_num = lsn_to_num(SHARED_LSN)
    base_num = lsn_to_num(BASE_LSN)
    conn.executemany(
        "INSERT INTO txns(xid, commit_lsn, commit_lsn_num, commit_ts)"
        " VALUES (?, ?, ?, ?)",
        [
            (900001, SHARED_LSN, shared_num, "2026-11-01 01:00:00+00"),
            (900002, SHARED_LSN, shared_num, "2026-11-01 01:00:01+00"),
        ],
    )
    conn.executemany(
        "INSERT INTO artifact_history(artifact_no, change_type, service_name,"
        " version, sha256, xid, commit_lsn, commit_lsn_num, captured_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (7000, "BASELINE", "legacy-svc", "0.1.0", SHA, None,
             BASE_LSN, base_num, "2026-11-01T00:00:00.000000Z"),
            (7001, "INSERT", "legacy-app", "L1", SHA, 900001,
             SHARED_LSN, shared_num, "2026-11-01T01:00:00.000000Z"),
            (7001, "UPDATE", "legacy-app", "L2", SHA, 900002,
             SHARED_LSN, shared_num, "2026-11-01T01:00:01.000000Z"),
        ],
    )
    conn.commit()
    conn.close()


class TempDBMixin(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="hist-test-")
        self.path = os.path.join(self.dir, "history.db")

    def tearDown(self):
        import shutil

        shutil.rmtree(self.dir, ignore_errors=True)


class FreshStoreTest(TempDBMixin):
    def test_fresh_db_is_v2(self):
        store = HistoryStore(self.path)
        self.assertEqual(store.get_state().get("schema_version"), "2")
        store.close()

    def test_persist_assigns_increasing_seq_and_dedups(self):
        store = HistoryStore(self.path)
        store.save_baseline([(1, "svc", "1.0", SHA)], "0/1000000")
        store.mark_streaming()
        ch = [{"artifact_no": 1, "change_type": "UPDATE", "service_name": "svc",
               "version": "1.1", "sha256": SHA}]
        self.assertTrue(store.persist_transaction(101, "0/1000028", None, ch))
        self.assertTrue(store.persist_transaction(102, "0/1000050", None, ch))
        # 重放同一 xid：幂等跳过，序号不重复分配
        self.assertFalse(store.persist_transaction(101, "0/1000028", None, ch))
        points = store.get_points()
        self.assertEqual([p["seq"] for p in points], [0, 1, 2])
        self.assertEqual(points[1]["lsn_kind"], "commit")
        self.assertEqual(points[2]["lsn"], "0/1000050")
        # 回看第一笔只含第一笔状态
        self.assertEqual(store.get_directory_at(1)[0]["version"], "1.1")
        store.close()

    def test_seq_survives_reopen(self):
        store = HistoryStore(self.path)
        store.save_baseline([(1, "svc", "1.0", SHA)], "0/1000000")
        store.mark_streaming()
        store.persist_transaction(
            101, "0/1000028", None,
            [{"artifact_no": 2, "change_type": "INSERT", "service_name": "x",
              "version": "1", "sha256": SHA}],
        )
        store.close()
        store2 = HistoryStore(self.path)  # 重开不丢序号、不重复迁移
        points = store2.get_points()
        self.assertEqual([p["seq"] for p in points], [0, 1])
        store2.persist_transaction(
            102, "0/1000060", None,
            [{"artifact_no": 3, "change_type": "INSERT", "service_name": "y",
              "version": "1", "sha256": SHA}],
        )
        self.assertEqual(store2.get_points()[-1]["seq"], 2)
        store2.close()


class MigrationTest(TempDBMixin):
    def setUp(self):
        super().setUp()
        make_v1_db(self.path)
        self.store = HistoryStore(self.path)  # 触发原地迁移

    def tearDown(self):
        self.store.close()
        super().tearDown()

    def test_migration_marks_version_and_preserves_rows(self):
        state = self.store.get_state()
        self.assertEqual(state["schema_version"], "2")
        self.assertIn("migrated_at", state)
        # 历史行原样保留
        hist = self.store.get_history_of(7001)
        self.assertEqual([h["change_type"] for h in hist], ["INSERT", "UPDATE"])
        self.assertEqual([h["version"] for h in hist], ["L1", "L2"])

    def test_legacy_txns_get_distinct_seqs_and_keep_recorded_lsn(self):
        points = self.store.get_points()
        self.assertEqual([p["seq"] for p in points], [0, 1, 2])
        legacy = [p for p in points if p["kind"] == "TXN"]
        # 不伪造真实源 LSN：保留原记录值并标记 recorded
        for p in legacy:
            self.assertEqual(p["lsn"], SHARED_LSN)
            self.assertEqual(p["lsn_kind"], "recorded")
            self.assertTrue(p["ambiguous"])
        # 基线不受影响
        self.assertEqual(points[0]["lsn_kind"], "snapshot")
        self.assertFalse(points[0]["ambiguous"])

    def test_directory_at_seq_only_includes_selected_and_prior(self):
        # 第一笔旧事务：只见 L1，不见第二笔的 L2
        d1 = {a["artifact_no"]: a for a in self.store.get_directory_at(1)}
        self.assertEqual(d1[7001]["version"], "L1")
        # 第二笔：L2
        d2 = {a["artifact_no"]: a for a in self.store.get_directory_at(2)}
        self.assertEqual(d2[7001]["version"], "L2")
        # 基线点：还没有 7001
        d0 = {a["artifact_no"]: a for a in self.store.get_directory_at(0)}
        self.assertNotIn(7001, d0)
        self.assertIn(7000, d0)

    def test_shared_lsn_resolves_to_two_candidates(self):
        cands = self.store.get_points_by_lsn_num(lsn_to_num(SHARED_LSN))
        self.assertEqual(len(cands), 2)
        self.assertEqual({c["xid"] for c in cands}, {900001, 900002})
        # 基线 LSN 无歧义
        base = self.store.get_points_by_lsn_num(lsn_to_num(BASE_LSN))
        self.assertEqual(len(base), 1)
        self.assertEqual(base[0]["kind"], "BASELINE")

    def test_mixed_legacy_and_new_records(self):
        # 迁移后继续捕获新事务：新事务排在所有旧事务之后
        self.store.persist_transaction(
            900003, "0/60000028", None,
            [{"artifact_no": 7001, "change_type": "UPDATE",
              "service_name": "legacy-app", "version": "N1", "sha256": SHA}],
        )
        points = self.store.get_points()
        self.assertEqual(points[-1]["seq"], 3)
        self.assertEqual(points[-1]["lsn_kind"], "commit")
        self.assertFalse(points[-1]["ambiguous"])
        # 新点回看包含旧状态 + 新变更
        d3 = {a["artifact_no"]: a for a in self.store.get_directory_at(3)}
        self.assertEqual(d3[7001]["version"], "N1")
        self.assertIn(7000, d3)
        # 旧点不包含新变更（只含所选事务及此前状态）
        d2 = {a["artifact_no"]: a for a in self.store.get_directory_at(2)}
        self.assertEqual(d2[7001]["version"], "L2")

    def test_reopen_after_migration_is_idempotent(self):
        self.store.close()
        store2 = HistoryStore(self.path)
        points = store2.get_points()
        self.assertEqual([p["seq"] for p in points], [0, 1, 2])
        self.assertEqual(points[1]["lsn_kind"], "recorded")
        store2.close()
        self.store = HistoryStore(self.path)  # 供 tearDown 关闭


if __name__ == "__main__":
    unittest.main()
