#!/usr/bin/env python3
"""生成一个 v1 旧版 SQLite 历史库，用于端到端自测"旧卷原地升级"。

用法：python3 tests/make_legacy_db.py /path/to/history.db

旧库内容（模拟旧版本 wal_end 缺陷的产物）：
- 基线 1 行（artifact 7000），基线点 LSN = 0/50000000；
- 两笔旧事务（xid 900001/900002）共享同一个记录 LSN 0/60000000：
  7001 INSERT 'L1'、7001 UPDATE 'L2'；
- 表结构为 v1（无 seq / lsn_kind 列），应用启动时应原地迁移。
"""
import sqlite3
import sys

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

BASE_LSN = "0/50000000"
SHARED_LSN = "0/60000000"
SHA = "b" * 64


def lsn_to_num(lsn: str) -> int:
    high, low = lsn.split("/", 1)
    return (int(high, 16) << 32) | int(low, 16)


def main() -> None:
    path = sys.argv[1]
    conn = sqlite3.connect(path)
    conn.executescript(V1_SCHEMA)
    state = {
        "phase": "STREAMING",
        "slot_name": "artifacts_history_slot",
        "baseline_lsn": BASE_LSN,
        "baseline_lsn_num": str(lsn_to_num(BASE_LSN)),
        "baseline_captured_at": "2026-11-01T00:00:00.000000Z",
        "initialized_at": "2026-11-01T00:00:00.000000Z",
        "last_commit_lsn": SHARED_LSN,
        "last_commit_lsn_num": str(lsn_to_num(SHARED_LSN)),
        "last_xid": "900002",
    }
    conn.executemany(
        "INSERT INTO capture_state(key, value) VALUES(?, ?)", state.items()
    )
    shared_num = lsn_to_num(SHARED_LSN)
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
             BASE_LSN, lsn_to_num(BASE_LSN), "2026-11-01T00:00:00.000000Z"),
            (7001, "INSERT", "legacy-app", "L1", SHA, 900001,
             SHARED_LSN, shared_num, "2026-11-01T01:00:00.000000Z"),
            (7001, "UPDATE", "legacy-app", "L2", SHA, 900002,
             SHARED_LSN, shared_num, "2026-11-01T01:00:01.000000Z"),
        ],
    )
    conn.commit()
    conn.close()
    print(f"legacy v1 db written: {path}")


if __name__ == "__main__":
    main()
