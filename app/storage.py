"""SQLite 历史库：只增不改的变更日志 + 捕获状态。

关键约束：
- 所有写入在一把进程内锁内串行化，SQLite 开启 WAL；
- 一个源事务的全部行变更与该事务的提交点在同一个 SQLite 事务里落盘，
  保证多行事务整体可见、不会出现半条事务；
- 每个提交点有全库唯一、单调递增的序号 seq（基线=0，之后每笔源事务 +1）。
  回看一律按 seq 定位：只包含所选事务及此前状态。LSN 文本仍保留用于
  展示与旧链接兼容，但不再作为回看的唯一键——旧版本以 LSN 为键且记录
  的是 wal_end（协议定义为"服务器 WAL 末端"，非事务标识），多笔事务
  可能共享同一记录值，单靠 LSN 无法区分这些事务；
- 复制槽的推进位置（last_commit_lsn）与对应数据在同一事务更新，
  崩溃重启后靠 xid 去重，绝不重复、也不丢数据。

schema v2（原地升级，不丢历史）：
- txns 增加 seq、lsn_kind 两列；artifact_history 增加 seq 列；
- 旧库启动时自动迁移：旧事务按 (commit_lsn_num, xid) 补发序号，
  lsn_kind 标记为 'recorded'（旧版记录值，可能不是真实提交 LSN，
  不伪造、不改写）；新事务一律记录真实提交 LSN（'commit'）。
"""
from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Iterable

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS capture_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS txns (
    xid            INTEGER PRIMARY KEY,
    commit_lsn     TEXT NOT NULL,
    commit_lsn_num INTEGER NOT NULL,
    commit_ts      TEXT,
    seq            INTEGER,          -- 提交点序号，全库唯一单调（基线=0）
    lsn_kind       TEXT              -- 'commit'=真实源提交LSN / 'recorded'=旧版记录值
);

CREATE TABLE IF NOT EXISTS artifact_history (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    artifact_no  INTEGER NOT NULL,
    change_type  TEXT NOT NULL,              -- BASELINE / INSERT / UPDATE / DELETE
    service_name TEXT,
    version      TEXT,
    sha256       TEXT,
    xid          INTEGER,
    commit_lsn   TEXT,
    commit_lsn_num INTEGER,
    seq          INTEGER,            -- 所属提交点序号（基线=0）
    captured_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_hist_artifact ON artifact_history(artifact_no, id);
CREATE INDEX IF NOT EXISTS idx_hist_lsn ON artifact_history(commit_lsn_num);
"""

# v2 依赖新列的索引在建列之后才能创建（旧库先 ALTER 再建索引）
INDEXES_V2_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_txns_seq ON txns(seq);
CREATE INDEX IF NOT EXISTS idx_hist_seq ON artifact_history(seq);
"""

SCHEMA_VERSION = "2"

PHASE_INITIALIZING = "INITIALIZING"
PHASE_STREAMING = "STREAMING"
PHASE_ERROR = "ERROR"

# lsn_kind 取值
LSN_KIND_COMMIT = "commit"        # 真实源提交 LSN（修复后记录）
LSN_KIND_RECORDED = "recorded"    # 旧版记录值（可能为多笔共享的 wal_end，不伪造）
LSN_KIND_SNAPSHOT = "snapshot"    # 基线一致性快照点 LSN


def lsn_to_num(lsn: str) -> int:
    """'0/16B3748' -> 整数，便于比较先后。"""
    high, low = lsn.split("/", 1)
    return (int(high, 16) << 32) | int(low, 16)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


class HistoryStore:
    def __init__(self, path: str) -> None:
        import os

        directory = os.path.dirname(path) or "."
        os.makedirs(directory, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=FULL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._conn.executescript(SCHEMA_SQL)
        self._ensure_v2_schema()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -------------------------------------------------------------- schema v2
    def _ensure_v2_schema(self) -> None:
        """把 v1 旧卷原地升级到 v2；新卷直接建索引、写版本号。幂等。"""
        with self._lock:
            cols = {
                r["name"]
                for r in self._conn.execute("PRAGMA table_info(txns)").fetchall()
            }
            if "seq" not in cols:
                self._migrate_v1_to_v2()
            self._conn.executescript(INDEXES_V2_SQL)
            if self._get_state("schema_version") is None:
                self._set_many({"schema_version": SCHEMA_VERSION})

    def _migrate_v1_to_v2(self) -> None:
        """旧库原地迁移：补列、补序号、标记旧 LSN 为记录值（不伪造）。

        旧事务的真实提交 LSN 已不可考（旧版记录的是 wal_end），迁移只补
        捕获顺序号：按 (commit_lsn_num, xid) 排序赋 1..N，基线为 0。
        同一 LSN 下的多笔旧事务由此获得各自独立的回看序号。
        """
        now = utc_now_iso()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            self._conn.execute("ALTER TABLE txns ADD COLUMN seq INTEGER")
            self._conn.execute("ALTER TABLE txns ADD COLUMN lsn_kind TEXT")
            self._conn.execute(
                "ALTER TABLE artifact_history ADD COLUMN seq INTEGER"
            )
            rows = self._conn.execute(
                "SELECT xid FROM txns ORDER BY commit_lsn_num, xid"
            ).fetchall()
            for i, row in enumerate(rows, start=1):
                self._conn.execute(
                    "UPDATE txns SET seq=?, lsn_kind=? WHERE xid=?",
                    (i, LSN_KIND_RECORDED, row["xid"]),
                )
            self._conn.execute(
                "UPDATE artifact_history SET seq=0 WHERE change_type='BASELINE'"
            )
            self._conn.execute(
                """
                UPDATE artifact_history
                   SET seq=(SELECT t.seq FROM txns t
                             WHERE t.xid = artifact_history.xid)
                 WHERE xid IS NOT NULL AND change_type != 'BASELINE'
                """
            )
            self._set_many(
                {
                    "schema_version": SCHEMA_VERSION,
                    "migrated_at": now,
                    "migration_note": (
                        "v1->v2: 旧事务保留原记录 LSN(recorded)，"
                        "按 (commit_lsn_num, xid) 补发回看序号"
                    ),
                }
            )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    # ------------------------------------------------------------------ state
    def _get_state(self, key: str) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM capture_state WHERE key=?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def get_state(self) -> dict[str, str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT key, value FROM capture_state"
            ).fetchall()
            return {r["key"]: r["value"] for r in rows}

    def get_phase(self) -> str:
        return self._get_state("phase") or PHASE_INITIALIZING

    def is_ready(self) -> bool:
        return self.get_phase() == PHASE_STREAMING

    def _set_many(self, items: dict[str, Any]) -> None:
        self._conn.executemany(
            """
            INSERT INTO capture_state(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            [(k, str(v)) for k, v in items.items()],
        )

    def mark_initializing(self, slot_name: str) -> None:
        with self._lock:
            self._set_many(
                {
                    "phase": PHASE_INITIALIZING,
                    "slot_name": slot_name,
                    "initialized_at": utc_now_iso(),
                    "last_error": "",
                }
            )

    def mark_error(self, message: str) -> None:
        with self._lock:
            self._set_many({"phase": PHASE_ERROR, "last_error": message})

    def clear_error(self) -> None:
        with self._lock:
            self._set_many({"last_error": ""})

    def update_last_message_lsn(self, lsn: str) -> None:
        with self._lock:
            self._set_many({"last_message_lsn": lsn})

    # ------------------------------------------------------- 续接状态（连接侧）
    def mark_stream_connected(self) -> None:
        """复制流（重）连成功：累计接通次数 +1，清除断线提示。"""
        with self._lock:
            count = int(self._get_state("connect_count") or 0) + 1
            self._set_many(
                {
                    "stream_connected": "1",
                    "connected_at": utc_now_iso(),
                    "connect_count": count,
                    "last_error": "",
                }
            )

    def mark_stream_disconnected(self, message: str) -> None:
        with self._lock:
            self._set_many(
                {
                    "stream_connected": "0",
                    "disconnected_at": utc_now_iso(),
                    "last_error": message,
                }
            )

    def record_confirmed(self, lsn: str, lsn_num: int) -> None:
        """记录最近一次已向源端确认（send_feedback）的落盘位置。"""
        with self._lock:
            self._set_many(
                {"last_confirmed_lsn": lsn, "last_confirmed_lsn_num": lsn_num}
            )

    # ----------------------------------------------------------------- writes
    def save_baseline(
        self, rows: Iterable[sqlite3.Row | tuple[int, str, str, str]], lsn: str
    ) -> None:
        """整批基线与基线提交点（seq=0）在一个 SQLite 事务里落盘。"""
        now = utc_now_iso()
        lsn_num = lsn_to_num(lsn)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._set_many(
                    {
                        "phase": PHASE_INITIALIZING,
                        "baseline_lsn": lsn,
                        "baseline_lsn_num": lsn_num,
                        "baseline_captured_at": now,
                    }
                )
                payload = [
                    (
                        r[0],
                        "BASELINE",
                        r[1],
                        r[2],
                        r[3],
                        None,
                        lsn,
                        lsn_num,
                        0,
                        now,
                    )
                    for r in rows
                ]
                self._conn.executemany(
                    """
                    INSERT INTO artifact_history(
                        artifact_no, change_type, service_name, version, sha256,
                        xid, commit_lsn, commit_lsn_num, seq, captured_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload,
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def mark_streaming(self) -> None:
        with self._lock:
            self._set_many({"phase": PHASE_STREAMING, "streaming_at": utc_now_iso()})

    def persist_transaction(
        self,
        xid: int,
        commit_lsn: str,
        commit_ts: str | None,
        changes: list[dict[str, Any]],
    ) -> bool:
        """落盘一个已提交源事务。返回 True=新写入，False=重复事务（已跳过）。

        每个新事务分配全库唯一递增的 seq；行变更、提交点、槽位推进位置
        在同一个 SQLite 事务里落盘。commit_lsn 必须是该事务真实的源提交
        LSN（复制消息的 data_start）。
        """
        lsn_num = lsn_to_num(commit_lsn)
        now = utc_now_iso()
        with self._lock:
            existed = self._conn.execute(
                "SELECT 1 FROM txns WHERE xid=?", (xid,)
            ).fetchone()
            if existed:
                return False
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                seq = self._conn.execute(
                    "SELECT COALESCE(MAX(seq), 0) + 1 AS s FROM txns"
                ).fetchone()["s"]
                self._conn.execute(
                    "INSERT INTO txns(xid, commit_lsn, commit_lsn_num, commit_ts,"
                    " seq, lsn_kind) VALUES (?, ?, ?, ?, ?, ?)",
                    (xid, commit_lsn, lsn_num, commit_ts, seq, LSN_KIND_COMMIT),
                )
                self._conn.executemany(
                    """
                    INSERT INTO artifact_history(
                        artifact_no, change_type, service_name, version, sha256,
                        xid, commit_lsn, commit_lsn_num, seq, captured_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            c["artifact_no"],
                            c["change_type"],
                            c.get("service_name"),
                            c.get("version"),
                            c.get("sha256"),
                            xid,
                            commit_lsn,
                            lsn_num,
                            seq,
                            now,
                        )
                        for c in changes
                    ],
                )
                # 槽位推进位置与数据同事务落盘
                self._set_many(
                    {
                        "last_commit_lsn": commit_lsn,
                        "last_commit_lsn_num": lsn_num,
                        "last_xid": xid,
                    }
                )
                self._conn.execute("COMMIT")
                return True
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    # ------------------------------------------------------------------ reads
    def get_points(self) -> list[dict[str, Any]]:
        """全部可回看提交点，按 seq 升序；标出共享同一 LSN 的歧义组。"""
        with self._lock:
            points: list[dict[str, Any]] = []
            baseline_lsn = self._get_state("baseline_lsn")
            if baseline_lsn:
                count = self._conn.execute(
                    "SELECT COUNT(*) AS c FROM artifact_history"
                    " WHERE change_type='BASELINE'"
                ).fetchone()["c"]
                points.append(
                    {
                        "kind": "BASELINE",
                        "seq": 0,
                        "lsn": baseline_lsn,
                        "lsn_num": lsn_to_num(baseline_lsn),
                        "xid": None,
                        "commit_ts": self._get_state("baseline_captured_at"),
                        "change_count": count,
                        "lsn_kind": LSN_KIND_SNAPSHOT,
                    }
                )
            for row in self._conn.execute(
                """
                SELECT t.xid, t.commit_lsn, t.commit_lsn_num, t.commit_ts,
                       t.seq, t.lsn_kind,
                       (SELECT COUNT(*) FROM artifact_history h
                         WHERE h.xid = t.xid) AS change_count
                FROM txns t ORDER BY t.seq
                """
            ).fetchall():
                points.append(
                    {
                        "kind": "TXN",
                        "seq": row["seq"],
                        "lsn": row["commit_lsn"],
                        "lsn_num": row["commit_lsn_num"],
                        "xid": row["xid"],
                        "commit_ts": row["commit_ts"],
                        "change_count": row["change_count"],
                        "lsn_kind": row["lsn_kind"] or LSN_KIND_RECORDED,
                    }
                )
            counts: dict[int, int] = {}
            for p in points:
                counts[p["lsn_num"]] = counts.get(p["lsn_num"], 0) + 1
            for p in points:
                p["ambiguous"] = counts[p["lsn_num"]] > 1
            return points

    def get_point_by_seq(self, seq: int) -> dict[str, Any] | None:
        for p in self.get_points():
            if p["seq"] == seq:
                return p
        return None

    def get_points_by_lsn_num(self, lsn_num: int) -> list[dict[str, Any]]:
        return [p for p in self.get_points() if p["lsn_num"] == lsn_num]

    def get_directory_at(self, seq: int) -> list[dict[str, Any]]:
        """回看某个提交点（含）为止的整目录：只含所选事务及此前状态。"""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT artifact_no, service_name, version, sha256, change_type
                FROM artifact_history
                WHERE id IN (
                    SELECT MAX(id) FROM artifact_history
                    WHERE seq <= ?
                    GROUP BY artifact_no
                )
                AND change_type != 'DELETE'
                ORDER BY artifact_no
                """,
                (seq,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_history_of(self, artifact_no: int) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT artifact_no, change_type, service_name, version, sha256,
                       xid, commit_lsn, commit_lsn_num, seq, captured_at
                FROM artifact_history WHERE artifact_no=?
                ORDER BY id
                """,
                (artifact_no,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_progress(self) -> dict[str, Any]:
        with self._lock:
            state = self.get_state()
            txn_count = self._conn.execute(
                "SELECT COUNT(*) AS c FROM txns"
            ).fetchone()["c"]
            history_rows = self._conn.execute(
                "SELECT COUNT(*) AS c FROM artifact_history"
            ).fetchone()["c"]
            return {
                "phase": state.get("phase", PHASE_INITIALIZING),
                "slot_name": state.get("slot_name"),
                "baseline_lsn": state.get("baseline_lsn"),
                "last_commit_lsn": state.get("last_commit_lsn"),
                "last_message_lsn": state.get("last_message_lsn"),
                "last_confirmed_lsn": state.get("last_confirmed_lsn"),
                "last_xid": int(state["last_xid"]) if state.get("last_xid") else None,
                "captured_transactions": txn_count,
                "history_rows": history_rows,
                "last_error": state.get("last_error") or None,
                "initialized_at": state.get("initialized_at"),
                # ---- 续接状态 ----
                "stream_connected": state.get("stream_connected") == "1",
                "connect_count": int(state.get("connect_count") or 0),
                "connected_at": state.get("connected_at"),
                "disconnected_at": state.get("disconnected_at"),
                # ---- 存储版本 ----
                "schema_version": state.get("schema_version"),
                "migrated_at": state.get("migrated_at"),
            }
