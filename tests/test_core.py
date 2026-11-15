#!/usr/bin/env python3
"""核心逻辑单元测试（仅标准库，本机可直接运行）：

  python3 -I tests/test_core.py

覆盖：LSN 换算、时间规范化、wal2json 事务应用的幂等与原子性、
     读事务快照一致性（每次取数只读同一已提交状态）。
"""
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "app"))

from canonical import canonical_line, norm_ts            # noqa: E402
from changes import apply_transaction, record_magnitude_anomalies  # noqa: E402
from lsnutil import int_to_lsn, lsn_to_int               # noqa: E402
from store import Store                                  # noqa: E402

TS = "2026-11-14 08:00:00.123456+00"


def change(kind, eid, ts=TS, lat=34.567, lon=100.123, mag=4.5):
    if kind == "delete":
        return {
            "kind": "delete", "schema": "public", "table": "seismic_events",
            "oldkeys": {"keynames": ["event_id"], "keytypes": ["text"], "keyvalues": [eid]},
        }
    return {
        "kind": kind, "schema": "public", "table": "seismic_events",
        "columnnames": ["event_id", "origin_time", "latitude", "longitude", "magnitude"],
        "columntypes": ["text", "timestamp with time zone", "numeric", "numeric", "numeric"],
        "columnvalues": [eid, ts, lat, lon, mag],
    }


def change_rename(eid_old, eid_new):
    ch = change("update", eid_new)
    ch["oldkeys"] = {"keynames": ["event_id"], "keytypes": ["text"], "keyvalues": [eid_old]}
    return ch


class LsnTest(unittest.TestCase):
    def test_roundtrip(self):
        for s in ("0/0", "0/1B3C4D0", "1B3/4A0", "FF/FFFFFFFF", "2/9A08C800"):
            self.assertEqual(int_to_lsn(lsn_to_int(s)), s)
        self.assertLess(lsn_to_int("0/10"), lsn_to_int("1/0"))


class NormTsTest(unittest.TestCase):
    def test_load_path_equals_replication_path(self):
        from_load = norm_ts(datetime(2026, 11, 14, 8, 0, 0, 123456, tzinfo=timezone.utc))
        from_wal = norm_ts("2026-11-14 08:00:00.123456+00")
        self.assertEqual(from_load, from_wal)
        self.assertEqual(from_wal, "2026-11-14T08:00:00.123456Z")

    def test_other_offsets_normalized_to_utc(self):
        self.assertEqual(
            norm_ts("2026-11-14 16:00:00+08:00"), "2026-11-14T08:00:00.000000Z"
        )

    def test_canonical_line_decimal_float_equal(self):
        a = canonical_line("EQ1", "T", Decimal("34.500"), Decimal("100.100"), Decimal("4.0"))
        b = canonical_line("EQ1", "T", 34.5, 100.1, 4.0)
        self.assertEqual(a, b)
        self.assertEqual(a, "EQ1|T|34.500|100.100|4.0")


class StoreTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(os.path.join(self.tmp.name, "mirror.db"))

    def tearDown(self):
        self.tmp.cleanup()

    def apply(self, payload, lsn):
        with self.store.write_txn() as cur:
            counts = apply_transaction(cur, payload)
            Store.set_meta(cur, "last_applied_lsn", lsn)
            return counts

    def rows(self):
        return self.store.read(
            "SELECT event_id, origin_time, latitude, longitude, magnitude FROM events ORDER BY event_id"
        )


class ApplyTest(StoreTestBase):
    def test_insert_update_delete_flow(self):
        self.apply({"change": [change("insert", "A"), change("insert", "B", mag=6.1)]}, "0/10")
        self.assertEqual([r[0] for r in self.rows()], ["A", "B"])

        self.apply({"change": [change("update", "A", mag=5.5)]}, "0/20")
        row_a = [r for r in self.rows() if r[0] == "A"][0]
        self.assertEqual(row_a[4], 5.5)
        self.assertEqual(row_a[1], "2026-11-14T08:00:00.123456Z")   # 时间已规范化

        self.apply({"change": [change("delete", "B")]}, "0/30")
        self.assertEqual([r[0] for r in self.rows()], ["A"])
        self.assertEqual(self.store.get_meta("last_applied_lsn"), "0/30")

    def test_replay_is_idempotent(self):
        """保存后未确认 -> 源库重发 -> 幂等重放，最终状态不变、不重。"""
        txns = [
            ({"change": [change("insert", "A"), change("insert", "B")]}, "0/10"),
            ({"change": [change("update", "A", mag=7.2)]}, "0/20"),
            ({"change": [change("delete", "B")]}, "0/30"),
        ]
        for payload, lsn in txns:
            self.apply(payload, lsn)
        before = self.rows()
        for payload, lsn in txns:          # 模拟崩溃重启后整段重发
            self.apply(payload, lsn)
        self.assertEqual(self.rows(), before)

    def test_old_value_never_overwrites_new(self):
        """按 LSN 顺序应用：旧事务重放只会被后续新事务再次覆盖为新值。"""
        self.apply({"change": [change("insert", "A", mag=3.0)]}, "0/10")
        self.apply({"change": [change("update", "A", mag=8.8)]}, "0/20")
        # 崩溃重放：旧 insert 又来一遍，随后新 update 也重放，最终仍是新值
        self.apply({"change": [change("insert", "A", mag=3.0)]}, "0/10")
        self.apply({"change": [change("update", "A", mag=8.8)]}, "0/20")
        self.assertEqual(self.rows()[0][4], 8.8)

    def test_primary_key_rename(self):
        self.apply({"change": [change("insert", "OLD")]}, "0/10")
        self.apply({"change": [change_rename("OLD", "NEW")]}, "0/20")
        self.assertEqual([r[0] for r in self.rows()], ["NEW"])

    def test_truncate_ignored(self):
        self.apply({"change": [change("insert", "A")]}, "0/10")
        counts = self.apply({"change": [{"kind": "truncate", "schema": "public", "table": "seismic_events"}]}, "0/20")
        self.assertEqual(counts["other"], 1)
        self.assertEqual(len(self.rows()), 1)

    def test_transaction_atomicity_on_failure(self):
        """一笔源事务应用一半失败 -> 整体回滚，数据与位置都不前移。"""
        with self.assertRaises(RuntimeError):
            with self.store.write_txn() as cur:
                apply_transaction(cur, {"change": [change("insert", "A"), change("insert", "B")]})
                Store.set_meta(cur, "last_applied_lsn", "0/10")
                raise RuntimeError("模拟写入失败")
        self.assertEqual(self.rows(), [])
        self.assertIsNone(self.store.get_meta("last_applied_lsn"))

    def test_whole_transaction_visible_together(self):
        """一笔多行事务提交后整笔可见；读事务只看到提交前或提交后。"""
        with self.store.write_txn() as cur:
            apply_transaction(cur, {"change": [change("insert", f"E{i:02d}") for i in range(20)]})
            Store.set_meta(cur, "last_applied_lsn", "0/10")
        self.assertEqual(len(self.rows()), 20)


class AnomalyTest(StoreTestBase):
    def apply_with_anomalies(self, payload, lsn, xid=1):
        """模拟 replicator 的流式续采：比较、记录、行变更、位置同一事务。"""
        with self.store.write_txn() as cur:
            recorded = record_magnitude_anomalies(cur, payload, lsn, xid)
            apply_transaction(cur, payload)
            Store.set_meta(cur, "last_applied_lsn", lsn)
        return recorded

    def anomalies(self):
        return self.store.read(
            "SELECT lsn, event_id, old_magnitude, new_magnitude, delta"
            " FROM magnitude_anomalies ORDER BY id"
        )

    def seed(self, mag=4.5):
        self.apply_with_anomalies({"change": [change("insert", "A", mag=mag)]}, "0/10")

    def test_records_when_delta_reaches_threshold(self):
        self.seed(4.5)
        rec = self.apply_with_anomalies({"change": [change("update", "A", mag=6.5)]}, "0/20")
        self.assertEqual(rec, [{"event_id": "A", "old_magnitude": 4.5,
                                "new_magnitude": 6.5, "delta": 2.0}])
        self.assertEqual(self.anomalies(), [("0/20", "A", 4.5, 6.5, 2.0)])

    def test_below_threshold_not_recorded(self):
        self.seed(4.5)
        rec = self.apply_with_anomalies({"change": [change("update", "A", mag=6.4)]}, "0/20")
        self.assertEqual(rec, [])
        self.assertEqual(self.anomalies(), [])

    def test_repeated_correction_in_one_txn_recorded_once_with_final_value(self):
        """一笔事务先 4.5->7.0 再 7.0->4.5：只按最终值 4.5 与事务前 4.5 比，不记录。"""
        self.seed(4.5)
        rec = self.apply_with_anomalies(
            {"change": [change("update", "A", mag=7.0), change("update", "A", mag=4.5)]},
            "0/20",
        )
        self.assertEqual(rec, [])
        self.assertEqual(self.anomalies(), [])

    def test_repeated_correction_recorded_once(self):
        """4.5 -> 7.0 -> 8.0：只记一条，旧值取事务前、新值取最终值。"""
        self.seed(4.5)
        self.apply_with_anomalies(
            {"change": [change("update", "A", mag=7.0), change("update", "A", mag=8.0)]},
            "0/20",
        )
        self.assertEqual(self.anomalies(), [("0/20", "A", 4.5, 8.0, 3.5)])

    def test_primary_key_rename_not_compared(self):
        """震例号被改不视为“同一震例号”的震级突变。"""
        self.seed(4.5)
        rec = self.apply_with_anomalies(
            {"change": [change_rename("A", "B")]}, "0/20"
        )
        self.assertEqual(rec, [])
        self.assertEqual(self.anomalies(), [])

    def test_insert_and_delete_not_recorded(self):
        self.apply_with_anomalies({"change": [change("insert", "NEW", mag=9.0)]}, "0/10")
        self.seed(4.5)
        # 同一事务里 4.5 -> 8.0 然后又删掉：最终没有该震例，不记
        self.apply_with_anomalies(
            {"change": [change("update", "A", mag=8.0), change("delete", "A")]}, "0/20"
        )
        self.assertEqual(self.anomalies(), [])

    def test_replay_does_not_duplicate(self):
        """保存后未确认 -> 源库重发同一事务：异常记录不重复。"""
        self.seed(4.5)
        payload = {"change": [change("update", "A", mag=7.5)]}
        self.apply_with_anomalies(payload, "0/20", xid=7)
        self.apply_with_anomalies(payload, "0/20", xid=7)
        self.assertEqual(self.anomalies(), [("0/20", "A", 4.5, 7.5, 3.0)])

    def test_old_value_taken_from_previous_committed_state(self):
        """事务前值 = 上一笔已提交事务后的值，而不是本事务中途的值。"""
        self.seed(4.5)
        self.apply_with_anomalies({"change": [change("update", "A", mag=6.0)]}, "0/20")
        self.apply_with_anomalies({"change": [change("update", "A", mag=8.0)]}, "0/30")
        self.assertEqual(self.anomalies(), [("0/30", "A", 6.0, 8.0, 2.0)])

    def test_rollback_keeps_no_anomaly_and_no_advance(self):
        self.seed(4.5)
        with self.assertRaises(RuntimeError):
            with self.store.write_txn() as cur:
                record_magnitude_anomalies(cur, {"change": [change("update", "A", mag=8.0)]}, "0/20")
                apply_transaction(cur, {"change": [change("update", "A", mag=8.0)]})
                Store.set_meta(cur, "last_applied_lsn", "0/20")
                raise RuntimeError("模拟写入失败")
        self.assertEqual(self.anomalies(), [])
        self.assertEqual(self.store.get_meta("last_applied_lsn"), "0/10")
        self.assertEqual(self.rows()[0][4], 4.5)


class ReplicatorWiringTest(StoreTestBase):
    """regression：只有流式续采（catchup/done）才记录突变，初次装载不记。"""

    def setUp(self):
        super().setUp()
        try:
            import replicator  # noqa: F401  依赖 psycopg2；无此依赖时跳过
        except ImportError:
            self.skipTest("psycopg2 不可用，跳过 replicator 装配测试")
        from replicator import Replicator
        self.rep = Replicator({}, "slot", self.store)

    def feed(self, state, body, lsn):
        with self.store.write_txn() as cur:
            Store.set_meta(cur, "load_state", state)
            cur.execute(
                "INSERT OR IGNORE INTO events"
                "(event_id, origin_time, latitude, longitude, magnitude)"
                " VALUES('A', 'T', 30.0, 100.0, 4.5)"
            )
            Store.set_meta(cur, "last_applied_lsn", "0/0")

        import json
        class Msg:
            data_start = lsn_to_int(lsn)
            payload = json.dumps(body)
            reply_requested = False

        class Cur:
            def send_feedback(self, **kw):
                pass

        self.rep._on_message(Cur(), Msg(), None)

    def test_initial_loading_does_not_record(self):
        self.feed("loading", {"change": [change("insert", "A", mag=8.0)]}, "0/10")
        self.assertEqual(self.store.read("SELECT count(*) FROM magnitude_anomalies")[0][0], 0)

    def test_catchup_and_done_record(self):
        for state, lsn, mag in (("catchup", "0/10", 7.0), ("done", "0/20", 9.0)):
            self.feed(state, {"change": [change("update", "A", mag=mag)]}, lsn)
        rows = self.store.read(
            "SELECT lsn, event_id, old_magnitude, new_magnitude FROM magnitude_anomalies ORDER BY id"
        )
        self.assertEqual(rows, [("0/10", "A", 4.5, 7.0), ("0/20", "A", 7.0, 9.0)])
        self.assertEqual(self.store.get_meta("magnitude_anomalies"), "2")


class SnapshotReadTest(StoreTestBase):
    def test_read_txn_sees_one_committed_state(self):
        self.apply({"change": [change("insert", "A")]}, "0/10")
        txn = self.store.read_txn()
        try:
            n1 = txn.execute("SELECT count(*) FROM events").fetchone()[0]
            # 读事务打开期间，写者提交了新事务
            self.apply({"change": [change("insert", "B")]}, "0/20")
            n2 = txn.execute("SELECT count(*) FROM events").fetchone()[0]
        finally:
            txn.close()
        self.assertEqual((n1, n2), (1, 1))          # 同一已提交状态
        self.assertEqual(self.store.read("SELECT count(*) FROM events")[0][0], 2)  # 新读见新态


if __name__ == "__main__":
    unittest.main(verbosity=2)
