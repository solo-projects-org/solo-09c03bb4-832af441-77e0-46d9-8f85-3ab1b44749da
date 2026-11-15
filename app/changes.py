"""wal2json（format-version 1）行变更 -> SQLite 镜像。

wal2json format 1：一条消息 = 一笔源事务，形如
  {"change": [ {"kind": "insert"|"update"|"delete", ...}, ... ], "xid": ..., ...}

应用规则：
- insert/update：按主键 upsert（幂等 —— 源库重发时重复应用结果不变）；
- update 若改了主键：先按 oldkeys 删旧行再 upsert 新行；
- delete：按 oldkeys 主键删除（幂等 —— 删不存在的行是无害空操作）；
- 只处理行变更：truncate 等其他消息记录并忽略。

变更只可能按 LSN 顺序被应用（单槽单消费者），且每条 wal2json 消息携带的是
该事务提交时的整行新值，因此旧值永远不会盖回新值。

震级突变（异常）记录：
- 只对**流式续采**中的每笔源事务做比较（初次装载不记录）；
- 比较口径是“事务前”（镜像里上一笔已提交事务之后的值）与“事务后”（本笔事务
  全部行变更应用完的最终值），同一震例在一笔事务里被反复更正也只按最终值记一次；
- 与传统位置、镜像行变更一起写入同一个 SQLite 事务；失败则整笔回滚、不确认源库；
- UNIQUE(lsn, event_id) + OR IGNORE：崩溃后源库重发同一事务不会重复记录。
"""
from canonical import norm_ts
from store import utcnow

ANOMALY_THRESHOLD = 2.0     # 震级差值达到 2.0 即留存复核

UPSERT_SQL = """
INSERT INTO events(event_id, origin_time, latitude, longitude, magnitude)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(event_id) DO UPDATE SET
    origin_time = excluded.origin_time,
    latitude    = excluded.latitude,
    longitude   = excluded.longitude,
    magnitude   = excluded.magnitude
"""

DELETE_SQL = "DELETE FROM events WHERE event_id = ?"


def _old_event_id(change):
    keys = change.get("oldkeys") or {}
    names = keys.get("keynames") or []
    if "event_id" in names:
        return keys["keyvalues"][names.index("event_id")]
    return None


def apply_change(cur, change):
    """应用一条行变更，返回 'insert'/'update'/'delete'；非行变更返回 None。"""
    kind = change.get("kind")
    if kind in ("insert", "update"):
        cols = dict(zip(change["columnnames"], change["columnvalues"]))
        if kind == "update":
            old_id = _old_event_id(change)
            if old_id is not None and old_id != cols["event_id"]:
                cur.execute(DELETE_SQL, (old_id,))   # 主键被改：先删旧行
        cur.execute(
            UPSERT_SQL,
            (
                cols["event_id"],
                norm_ts(cols["origin_time"]),
                float(cols["latitude"]),
                float(cols["longitude"]),
                float(cols["magnitude"]),
            ),
        )
        return kind
    if kind == "delete":
        old_id = _old_event_id(change)
        if old_id is not None:
            cur.execute(DELETE_SQL, (old_id,))
        return kind
    return None  # truncate 等：只处理行变更


def _row_magnitudes(change):
    """返回 (event_id, 行内震级)；delete、缺列、或震例号被改（非同一震例号）时返回 None。"""
    kind = change.get("kind")
    if kind not in ("insert", "update"):
        return None
    names = change.get("columnnames") or []
    if "event_id" not in names or "magnitude" not in names:
        return None
    values = change["columnvalues"]
    eid = values[names.index("event_id")]
    if eid is None:
        return None
    if kind == "update":
        old_id = _old_event_id(change)
        if old_id is not None and old_id != eid:
            return None           # 震例号本身被改：不是“同一震例号”，不做前后比较
    return eid, float(values[names.index("magnitude")])


def record_magnitude_anomalies(cur, payload, lsn, xid=None):
    """在应用本笔事务的行变更**之前**调用：比较事务前后震级，留存突变记录。

    - 事务前值取镜像当前已提交状态（即上一笔事务应用后的值）；
    - 事务后值取本笔事务内该震例最后一条 insert/update 的震级 ——
      同一震例被反复更正也只按最终值记录一次；
    - 新增（事务前不存在）与删除不算震级骤变，不记录；
    - 差值严格“达到” 2.0 才记录（含浮点容差，避免 6.5-4.5 之类的表示误差漏记）。
    """
    finals = {}                       # event_id -> 事务后最终震级（保持出现顺序）
    for change in payload.get("change", []):
        if change.get("kind") == "delete":
            # 本笔事务最终删除了该震例：没有“事务后震级”，不构成震级骤变
            finals.pop(_old_event_id(change), None)
            continue
        row = _row_magnitudes(change)
        if row is not None:
            eid, mag = row
            finals.pop(eid, None)     # 移到末尾，保留“最后一次更正”的顺序
            finals[eid] = mag

    if not finals:
        return []

    recorded = []
    detected_at = utcnow()
    for eid, new_mag in finals.items():
        row = cur.execute(
            "SELECT magnitude FROM events WHERE event_id=?", (eid,)
        ).fetchone()
        if row is None:               # 本笔事务中新插入的震例：没有“事务前震级”
            continue
        old_mag = float(row[0])
        delta = abs(new_mag - old_mag)
        if delta + 1e-9 < ANOMALY_THRESHOLD:
            continue
        cur.execute(
            "INSERT OR IGNORE INTO magnitude_anomalies"
            "(lsn, xid, event_id, old_magnitude, new_magnitude, delta, detected_at)"
            " VALUES(?, ?, ?, ?, ?, ?, ?)",
            (lsn, None if xid is None else str(xid), eid,
             old_mag, new_mag, round(delta, 6), detected_at),
        )
        if cur.rowcount:              # 重发事务的重复记录被忽略，不算新记录
            recorded.append({
                "event_id": eid,
                "old_magnitude": old_mag,
                "new_magnitude": new_mag,
                "delta": round(delta, 6),
            })
    return recorded


def apply_transaction(cur, payload):
    """把一笔源事务的全部行变更应用到（调用方持有的）同一个 SQLite 事务里。

    调用方负责提交/回滚 —— 整笔源事务要么整笔可见，要么完全不可见。
    """
    counts = {"insert": 0, "update": 0, "delete": 0, "other": 0}
    for change in payload.get("change", []):
        kind = apply_change(cur, change)
        counts[kind if kind else "other"] += 1
    return counts
