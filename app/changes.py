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

震级突变：在一笔源事务前后，对同一震例号比较镜像中的震级。若同一震例在一笔
事务里被反复更正，只取该事务开始前的值与事务内最后一次（最终）值比较，
差值的绝对值达到阈值（默认 2.0）即产出一条异常记录 —— 每笔事务每个震例至多一条。
即使之后震级被改回，已产出的记录也不会撤销。初次装载不经过本模块，故不记异常。
"""
from canonical import norm_ts

MAGNITUDE_JUMP_THRESHOLD = 2.0

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


def _change_event_id(change):
    """一条行变更最终写向哪个震例号（insert/update 取新值，delete 取旧主键）。"""
    kind = change.get("kind")
    if kind in ("insert", "update"):
        cols = dict(zip(change["columnnames"], change["columnvalues"]))
        return cols.get("event_id")
    if kind == "delete":
        return _old_event_id(change)
    return None


def _current_magnitude(cur, event_id):
    """事务开始前镜像里该震例的震级；不存在（新震例）返回 None。"""
    cur.execute("SELECT magnitude FROM events WHERE event_id = ?", (event_id,))
    row = cur.fetchone()
    return float(row[0]) if row else None


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


def apply_transaction(cur, payload, lsn=None, xid=None,
                      threshold=MAGNITUDE_JUMP_THRESHOLD):
    """把一笔源事务的全部行变更应用到（调用方持有的）同一个 SQLite 事务里。

    调用方负责提交/回滚 —— 整笔源事务要么整笔可见，要么完全不可见。

    同时比较“事务开始前”与“事务内最终”的震级：同一震例在一笔事务里被反复更正
    也只按最终值产出至多一条异常。返回的 counts["anomalies"] 由调用方
    在同一个事务里落库（见 Store.record_anomalies），保证与镜像、位置同生共死。
    """
    counts = {"insert": 0, "update": 0, "delete": 0, "other": 0, "anomalies": []}
    if xid is None:
        xid = payload.get("xid")
    # 事务开始前每个受影响震例的震级（首次遇到时读取，之后事务内的改动不影响它）
    before = {}
    for change in payload.get("change", []):
        target_id = _change_event_id(change)
        if target_id is not None and target_id not in before:
            before[target_id] = _current_magnitude(cur, target_id)
        kind = apply_change(cur, change)
        counts[kind if kind else "other"] += 1

    for event_id, old_mag in before.items():
        new_mag = _current_magnitude(cur, event_id)
        if old_mag is None or new_mag is None:      # 新震例 / 事务内被删除：无“前后”可比
            continue
        if round(abs(new_mag - old_mag), 6) >= threshold:
            counts["anomalies"].append({
                "event_id": event_id,
                "old_magnitude": old_mag,
                "new_magnitude": new_mag,
                "lsn": lsn,
                "xid": None if xid is None else str(xid),
            })
    return counts
