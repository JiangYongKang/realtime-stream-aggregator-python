"""Low-level durability: torn trailing WAL bytes from a hard crash are ignored
deterministically; snapshot + suffix replay reproduces the same aggregate."""

import json

from streamagg.aggregator import Aggregator
from streamagg.config import Config
from streamagg.models import Event
from streamagg.storage import WalStorage


def test_torn_wal_tail_is_ignored(tmp_path):
    cfg = Config(data_dir=str(tmp_path), window_ms=1000,
                 allowed_lateness_ms=500, retention_windows=10)
    st = WalStorage(cfg)
    agg = Aggregator(cfg)
    for eid, ts in [("e1", 100), ("e2", 200), ("e3", 1500)]:
        st.append_event(Event(eid, "t", "s", ts).__dict__)
    st.close()

    # simulate a torn append: half-written JSON line at the end
    with open(tmp_path / "wal.jsonl", "ab") as f:
        f.write(b'{"seq": 4, "type": "even')

    st2 = WalStorage(cfg)
    recs = st2.read_records()
    print("\nrecovered seq numbers after torn append:", [r["seq"] for r in recs])
    assert [r["seq"] for r in recs] == [1, 2, 3]

    agg2 = Aggregator(cfg)
    for r in recs:
        agg2.apply(Event(**r["event"]))
    wins = agg2.get_or_create("t/s").list_windows()
    print("rebuilt windows:",
          [(w.window_start, w.count) for w in wins])
    assert {w.window_start: w.count for w in wins} == {0: 2, 1000: 1}


def test_snapshot_plus_wal_suffix_replay(tmp_path):
    cfg = Config(data_dir=str(tmp_path), window_ms=1000,
                 allowed_lateness_ms=500, retention_windows=10)
    st = WalStorage(cfg)
    agg = Aggregator(cfg)
    # first two events snapshotted
    for eid, ts in [("e1", 100), ("e2", 200)]:
        st.append_event(Event(eid, "t", "s", ts).__dict__)
    for r in st.read_records():
        agg.apply(Event(**r["event"]))
    st.save_snapshot({"seq": 2, "aggregator": agg.snapshot(), "offsets": {}})
    st.truncate(2)
    # third event arrives after snapshot
    st.append_event(Event("e3", "t", "s", 1500).__dict__)

    # fresh process: snapshot + suffix
    st2 = WalStorage(cfg)
    snap = st2.load_snapshot()
    agg2 = Aggregator(cfg)
    agg2.restore(snap["aggregator"])
    base = snap["seq"]
    for r in st2.read_records():
        if r["seq"] > base:
            agg2.apply(Event(**r["event"]))
    wins = agg2.get_or_create("t/s").list_windows()
    print("\nsnapshot+suffix windows:",
          [(w.window_start, w.count) for w in wins])
    assert {w.window_start: w.count for w in wins} == {0: 2, 1000: 1}
