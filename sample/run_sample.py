"""Run 30 fake daily collections, then measure time to close.

    python3 sample/run_sample.py [--out sample/out]
"""

import argparse
import shutil
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fake_market  # noqa: E402
from kaplan_meier import closed_only_median, km_median_python, km_median_sql  # noqa: E402
from lifecycle import CLOSED, LifecycleTracker, run_dates  # noqa: E402

DAYS = 30


def simulate(out_dir, days=DAYS):
    jobs = fake_market.generate(days=days)
    tracker = LifecycleTracker(out_dir)
    for day, run_at in enumerate(run_dates(days)):
        rows, health = fake_market.listing(jobs, day)
        tracker.observe_run(run_at, rows, health)
    return tracker


def durations(tracker, source=None):
    """(days_open, closed) for new postings only: for the others, first seen is not published."""
    return [
        ((rec["last_seen_at"] - rec["first_seen_at"]).days, int(rec["status"] == CLOSED))
        for rec in tracker.state.values()
        if rec["open_reason"] == "NEW_POSTING" and (source is None or rec["source"] == source)
    ]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=str(Path(__file__).resolve().parent / "out"))
    out = Path(parser.parse_args().out)
    shutil.rmtree(out, ignore_errors=True)

    tracker = simulate(out)
    print(f"{tracker.runs} runs, {len(tracker.state)} jobs, "
          f"{len(list((out / 'snapshots').glob('*.jsonl')))} immutable snapshots in {out}")

    print("\nEvents:")
    for kind, n in Counter(e["event"] for e in tracker.events).most_common():
        print(f"  {kind:<14} {n:>5}")
    reasons = Counter(e["open_reason"] for e in tracker.events if e["event"] == "JOB_OPENED")
    print("Opened by reason:", dict(reasons))

    print(f"\nDay {fake_market.BOARD_FAILS_DAY}: the board fetch failed. "
          "Its jobs were not marked missing that day.")

    print("\nMedian days open, new postings only:")
    print(f"  {'group':<16} {'jobs':>5} {'closed':>7} {'Kaplan-Meier':>13} {'closed only':>12}")
    for label, source in [("all", None), ("ATS board", fake_market.BOARD), ("RSS feed", fake_market.FEED)]:
        d = durations(tracker, source)
        sql, py = km_median_sql(d), km_median_python(d)
        assert sql == py, (sql, py)
        km = "not reached" if sql is None else sql
        print(f"  {label:<16} {len(d):>5} {sum(c for _, c in d):>7} {km:>13} {closed_only_median(d)!s:>12}")
    print("\nFeed jobs 'close' when they fall off the feed, not when they are filled.")


if __name__ == "__main__":
    main()
