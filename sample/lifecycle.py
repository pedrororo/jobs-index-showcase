"""Lifecycle tracking with immutable snapshots, as in the real pipeline (simplified).

Each run writes one snapshot file that is never rewritten, and one event per
change. The state keeps first_seen_at, last_seen_at and the status of every job.
"""

import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path

CLOSE_AFTER_MISSES = 3
MIN_CLOSE_HOURS = 36

OPEN = "OPEN"
POSSIBLY_CLOSED = "POSSIBLY_CLOSED"
CLOSED = "CLOSED"


def _sha(text):
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def identity_hash(row):
    """Stable across runs: which posting this is."""
    return _sha(f"{row['source']}|{row['company']}|{row['external_id']}")


def content_hash(row):
    """Changes when a field we track changes."""
    return _sha(f"{row['title']}|{row['location']}")


class LifecycleTracker:
    def __init__(self, out_dir):
        self.out_dir = Path(out_dir)
        (self.out_dir / "snapshots").mkdir(parents=True, exist_ok=True)
        self.state = {}
        self.events = []
        self.known_sources = set()
        self.runs = 0

    def observe_run(self, run_at, rows_by_source, source_health):
        run_id = run_at.strftime("%Y%m%dT%H%M")
        self._write_snapshot(run_id, rows_by_source)
        first_run = self.runs == 0
        seen = set()

        for source, rows in rows_by_source.items():
            if not source_health.get(source, False):
                continue
            new_cohort = source not in self.known_sources
            self.known_sources.add(source)
            for row in rows:
                job_id = identity_hash(row)
                seen.add(job_id)
                self._seen(job_id, row, run_at, first_run, new_cohort)

        for job_id, rec in self.state.items():
            if job_id in seen or rec["status"] == CLOSED:
                continue
            # A failed source says nothing about its jobs: keep them as they are.
            if not source_health.get(rec["source"], False):
                continue
            self._missed(job_id, rec, run_at)

        self.runs += 1

    def _write_snapshot(self, run_id, rows_by_source):
        # Mode "x" refuses to overwrite: a snapshot, once written, is immutable.
        path = self.out_dir / "snapshots" / f"run={run_id}.jsonl"
        with path.open("x") as f:
            for rows in rows_by_source.values():
                for row in rows:
                    f.write(json.dumps({**row, "job_id": identity_hash(row),
                                        "content_hash": content_hash(row)}) + "\n")

    def _event(self, kind, job_id, run_at, **extra):
        self.events.append({"event": kind, "job_id": job_id, "at": run_at.isoformat(), **extra})

    def _seen(self, job_id, row, run_at, first_run, new_cohort):
        rec = self.state.get(job_id)
        if rec is None:
            reason = "BASELINE" if first_run else "NEW_COHORT" if new_cohort else "NEW_POSTING"
            self.state[job_id] = {
                "source": row["source"], "status": OPEN, "open_reason": reason,
                "content_hash": content_hash(row), "first_seen_at": run_at,
                "last_seen_at": run_at, "misses": 0, "first_missed_at": None,
            }
            self._event("JOB_OPENED", job_id, run_at, open_reason=reason)
            return
        if rec["status"] == POSSIBLY_CLOSED:
            self._event("JOB_RETURNED", job_id, run_at)
        elif rec["status"] == CLOSED:
            self._event("JOB_REOPENED", job_id, run_at)
        new_hash = content_hash(row)
        if new_hash != rec["content_hash"]:
            self._event("JOB_UPDATED", job_id, run_at)
        rec.update(status=OPEN, content_hash=new_hash, last_seen_at=run_at,
                   misses=0, first_missed_at=None)

    def _missed(self, job_id, rec, run_at):
        rec["misses"] += 1
        if rec["first_missed_at"] is None:
            rec["first_missed_at"] = run_at
        waited = run_at - rec["first_missed_at"]
        if rec["misses"] >= CLOSE_AFTER_MISSES and waited >= timedelta(hours=MIN_CLOSE_HOURS):
            rec["status"] = CLOSED
            self._event("JOB_CLOSED", job_id, run_at)
        elif rec["status"] == OPEN:
            rec["status"] = POSSIBLY_CLOSED
            self._event("JOB_MISSING", job_id, run_at)


def run_dates(days, start=datetime(2026, 1, 1, 6, 17)):
    return [start + timedelta(days=d) for d in range(days)]
