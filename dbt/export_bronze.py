"""Write fake bronze tables for the dbt project, from the sample's fake job market.

    python dbt/export_bronze.py [--out dbt/lakehouse]

Runs the same 30-day simulation as sample/run_sample.py and writes the four
bronze inputs the dbt models read, in the layout the real pipeline uses:

    bronze/events/run_id=<run>/part-0.parquet          one row per lifecycle event
    bronze/job_attributes/run_id=<run>/part-0.parquet  classification of each exported job
    bronze/run_health/run_id=<run>/part-0.parquet      status and volume per source
    bronze/job_state.parquet                           current state per job

No real companies, postings or sources: see sample/fake_market.py.
"""

import argparse
import shutil
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent / "sample"))

import fake_market  # noqa: E402
from lifecycle import LifecycleTracker, identity_hash, run_dates  # noqa: E402

DAYS = 30

ROLE_FAMILY = {
    "Data Engineer": ("data", "data_engineering"),
    "Analytics Engineer": ("data", "analytics_engineering"),
    "Backend Developer": ("software", "backend"),
    "Product Designer": ("design", "product_design"),
    "Accountant": ("finance", "accounting"),
}
JOB_CATEGORY = {"data": "Data", "software": "Software Development", "design": "Design", "finance": "Finance"}


def simulate(snapshot_dir):
    """Run the fake collections; keep per-run listings and health for the bronze tables."""
    jobs = fake_market.generate(days=DAYS)
    tracker = LifecycleTracker(snapshot_dir)
    runs = []
    for day, run_at in enumerate(run_dates(DAYS)):
        rows, health = fake_market.listing(jobs, day)
        tracker.observe_run(run_at, rows, health)
        runs.append((run_at, rows, health))
    return tracker, runs


def base_title(title):
    return title.removeprefix("Senior ")


def build_rows(tracker, runs):
    latest = {}  # job_id -> latest listed row
    events, attributes, health_rows = [], [], []

    for run_at, rows_by_source, health in runs:
        run_id = run_at.strftime("%Y%m%dT%H%M")
        raw_jobs = sum(len(r) for r in rows_by_source.values())
        for source, ok in health.items():
            listed = rows_by_source.get(source, [])
            health_rows.append({
                "run_id": run_id, "run_status": "SUCCESS", "started_at": run_at,
                "run_duration_s": 600.0 + raw_jobs, "raw_jobs": raw_jobs,
                "source": source, "source_status": "ok" if ok else "error",
                "source_error": "" if ok else "HTTP 503 from the fake board",
                "jobs_fetched": len(listed) if ok else 0,
            })
            for row in listed:
                job_id = identity_hash(row)
                latest[job_id] = row
                family, spec = ROLE_FAMILY[base_title(row["title"])]
                attributes.append({
                    "run_id": run_id, "job_tracking_id": job_id, "role_family": family,
                    "role_specialization": spec,
                    "seniority": "senior" if row["title"].startswith("Senior ") else "mid",
                    "workplace_type": "remote" if row["location"].startswith("Remote") else "onsite",
                    "remote_scope": row["location"].removeprefix("Remote, ") if row["location"].startswith("Remote") else None,
                    "posted_at": None, "classification_confidence": "0.9", "export_at": run_at,
                })

    misses = defaultdict(int)
    for i, e in enumerate(tracker.events):
        row = latest[e["job_id"]]
        run_at = e["at"]
        misses[e["job_id"]] = misses[e["job_id"]] + 1 if e["event"] == "JOB_MISSING" else 0
        events.append({
            "event_id": f"evt-{i:06d}", "run_id": run_at.replace("-", "").replace(":", "")[:13],
            "event_at": run_at, "collection_date": run_at[:10], "event_type": e["event"],
            "job_tracking_id": e["job_id"], "open_reason": e.get("open_reason"),
            "collector": row["source"], "company_key": row["company"].lower(),
            "company": row["company"], "title": row["title"],
            "link": f"https://example.com/{row['source']}/{row['external_id']}",
            "changed_fields": ["title"] if e["event"] == "JOB_UPDATED" else None,
            "field_changed": "title" if e["event"] == "JOB_UPDATED" else None,
            "missing_count": misses[e["job_id"]] or None, "hours_missing": None,
        })

    state = []
    for job_id, rec in tracker.state.items():
        row = latest[job_id]
        family, _ = ROLE_FAMILY[base_title(row["title"])]
        state.append({
            "job_tracking_id": job_id, "status": rec["status"], "open_reason": rec["open_reason"],
            "first_seen_at": rec["first_seen_at"], "last_seen_at": rec["last_seen_at"],
            "first_missed_at": rec["first_missed_at"],
            "closed_at": rec["last_seen_at"] if rec["status"] == "CLOSED" else None,
            "collector": rec["source"], "company_key": row["company"].lower(),
            "company": row["company"], "title": row["title"], "job_category": JOB_CATEGORY[family],
        })
    return events, attributes, health_rows, state


def write_partitioned(con, rows, folder):
    by_run = defaultdict(list)
    for r in rows:
        by_run[r["run_id"]].append(r)
    for run_id, part in by_run.items():
        target = folder / f"run_id={run_id}"
        target.mkdir(parents=True)
        write(con, [{k: v for k, v in r.items() if k != "run_id"} for r in part], target / "part-0.parquet")


def write(con, rows, path):
    import pyarrow as pa
    table = pa.Table.from_pylist(rows)
    con.register("rows_tbl", table)
    con.execute(f"COPY rows_tbl TO '{path}' (FORMAT parquet)")
    con.unregister("rows_tbl")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=str(ROOT / "lakehouse"))
    out = Path(parser.parse_args().out)
    bronze = out / "bronze"
    shutil.rmtree(out, ignore_errors=True)
    bronze.mkdir(parents=True)

    with tempfile.TemporaryDirectory() as tmp:
        tracker, runs = simulate(tmp)
    events, attributes, health, state = build_rows(tracker, runs)

    con = duckdb.connect()
    write_partitioned(con, events, bronze / "events")
    write_partitioned(con, attributes, bronze / "job_attributes")
    write_partitioned(con, health, bronze / "run_health")
    write(con, state, bronze / "job_state.parquet")
    print(f"bronze in {bronze}: {len(events)} events, {len(attributes)} attribute rows, "
          f"{len(health)} source-run rows, {len(state)} jobs")


if __name__ == "__main__":
    main()
