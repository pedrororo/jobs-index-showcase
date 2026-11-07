import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import run_sample  # noqa: E402
from kaplan_meier import closed_only_median, km_median_python, km_median_sql  # noqa: E402
from lifecycle import CLOSED, OPEN, POSSIBLY_CLOSED, LifecycleTracker  # noqa: E402

T0 = datetime(2026, 1, 1, 6, 0)
JOB = {"source": "board", "company": "Fake Company 01", "external_id": "1",
       "title": "Data Engineer", "location": "Remote"}


class KaplanMeierTest(unittest.TestCase):
    # Worked by hand: day 1, 1 of 5 at risk closes -> S = 0.8;
    # day 3, 1 of 3 -> 0.533; day 4, 1 of 2 -> 0.267, the first value <= 0.5.
    HAND = [(1, 1), (2, 0), (3, 1), (4, 1), (5, 0)]

    def test_hand_computed_median(self):
        self.assertEqual(km_median_sql(self.HAND), 4)
        self.assertEqual(km_median_python(self.HAND), 4)

    def test_closed_only_median_is_shorter(self):
        self.assertEqual(closed_only_median(self.HAND), 3)

    def test_everyone_at_risk_closes(self):
        # ln(0) case: the last two jobs both close on day 3.
        data = [(1, 0), (2, 0), (3, 1), (3, 1)]
        self.assertEqual(km_median_sql(data), 3)
        self.assertEqual(km_median_python(data), 3)

    def test_median_not_reached(self):
        self.assertIsNone(km_median_sql([(1, 1), (2, 0), (3, 0)]))

    def test_sql_matches_python_on_simulation(self):
        with tempfile.TemporaryDirectory() as out:
            tracker = run_sample.simulate(out)
        for source in (None, "fake_ats_board", "fake_rss_feed"):
            d = run_sample.durations(tracker, source)
            self.assertEqual(km_median_sql(d), km_median_python(d))


class LifecycleTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.tracker = LifecycleTracker(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def run_at(self, when, rows, healthy=True):
        self.tracker.observe_run(when, {"board": rows}, {"board": healthy})

    def status(self):
        return next(iter(self.tracker.state.values()))["status"]

    def test_snapshots_are_immutable(self):
        self.run_at(T0, [JOB])
        with self.assertRaises(FileExistsError):
            self.run_at(T0, [JOB])

    def test_three_reruns_in_one_day_do_not_close(self):
        self.run_at(T0, [JOB])
        for hours in (24, 26, 28):
            self.run_at(T0 + timedelta(hours=hours), [])
        self.assertEqual(self.status(), POSSIBLY_CLOSED)

    def test_closes_after_three_daily_misses(self):
        self.run_at(T0, [JOB])
        for day in (1, 2, 3):
            self.run_at(T0 + timedelta(days=day), [])
        self.assertEqual(self.status(), CLOSED)

    def test_failed_source_does_not_count_as_a_miss(self):
        self.run_at(T0, [JOB])
        self.run_at(T0 + timedelta(days=1), [], healthy=False)
        self.assertEqual(self.status(), OPEN)

    def test_return_and_update_are_events(self):
        self.run_at(T0, [JOB])
        self.run_at(T0 + timedelta(days=1), [])
        self.run_at(T0 + timedelta(days=2), [{**JOB, "title": "Senior Data Engineer"}])
        kinds = [e["event"] for e in self.tracker.events]
        self.assertEqual(kinds, ["JOB_OPENED", "JOB_MISSING", "JOB_RETURNED", "JOB_UPDATED"])

    def test_open_reasons(self):
        self.run_at(T0, [JOB])
        later = T0 + timedelta(days=1)
        self.tracker.observe_run(later, {"board": [JOB, {**JOB, "external_id": "2"}],
                                         "new_source": [{**JOB, "source": "new_source"}]},
                                 {"board": True, "new_source": True})
        reasons = [e["open_reason"] for e in self.tracker.events if e["event"] == "JOB_OPENED"]
        self.assertEqual(reasons, ["BASELINE", "NEW_POSTING", "NEW_COHORT"])


if __name__ == "__main__":
    unittest.main()
