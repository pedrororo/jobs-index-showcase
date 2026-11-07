# Sample: lifecycle snapshots and Kaplan-Meier on fake data

A small version of two ideas from the Jobs Index pipeline. Everything here is
made up: "Fake Company 01" to "25", one fake ATS board and one fake RSS feed.
Python 3.10+, standard library only (SQLite for the SQL).

```bash
python3 sample/run_sample.py
python3 -m unittest discover -s sample -v
```

| File | What it shows |
| --- | --- |
| `fake_market.py` | A deterministic market: a board that lists every open job, a feed that lists only the last 2 days and is added on day 5, and one day when the board fetch fails. |
| `lifecycle.py` | Identity and content hashes, one immutable snapshot per run (written with mode `x`, so a rerun cannot overwrite it), events per change, the reason each job opened, and the close rule: 3 misses **and** 36 h since the first miss, never on a failed source. |
| `kaplan_meier.py` | The Kaplan-Meier median in SQL with `exp(sum(ln(1 - closed / at_risk)))`, a plain Python version to check it, and the naive closed-only median. |
| `test_sample.py` | A hand-computed Kaplan-Meier case, the `ln(0)` edge case, reruns on one day not closing a job, a failed source not counting as a miss. |

What the output shows: the closed-only median is shorter than the
Kaplan-Meier median, because it ignores the jobs still open; and the feed's
jobs "close" in about 2 days only because they fall off the feed.
