"""Kaplan-Meier median time to close, in SQL (SQLite) and in plain Python.

Open jobs are right-censored: they count as "lasted at least this long".
The SQL uses the same trick as the real gold model: the survival product is
exp(sum(ln(1 - closed / at_risk))) over a window ordered by days.
"""

import sqlite3
import statistics

KM_MEDIAN_SQL = """
WITH by_day AS (
    SELECT days_open, SUM(closed) AS closed_n, COUNT(*) AS jobs_n
    FROM durations
    GROUP BY days_open
),
at_risk AS (
    SELECT days_open, closed_n,
           SUM(jobs_n) OVER (ORDER BY days_open DESC) AS at_risk_n
    FROM by_day
),
survival AS (
    SELECT days_open,
           -- ln(0) is undefined: once every job at risk closed, survival is 0.
           CASE WHEN MAX(closed_n = at_risk_n) OVER (ORDER BY days_open) = 1 THEN 0.0
                ELSE exp(SUM(ln(1.0 - 1.0 * closed_n / at_risk_n)) OVER (ORDER BY days_open))
           END AS s
    FROM at_risk
    WHERE closed_n > 0
)
SELECT MIN(days_open) FROM survival WHERE s <= 0.5
"""


def km_median_sql(durations):
    """durations: [(days_open, closed 0/1)]. Returns None if survival never reaches 0.5."""
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE durations (days_open INTEGER, closed INTEGER)")
    con.executemany("INSERT INTO durations VALUES (?, ?)", durations)
    return con.execute(KM_MEDIAN_SQL).fetchone()[0]


def km_median_python(durations):
    survival = 1.0
    for day in sorted({d for d, _ in durations}):
        at_risk = sum(1 for d, _ in durations if d >= day)
        closed = sum(c for d, c in durations if d == day)
        if closed:
            survival *= 1 - closed / at_risk
            if survival <= 0.5:
                return day
    return None


def closed_only_median(durations):
    closed = [d for d, c in durations if c]
    return statistics.median(closed) if closed else None
