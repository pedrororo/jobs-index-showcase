"""A deterministic fake job market. No real companies, postings or sources."""

import random
from dataclasses import dataclass


@dataclass
class FakeJob:
    source: str
    company: str
    external_id: str
    title: str
    location: str
    opens_day: int
    closes_day: int | None  # None: still open at the end of the simulation
    updated_day: int | None  # the day the title changes, if it does


TITLES = ["Data Engineer", "Analytics Engineer", "Backend Developer", "Product Designer", "Accountant"]
LOCATIONS = ["Remote, LATAM", "Remote, Worldwide", "Sao Paulo, BR", "Lisbon, PT"]

# A board lists every open job; a feed lists only the last few days.
BOARD = "fake_ats_board"
FEED = "fake_rss_feed"
FEED_STARTS_DAY = 5  # the feed is added later: its first jobs are a coverage change
FEED_WINDOW_DAYS = 2
BOARD_FAILS_DAY = 12  # one run where the board fetch fails


def generate(days=30, n_jobs=400, seed=7):
    rng = random.Random(seed)
    jobs = []
    for i in range(n_jobs):
        source = BOARD if rng.random() < 0.7 else FEED
        first_day = FEED_STARTS_DAY if source == FEED else 0
        # A quarter of the jobs already exist when collection starts.
        opens = first_day if rng.random() < 0.25 else rng.randint(first_day, days - 1)
        lasts = max(1, int(rng.expovariate(1 / 20)))
        closes = opens + lasts if opens + lasts < days else None
        updated = opens + rng.randint(1, lasts) if rng.random() < 0.15 else None
        jobs.append(FakeJob(
            source=source,
            company=f"Fake Company {rng.randint(1, 25):02d}",
            external_id=f"{i:05d}",
            title=rng.choice(TITLES),
            location=rng.choice(LOCATIONS),
            opens_day=opens,
            closes_day=closes,
            updated_day=updated,
        ))
    return jobs


def listing(jobs, day):
    """What each source returns on `day`: ({source: [rows]}, {source: healthy})."""
    by_source = {BOARD: [], FEED: []}
    health = {BOARD: day != BOARD_FAILS_DAY, FEED: True}
    if day < FEED_STARTS_DAY:
        del by_source[FEED], health[FEED]
    for job in jobs:
        if job.source not in by_source or not health[job.source]:
            continue
        is_open = job.opens_day <= day and (job.closes_day is None or day < job.closes_day)
        if not is_open:
            continue
        if job.source == FEED and day - job.opens_day > FEED_WINDOW_DAYS:
            continue
        title = job.title
        if job.updated_day is not None and day >= job.updated_day:
            title = "Senior " + title
        by_source[job.source].append({
            "source": job.source,
            "company": job.company,
            "external_id": job.external_id,
            "title": title,
            "location": job.location,
        })
    return by_source, health
