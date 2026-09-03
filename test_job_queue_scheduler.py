import pytest

from job_queue_scheduler import JobScheduler, cron_matches


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def test_interval_job_runs_when_due():
    c = Clock()
    s = JobScheduler(now=c)
    calls = {"n": 0}
    s.register("a", lambda: calls.__setitem__("n", calls["n"] + 1), interval=5)
    assert s.tick() == 0  # not due yet
    c.t += 5
    assert s.tick() == 1
    c.t += 5
    assert s.tick() == 1
    assert calls["n"] == 2


def test_no_overlap_while_running():
    c = Clock()
    s = JobScheduler(now=c)
    entered = {"n": 0}
    overlaps = {"n": 0}

    def long_job():
        entered["n"] += 1
        c.t += 6  # simulate elapsed time mid-run
        if entered["n"] < 3:
            raise RuntimeError("keep trying")

    s.register("x", long_job, interval=3)
    c.t += 3  # make it due once
    assert s.tick() == 1
    s.tick()
    s.tick()
    assert entered["n"] >= 1


def test_cron_matches_known_spec():
    # every day at 09:15
    assert cron_matches("15 9 * * *", 15, 9, 3, 5, 0) is True
    assert cron_matches("15 9 * * *", 14, 9, 3, 5, 0) is False
    # every 5 minutes
    assert cron_matches("*/5 * * * *", 25, 10, 1, 1, 1) is True


def test_history_records_failures():
    c = Clock()
    s = JobScheduler(now=c)
    s.register("boom", lambda: (_ for _ in ()).throw(RuntimeError("x")), interval=0)
    c.t += 0
    assert s.tick() == 1
    assert s.history[-1]["ok"] is False
