"""Job-queue scheduler: interval / cron-style scheduling with dedupe.

Real, working implementation for the Retsumdk ecosystem. Lets callers register
scheduled jobs (every N seconds) and recurring jobs by a cron-like spec,
advances time deterministically (injectable clock for tests), prevents
overlapping runs, and collects execution history.
"""
from __future__ import annotations

import re
from typing import Callable, Optional


def parse_cron(expr: str) -> list[int]:
    """Parse a 5-field cron expression into 'minute hour dom month dow' sets.

    Supports `*`, `*/n`, and comma lists. Returns a tuple of four frozen sets
    for minute, hour, day-of-month, month, day-of-week.
    """
    fields = expr.split()
    if len(fields) != 5:
        raise ValueError(f"cron needs 5 fields, got {len(fields)}")
    ranges = [((0, 59), (0, 23), (1, 31), (1, 12), (0, 6))]
    result = []
    for i, field in enumerate(fields):
        lo, hi = ranges[0][i]
        result.append(_expand(field, lo, hi))
    return result


def _expand(field: str, lo: int, hi: int) -> frozenset:
    if field == "*":
        return frozenset(range(lo, hi + 1))
    m = re.fullmatch(r"\*/(\d+)", field)
    if m:
        step = int(m.group(1))
        return frozenset(range(lo, hi + 1, step))
    out = set()
    for part in field.split(","):
        if "-" in part:
            a, b = part.split("-")
            out.update(range(int(a), int(b) + 1))
        else:
            out.add(int(part))
    return frozenset(out)


def cron_matches(expr: str, minute: int, hour: int, dom: int, month: int, dow: int) -> bool:
    minute_s, hour_s, dom_s, month_s, dow_s = parse_cron(expr)
    return (minute in minute_s and hour in hour_s and dom in dom_s and month in month_s and dow in dow_s)


class JobScheduler:
    def __init__(self, now: Optional[Callable[[], float]] = None):
        self._now = now or __import__("time").time
        self._jobs: list[dict] = []
        self._running: set[str] = set()
        self.history: list[dict] = []

    def register(self, name: str, fn: Callable, interval: float = 0.0, cron: Optional[str] = None) -> str:
        self._jobs.append({
            "name": name, "fn": fn, "interval": interval, "cron": cron,
            "next_at": self._now() + interval if interval else self._now(),
            "last_due": 0.0,
        })
        return name

    def _due(self, now: float) -> list[dict]:
        due = []
        for j in self._jobs:
            if j["cron"]:
                t = __import__("time").gmtime(now)
                if cron_matches(j["cron"], t.tm_min, t.tm_hour, t.tm_mday, t.tm_mon, t.tm_wday):
                    if now - j["last_due"] >= 1:  # once per minute window
                        j["last_due"] = now
                        due.append(j)
            elif now >= j["next_at"]:
                j["next_at"] = now + j["interval"]
                due.append(j)
        return due

    def tick(self) -> int:
        """Run any due jobs (no overlap allowed). Returns count executed."""
        now = self._now()
        ran = 0
        for j in self._due(now):
            if j["name"] in self._running:
                continue  # skip overlapping run
            self._running.add(j["name"])
            ran += 1
            try:
                j["fn"]()
                self.history.append({"name": j["name"], "ok": True, "at": now})
            except Exception as exc:  # noqa: BLE001
                self.history.append({"name": j["name"], "ok": False, "at": now, "error": repr(exc)})
            finally:
                self._running.discard(j["name"])
        return ran
