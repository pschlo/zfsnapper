from __future__ import annotations
from typing import Any, Callable, Optional
from collections.abc import Collection
from dataclasses import dataclass
import random
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re
import logging

from zfsnapper.common.zfs import Snapshot
from zfsnapper.common.sort import sortkey_snap_by_time


log = logging.getLogger(__name__)


@dataclass
class CountBucket:
    count: int
    func: Callable[[datetime], int]
    last: int

@dataclass
class WithinBucket:
    within: relativedelta
    func: Callable[[datetime], int]
    last: int


@dataclass
class KeepPolicy:
    last: int = 0
    hourly: int = 0
    daily: int = 0
    weekly: int = 0
    monthly: int = 0
    yearly: int = 0

    within: relativedelta = relativedelta()
    within_hourly: relativedelta = relativedelta()
    within_daily: relativedelta = relativedelta()
    within_weekly: relativedelta = relativedelta()
    within_monthly: relativedelta = relativedelta()
    within_yearly: relativedelta = relativedelta()

    name: Optional[re.Pattern] = None
    tags: frozenset[str] = frozenset()


def unique_bucket(_: datetime) -> int:
    return random.getrandbits(128)

def hour_bucket(date: datetime) -> int:
    return date.year*1_000_000 + date.month*10_000 + date.day*100 + date.hour

def day_bucket(date: datetime) -> int:
    return date.year*10_000 + date.month*100 + date.day

def week_bucket(date: datetime) -> int:
    year, week, _ = date.isocalendar()
    return year*100 + week

def month_bucket(date: datetime) -> int:
    return date.year*100 + date.month

def year_bucket(date: datetime) -> int:
    return date.year


"""
Returns tuple (keep, destroy)
Keeps snapshot ordering intact
"""
def apply_policy(snapshots: Collection[Snapshot], policy: KeepPolicy) -> tuple[list[Snapshot], list[Snapshot]]:
    # All snapshots, sorted from latest to oldest. Sorting is important for the algorithm to work correctly.
    snaps = sorted(snapshots, key=sortkey_snap_by_time, reverse=True)
    keep: set[Snapshot] = set()
    destroy: set[Snapshot] = set()

    count_buckets: list[CountBucket] = [
        CountBucket(policy.last, unique_bucket, -1),
        CountBucket(policy.hourly, hour_bucket, -1),
        CountBucket(policy.daily, day_bucket, -1),
        CountBucket(policy.weekly, week_bucket, -1),
        CountBucket(policy.monthly, month_bucket, -1),
        CountBucket(policy.yearly, year_bucket, -1)
    ]

    within_buckets: list[WithinBucket] = [
        WithinBucket(policy.within, unique_bucket, -1),
        WithinBucket(policy.within_hourly, hour_bucket, -1),
        WithinBucket(policy.within_daily, day_bucket, -1),
        WithinBucket(policy.within_weekly, week_bucket, -1),
        WithinBucket(policy.within_monthly, month_bucket, -1),
        WithinBucket(policy.within_yearly, year_bucket, -1)
    ]

    for snap in snaps:
        keep_snap = False

        # keep matching name
        if policy.name is not None and policy.name.fullmatch(snap.shortname):
            keep_snap = True

        # keep matching tag
        if policy.tags:
            if snap.tags is None:
                log.warning(f"Snapshot {snap.longname} was created externally and will be kept regardless of keep-tag policy")
                keep_snap = True
            else:
                for tag in policy.tags:
                    if tag in snap.tags:
                        keep_snap = True

        # keep count-based
        for bucket in count_buckets:
            if bucket.count == 0:
                continue
            value = bucket.func(snap.timestamp)
            if value != bucket.last:
                keep_snap = True
                bucket.last = value
                if bucket.count > 0:
                    bucket.count -= 1

        # keep duration-based
        now = datetime.now()
        for bucket in within_buckets:
            if snap.timestamp <= now - bucket.within:
                # snap too old
                continue
            value = bucket.func(snap.timestamp)
            if value != bucket.last:
                keep_snap = True
                bucket.last = value

        if keep_snap:
            keep.add(snap)
        else:
            destroy.add(snap)

    return [s for s in snapshots if s in keep], [s for s in snapshots if s in destroy]
