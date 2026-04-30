from __future__ import annotations

from datetime import datetime, timedelta, timezone


INTERVAL_SUFFIX_TO_SECONDS = {
    "m": 60,
    "h": 60 * 60,
    "d": 60 * 60 * 24,
    "w": 60 * 60 * 24 * 7,
}


def interval_to_timedelta(interval_value: str) -> timedelta:
    unit = interval_value[-1]
    if unit not in INTERVAL_SUFFIX_TO_SECONDS:
        raise ValueError(f"Unsupported interval: {interval_value}")

    amount = int(interval_value[:-1])
    return timedelta(seconds=amount * INTERVAL_SUFFIX_TO_SECONDS[unit])


def interval_to_milliseconds(interval_value: str) -> int:
    return int(interval_to_timedelta(interval_value).total_seconds() * 1000)


def ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_naive_utc(value: datetime | None) -> datetime | None:
    utc_value = ensure_utc(value)
    if utc_value is None:
        return None
    return utc_value.replace(tzinfo=None)


def datetime_to_milliseconds(value: datetime) -> int:
    utc_value = ensure_utc(value)
    if utc_value is None:
        raise ValueError("datetime_to_milliseconds requires a datetime value.")
    return int(utc_value.timestamp() * 1000)


def milliseconds_to_datetime(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def floor_closed_bar_open(now_value: datetime, interval_value: str) -> datetime:
    now_utc = ensure_utc(now_value)
    if now_utc is None:
        raise ValueError("now_value is required.")
    interval_ms = interval_to_milliseconds(interval_value)
    now_ms = int(now_utc.timestamp() * 1000)
    closed_boundary_ms = (now_ms // interval_ms) * interval_ms
    last_open_ms = closed_boundary_ms - interval_ms
    return milliseconds_to_datetime(last_open_ms)
