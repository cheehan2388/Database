from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from .models import BarRecord, IngestionJob, QualityIssueRecord
from .time_utils import ensure_utc, to_naive_utc


def _build_issue(
    *,
    job: IngestionJob,
    run_id,
    issue_type: str,
    severity: str,
    detail: dict[str, Any],
    issue_start_time: datetime | None = None,
    issue_end_time: datetime | None = None,
    expected_count: int | None = None,
    actual_count: int | None = None,
) -> QualityIssueRecord:
    return QualityIssueRecord(
        exchange=job.exchange,
        symbol=job.symbol,
        market_type=job.market_type,
        bar_interval=job.bar_interval,
        issue_type=issue_type,
        issue_start_time=to_naive_utc(issue_start_time),
        issue_end_time=to_naive_utc(issue_end_time),
        expected_count=expected_count,
        actual_count=actual_count,
        severity=severity,
        detail=detail,
        run_id=run_id,
    )


def build_quality_issues(
    *,
    job: IngestionJob,
    run_id,
    bars: list[BarRecord],
    interval_delta: timedelta,
    previous_open_time: datetime | None,
    target_last_open_time: datetime | None,
    duplicate_open_times: list[datetime],
    late_data_intervals: int,
) -> list[QualityIssueRecord]:
    issues: list[QualityIssueRecord] = []
    ordered_bars = sorted(bars, key=lambda item: item.open_time)

    if duplicate_open_times:
        normalized_timestamps = [to_naive_utc(item) for item in sorted(duplicate_open_times)]
        issues.append(
            _build_issue(
                job=job,
                run_id=run_id,
                issue_type="duplicate_bar",
                severity="warning",
                detail={
                    "duplicate_count": len(normalized_timestamps),
                    "duplicate_open_times": [
                        timestamp.isoformat() for timestamp in normalized_timestamps[:20]
                    ],
                },
                issue_start_time=normalized_timestamps[0],
                issue_end_time=normalized_timestamps[-1],
                expected_count=len(normalized_timestamps),
                actual_count=0,
            )
        )

    invalid_ohlc_times: list[datetime] = []
    negative_volume_times: list[datetime] = []
    for bar in ordered_bars:
        if bar.high < bar.low or bar.open < bar.low or bar.open > bar.high or bar.close < bar.low or bar.close > bar.high:
            invalid_ohlc_times.append(bar.open_time)
        if bar.volume < 0 or (bar.quote_volume is not None and bar.quote_volume < 0):
            negative_volume_times.append(bar.open_time)

    if invalid_ohlc_times:
        issues.append(
            _build_issue(
                job=job,
                run_id=run_id,
                issue_type="invalid_ohlc",
                severity="error",
                detail={
                    "affected_count": len(invalid_ohlc_times),
                    "open_times": [to_naive_utc(value).isoformat() for value in invalid_ohlc_times[:20]],
                },
                issue_start_time=min(invalid_ohlc_times),
                issue_end_time=max(invalid_ohlc_times),
                actual_count=len(invalid_ohlc_times),
            )
        )

    if negative_volume_times:
        issues.append(
            _build_issue(
                job=job,
                run_id=run_id,
                issue_type="negative_volume",
                severity="error",
                detail={
                    "affected_count": len(negative_volume_times),
                    "open_times": [to_naive_utc(value).isoformat() for value in negative_volume_times[:20]],
                },
                issue_start_time=min(negative_volume_times),
                issue_end_time=max(negative_volume_times),
                actual_count=len(negative_volume_times),
            )
        )

    previous_reference = ensure_utc(previous_open_time)
    for bar in ordered_bars:
        current_open = ensure_utc(bar.open_time)
        if previous_reference is not None and current_open is not None:
            gap_delta = current_open - previous_reference
            if gap_delta > interval_delta:
                missing_count = int(gap_delta // interval_delta) - 1
                gap_start = previous_reference + interval_delta
                gap_end = current_open - interval_delta
                issues.append(
                    _build_issue(
                        job=job,
                        run_id=run_id,
                        issue_type="missing_bar",
                        severity="warning",
                        detail={
                            "gap_from": to_naive_utc(gap_start).isoformat(),
                            "gap_to": to_naive_utc(gap_end).isoformat(),
                            "missing_count": missing_count,
                        },
                        issue_start_time=gap_start,
                        issue_end_time=gap_end,
                        expected_count=missing_count,
                        actual_count=0,
                    )
                )
        previous_reference = current_open

    latest_observed = ordered_bars[-1].open_time if ordered_bars else previous_open_time
    latest_observed_utc = ensure_utc(latest_observed)
    target_last_open_utc = ensure_utc(target_last_open_time)
    allowed_lag = interval_delta * late_data_intervals

    if (
        job.fetch_mode == "incremental"
        and latest_observed_utc is not None
        and target_last_open_utc is not None
    ):
        lag = target_last_open_utc - latest_observed_utc
        if lag > allowed_lag:
            issues.append(
                _build_issue(
                    job=job,
                    run_id=run_id,
                    issue_type="late_data",
                    severity="warning",
                    detail={
                        "latest_observed_open_time": to_naive_utc(latest_observed_utc).isoformat(),
                        "expected_latest_open_time": to_naive_utc(target_last_open_utc).isoformat(),
                        "lag_seconds": int(lag.total_seconds()),
                    },
                    issue_start_time=latest_observed_utc,
                    issue_end_time=target_last_open_utc,
                    expected_count=1,
                    actual_count=0,
                )
            )

    return issues
