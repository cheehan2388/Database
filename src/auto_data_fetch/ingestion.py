from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
import json
import logging
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .config import Settings
from .db import Database
from .exchanges import create_exchange_client, resolve_market
from .models import (
    BarRecord,
    FundingRateRecord,
    IngestionJob,
    LiquidationRecord,
    OpenInterestRecord,
    RunResult,
    Watermark,
)
from .quality import build_quality_issues
from .time_utils import (
    datetime_to_milliseconds,
    ensure_utc,
    floor_closed_bar_open,
    interval_to_milliseconds,
    interval_to_timedelta,
    milliseconds_to_datetime,
    to_naive_utc,
    utc_now,
)


LOGGER = logging.getLogger(__name__)

OHLCV_DATASETS = {"kline", "mark_price_kline", "index_price_kline"}
KUCOIN_OPEN_INTEREST_URL = "https://api.kucoin.com/api/ua/v1/market/open-interest"
KUCOIN_OPEN_INTEREST_INTERVALS = {
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1hour",
    "4h": "4hour",
    "1d": "1day",
}


def _compute_request_bounds(
    job: IngestionJob,
    watermark: Watermark | None,
) -> tuple[datetime | None, datetime, datetime | None]:
    interval_delta = interval_to_timedelta(job.bar_interval)
    earliest_existing_open = ensure_utc(watermark.first_open_time) if watermark else None
    latest_existing_open = ensure_utc(watermark.last_open_time) if watermark else None
    job_start_time = ensure_utc(job.start_time)
    job_end_time = ensure_utc(job.end_time)

    if job.fetch_mode == "backfill":
        if job_start_time is None:
            raise RuntimeError(
                f"Job {job.job_name!r} is in backfill mode but start_time is empty. "
                "Set start_time to the historical lower bound you want to fetch."
            )

        if earliest_existing_open is not None:
            target_last_open_time = earliest_existing_open - interval_delta
        elif job_end_time is not None:
            target_last_open_time = job_end_time
        else:
            target_last_open_time = floor_closed_bar_open(utc_now(), job.bar_interval)

        if job_end_time is not None:
            target_last_open_time = min(target_last_open_time, job_end_time)

        request_start = job_start_time
        quality_anchor_open_time = None
    else:
        target_last_open_time = floor_closed_bar_open(utc_now(), job.bar_interval)
        if job_end_time is not None:
            target_last_open_time = min(target_last_open_time, job_end_time)

        if latest_existing_open is not None:
            request_start = latest_existing_open + interval_delta
        elif job_start_time is not None:
            request_start = job_start_time
        else:
            request_start = None
        quality_anchor_open_time = latest_existing_open

    return request_start, target_last_open_time, quality_anchor_open_time


def _fetch_ohlcv_rows(
    *,
    client,
    fetch_symbol: str,
    timeframe: str,
    request_start: datetime | None,
    target_last_open_time: datetime,
    fetch_limit: int,
    params: dict[str, Any] | None = None,
) -> list[list]:
    interval_ms = interval_to_milliseconds(timeframe)
    target_last_open_ms = datetime_to_milliseconds(target_last_open_time)
    since_ms = datetime_to_milliseconds(request_start) if request_start else None
    last_seen_open_ms: int | None = None
    raw_rows: list[list] = []

    while True:
        batch = client.fetch_ohlcv(
            fetch_symbol,
            timeframe=timeframe,
            since=since_ms,
            limit=fetch_limit,
            params=params or {},
        )
        if not batch:
            break

        clipped_batch = [row for row in batch if row[0] <= target_last_open_ms]
        raw_rows.extend(clipped_batch)

        batch_last_open_ms = batch[-1][0]
        if batch_last_open_ms >= target_last_open_ms:
            break
        if last_seen_open_ms == batch_last_open_ms:
            break
        last_seen_open_ms = batch_last_open_ms
        since_ms = batch_last_open_ms + interval_ms

    return raw_rows


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def _source_dataset_params(source_dataset: str) -> dict[str, Any]:
    if source_dataset == "mark_price_kline":
        return {"price": "mark"}
    if source_dataset == "index_price_kline":
        return {"price": "index"}
    return {}


def _normalize_rows(
    *,
    job: IngestionJob,
    raw_rows: list[list],
    run_id,
) -> tuple[list[BarRecord], list[datetime]]:
    interval_delta = interval_to_timedelta(job.bar_interval)
    rows_by_open_time: dict[datetime, BarRecord] = {}
    duplicate_open_times: list[datetime] = []

    for row in raw_rows:
        open_time = milliseconds_to_datetime(int(row[0]))
        close_time = open_time + interval_delta
        open_value = Decimal(str(row[1]))
        high_value = Decimal(str(row[2]))
        low_value = Decimal(str(row[3]))
        close_value = Decimal(str(row[4]))
        volume_value = _decimal_or_none(row[5]) or Decimal("0")

        normalized_bar = BarRecord(
            exchange=job.exchange,
            symbol=job.symbol,
            market_type=job.market_type,
            bar_interval=job.bar_interval,
            open_time=to_naive_utc(open_time),
            close_time=to_naive_utc(close_time),
            open=open_value,
            high=high_value,
            low=low_value,
            close=close_value,
            volume=volume_value,
            quote_volume=None,
            trade_count=None,
            source_dataset=job.source_dataset,
            run_id=run_id,
        )

        if normalized_bar.open_time in rows_by_open_time:
            duplicate_open_times.append(normalized_bar.open_time)
        rows_by_open_time[normalized_bar.open_time] = normalized_bar

    bars = [rows_by_open_time[key] for key in sorted(rows_by_open_time)]
    return bars, duplicate_open_times


def _run_ohlcv_job(job: IngestionJob, db: Database, settings: Settings) -> RunResult:
    watermark = db.get_watermark(job)
    request_start, target_last_open_time, quality_anchor_open_time = _compute_request_bounds(job, watermark)

    if request_start is not None and request_start > target_last_open_time:
        run_id = db.create_run_log(
            job,
            request_start=to_naive_utc(request_start),
            request_end=to_naive_utc(request_start),
        )
        db.finalize_run_log(
            run_id,
            status="skipped",
            rows_fetched=0,
            rows_inserted=0,
            rows_updated=0,
            error_message=None,
        )
        return RunResult(
            job_name=job.job_name,
            run_id=run_id,
            status="skipped",
            rows_fetched=0,
            rows_inserted=0,
            rows_updated=0,
            issue_count=0,
        )

    run_id = db.create_run_log(
        job,
        request_start=to_naive_utc(request_start),
        request_end=to_naive_utc(target_last_open_time),
    )
    client = None

    try:
        client = create_exchange_client(job, timeout_ms=settings.ccxt_timeout_ms)
        market = resolve_market(client, job)
        db.upsert_asset_registry(
            exchange=job.exchange,
            symbol=job.symbol,
            market_type=job.market_type,
            base_asset=market.base_asset,
            quote_asset=market.quote_asset,
        )

        raw_rows = _fetch_ohlcv_rows(
            client=client,
            fetch_symbol=market.fetch_symbol,
            timeframe=job.bar_interval,
            request_start=request_start,
            target_last_open_time=target_last_open_time,
            fetch_limit=settings.fetch_limit,
            params=_source_dataset_params(job.source_dataset),
        )
        bars, duplicate_open_times = _normalize_rows(job=job, raw_rows=raw_rows, run_id=run_id)
        issues = build_quality_issues(
            job=job,
            run_id=run_id,
            bars=bars,
            interval_delta=interval_to_timedelta(job.bar_interval),
            previous_open_time=quality_anchor_open_time,
            target_last_open_time=target_last_open_time,
            duplicate_open_times=duplicate_open_times,
            late_data_intervals=settings.late_data_intervals,
        )

        rows_inserted = db.insert_market_bars(bars)
        db.insert_quality_issues(issues)

        status = "partial_success" if any(issue.severity in {"error", "critical"} for issue in issues) else "success"
        db.finalize_run_log(
            run_id,
            status=status,
            rows_fetched=len(raw_rows),
            rows_inserted=rows_inserted,
            rows_updated=0,
            error_message=None,
        )

        return RunResult(
            job_name=job.job_name,
            run_id=run_id,
            status=status,
            rows_fetched=len(raw_rows),
            rows_inserted=rows_inserted,
            rows_updated=0,
            issue_count=len(issues),
        )
    except Exception as exc:
        LOGGER.exception("Job %s failed.", job.job_name)
        db.finalize_run_log(
            run_id,
            status="failed",
            rows_fetched=0,
            rows_inserted=0,
            rows_updated=0,
            error_message=str(exc),
        )
        raise
    finally:
        if client is not None and hasattr(client, "close"):
            try:
                client.close()
            except Exception:  # pragma: no cover
                LOGGER.debug("Ignoring client close failure.", exc_info=True)


def _compute_simple_bounds(job: IngestionJob, watermark: Watermark | None) -> tuple[datetime | None, datetime]:
    target_end = ensure_utc(job.end_time) or utc_now()
    if job.fetch_mode == "backfill":
        if job.start_time is None:
            raise RuntimeError(f"Job {job.job_name!r} requires start_time for backfill mode.")
        return ensure_utc(job.start_time), target_end

    if watermark and watermark.last_open_time is not None:
        return ensure_utc(watermark.last_open_time), target_end
    return ensure_utc(job.start_time), target_end


def _finalize_success(
    db: Database,
    run_id,
    job_name: str,
    rows_fetched: int,
    rows_inserted: int,
) -> RunResult:
    db.finalize_run_log(
        run_id,
        status="success",
        rows_fetched=rows_fetched,
        rows_inserted=rows_inserted,
        rows_updated=0,
        error_message=None,
    )
    return RunResult(
        job_name=job_name,
        run_id=run_id,
        status="success",
        rows_fetched=rows_fetched,
        rows_inserted=rows_inserted,
        rows_updated=0,
        issue_count=0,
    )


def _kucoin_open_interest_interval(interval: str) -> str:
    try:
        return KUCOIN_OPEN_INTEREST_INTERVALS[interval]
    except KeyError as exc:
        raise RuntimeError(f"KuCoin open interest does not support interval={interval!r}.") from exc


def _kucoin_open_interest_market_id(client, fetch_symbol: str) -> str:
    market = client.markets.get(fetch_symbol)
    if market is None:
        try:
            market = client.market(fetch_symbol)
        except Exception:
            market = None
    if market is None:
        for candidate in client.markets.values():
            if candidate.get("symbol") == fetch_symbol:
                market = candidate
                break
    if market is None or not market.get("id"):
        raise RuntimeError(f"Could not resolve KuCoin market id for {fetch_symbol!r}.")
    return str(market["id"])


def _fetch_kucoin_open_interest_rows(
    *,
    market_id: str,
    interval: str,
    request_start: datetime | None,
    request_end: datetime,
    fetch_limit: int,
    timeout_ms: int,
) -> list[dict[str, Any]]:
    api_interval = _kucoin_open_interest_interval(interval)
    retention_days = 70 if api_interval == "1day" else 7
    retention_start = request_end - timedelta(days=retention_days)
    effective_start = max(request_start, retention_start) if request_start else retention_start
    if effective_start > request_end:
        return []

    start_ms = datetime_to_milliseconds(effective_start)
    end_ms = datetime_to_milliseconds(request_end)
    page_size = max(1, min(fetch_limit, 200))
    timeout_seconds = max(1, timeout_ms / 1000)
    rows: list[dict[str, Any]] = []
    previous_oldest_ms: int | None = None

    while True:
        params = {
            "symbol": market_id,
            "interval": api_interval,
            "startAt": start_ms,
            "endAt": end_ms,
            "pageSize": page_size,
        }
        request = Request(
            f"{KUCOIN_OPEN_INTEREST_URL}?{urlencode(params)}",
            headers={"User-Agent": "auto_data_fetch/0.1"},
        )
        with urlopen(request, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))

        if payload.get("code") != "200000":
            raise RuntimeError(f"KuCoin open interest API failed: {payload}")

        batch = payload.get("data") or []
        if not batch:
            break

        clipped_batch = [
            row
            for row in batch
            if row.get("ts") is not None and start_ms <= int(row["ts"]) <= end_ms
        ]
        rows.extend(clipped_batch)

        timestamp_values = [int(row["ts"]) for row in batch if row.get("ts") is not None]
        if not timestamp_values:
            break
        oldest_ms = min(timestamp_values)
        if oldest_ms <= start_ms:
            break
        if previous_oldest_ms == oldest_ms:
            break
        if len(batch) < page_size:
            break

        previous_oldest_ms = oldest_ms
        end_ms = oldest_ms - 1

    return rows


def _run_open_interest_job(job: IngestionJob, db: Database, settings: Settings) -> RunResult:
    watermark = db.get_open_interest_watermark(job)
    request_start, request_end = _compute_simple_bounds(job, watermark)
    if job.exchange == "binance" and request_start is not None:
        # Binance futures data endpoints only retain a recent window for open-interest history.
        request_start = max(request_start, request_end - timedelta(days=29))
    run_id = db.create_run_log(job, to_naive_utc(request_start), to_naive_utc(request_end))
    client = None
    try:
        client = create_exchange_client(job, timeout_ms=settings.ccxt_timeout_ms)
        market = resolve_market(client, job)
        if job.exchange == "kucoinfutures":
            rows = _fetch_kucoin_open_interest_rows(
                market_id=_kucoin_open_interest_market_id(client, market.fetch_symbol),
                interval=job.bar_interval,
                request_start=request_start,
                request_end=request_end,
                fetch_limit=settings.fetch_limit,
                timeout_ms=settings.ccxt_timeout_ms,
            )
            records = [
                OpenInterestRecord(
                    exchange=job.exchange,
                    symbol=job.symbol,
                    market_type=job.market_type,
                    bar_interval=job.bar_interval,
                    open_time=to_naive_utc(milliseconds_to_datetime(int(row["ts"]))),
                    open_interest_amount=_decimal_or_none(row.get("openInterest")),
                    open_interest_value=None,
                    base_volume=None,
                    quote_volume=None,
                    source_dataset=job.source_dataset,
                    run_id=run_id,
                )
                for row in rows
                if row.get("ts") is not None
            ]
            return _finalize_success(db, run_id, job.job_name, len(rows), db.insert_open_interest(records))

        if not client.has.get("fetchOpenInterestHistory"):
            db.finalize_run_log(run_id, status="skipped", rows_fetched=0, rows_inserted=0, rows_updated=0, error_message="fetchOpenInterestHistory is not supported")
            return RunResult(job.job_name, run_id, "skipped", 0, 0, 0, 0)

        interval_ms = interval_to_milliseconds(job.bar_interval)
        target_end_ms = datetime_to_milliseconds(request_end)
        limit = min(settings.fetch_limit, 500) if job.exchange == "binance" else settings.fetch_limit
        since_ms = datetime_to_milliseconds(request_start) if request_start else None
        rows = []
        last_seen_ms: int | None = None
        while True:
            batch = client.fetch_open_interest_history(market.fetch_symbol, job.bar_interval, since_ms, limit)
            if not batch:
                break
            clipped_batch = [row for row in batch if row.get("timestamp") is not None and int(row["timestamp"]) <= target_end_ms]
            rows.extend(clipped_batch)
            batch_last_ms = int(batch[-1]["timestamp"]) if batch[-1].get("timestamp") is not None else None
            if batch_last_ms is None or batch_last_ms >= target_end_ms:
                break
            if last_seen_ms == batch_last_ms:
                break
            last_seen_ms = batch_last_ms
            since_ms = batch_last_ms + interval_ms
        records = [
            OpenInterestRecord(
                exchange=job.exchange,
                symbol=job.symbol,
                market_type=job.market_type,
                bar_interval=job.bar_interval,
                open_time=to_naive_utc(milliseconds_to_datetime(int(row["timestamp"]))),
                open_interest_amount=_decimal_or_none(row.get("openInterestAmount")),
                open_interest_value=_decimal_or_none(row.get("openInterestValue")),
                base_volume=_decimal_or_none(row.get("baseVolume")),
                quote_volume=_decimal_or_none(row.get("quoteVolume")),
                source_dataset=job.source_dataset,
                run_id=run_id,
            )
            for row in rows
            if row.get("timestamp") is not None
        ]
        return _finalize_success(db, run_id, job.job_name, len(rows), db.insert_open_interest(records))
    except Exception as exc:
        LOGGER.exception("Job %s failed.", job.job_name)
        db.finalize_run_log(run_id, status="failed", rows_fetched=0, rows_inserted=0, rows_updated=0, error_message=str(exc))
        raise
    finally:
        if client is not None and hasattr(client, "close"):
            client.close()


def _run_funding_rate_job(job: IngestionJob, db: Database, settings: Settings) -> RunResult:
    watermark = db.get_event_watermark(job, "funding_rate_history", "funding_time")
    request_start, request_end = _compute_simple_bounds(job, watermark)
    run_id = db.create_run_log(job, to_naive_utc(request_start), to_naive_utc(request_end))
    client = None
    try:
        client = create_exchange_client(job, timeout_ms=settings.ccxt_timeout_ms)
        market = resolve_market(client, job)
        if not client.has.get("fetchFundingRateHistory"):
            db.finalize_run_log(run_id, status="skipped", rows_fetched=0, rows_inserted=0, rows_updated=0, error_message="fetchFundingRateHistory is not supported")
            return RunResult(job.job_name, run_id, "skipped", 0, 0, 0, 0)

        since_ms = datetime_to_milliseconds(request_start) + 1 if request_start else None
        params: dict[str, Any] = {}
        if job.exchange == "kucoinfutures":
            # KuCoin's ccxt adapter can request past the current time and return
            # data=None when there is no new funding print. Bound the request and
            # normalize that empty response instead of crashing the live loop.
            params["until"] = datetime_to_milliseconds(request_end)
        try:
            rows = client.fetch_funding_rate_history(
                market.fetch_symbol,
                since_ms,
                settings.fetch_limit,
                params,
            )
        except TypeError as exc:
            if job.exchange == "kucoinfutures" and "NoneType" in str(exc):
                rows = []
            else:
                raise
        rows = rows or []
        records: list[FundingRateRecord] = []
        for row in rows:
            timestamp = row.get("timestamp")
            if timestamp is None or row.get("fundingRate") is None:
                continue
            info = row.get("info") or {}
            records.append(
                FundingRateRecord(
                    exchange=job.exchange,
                    symbol=job.symbol,
                    market_type=job.market_type,
                    funding_time=to_naive_utc(milliseconds_to_datetime(int(timestamp))),
                    funding_rate=Decimal(str(row["fundingRate"])),
                    mark_price=_decimal_or_none(info.get("markPrice")),
                    index_price=_decimal_or_none(info.get("indexPrice")),
                    next_funding_time=to_naive_utc(milliseconds_to_datetime(int(info["nextFundingTime"]))) if info.get("nextFundingTime") else None,
                    source_dataset=job.source_dataset,
                    run_id=run_id,
                )
            )
        return _finalize_success(db, run_id, job.job_name, len(rows), db.insert_funding_rates(records))
    except Exception as exc:
        LOGGER.exception("Job %s failed.", job.job_name)
        db.finalize_run_log(run_id, status="failed", rows_fetched=0, rows_inserted=0, rows_updated=0, error_message=str(exc))
        raise
    finally:
        if client is not None and hasattr(client, "close"):
            client.close()


def _run_liquidation_job(job: IngestionJob, db: Database, settings: Settings) -> RunResult:
    watermark = db.get_event_watermark(job, "liquidation_event", "liquidation_time")
    request_start, request_end = _compute_simple_bounds(job, watermark)
    run_id = db.create_run_log(job, to_naive_utc(request_start), to_naive_utc(request_end))
    client = None
    try:
        if job.exchange == "binance":
            message = "Binance market liquidations are WebSocket snapshots; use run-binance-liquidations."
            db.finalize_run_log(
                run_id,
                status="skipped",
                rows_fetched=0,
                rows_inserted=0,
                rows_updated=0,
                error_message=message,
            )
            return RunResult(job.job_name, run_id, "skipped", 0, 0, 0, 0)

        client = create_exchange_client(job, timeout_ms=settings.ccxt_timeout_ms)
        market = resolve_market(client, job)
        if not client.has.get("fetchLiquidations"):
            message = "Public fetchLiquidations is not supported by this ccxt exchange adapter."
            db.finalize_run_log(run_id, status="skipped", rows_fetched=0, rows_inserted=0, rows_updated=0, error_message=message)
            return RunResult(job.job_name, run_id, "skipped", 0, 0, 0, 0)

        since_ms = datetime_to_milliseconds(request_start) + 1 if request_start else None
        rows = client.fetch_liquidations(market.fetch_symbol, since_ms, settings.fetch_limit)
        records = [
            LiquidationRecord(
                exchange=job.exchange,
                symbol=job.symbol,
                market_type=job.market_type,
                liquidation_time=to_naive_utc(milliseconds_to_datetime(int(row["timestamp"]))),
                side=row.get("side"),
                price=_decimal_or_none(row.get("price")),
                amount=_decimal_or_none(row.get("amount") or row.get("contracts")),
                cost=_decimal_or_none(row.get("cost")),
                source_dataset=job.source_dataset,
                raw=row,
                run_id=run_id,
            )
            for row in rows
            if row.get("timestamp") is not None
        ]
        return _finalize_success(db, run_id, job.job_name, len(rows), db.insert_liquidations(records))
    except Exception as exc:
        LOGGER.exception("Job %s failed.", job.job_name)
        db.finalize_run_log(run_id, status="failed", rows_fetched=0, rows_inserted=0, rows_updated=0, error_message=str(exc))
        raise
    finally:
        if client is not None and hasattr(client, "close"):
            client.close()


def run_job(job: IngestionJob, db: Database, settings: Settings) -> RunResult:
    if job.source_dataset in OHLCV_DATASETS:
        return _run_ohlcv_job(job, db, settings)
    if job.source_dataset == "open_interest":
        return _run_open_interest_job(job, db, settings)
    if job.source_dataset == "funding_rate":
        return _run_funding_rate_job(job, db, settings)
    if job.source_dataset == "liquidation":
        return _run_liquidation_job(job, db, settings)
    raise RuntimeError(f"Unsupported source_dataset={job.source_dataset!r}.")


def run_active_jobs(db: Database, settings: Settings) -> list[RunResult]:
    results: list[RunResult] = []
    for job in db.fetch_jobs():
        LOGGER.info("Running job %s", job.job_name)
        results.append(run_job(job, db, settings))
    return results
