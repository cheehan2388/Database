from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import json
import logging
import time
from typing import Any, Callable

import websocket

from .config import Settings
from .db import Database
from .models import IngestionJob, LiquidationRecord
from .time_utils import milliseconds_to_datetime, to_naive_utc


LOGGER = logging.getLogger(__name__)

BINANCE_FSTREAM_MARKET_BASE_URL = "wss://fstream.binance.com/market"


@dataclass(frozen=True)
class PendingLiquidationEvent:
    native_symbol: str
    exchange: str
    symbol: str
    market_type: str
    liquidation_time: datetime
    side: str | None
    price: Decimal | None
    amount: Decimal | None
    cost: Decimal | None
    source_dataset: str
    raw: dict[str, Any]


@dataclass(frozen=True)
class LiquidationFlushSummary:
    job_name: str
    run_id: object
    rows_fetched: int
    rows_inserted: int


@dataclass(frozen=True)
class BinanceLiquidationStreamResult:
    subscribed_symbols: tuple[str, ...]
    events_seen: int
    rows_inserted: int


def _job_to_binance_native_symbol(job: IngestionJob) -> str:
    base, _, quote_part = job.symbol.partition("/")
    quote = quote_part.split(":", 1)[0] if quote_part else "USDT"
    if not base or not quote:
        raise RuntimeError(f"Cannot convert job symbol to Binance stream symbol: {job.symbol!r}.")
    return f"{base}{quote}".upper()


def _build_stream_url(native_symbols: tuple[str, ...]) -> str:
    streams = "/".join(f"{symbol.lower()}@forceOrder" for symbol in native_symbols)
    return f"{BINANCE_FSTREAM_MARKET_BASE_URL}/stream?streams={streams}"


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    parsed = Decimal(str(value))
    return parsed


def _first_non_zero_decimal(order: dict[str, Any], *keys: str) -> Decimal | None:
    for key in keys:
        value = _decimal_or_none(order.get(key))
        if value is not None and value != 0:
            return value
    return None


def _pending_event_from_payload(
    payload: dict[str, Any],
    jobs_by_native_symbol: dict[str, IngestionJob],
) -> PendingLiquidationEvent | None:
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    order = data.get("o") if isinstance(data, dict) else None
    if not isinstance(order, dict):
        return None

    native_symbol = str(order.get("s") or "").upper()
    job = jobs_by_native_symbol.get(native_symbol)
    if job is None:
        return None

    event_ms = order.get("T") or data.get("E")
    if event_ms is None:
        return None

    price = _first_non_zero_decimal(order, "ap", "p")
    amount = _first_non_zero_decimal(order, "z", "l", "q")
    cost = price * amount if price is not None and amount is not None else None

    return PendingLiquidationEvent(
        native_symbol=native_symbol,
        exchange=job.exchange,
        symbol=job.symbol,
        market_type=job.market_type,
        liquidation_time=to_naive_utc(milliseconds_to_datetime(int(event_ms))),
        side=order.get("S"),
        price=price,
        amount=amount,
        cost=cost,
        source_dataset=job.source_dataset,
        raw=payload,
    )


def _flush_buffers(
    db: Database,
    buffers: dict[str, list[PendingLiquidationEvent]],
    jobs_by_native_symbol: dict[str, IngestionJob],
) -> list[LiquidationFlushSummary]:
    summaries: list[LiquidationFlushSummary] = []

    for native_symbol, pending_events in list(buffers.items()):
        if not pending_events:
            continue

        job = jobs_by_native_symbol[native_symbol]
        request_start = min(event.liquidation_time for event in pending_events)
        request_end = max(event.liquidation_time for event in pending_events)
        run_id = db.create_run_log(job, request_start, request_end)

        try:
            records = [
                LiquidationRecord(
                    exchange=event.exchange,
                    symbol=event.symbol,
                    market_type=event.market_type,
                    liquidation_time=event.liquidation_time,
                    side=event.side,
                    price=event.price,
                    amount=event.amount,
                    cost=event.cost,
                    source_dataset=event.source_dataset,
                    raw=event.raw,
                    run_id=run_id,
                )
                for event in pending_events
            ]
            rows_inserted = db.insert_liquidations(records)
            db.finalize_run_log(
                run_id,
                status="success",
                rows_fetched=len(records),
                rows_inserted=rows_inserted,
                rows_updated=0,
                error_message=None,
            )
        except Exception as exc:
            db.finalize_run_log(
                run_id,
                status="failed",
                rows_fetched=len(pending_events),
                rows_inserted=0,
                rows_updated=0,
                error_message=str(exc),
            )
            raise

        summaries.append(
            LiquidationFlushSummary(
                job_name=job.job_name,
                run_id=run_id,
                rows_fetched=len(records),
                rows_inserted=rows_inserted,
            )
        )
        pending_events.clear()

    return summaries


def run_binance_liquidation_stream(
    *,
    db: Database,
    settings: Settings,
    assets: tuple[str, ...],
    flush_seconds: int = 10,
    flush_events: int = 25,
    timeout_seconds: int | None = None,
    max_events: int | None = None,
    reconnect_seconds: int = 5,
    on_flush: Callable[[list[LiquidationFlushSummary]], None] | None = None,
) -> BinanceLiquidationStreamResult:
    jobs = db.fetch_watchlist_jobs(
        base_assets=assets,
        exchanges=("binance",),
        intervals=("event",),
        datasets=("liquidation",),
    )
    if not jobs:
        raise RuntimeError(
            "No active Binance liquidation watchlist jobs were found. "
            "Run `python -m auto_data_fetch seed-watchlist --exchanges binance --datasets liquidation` first."
        )

    jobs_by_native_symbol = {_job_to_binance_native_symbol(job): job for job in jobs}
    native_symbols = tuple(sorted(jobs_by_native_symbol))
    stream_url = _build_stream_url(native_symbols)
    receive_timeout_seconds = max(1, min(5, int(settings.ccxt_timeout_ms / 1000)))
    deadline = time.monotonic() + timeout_seconds if timeout_seconds else None
    buffers: dict[str, list[PendingLiquidationEvent]] = defaultdict(list)
    rows_inserted = 0
    events_seen = 0
    last_flush_at = time.monotonic()

    def flush_if_needed(force: bool = False) -> None:
        nonlocal rows_inserted, last_flush_at
        buffered_count = sum(len(items) for items in buffers.values())
        if not buffered_count:
            return
        if not force and buffered_count < flush_events and time.monotonic() - last_flush_at < flush_seconds:
            return
        summaries = _flush_buffers(db, buffers, jobs_by_native_symbol)
        if summaries:
            rows_inserted += sum(summary.rows_inserted for summary in summaries)
            last_flush_at = time.monotonic()
            if on_flush is not None:
                on_flush(summaries)

    should_stop = False
    LOGGER.info("Subscribing to Binance liquidation streams: %s", ",".join(native_symbols))

    while not should_stop:
        if deadline is not None and time.monotonic() >= deadline:
            break

        ws = None
        try:
            if deadline is not None:
                connect_timeout_seconds = max(1, min(receive_timeout_seconds, int(deadline - time.monotonic()) + 1))
            else:
                connect_timeout_seconds = receive_timeout_seconds
            ws = websocket.create_connection(stream_url, timeout=connect_timeout_seconds)
            ws.settimeout(receive_timeout_seconds)
            while True:
                if deadline is not None and time.monotonic() >= deadline:
                    should_stop = True
                    break

                try:
                    message = ws.recv()
                except websocket.WebSocketTimeoutException:
                    flush_if_needed()
                    continue

                payload = json.loads(message)
                if not isinstance(payload, dict):
                    continue

                event = _pending_event_from_payload(payload, jobs_by_native_symbol)
                if event is None:
                    continue

                buffers[event.native_symbol].append(event)
                events_seen += 1
                flush_if_needed()

                if max_events is not None and events_seen >= max_events:
                    should_stop = True
                    break
        except KeyboardInterrupt:
            should_stop = True
        except (
            websocket.WebSocketConnectionClosedException,
            websocket.WebSocketException,
            OSError,
        ) as exc:
            LOGGER.warning("Binance liquidation stream disconnected: %s", exc)
            if deadline is not None and time.monotonic() >= deadline:
                should_stop = True
            else:
                time.sleep(max(1, reconnect_seconds))
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    LOGGER.debug("Ignoring Binance websocket close failure.", exc_info=True)
            flush_if_needed(force=True)

    return BinanceLiquidationStreamResult(
        subscribed_symbols=native_symbols,
        events_seen=events_seen,
        rows_inserted=rows_inserted,
    )
