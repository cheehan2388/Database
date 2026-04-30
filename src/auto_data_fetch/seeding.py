from __future__ import annotations

from datetime import datetime
import hashlib
import re

import ccxt

from .db import Database
from .exchanges import default_type_for_exchange
from .models import SeedJobRecord, SeedResult
from .time_utils import to_naive_utc


DEFAULT_EXCHANGES = ("binance", "bybit", "coinbase", "kucoin", "kucoinfutures")
DEFAULT_INTERVALS = ("1h",)
DEFAULT_WATCHLIST_ASSETS = ("BTC", "ETH", "MYX", "MOVR", "UB", "HUMA", "RAVE", "BNT", "TAIKO")
DEFAULT_DASHBOARD_INTERVALS = ("1m", "5m", "1h")
DEFAULT_DASHBOARD_EXCHANGES = ("binance", "bybit", "kucoinfutures", "coinbase")
DEFAULT_DASHBOARD_DATASETS = (
    "kline",
    "mark_price_kline",
    "index_price_kline",
    "open_interest",
    "funding_rate",
    "liquidation",
)
DEFAULT_DASHBOARD_SPOT_QUOTES = ("USD", "USDC", "USDT")
DEFAULT_MARKET_TYPES_BY_EXCHANGE = {
    "binance": ("spot", "perpetual", "future"),
    "bybit": ("spot", "perpetual", "future"),
    "coinbase": ("spot",),
    "kucoin": ("spot",),
    "kucoinfutures": ("perpetual", "future"),
}


def _create_exchange_client(exchange: str, market_type: str, timeout_ms: int):
    try:
        exchange_class = getattr(ccxt, exchange)
    except AttributeError as exc:
        raise RuntimeError(f"Unsupported exchange for ccxt: {exchange}") from exc

    options = {
        "enableRateLimit": True,
        "timeout": timeout_ms,
        "options": {},
    }
    default_type = default_type_for_exchange(exchange, market_type)
    if default_type:
        options["options"]["defaultType"] = default_type

    client = exchange_class(options)
    client.load_markets()
    return client


def _market_matches_type(market: dict, market_type: str) -> bool:
    if market_type == "spot":
        return bool(market.get("spot"))
    if market_type == "perpetual":
        return bool(market.get("swap")) or bool(market.get("contract"))
    if market_type == "future":
        return bool(market.get("future"))
    return False


def _sanitize_job_part(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", value).strip("_").lower()
    return normalized or "unknown"


def _stable_suffix(*values: str) -> str:
    raw_value = "|".join(values)
    return hashlib.sha1(raw_value.encode("utf-8")).hexdigest()[:8]


def build_job_name(
    exchange: str,
    symbol: str,
    market_type: str,
    interval: str,
    market_id: str | None = None,
    source_dataset: str = "kline",
) -> str:
    parts = [
        _sanitize_job_part(exchange),
        _sanitize_job_part(symbol),
    ]
    if market_id:
        parts.append(_sanitize_job_part(market_id))
    parts.extend(
        [
            _sanitize_job_part(market_type),
            _sanitize_job_part(interval),
            _sanitize_job_part(source_dataset),
            _stable_suffix(exchange, symbol, market_type, interval, source_dataset, market_id or ""),
        ]
    )
    return "_".join(
        parts
    )


def _market_to_jobs(
    *,
    exchange: str,
    market_type: str,
    market: dict,
    intervals: tuple[str, ...],
    fetch_mode: str,
    start_time: datetime | None,
    end_time: datetime | None,
    is_active: bool,
) -> list[SeedJobRecord]:
    symbol = market["symbol"]
    market_id = str(market.get("id") or "")
    base_asset = str(market.get("base") or "")
    quote_asset = str(market.get("quote") or "")

    return [
        SeedJobRecord(
            job_name=build_job_name(exchange, symbol, market_type, interval, market_id, "kline"),
            exchange=exchange,
            symbol=symbol,
            market_type=market_type,
            bar_interval=interval,
            source_dataset="kline",
            fetch_mode=fetch_mode,
            start_time=to_naive_utc(start_time),
            end_time=to_naive_utc(end_time),
            is_active=is_active,
            base_asset=base_asset,
            quote_asset=quote_asset,
        )
        for interval in intervals
    ]


def collect_seed_jobs(
    *,
    exchange: str,
    market_type: str,
    intervals: tuple[str, ...],
    fetch_mode: str,
    start_time: datetime | None,
    end_time: datetime | None,
    is_active: bool,
    timeout_ms: int,
    quote_assets: set[str] | None = None,
    base_assets: set[str] | None = None,
    limit: int | None = None,
) -> list[SeedJobRecord]:
    client = _create_exchange_client(exchange, market_type, timeout_ms)
    try:
        seed_jobs: list[SeedJobRecord] = []
        for market in client.markets.values():
            if not _market_matches_type(market, market_type):
                continue
            if not market.get("active", True):
                continue
            if market.get("base") and base_assets and str(market["base"]).upper() not in base_assets:
                continue
            if market.get("quote") and quote_assets and str(market["quote"]).upper() not in quote_assets:
                continue
            if not market.get("has", {}).get("fetchOHLCV", True) and not client.has.get("fetchOHLCV"):
                continue

            seed_jobs.extend(
                _market_to_jobs(
                    exchange=exchange,
                    market_type=market_type,
                    market=market,
                    intervals=intervals,
                    fetch_mode=fetch_mode,
                    start_time=start_time,
                    end_time=end_time,
                    is_active=is_active,
                )
            )

            if limit is not None and len(seed_jobs) >= limit:
                return seed_jobs[:limit]

        return seed_jobs
    finally:
        if hasattr(client, "close"):
            client.close()


def seed_exchange_jobs(
    *,
    db: Database,
    exchanges: tuple[str, ...],
    intervals: tuple[str, ...],
    fetch_mode: str,
    start_time: datetime | None,
    end_time: datetime | None,
    is_active: bool,
    timeout_ms: int,
    quote_assets: set[str] | None = None,
    base_assets: set[str] | None = None,
    limit_per_market_type: int | None = None,
    dry_run: bool = False,
) -> list[SeedResult]:
    results: list[SeedResult] = []

    for exchange in exchanges:
        market_types = DEFAULT_MARKET_TYPES_BY_EXCHANGE.get(exchange, ("spot",))
        for market_type in market_types:
            jobs = collect_seed_jobs(
                exchange=exchange,
                market_type=market_type,
                intervals=intervals,
                fetch_mode=fetch_mode,
                start_time=start_time,
                end_time=end_time,
                is_active=is_active,
                timeout_ms=timeout_ms,
                quote_assets=quote_assets,
                base_assets=base_assets,
                limit=limit_per_market_type,
            )

            if not dry_run:
                db.upsert_seed_assets(jobs)
                db.upsert_seed_jobs(jobs)

            results.append(
                SeedResult(
                    exchange=exchange,
                    market_type=market_type,
                    discovered_markets=len({job.symbol for job in jobs}),
                    created_or_updated_jobs=len(jobs) if not dry_run else 0,
                )
            )

    return results


def _find_usdt_perpetual_market(client, base_asset: str) -> dict | None:
    base_asset = base_asset.upper()
    for market in client.markets.values():
        if not _market_matches_type(market, "perpetual"):
            continue
        if not market.get("active", True):
            continue
        if str(market.get("base") or "").upper() != base_asset:
            continue
        if str(market.get("quote") or "").upper() != "USDT":
            continue
        return market
    return None


def _find_dashboard_spot_market(client, base_asset: str) -> dict | None:
    base_asset = base_asset.upper()
    candidates = []
    for market in client.markets.values():
        if not _market_matches_type(market, "spot"):
            continue
        if not market.get("active", True):
            continue
        if str(market.get("base") or "").upper() != base_asset:
            continue
        quote = str(market.get("quote") or "").upper()
        if quote not in DEFAULT_DASHBOARD_SPOT_QUOTES:
            continue
        candidates.append(market)

    quote_priority = {quote: index for index, quote in enumerate(DEFAULT_DASHBOARD_SPOT_QUOTES)}
    return min(candidates, key=lambda market: quote_priority.get(str(market.get("quote") or "").upper(), 999), default=None)


def _dashboard_jobs_for_market(
    *,
    exchange: str,
    market_type: str,
    market: dict,
    intervals: tuple[str, ...],
    datasets: tuple[str, ...],
    fetch_mode: str,
    start_time: datetime | None,
    end_time: datetime | None,
    is_active: bool,
) -> list[SeedJobRecord]:
    symbol = market["symbol"]
    market_id = str(market.get("id") or "")
    base_asset = str(market.get("base") or "")
    quote_asset = str(market.get("quote") or "")
    jobs: list[SeedJobRecord] = []

    for dataset in datasets:
        if market_type == "spot" and dataset != "kline":
            continue

        if dataset in {"kline", "mark_price_kline", "index_price_kline"}:
            target_intervals = intervals
        elif dataset == "open_interest":
            target_intervals = tuple(interval for interval in intervals if interval in {"5m", "1h"})
        elif dataset == "funding_rate":
            target_intervals = ("8h",)
        elif dataset == "liquidation":
            target_intervals = ("event",)
        else:
            continue

        for interval in target_intervals:
            jobs.append(
                SeedJobRecord(
                    job_name=build_job_name(exchange, symbol, market_type, interval, market_id, dataset),
                    exchange=exchange,
                    symbol=symbol,
                    market_type=market_type,
                    bar_interval=interval,
                    source_dataset=dataset,
                    fetch_mode=fetch_mode,
                    start_time=to_naive_utc(start_time),
                    end_time=to_naive_utc(end_time),
                    is_active=is_active,
                    base_asset=base_asset,
                    quote_asset=quote_asset,
                )
            )

    return jobs


def seed_dashboard_watchlist(
    *,
    db: Database,
    assets: tuple[str, ...] = DEFAULT_WATCHLIST_ASSETS,
    exchanges: tuple[str, ...] = DEFAULT_DASHBOARD_EXCHANGES,
    intervals: tuple[str, ...] = DEFAULT_DASHBOARD_INTERVALS,
    datasets: tuple[str, ...] = DEFAULT_DASHBOARD_DATASETS,
    fetch_mode: str = "incremental",
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    is_active: bool = True,
    timeout_ms: int = 30000,
    dry_run: bool = False,
) -> tuple[list[SeedJobRecord], list[str]]:
    all_jobs: list[SeedJobRecord] = []
    missing: list[str] = []

    for exchange in exchanges:
        dashboard_market_type = "spot" if exchange == "coinbase" else "perpetual"
        client = _create_exchange_client(exchange, dashboard_market_type, timeout_ms)
        try:
            for asset in assets:
                market = _find_dashboard_spot_market(client, asset) if dashboard_market_type == "spot" else _find_usdt_perpetual_market(client, asset)
                if market is None:
                    missing.append(f"{exchange}:{asset}")
                    continue
                all_jobs.extend(
                    _dashboard_jobs_for_market(
                        exchange=exchange,
                        market_type=dashboard_market_type,
                        market=market,
                        intervals=intervals,
                        datasets=datasets,
                        fetch_mode=fetch_mode,
                        start_time=start_time,
                        end_time=end_time,
                        is_active=is_active,
                    )
                )
        finally:
            if hasattr(client, "close"):
                client.close()

    if not dry_run:
        db.upsert_watchlist_assets(assets)
        db.upsert_seed_assets(all_jobs)
        db.upsert_seed_jobs(all_jobs)

    return all_jobs, missing
