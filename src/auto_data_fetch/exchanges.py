from __future__ import annotations

import re

import ccxt

from .models import IngestionJob, MarketMetadata


MARKET_TYPE_TO_CCXT_DEFAULT = {
    "spot": "spot",
    "perpetual": "swap",
    "future": "future",
}

EXCHANGE_MARKET_TYPE_TO_CCXT_DEFAULT = {
    ("binance", "perpetual"): "future",
    ("binance", "future"): "delivery",
    ("bybit", "perpetual"): "swap",
    ("bybit", "future"): "future",
    ("kucoinfutures", "perpetual"): "swap",
    ("kucoinfutures", "future"): "future",
}


def default_type_for_exchange(exchange: str, market_type: str) -> str | None:
    exchange_key = exchange.lower()
    market_type_key = market_type.lower()
    return EXCHANGE_MARKET_TYPE_TO_CCXT_DEFAULT.get(
        (exchange_key, market_type_key),
        MARKET_TYPE_TO_CCXT_DEFAULT.get(market_type_key),
    )


def _normalize_symbol(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", value.upper())


def _market_matches_type(market: dict, market_type: str) -> bool:
    if market_type == "spot":
        return bool(market.get("spot"))
    if market_type == "perpetual":
        return bool(market.get("swap")) or bool(market.get("contract"))
    if market_type == "future":
        return bool(market.get("future"))
    return False


def create_exchange_client(job: IngestionJob, timeout_ms: int):
    exchange_name = job.exchange.lower()
    try:
        exchange_class = getattr(ccxt, exchange_name)
    except AttributeError as exc:
        raise RuntimeError(f"Unsupported exchange for ccxt: {job.exchange}") from exc

    options = {
        "enableRateLimit": True,
        "timeout": timeout_ms,
        "options": {},
    }

    default_type = default_type_for_exchange(job.exchange, job.market_type)
    if default_type:
        options["options"]["defaultType"] = default_type

    client = exchange_class(options)
    client.load_markets()
    return client


def resolve_market(client, job: IngestionJob) -> MarketMetadata:
    requested_symbol = job.symbol
    requested_key = _normalize_symbol(requested_symbol)

    exact_market = client.markets.get(requested_symbol)
    if exact_market and _market_matches_type(exact_market, job.market_type):
        return MarketMetadata(
            fetch_symbol=exact_market["symbol"],
            base_asset=exact_market.get("base", ""),
            quote_asset=exact_market.get("quote", ""),
        )

    candidates: list[dict] = []
    for market in client.markets.values():
        if not _market_matches_type(market, job.market_type):
            continue

        aliases = {
            _normalize_symbol(str(market.get("symbol", ""))),
            _normalize_symbol(str(market.get("id", ""))),
            _normalize_symbol(str(market.get("base", "")) + str(market.get("quote", ""))),
        }
        if requested_key in aliases:
            candidates.append(market)

    if not candidates:
        raise RuntimeError(
            f"Could not resolve symbol {job.symbol!r} on exchange {job.exchange!r} "
            f"for market_type={job.market_type!r}."
        )

    active_candidates = [market for market in candidates if market.get("active", True)]
    selected_market = active_candidates[0] if active_candidates else candidates[0]

    return MarketMetadata(
        fetch_symbol=selected_market["symbol"],
        base_asset=selected_market.get("base", ""),
        quote_asset=selected_market.get("quote", ""),
    )
