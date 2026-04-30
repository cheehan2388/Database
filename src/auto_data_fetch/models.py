from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID


@dataclass(frozen=True)
class IngestionJob:
    job_id: UUID
    job_name: str
    exchange: str
    symbol: str
    market_type: str
    bar_interval: str
    source_dataset: str
    fetch_mode: str
    start_time: datetime | None
    end_time: datetime | None
    is_active: bool
    notes: str | None


@dataclass(frozen=True)
class Watermark:
    first_open_time: datetime | None
    last_open_time: datetime | None
    last_close_time: datetime | None
    row_count: int
    last_ingested_at: datetime | None


@dataclass(frozen=True)
class MarketMetadata:
    fetch_symbol: str
    base_asset: str
    quote_asset: str


@dataclass(frozen=True)
class BarRecord:
    exchange: str
    symbol: str
    market_type: str
    bar_interval: str
    open_time: datetime
    close_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal | None
    trade_count: int | None
    source_dataset: str
    run_id: UUID


@dataclass(frozen=True)
class OpenInterestRecord:
    exchange: str
    symbol: str
    market_type: str
    bar_interval: str
    open_time: datetime
    open_interest_amount: Decimal | None
    open_interest_value: Decimal | None
    base_volume: Decimal | None
    quote_volume: Decimal | None
    source_dataset: str
    run_id: UUID


@dataclass(frozen=True)
class FundingRateRecord:
    exchange: str
    symbol: str
    market_type: str
    funding_time: datetime
    funding_rate: Decimal
    mark_price: Decimal | None
    index_price: Decimal | None
    next_funding_time: datetime | None
    source_dataset: str
    run_id: UUID


@dataclass(frozen=True)
class LiquidationRecord:
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
    run_id: UUID


@dataclass(frozen=True)
class QualityIssueRecord:
    exchange: str
    symbol: str
    market_type: str
    bar_interval: str
    issue_type: str
    issue_start_time: datetime | None
    issue_end_time: datetime | None
    expected_count: int | None
    actual_count: int | None
    severity: str
    detail: dict[str, Any]
    run_id: UUID


@dataclass(frozen=True)
class RunResult:
    job_name: str
    run_id: UUID
    status: str
    rows_fetched: int
    rows_inserted: int
    rows_updated: int
    issue_count: int


@dataclass(frozen=True)
class SeedJobRecord:
    job_name: str
    exchange: str
    symbol: str
    market_type: str
    bar_interval: str
    source_dataset: str
    fetch_mode: str
    start_time: datetime | None
    end_time: datetime | None
    is_active: bool
    base_asset: str
    quote_asset: str


@dataclass(frozen=True)
class SeedResult:
    exchange: str
    market_type: str
    discovered_markets: int
    created_or_updated_jobs: int
