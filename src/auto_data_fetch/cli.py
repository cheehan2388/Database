from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import logging
import sys
import time

from .config import load_settings
from .db import Database
from .binance_liquidations import LiquidationFlushSummary, run_binance_liquidation_stream
from .ingestion import run_active_jobs, run_job
from .models import IngestionJob, RunResult
from .seeding import (
    DEFAULT_DASHBOARD_EXCHANGES,
    DEFAULT_DASHBOARD_DATASETS,
    DEFAULT_DASHBOARD_INTERVALS,
    DEFAULT_EXCHANGES,
    DEFAULT_INTERVALS,
    DEFAULT_WATCHLIST_ASSETS,
    seed_dashboard_watchlist,
    seed_exchange_jobs,
)


def _parse_csv(value: str) -> tuple[str, ...]:
    return tuple(item.strip().lower() for item in value.split(",") if item.strip())


def _parse_asset_csv(value: str) -> tuple[str, ...]:
    return tuple(item.strip().upper() for item in value.split(",") if item.strip())


def _parse_dataset_csv(value: str) -> tuple[str, ...]:
    return tuple(item.strip().lower() for item in value.split(",") if item.strip())


def _parse_optional_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Market data ingestion runner.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("apply-schema", help="Apply the PostgreSQL schema file.")

    seed_parser = subparsers.add_parser(
        "seed-jobs",
        help="Sync exchange markets into asset_registry and create ingestion jobs.",
    )
    seed_parser.add_argument(
        "--exchanges",
        default=",".join(DEFAULT_EXCHANGES),
        help=f"Comma-separated ccxt exchange ids. Default: {','.join(DEFAULT_EXCHANGES)}.",
    )
    seed_parser.add_argument(
        "--intervals",
        default=",".join(DEFAULT_INTERVALS),
        help="Comma-separated OHLCV intervals to create jobs for. Default: 1h.",
    )
    seed_parser.add_argument(
        "--quotes",
        default=None,
        help="Optional comma-separated quote assets, for example USDT,USDC,USD.",
    )
    seed_parser.add_argument(
        "--base-assets",
        default=None,
        help="Optional comma-separated base assets, for example BTC,ETH,RAVE.",
    )
    seed_parser.add_argument(
        "--fetch-mode",
        choices=("incremental", "backfill"),
        default="incremental",
        help="Fetch mode assigned to generated jobs.",
    )
    seed_parser.add_argument(
        "--start-time",
        default=None,
        help="Optional UTC start time, for example 2020-01-01T00:00:00.",
    )
    seed_parser.add_argument(
        "--end-time",
        default=None,
        help="Optional UTC end time, for example 2022-01-01T00:00:00.",
    )
    seed_parser.add_argument(
        "--active",
        action="store_true",
        help="Create jobs as active. Without this flag, jobs are registered but inactive.",
    )
    seed_parser.add_argument(
        "--limit-per-market-type",
        type=int,
        default=None,
        help="Testing helper: limit generated jobs per exchange market type.",
    )
    seed_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover markets and print counts without writing to PostgreSQL.",
    )

    seed_watchlist_parser = subparsers.add_parser(
        "seed-watchlist",
        help="Create dashboard jobs for the configured watchlist assets.",
    )
    seed_watchlist_parser.add_argument(
        "--assets",
        default=",".join(DEFAULT_WATCHLIST_ASSETS),
        help=f"Comma-separated base assets. Default: {','.join(DEFAULT_WATCHLIST_ASSETS)}.",
    )
    seed_watchlist_parser.add_argument(
        "--exchanges",
        default=",".join(DEFAULT_DASHBOARD_EXCHANGES),
        help=f"Comma-separated exchange ids. Default: {','.join(DEFAULT_DASHBOARD_EXCHANGES)}.",
    )
    seed_watchlist_parser.add_argument(
        "--intervals",
        default=",".join(DEFAULT_DASHBOARD_INTERVALS),
        help="Comma-separated K-line intervals. Default: 1m,5m,1h.",
    )
    seed_watchlist_parser.add_argument(
        "--datasets",
        default=None,
        help="Optional comma-separated datasets. Default: all dashboard datasets.",
    )
    seed_watchlist_parser.add_argument("--start-time", default=None)
    seed_watchlist_parser.add_argument("--end-time", default=None)
    seed_watchlist_parser.add_argument(
        "--inactive",
        action="store_true",
        help="Create jobs as inactive. By default watchlist jobs are active.",
    )
    seed_watchlist_parser.add_argument("--dry-run", action="store_true")

    run_watchlist_parser = subparsers.add_parser(
        "run-watchlist",
        help="Run active jobs for watchlist assets only.",
    )
    run_watchlist_parser.add_argument(
        "--assets",
        default=",".join(DEFAULT_WATCHLIST_ASSETS),
        help=f"Comma-separated base assets. Default: {','.join(DEFAULT_WATCHLIST_ASSETS)}.",
    )
    run_watchlist_parser.add_argument(
        "--exchanges",
        default=",".join(DEFAULT_DASHBOARD_EXCHANGES),
        help=f"Comma-separated exchange ids. Default: {','.join(DEFAULT_DASHBOARD_EXCHANGES)}.",
    )
    run_watchlist_parser.add_argument(
        "--intervals",
        default=None,
        help="Optional comma-separated intervals, for example 1m,5m,1h,8h.",
    )
    run_watchlist_parser.add_argument(
        "--datasets",
        default=None,
        help="Optional comma-separated datasets, for example kline,mark_price_kline,index_price_kline.",
    )

    run_watchlist_loop_parser = subparsers.add_parser(
        "run-watchlist-loop",
        help="Continuously run dashboard watchlist jobs, useful while the UI is open.",
    )
    run_watchlist_loop_parser.add_argument(
        "--assets",
        default=",".join(DEFAULT_WATCHLIST_ASSETS),
        help=f"Comma-separated base assets. Default: {','.join(DEFAULT_WATCHLIST_ASSETS)}.",
    )
    run_watchlist_loop_parser.add_argument(
        "--exchanges",
        default=",".join(DEFAULT_DASHBOARD_EXCHANGES),
        help=f"Comma-separated exchange ids. Default: {','.join(DEFAULT_DASHBOARD_EXCHANGES)}.",
    )
    run_watchlist_loop_parser.add_argument(
        "--intervals",
        default="1m",
        help="Comma-separated intervals to keep live. Default: 1m.",
    )
    run_watchlist_loop_parser.add_argument(
        "--datasets",
        default="kline,mark_price_kline,index_price_kline",
        help="Comma-separated datasets. Default: 1m price/mark/index streams.",
    )
    run_watchlist_loop_parser.add_argument(
        "--poll-seconds",
        type=int,
        default=15,
        help="How often the loop checks for due jobs. Default: 15.",
    )
    run_watchlist_loop_parser.add_argument(
        "--once",
        action="store_true",
        help="Run only one due cycle. Useful for testing.",
    )
    run_watchlist_loop_parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Maximum concurrent jobs per due cycle. Default: 4.",
    )

    binance_liquidations_parser = subparsers.add_parser(
        "run-binance-liquidations",
        help="Stream live Binance USD-M liquidation events into PostgreSQL.",
    )
    binance_liquidations_parser.add_argument(
        "--assets",
        default=",".join(DEFAULT_WATCHLIST_ASSETS),
        help=f"Comma-separated base assets. Default: {','.join(DEFAULT_WATCHLIST_ASSETS)}.",
    )
    binance_liquidations_parser.add_argument(
        "--flush-seconds",
        type=int,
        default=10,
        help="Flush buffered events to PostgreSQL at least this often. Default: 10.",
    )
    binance_liquidations_parser.add_argument(
        "--flush-events",
        type=int,
        default=25,
        help="Flush after this many buffered events. Default: 25.",
    )
    binance_liquidations_parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=None,
        help="Optional testing limit. Stop after this many seconds.",
    )
    binance_liquidations_parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Optional testing limit. Stop after this many events.",
    )
    binance_liquidations_parser.add_argument(
        "--reconnect-seconds",
        type=int,
        default=5,
        help="Seconds to wait before reconnecting after a disconnect. Default: 5.",
    )

    run_parser = subparsers.add_parser("run", help="Run one active ingestion job.")
    run_parser.add_argument("--job-name", required=True, help="Job name from ingestion_job_config.")

    subparsers.add_parser("run-all", help="Run all active ingestion jobs.")

    return parser


def _interval_to_seconds(interval: str) -> int:
    unit = interval[-1].lower()
    try:
        value = int(interval[:-1])
    except ValueError:
        return 60
    if unit == "m":
        return value * 60
    if unit == "h":
        return value * 60 * 60
    if unit == "d":
        return value * 24 * 60 * 60
    return 60


def _job_cadence_seconds(job: IngestionJob) -> int:
    if job.source_dataset == "funding_rate":
        return 5 * 60
    if job.source_dataset == "liquidation":
        return 60
    return max(30, _interval_to_seconds(job.bar_interval))


def _print_run_result(result: RunResult) -> None:
    print(
        f"{result.job_name} status={result.status} "
        f"run_id={result.run_id} fetched={result.rows_fetched} "
        f"inserted={result.rows_inserted} issues={result.issue_count}"
    )


def _print_liquidation_flush(summaries: list[LiquidationFlushSummary]) -> None:
    for summary in summaries:
        print(
            f"{summary.job_name} status=success "
            f"run_id={summary.run_id} fetched={summary.rows_fetched} "
            f"inserted={summary.rows_inserted} issues=0"
        )


def _run_watchlist_jobs(
    *,
    db: Database,
    settings,
    jobs: list[IngestionJob],
) -> list[RunResult]:
    results: list[RunResult] = []
    for job in jobs:
        try:
            result = run_job(job, db, settings)
        except Exception as exc:
            print(f"{job.job_name} status=failed error={exc}")
            continue
        _print_run_result(result)
        results.append(result)
    return results


def _run_loop_job(job: IngestionJob, db: Database, settings) -> tuple[IngestionJob, RunResult | None, Exception | None]:
    try:
        return job, run_job(job, db, settings), None
    except Exception as exc:
        return job, None, exc


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def main(argv: list[str] | None = None) -> int:
    _configure_logging()
    parser = _build_parser()
    args = parser.parse_args(argv)

    settings = load_settings()
    db = Database(settings)

    if args.command == "apply-schema":
        db.apply_schema()
        print(f"Schema applied from {settings.schema_path}")
        return 0

    if args.command == "seed-jobs":
        quote_assets = None
        if args.quotes:
            quote_assets = {quote.upper() for quote in _parse_csv(args.quotes)}
        base_assets = None
        if args.base_assets:
            base_assets = {asset.upper() for asset in _parse_csv(args.base_assets)}

        results = seed_exchange_jobs(
            db=db,
            exchanges=_parse_csv(args.exchanges),
            intervals=_parse_csv(args.intervals),
            fetch_mode=args.fetch_mode,
            start_time=_parse_optional_datetime(args.start_time),
            end_time=_parse_optional_datetime(args.end_time),
            is_active=args.active,
            timeout_ms=settings.ccxt_timeout_ms,
            quote_assets=quote_assets,
            base_assets=base_assets,
            limit_per_market_type=args.limit_per_market_type,
            dry_run=args.dry_run,
        )
        for result in results:
            print(
                f"{result.exchange}/{result.market_type} "
                f"markets={result.discovered_markets} "
                f"jobs_written={result.created_or_updated_jobs}"
            )
        return 0

    if args.command == "seed-watchlist":
        jobs, missing = seed_dashboard_watchlist(
            db=db,
            assets=_parse_asset_csv(args.assets),
            exchanges=_parse_csv(args.exchanges),
            intervals=_parse_csv(args.intervals),
            datasets=_parse_dataset_csv(args.datasets) if args.datasets else DEFAULT_DASHBOARD_DATASETS,
            fetch_mode="incremental",
            start_time=_parse_optional_datetime(args.start_time),
            end_time=_parse_optional_datetime(args.end_time),
            is_active=not args.inactive,
            timeout_ms=settings.ccxt_timeout_ms,
            dry_run=args.dry_run,
        )
        print(f"watchlist_jobs={len(jobs)} missing={','.join(missing) if missing else 'none'}")
        return 0

    if args.command == "run-watchlist":
        jobs = db.fetch_watchlist_jobs(
            base_assets=_parse_asset_csv(args.assets),
            exchanges=_parse_csv(args.exchanges),
            intervals=_parse_csv(args.intervals) if args.intervals else None,
            datasets=_parse_dataset_csv(args.datasets) if args.datasets else None,
        )
        _run_watchlist_jobs(db=db, settings=settings, jobs=jobs)
        print(f"watchlist_jobs_run={len(jobs)}")
        return 0

    if args.command == "run-watchlist-loop":
        jobs = db.fetch_watchlist_jobs(
            base_assets=_parse_asset_csv(args.assets),
            exchanges=_parse_csv(args.exchanges),
            intervals=_parse_csv(args.intervals) if args.intervals else None,
            datasets=_parse_dataset_csv(args.datasets) if args.datasets else None,
        )
        if not jobs:
            print("No active watchlist jobs matched the loop filters.")
            return 1

        print(
            f"watchlist_loop_jobs={len(jobs)} "
            f"intervals={args.intervals} datasets={args.datasets} "
            f"poll_seconds={args.poll_seconds}"
        )
        next_due = {job.job_name: 0.0 for job in jobs}
        cycle = 0
        try:
            while True:
                now = time.monotonic()
                due_jobs = [job for job in jobs if next_due[job.job_name] <= now]
                if due_jobs:
                    cycle += 1
                    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
                    max_workers = max(1, args.max_workers)
                    print(f"loop_cycle={cycle} due_jobs={len(due_jobs)} max_workers={max_workers} utc={timestamp}")
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures = [executor.submit(_run_loop_job, job, db, settings) for job in due_jobs]
                        for future in as_completed(futures):
                            job, result, exc = future.result()
                            if result is not None:
                                _print_run_result(result)
                            else:
                                print(f"{job.job_name} status=failed error={exc}")
                            next_due[job.job_name] = time.monotonic() + _job_cadence_seconds(job)
                    if args.once:
                        break
                time.sleep(max(1, args.poll_seconds))
        except KeyboardInterrupt:
            print("watchlist_loop_stopped=keyboard_interrupt")
        return 0

    if args.command == "run-binance-liquidations":
        result = run_binance_liquidation_stream(
            db=db,
            settings=settings,
            assets=_parse_asset_csv(args.assets),
            flush_seconds=args.flush_seconds,
            flush_events=args.flush_events,
            timeout_seconds=args.timeout_seconds,
            max_events=args.max_events,
            reconnect_seconds=args.reconnect_seconds,
            on_flush=_print_liquidation_flush,
        )
        print(
            "binance_liquidation_stream_stopped "
            f"symbols={','.join(result.subscribed_symbols)} "
            f"events_seen={result.events_seen} rows_inserted={result.rows_inserted}"
        )
        return 0

    if args.command == "run":
        job = db.get_job(args.job_name)
        result = run_job(job, db, settings)
        print(
            f"{result.job_name} status={result.status} "
            f"run_id={result.run_id} fetched={result.rows_fetched} "
            f"inserted={result.rows_inserted} issues={result.issue_count}"
        )
        return 0

    if args.command == "run-all":
        results = run_active_jobs(db, settings)
        for result in results:
            print(
                f"{result.job_name} status={result.status} "
                f"run_id={result.run_id} fetched={result.rows_fetched} "
                f"inserted={result.rows_inserted} issues={result.issue_count}"
            )
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
