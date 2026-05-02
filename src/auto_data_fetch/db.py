from __future__ import annotations

from pathlib import Path
from typing import Iterable
from uuid import UUID

from psycopg import Connection, connect
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .config import Settings
from .models import (
    BarRecord,
    FundingRateRecord,
    IngestionJob,
    LiquidationRecord,
    LongShortRatioRecord,
    OpenInterestRecord,
    QualityIssueRecord,
    SeedJobRecord,
    TakerBuySellVolumeRecord,
    Watermark,
)


class Database:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def connect(self) -> Connection:
        return connect(self.settings.database_url, row_factory=dict_row)

    def apply_schema(self, schema_path: Path | None = None) -> None:
        target_path = schema_path or self.settings.schema_path
        sql_text = target_path.read_text(encoding="utf-8")
        with self.connect() as conn:
            conn.execute(sql_text)

    def fetch_jobs(self, job_name: str | None = None) -> list[IngestionJob]:
        sql_parts = [
            """
            SELECT
              job_id,
              job_name,
              exchange,
              symbol,
              market_type,
              bar_interval,
              source_dataset,
              fetch_mode,
              start_time,
              end_time,
              is_active,
              notes
            FROM market_data.ingestion_job_config
            WHERE is_active = TRUE
            """
        ]
        params: list[object] = []
        if job_name:
            sql_parts.append("AND job_name = %s")
            params.append(job_name)
        sql_parts.append("ORDER BY job_name")

        with self.connect() as conn:
            rows = conn.execute("\n".join(sql_parts), params).fetchall()

        return [
            IngestionJob(
                job_id=row["job_id"],
                job_name=row["job_name"],
                exchange=row["exchange"],
                symbol=row["symbol"],
                market_type=row["market_type"],
                bar_interval=row["bar_interval"],
                source_dataset=row["source_dataset"],
                fetch_mode=row["fetch_mode"],
                start_time=row["start_time"],
                end_time=row["end_time"],
                is_active=row["is_active"],
                notes=row["notes"],
            )
            for row in rows
        ]

    def fetch_watchlist_jobs(
        self,
        *,
        base_assets: tuple[str, ...],
        exchanges: tuple[str, ...],
        intervals: tuple[str, ...] | None = None,
        datasets: tuple[str, ...] | None = None,
    ) -> list[IngestionJob]:
        query_parts = [
            """
            SELECT
              j.job_id,
              j.job_name,
              j.exchange,
              j.symbol,
              j.market_type,
              j.bar_interval,
              j.source_dataset,
              j.fetch_mode,
              j.start_time,
              j.end_time,
              j.is_active,
              j.notes
            FROM market_data.ingestion_job_config j
            JOIN market_data.asset_registry a
              ON a.exchange = j.exchange
             AND a.symbol = j.symbol
             AND a.market_type = j.market_type
            WHERE j.is_active = TRUE
              AND upper(a.base_asset) = ANY(%s)
              AND j.exchange = ANY(%s)
            """
        ]
        params: list[object] = [[item.upper() for item in base_assets], list(exchanges)]
        if intervals:
            query_parts.append("AND j.bar_interval = ANY(%s)")
            params.append(list(intervals))
        if datasets:
            query_parts.append("AND j.source_dataset = ANY(%s)")
            params.append(list(datasets))
        query_parts.append("ORDER BY j.exchange, a.base_asset, j.source_dataset, j.bar_interval")

        with self.connect() as conn:
            rows = conn.execute("\n".join(query_parts), params).fetchall()

        return [
            IngestionJob(
                job_id=row["job_id"],
                job_name=row["job_name"],
                exchange=row["exchange"],
                symbol=row["symbol"],
                market_type=row["market_type"],
                bar_interval=row["bar_interval"],
                source_dataset=row["source_dataset"],
                fetch_mode=row["fetch_mode"],
                start_time=row["start_time"],
                end_time=row["end_time"],
                is_active=row["is_active"],
                notes=row["notes"],
            )
            for row in rows
        ]

    def get_job(self, job_name: str) -> IngestionJob:
        jobs = self.fetch_jobs(job_name=job_name)
        if not jobs:
            active_jobs = self.fetch_jobs()
            if not active_jobs:
                raise RuntimeError(
                    f"No active job found for job_name={job_name!r}. "
                    "The current database has no active rows in "
                    "market_data.ingestion_job_config. "
                    "Insert a job first, or mark an existing job with is_active = TRUE."
                )

            available_names = ", ".join(sorted(job.job_name for job in active_jobs))
            raise RuntimeError(
                f"No active job found for job_name={job_name!r}. "
                f"Available active jobs: {available_names}"
            )
        return jobs[0]

    def get_watermark(self, job: IngestionJob) -> Watermark | None:
        query = """
            SELECT
              first_open_time,
              last_open_time,
              last_close_time,
              row_count,
              last_ingested_at
            FROM market_data.v_market_data_watermark
            WHERE exchange = %s
              AND symbol = %s
              AND market_type = %s
              AND bar_interval = %s
              AND source_dataset = %s
        """
        params = (
            job.exchange,
            job.symbol,
            job.market_type,
            job.bar_interval,
            job.source_dataset,
        )

        with self.connect() as conn:
            row = conn.execute(query, params).fetchone()

        if row is None:
            return None

        return Watermark(
            first_open_time=row["first_open_time"],
            last_open_time=row["last_open_time"],
            last_close_time=row["last_close_time"],
            row_count=row["row_count"],
            last_ingested_at=row["last_ingested_at"],
        )

    def get_open_interest_watermark(self, job: IngestionJob) -> Watermark | None:
        query = """
            SELECT
              MIN(open_time) AS first_open_time,
              MAX(open_time) AS last_open_time,
              MAX(open_time) AS last_close_time,
              COUNT(*) AS row_count,
              MAX(ingested_at) AS last_ingested_at
            FROM market_data.open_interest_history
            WHERE exchange = %s
              AND symbol = %s
              AND market_type = %s
              AND bar_interval = %s
        """
        with self.connect() as conn:
            row = conn.execute(
                query,
                (job.exchange, job.symbol, job.market_type, job.bar_interval),
            ).fetchone()
        if row is None or row["row_count"] == 0:
            return None
        return Watermark(
            first_open_time=row["first_open_time"],
            last_open_time=row["last_open_time"],
            last_close_time=row["last_close_time"],
            row_count=row["row_count"],
            last_ingested_at=row["last_ingested_at"],
        )

    def get_long_short_ratio_watermark(self, job: IngestionJob) -> Watermark | None:
        query = """
            SELECT
              MIN(open_time) AS first_open_time,
              MAX(open_time) AS last_open_time,
              MAX(open_time) AS last_close_time,
              COUNT(*) AS row_count,
              MAX(ingested_at) AS last_ingested_at
            FROM market_data.long_short_ratio_history
            WHERE exchange = %s
              AND symbol = %s
              AND market_type = %s
              AND bar_interval = %s
        """
        with self.connect() as conn:
            row = conn.execute(
                query,
                (job.exchange, job.symbol, job.market_type, job.bar_interval),
            ).fetchone()
        if row is None or row["row_count"] == 0:
            return None
        return Watermark(
            first_open_time=row["first_open_time"],
            last_open_time=row["last_open_time"],
            last_close_time=row["last_close_time"],
            row_count=row["row_count"],
            last_ingested_at=row["last_ingested_at"],
        )

    def get_taker_buy_sell_volume_watermark(self, job: IngestionJob) -> Watermark | None:
        query = """
            SELECT
              MIN(open_time) AS first_open_time,
              MAX(open_time) AS last_open_time,
              MAX(open_time) AS last_close_time,
              COUNT(*) AS row_count,
              MAX(ingested_at) AS last_ingested_at
            FROM market_data.taker_buy_sell_volume_history
            WHERE exchange = %s
              AND symbol = %s
              AND market_type = %s
              AND bar_interval = %s
        """
        with self.connect() as conn:
            row = conn.execute(
                query,
                (job.exchange, job.symbol, job.market_type, job.bar_interval),
            ).fetchone()
        if row is None or row["row_count"] == 0:
            return None
        return Watermark(
            first_open_time=row["first_open_time"],
            last_open_time=row["last_open_time"],
            last_close_time=row["last_close_time"],
            row_count=row["row_count"],
            last_ingested_at=row["last_ingested_at"],
        )

    def get_event_watermark(self, job: IngestionJob, table_name: str, time_column: str) -> Watermark | None:
        query = f"""
            SELECT
              MIN({time_column}) AS first_open_time,
              MAX({time_column}) AS last_open_time,
              MAX({time_column}) AS last_close_time,
              COUNT(*) AS row_count,
              MAX(ingested_at) AS last_ingested_at
            FROM market_data.{table_name}
            WHERE exchange = %s
              AND symbol = %s
              AND market_type = %s
        """
        with self.connect() as conn:
            row = conn.execute(
                query,
                (job.exchange, job.symbol, job.market_type),
            ).fetchone()
        if row is None or row["row_count"] == 0:
            return None
        return Watermark(
            first_open_time=row["first_open_time"],
            last_open_time=row["last_open_time"],
            last_close_time=row["last_close_time"],
            row_count=row["row_count"],
            last_ingested_at=row["last_ingested_at"],
        )

    def create_run_log(
        self,
        job: IngestionJob,
        request_start,
        request_end,
    ) -> UUID:
        query = """
            INSERT INTO market_data.ingestion_run_log (
              job_id,
              job_name,
              exchange,
              symbol,
              market_type,
              bar_interval,
              dataset,
              request_start,
              request_end,
              status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'running')
            RETURNING run_id
        """
        params = (
            job.job_id,
            job.job_name,
            job.exchange,
            job.symbol,
            job.market_type,
            job.bar_interval,
            job.source_dataset,
            request_start,
            request_end,
        )

        with self.connect() as conn:
            row = conn.execute(query, params).fetchone()

        return row["run_id"]

    def finalize_run_log(
        self,
        run_id: UUID,
        *,
        status: str,
        rows_fetched: int,
        rows_inserted: int,
        rows_updated: int,
        error_message: str | None = None,
    ) -> None:
        query = """
            UPDATE market_data.ingestion_run_log
            SET status = %s,
                rows_fetched = %s,
                rows_inserted = %s,
                rows_updated = %s,
                error_message = %s,
                ended_at = CURRENT_TIMESTAMP AT TIME ZONE 'UTC'
            WHERE run_id = %s
        """
        params = (
            status,
            rows_fetched,
            rows_inserted,
            rows_updated,
            error_message,
            run_id,
        )

        with self.connect() as conn:
            conn.execute(query, params)

    def upsert_asset_registry(
        self,
        *,
        exchange: str,
        symbol: str,
        market_type: str,
        base_asset: str,
        quote_asset: str,
    ) -> None:
        query = """
            INSERT INTO market_data.asset_registry (
              exchange,
              symbol,
              market_type,
              base_asset,
              quote_asset,
              is_active
            ) VALUES (%s, %s, %s, %s, %s, TRUE)
            ON CONFLICT (exchange, symbol, market_type)
            DO UPDATE
            SET base_asset = EXCLUDED.base_asset,
                quote_asset = EXCLUDED.quote_asset,
                is_active = TRUE,
                updated_at = CURRENT_TIMESTAMP AT TIME ZONE 'UTC'
        """
        params = (exchange, symbol, market_type, base_asset, quote_asset)

        with self.connect() as conn:
            conn.execute(query, params)

    def upsert_seed_jobs(self, jobs: Iterable[SeedJobRecord]) -> int:
        job_list = list(jobs)
        if not job_list:
            return 0

        query = """
            INSERT INTO market_data.ingestion_job_config (
              job_name,
              exchange,
              symbol,
              market_type,
              bar_interval,
              source_dataset,
              fetch_mode,
              start_time,
              end_time,
              is_active,
              notes
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (exchange, symbol, market_type, bar_interval, source_dataset)
            DO UPDATE
            SET job_name = EXCLUDED.job_name,
                fetch_mode = EXCLUDED.fetch_mode,
                start_time = EXCLUDED.start_time,
                end_time = EXCLUDED.end_time,
                is_active = EXCLUDED.is_active,
                notes = EXCLUDED.notes,
                updated_at = CURRENT_TIMESTAMP AT TIME ZONE 'UTC'
        """
        params = [
            (
                job.job_name,
                job.exchange,
                job.symbol,
                job.market_type,
                job.bar_interval,
                job.source_dataset,
                job.fetch_mode,
                job.start_time,
                job.end_time,
                job.is_active,
                "Generated by seed-jobs from exchange market metadata.",
            )
            for job in job_list
        ]

        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, params)
                return cursor.rowcount

    def upsert_seed_assets(self, jobs: Iterable[SeedJobRecord]) -> int:
        job_list = list(jobs)
        if not job_list:
            return 0

        query = """
            INSERT INTO market_data.asset_registry (
              exchange,
              symbol,
              market_type,
              base_asset,
              quote_asset,
              is_active
            ) VALUES (%s, %s, %s, %s, %s, TRUE)
            ON CONFLICT (exchange, symbol, market_type)
            DO UPDATE
            SET base_asset = EXCLUDED.base_asset,
                quote_asset = EXCLUDED.quote_asset,
                is_active = TRUE,
                updated_at = CURRENT_TIMESTAMP AT TIME ZONE 'UTC'
        """
        params = [
            (
                job.exchange,
                job.symbol,
                job.market_type,
                job.base_asset,
                job.quote_asset,
            )
            for job in job_list
        ]

        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, params)
                return cursor.rowcount

    def upsert_watchlist_assets(self, base_assets: Iterable[str]) -> int:
        assets = [asset.upper() for asset in base_assets]
        if not assets:
            return 0

        query = """
            INSERT INTO market_data.watchlist_asset (
              base_asset,
              display_order,
              is_active
            ) VALUES (%s, %s, TRUE)
            ON CONFLICT (base_asset)
            DO UPDATE
            SET display_order = EXCLUDED.display_order,
                is_active = TRUE,
                updated_at = CURRENT_TIMESTAMP AT TIME ZONE 'UTC'
        """
        params = [(asset, index) for index, asset in enumerate(assets, start=1)]
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, params)
                return cursor.rowcount

    def insert_market_bars(self, bars: Iterable[BarRecord]) -> int:
        bar_list = list(bars)
        if not bar_list:
            return 0

        query = """
            INSERT INTO market_data.market_data_raw (
              exchange,
              symbol,
              market_type,
              bar_interval,
              open_time,
              close_time,
              open,
              high,
              low,
              close,
              volume,
              quote_volume,
              trade_count,
              source_dataset,
              ingested_at,
              run_id
            ) VALUES (
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              CURRENT_TIMESTAMP AT TIME ZONE 'UTC', %s
            )
            ON CONFLICT (
              exchange,
              symbol,
              market_type,
              bar_interval,
              source_dataset,
              open_time
            ) DO NOTHING
        """
        params = [
            (
                bar.exchange,
                bar.symbol,
                bar.market_type,
                bar.bar_interval,
                bar.open_time,
                bar.close_time,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
                bar.quote_volume,
                bar.trade_count,
                bar.source_dataset,
                bar.run_id,
            )
            for bar in bar_list
        ]

        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, params)
                return cursor.rowcount

    def insert_quality_issues(self, issues: Iterable[QualityIssueRecord]) -> int:
        issue_list = list(issues)
        if not issue_list:
            return 0

        query = """
            INSERT INTO market_data.data_quality_issue (
              exchange,
              symbol,
              market_type,
              bar_interval,
              issue_type,
              issue_start_time,
              issue_end_time,
              expected_count,
              actual_count,
              severity,
              detail,
              detected_at,
              run_id
            ) VALUES (
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              CURRENT_TIMESTAMP AT TIME ZONE 'UTC', %s
            )
        """
        params = [
            (
                issue.exchange,
                issue.symbol,
                issue.market_type,
                issue.bar_interval,
                issue.issue_type,
                issue.issue_start_time,
                issue.issue_end_time,
                issue.expected_count,
                issue.actual_count,
                issue.severity,
                Jsonb(issue.detail),
                issue.run_id,
            )
            for issue in issue_list
        ]

        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, params)
                return cursor.rowcount

    def insert_open_interest(self, records: Iterable[OpenInterestRecord]) -> int:
        record_list = list(records)
        if not record_list:
            return 0

        query = """
            INSERT INTO market_data.open_interest_history (
              exchange,
              symbol,
              market_type,
              bar_interval,
              open_time,
              open_interest_amount,
              open_interest_value,
              base_volume,
              quote_volume,
              source_dataset,
              ingested_at,
              run_id
            ) VALUES (
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              CURRENT_TIMESTAMP AT TIME ZONE 'UTC', %s
            )
            ON CONFLICT (exchange, symbol, market_type, bar_interval, open_time)
            DO NOTHING
        """
        params = [
            (
                item.exchange,
                item.symbol,
                item.market_type,
                item.bar_interval,
                item.open_time,
                item.open_interest_amount,
                item.open_interest_value,
                item.base_volume,
                item.quote_volume,
                item.source_dataset,
                item.run_id,
            )
            for item in record_list
        ]
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, params)
                return cursor.rowcount

    def insert_funding_rates(self, records: Iterable[FundingRateRecord]) -> int:
        record_list = list(records)
        if not record_list:
            return 0

        query = """
            INSERT INTO market_data.funding_rate_history (
              exchange,
              symbol,
              market_type,
              funding_time,
              funding_rate,
              mark_price,
              index_price,
              next_funding_time,
              source_dataset,
              ingested_at,
              run_id
            ) VALUES (
              %s, %s, %s, %s, %s, %s, %s, %s, %s,
              CURRENT_TIMESTAMP AT TIME ZONE 'UTC', %s
            )
            ON CONFLICT (exchange, symbol, market_type, funding_time)
            DO NOTHING
        """
        params = [
            (
                item.exchange,
                item.symbol,
                item.market_type,
                item.funding_time,
                item.funding_rate,
                item.mark_price,
                item.index_price,
                item.next_funding_time,
                item.source_dataset,
                item.run_id,
            )
            for item in record_list
        ]
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, params)
                return cursor.rowcount

    def insert_long_short_ratios(self, records: Iterable[LongShortRatioRecord]) -> int:
        record_list = list(records)
        if not record_list:
            return 0

        query = """
            INSERT INTO market_data.long_short_ratio_history (
              exchange,
              symbol,
              market_type,
              bar_interval,
              open_time,
              long_short_ratio,
              long_account,
              short_account,
              source_dataset,
              ingested_at,
              run_id
            ) VALUES (
              %s, %s, %s, %s, %s, %s, %s, %s, %s,
              CURRENT_TIMESTAMP AT TIME ZONE 'UTC', %s
            )
            ON CONFLICT (exchange, symbol, market_type, bar_interval, open_time)
            DO NOTHING
        """
        params = [
            (
                item.exchange,
                item.symbol,
                item.market_type,
                item.bar_interval,
                item.open_time,
                item.long_short_ratio,
                item.long_account,
                item.short_account,
                item.source_dataset,
                item.run_id,
            )
            for item in record_list
        ]
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, params)
                return cursor.rowcount

    def insert_taker_buy_sell_volumes(self, records: Iterable[TakerBuySellVolumeRecord]) -> int:
        record_list = list(records)
        if not record_list:
            return 0

        query = """
            INSERT INTO market_data.taker_buy_sell_volume_history (
              exchange,
              symbol,
              market_type,
              bar_interval,
              open_time,
              buy_volume,
              sell_volume,
              buy_sell_ratio,
              source_dataset,
              ingested_at,
              run_id
            ) VALUES (
              %s, %s, %s, %s, %s, %s, %s, %s, %s,
              CURRENT_TIMESTAMP AT TIME ZONE 'UTC', %s
            )
            ON CONFLICT (exchange, symbol, market_type, bar_interval, open_time)
            DO NOTHING
        """
        params = [
            (
                item.exchange,
                item.symbol,
                item.market_type,
                item.bar_interval,
                item.open_time,
                item.buy_volume,
                item.sell_volume,
                item.buy_sell_ratio,
                item.source_dataset,
                item.run_id,
            )
            for item in record_list
        ]
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, params)
                return cursor.rowcount

    def insert_liquidations(self, records: Iterable[LiquidationRecord]) -> int:
        record_list = list(records)
        if not record_list:
            return 0

        query = """
            INSERT INTO market_data.liquidation_event (
              exchange,
              symbol,
              market_type,
              liquidation_time,
              side,
              price,
              amount,
              cost,
              source_dataset,
              raw,
              ingested_at,
              run_id
            ) VALUES (
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              CURRENT_TIMESTAMP AT TIME ZONE 'UTC', %s
            )
        """
        params = [
            (
                item.exchange,
                item.symbol,
                item.market_type,
                item.liquidation_time,
                item.side,
                item.price,
                item.amount,
                item.cost,
                item.source_dataset,
                Jsonb(item.raw),
                item.run_id,
            )
            for item in record_list
        ]
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, params)
                return cursor.rowcount
