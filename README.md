# auto_data_fetch

PostgreSQL + `ccxt` market-data ingestion pipeline for collecting, monitoring, and visualizing crypto market data.

This project is designed to help you build a local research database for crypto markets. It can discover exchange markets, create ingestion jobs, collect historical and live data, store normalized records in PostgreSQL, run data-quality checks, and power a local market-intelligence dashboard.

Dashboard demonstrate:
![alt text](image-1.png)
## Table of contents

- [What this project does](#what-this-project-does)
- [Project guides](#project-guides)
- [Setup](#setup)
- [Seed Binance, Bybit, Coinbase, and KuCoin jobs](#seed-binance-bybit-coinbase-and-kucoin-jobs)
- [Crypto Market Intelligence Dashboard data](#crypto-market-intelligence-dashboard-data)
- [Run the dashboard UI](#run-the-dashboard-ui)
- [Create a job manually](#create-a-job-manually)
- [Run ingestion jobs](#run-ingestion-jobs)
- [Database structure](#database-structure)
- [Switch one job between history backfill and latest update](#switch-one-job-between-history-backfill-and-latest-update)
- [Main files](#main-files)

## What this project does

`auto_data_fetch` is a market-data collection system with a PostgreSQL backend and a Next.js dashboard frontend.

It currently supports these workflows:

- Reads active jobs from `market_data.ingestion_job_config`.
- Pulls OHLCV bars from exchanges through `ccxt`.
- Pulls dashboard data for selected perpetual watchlists:
  - K-line
  - mark-price K-line
  - index-price K-line
  - open interest
  - funding rate
  - Binance global long/short account ratio
  - Binance taker buy/sell volume
  - Binance live liquidation events
- Writes normalized bars into `market_data.market_data_raw`.
- Stores derivatives metrics in dedicated tables for:
  - open interest
  - funding rate
  - long/short account ratio
  - taker buy/sell volume
  - liquidation events
- Records execution state in `market_data.ingestion_run_log`.
- Writes data-quality findings into `market_data.data_quality_issue`.
- Seeds Binance, Bybit, Coinbase, KuCoin, and KuCoin Futures market jobs from exchange metadata.

## Project guides

For a full Chinese operating guide that explains daily workflows, adding new assets, adding exchanges, backfilling history, and extending datasets, read:

```text
docs/operation_workflow_guide.md
```

For adding formula-based factors such as `OI / Volume`, basis, z-scores, or liquidation ratios, read:

```text
docs/factor_formula_workflow.md
```

For moving this project together with the PostgreSQL database to another device, read:

```text
docs/migration_guide.md
```

For a portable Codex handoff skill that summarizes this project's workflows, commands, schema, frontend, and migration notes, read or copy:

```text
skills/auto-data-fetch/SKILL.md
```

## Setup

### 1. Create a Python environment

Create and activate a Python 3.11+ environment.

### 2. Install the package

```bash
pip install -e .
```

### 3. Set the database connection

For PowerShell:

```powershell
$env:DATABASE_URL="postgresql://postgres:postgres@localhost:5432/market_data"
```

Or create a `.env` file in the project root:

```dotenv
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/market_data
FETCH_LIMIT=500
CCXT_TIMEOUT_MS=30000
LATE_DATA_INTERVALS=2
```

### 4. Apply the schema

```bash
python -m auto_data_fetch apply-schema
```

## Seed Binance, Bybit, Coinbase, and KuCoin jobs

The project does not hard-code every symbol manually. It asks each exchange for its current market list through `ccxt`, then writes:

- active symbols into `market_data.asset_registry`
- runnable data streams into `market_data.ingestion_job_config`

### Register default exchange markets

Register all default exchange markets for 1h K-line jobs:

```bash
python -m auto_data_fetch seed-jobs --intervals 1h
```

By default, generated jobs are inactive. This keeps `run-all` from accidentally launching thousands of API requests.

To create active jobs:

```bash
python -m auto_data_fetch seed-jobs --intervals 1h --active
```

To restrict to common USD stablecoin markets:

```bash
python -m auto_data_fetch seed-jobs --exchanges binance,bybit,coinbase --intervals 1h --quotes USDT,USDC,USD
```

To test discovery without writing to PostgreSQL:

```bash
python -m auto_data_fetch seed-jobs --exchanges binance,bybit,coinbase --intervals 1h --limit-per-market-type 5 --dry-run
```

### Default exchange coverage

| Exchange | Market types |
| --- | --- |
| `binance` | `spot`, `perpetual`, `future` |
| `bybit` | `spot`, `perpetual`, `future` |
| `coinbase` | `spot` |
| `kucoin` | `spot` |
| `kucoinfutures` | `perpetual`, `future` |

## Crypto Market Intelligence Dashboard data

The dashboard watchlist is designed for:

- `BTC`
- `ETH`
- `MYX`
- `MOVR`
- `UB`
- `HUMA`
- `RAVE`
- `BNT`
- `TAIKO`
- `LAB`

The dashboard data layer currently targets Binance, Bybit, and KuCoin Futures USDT perpetual markets, plus Coinbase spot markets where the asset is listed.

### Perpetual market streams

For perpetual markets, the dashboard creates these streams:

| Stream | Meaning |
| --- | --- |
| `kline` | Candlestick and volume data |
| `mark_price_kline` | Mark-price candles |
| `index_price_kline` | Index-price candles |
| `open_interest` | Derivatives positioning |
| `funding_rate` | Perpetual funding history |
| `long_short_ratio` | Binance global long/short account ratio; Binance supports 5m+ periods, so dashboard 1m uses 5m ratio data |
| `taker_buy_sell_volume` | Binance taker buy/sell volume and buy/sell ratio; Binance supports 5m+ periods, so dashboard 1m uses 5m data |
| `liquidation` | Liquidation event stream; Binance uses its official USD-M Futures WebSocket liquidation stream |

### Coinbase spot support

For Coinbase spot markets, the dashboard creates `kline` jobs only.

Coinbase does not provide the perpetual derivatives streams used here, so these panels stay empty for Coinbase selections:

- open interest
- funding
- long/short ratio
- taker buy/sell volume
- mark price
- index price
- liquidation

### KuCoin Futures open interest

KuCoin Futures open interest uses the native public endpoint:

```text
/api/ua/v1/market/open-interest
```

This is used because the current `ccxt` adapter does not expose that history method.

KuCoin intraday OI retention is limited by KuCoin to 7 days.

### Binance market liquidations

Binance market liquidations use Binance's official USD-M Futures WebSocket market stream:

- Individual symbol stream: [`<symbol>@forceOrder`](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams)
- Combined stream routing: [`wss://fstream.binance.com/market/stream?streams=...`](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Connect)

Important distinction: Binance's REST endpoint [`/fapi/v1/forceOrders`](https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Users-Force-Orders) is `USER_DATA`, so it queries your own account's force orders, not public market-wide liquidations.

For the dashboard's market liquidation panel, keep the WebSocket collector running. It starts collecting from the moment it is opened; it is not a historical backfill source.

### Seed dashboard jobs

```bash
python -m auto_data_fetch seed-watchlist
```

### Run only dashboard watchlist jobs

```bash
python -m auto_data_fetch run-watchlist
```

### Keep 1m dashboard data updating while the UI is open

```bash
python -m auto_data_fetch run-watchlist-loop --intervals 1m --datasets kline,mark_price_kline,index_price_kline
```

This loop keeps the 1m price, mark-price, and index-price streams fresh. It is intentionally narrower than `run-watchlist`, so it does not repeatedly call every dashboard job every minute.

If you also want the derivative panels to refresh from the database:

```bash
python -m auto_data_fetch run-watchlist-loop --intervals 1m,5m,1h,8h --datasets kline,mark_price_kline,index_price_kline,open_interest,funding_rate,long_short_ratio,taker_buy_sell_volume --max-workers 4
```

### Keep Binance liquidation events collecting

Run this in a separate terminal:

```bash
python -m auto_data_fetch run-binance-liquidations --assets BTC,ETH,MYX,MOVR,UB,HUMA,RAVE,BNT,TAIKO,LAB
```

For a short connectivity test:

```bash
python -m auto_data_fetch run-binance-liquidations --assets BTC --timeout-seconds 10
```

If the test inserts `0` rows, that only means no subscribed Binance symbol emitted a liquidation snapshot during that short window.

### Limit to specific assets or exchanges

```bash
python -m auto_data_fetch run-watchlist --assets BTC,ETH --exchanges binance,bybit,kucoinfutures
```

### Seed only one base asset from a broad exchange

This is useful for Coinbase spot data when you do not want to register the entire exchange:

```bash
python -m auto_data_fetch seed-jobs --exchanges coinbase --base-assets RAVE --quotes USD,USDC --intervals 1m,5m,1h --active
```

### Inspect data readiness for the frontend

```sql
SELECT *
FROM market_data.v_dashboard_data_status
ORDER BY exchange, symbol, source_dataset, bar_interval;
```

### Current liquidation limitation

Binance market liquidations are supported through the live WebSocket collector above.

Bybit and KuCoin Futures liquidation ingestion still need native exchange connectors because the installed `ccxt` adapters do not expose public `fetchLiquidations` support for those markets.

## Run the dashboard UI

The Next.js + React + Tailwind frontend lives in `web/`.

Start the local dashboard:

```bash
cd web
npm run dev
```

Then open:

```text
http://127.0.0.1:3000
```

The UI reads PostgreSQL through server-side API routes:

| Route | Data returned |
| --- | --- |
| `/api/watchlist` | Active dashboard assets |
| `/api/chart` | Candlestick, volume, mark price, and index price |
| `/api/derivatives` | Open interest, funding rate, long/short ratio, taker buy/sell volume, and liquidation events |
| `/api/status` | Dashboard data freshness and row counts |

The frontend loads `DATABASE_URL` from the parent project `.env` first, so you do not need to duplicate the database password in `web/.env.local`.

The main chart uses a Coinglass-style stacked layout with one synchronized time axis:

| Row | Panel |
| --- | --- |
| Row 1 | Price candles, volume, mark price, and index price |
| Row 2 | Open interest history |
| Row 3 | OI / Volume ratio |
| Row 4 | Long/short account ratio |
| Row 5 | Taker buy/sell volume |
| Row 6 | Liquidation buckets |
| Row 7 | Funding rate history |

Historical data and the latest available database point are plotted together.

Binance liquidation will appear after `run-binance-liquidations` has captured events for the selected symbol. Bybit and KuCoin Futures liquidation will remain empty until their native connectors are added.

Long/short ratio currently uses Binance's public USD-M Futures `globalLongShortAccountRatio` data. It is an account ratio, not a notional value. Binance does not expose a 1m period for this endpoint, so the dashboard uses 5m ratio data when the chart interval is 1m.

Taker buy/sell volume currently uses Binance's public USD-M Futures `takerlongshortRatio` data. It contains taker buy volume, taker sell volume, and buy/sell ratio. Binance does not expose a 1m period for this endpoint, so the dashboard uses 5m taker data when the chart interval is 1m.

The browser refreshes chart, derivatives, and data-quality APIs every 30 seconds while the tab is visible.

To collect new exchange data at the same time, keep `run-watchlist-loop` running in a separate terminal.

## Create a job manually

Insert at least one active job before running the pipeline:

```sql
INSERT INTO market_data.ingestion_job_config (
  job_name,
  exchange,
  symbol,
  market_type,
  bar_interval,
  source_dataset,
  fetch_mode,
  start_time,
  is_active
) VALUES (
  'binance_btcusdt_spot_1h',
  'binance',
  'BTCUSDT',
  'spot',
  '1h',
  'kline',
  'incremental',
  '2024-01-01 00:00:00',
  TRUE
);
```

Notes:

- `symbol` may be exchange-native like `BTCUSDT`, or a unified value like `BTC/USDT`.
- `market_type` currently supports `spot`, `perpetual`, and `future`.
- If there is already data in `market_data_raw`, the runner resumes from the latest stored bar.
- If there is no prior data and `start_time` is empty, the first run fetches the latest page only.
- `fetch_mode='incremental'` means "continue from the latest stored bar toward now".
- `fetch_mode='backfill'` means "fill an older historical window from `start_time` up to `end_time` or up to the earliest stored bar".

## Run ingestion jobs

Run one job:

```bash
python -m auto_data_fetch run --job-name binance_btcusdt_spot_1h
```

Run every active job:

```bash
python -m auto_data_fetch run-all
```

## Database structure

```mermaid
flowchart TD
  A["ccxt exchange markets"] --> B["seed-jobs"]
  B --> C["asset_registry"]
  B --> D["ingestion_job_config"]
  D --> E["run / run-all"]
  E --> F["ingestion_run_log"]
  E --> G["market_data_raw"]
  E --> L["open_interest_history"]
  E --> M["funding_rate_history"]
  E --> N["liquidation_event"]
  E --> P["long_short_ratio_history"]
  E --> Q["taker_buy_sell_volume_history"]
  G --> H["quality checks"]
  H --> I["data_quality_issue"]
  G --> J["v_market_data_watermark"]
  F --> K["v_job_latest_run"]
  G --> O["v_dashboard_data_status"]
  L --> O
  M --> O
  N --> O
  P --> O
  Q --> O
```

### Core tables

| Table | Purpose |
| --- | --- |
| `asset_registry` | Exchange symbol master list |
| `ingestion_job_config` | One row per data stream, such as Binance BTC perpetual 1h |
| `watchlist_asset` | Dashboard watchlist base assets |
| `market_data_raw` | Normalized OHLCV bars |
| `open_interest_history` | Open interest time series |
| `funding_rate_history` | Funding rate time series |
| `long_short_ratio_history` | Binance global long/short account ratio time series |
| `taker_buy_sell_volume_history` | Binance taker buy/sell volume and ratio time series |
| `liquidation_event` | Liquidation event table |
| `ingestion_run_log` | One row per ingestion execution |
| `data_quality_issue` | Detected gaps, duplicates, invalid OHLC, negative volume, late data |

### Useful views

| View | Purpose |
| --- | --- |
| `v_market_data_watermark` | First/latest bar per data stream |
| `v_job_latest_run` | Latest run state per job |
| `v_dashboard_data_status` | Frontend-friendly availability and freshness status |

For a fuller explanation, read:

```text
docs/database_structure_and_usage_report.md
```

## Switch one job between history backfill and latest update

Assume the database already has Binance perpetual 1h data starting from some point in 2022.

### Case 1: backfill older history such as 2020-2022

Update the same job into `backfill` mode:

```sql
UPDATE market_data.ingestion_job_config
SET fetch_mode = 'backfill',
    start_time = '2020-01-01 00:00:00',
    end_time = '2021-12-31 23:00:00',
    is_active = TRUE
WHERE job_name = 'binance_btcusdt_perpetual_1h';
```

Then run:

```bash
python -m auto_data_fetch run --job-name binance_btcusdt_perpetual_1h
```

What the runner does:

- Reads the earliest existing bar for that dataset.
- Starts from `start_time`.
- Stops at the earlier of:
  - your configured `end_time`
  - the bar right before the earliest existing row in the database

This lets you fill older history without overwriting the 2022+ data you already have.

### Case 2: continue from the latest stored bar to today

Switch the same job back to `incremental` mode:

```sql
UPDATE market_data.ingestion_job_config
SET fetch_mode = 'incremental',
    start_time = NULL,
    end_time = NULL,
    is_active = TRUE
WHERE job_name = 'binance_btcusdt_perpetual_1h';
```

Then run:

```bash
python -m auto_data_fetch run --job-name binance_btcusdt_perpetual_1h
```

What the runner does:

- Reads the latest existing bar for that dataset.
- Starts from the next bar after that.
- Pulls forward until the latest closed 1h bar available now.

### Why keep one explicit job instead of typing the interval every time

Because the job is the stable identity of one data stream.

For `binance_btcusdt_perpetual_1h`, the system can consistently track:

- the latest watermark
- the earliest watermark
- each run log
- each quality issue
- whether you are backfilling history or updating to the present

## Main files

| File | Purpose |
| --- | --- |
| `sql/postgresql_schema.sql` | Database schema |
| `docs/database_design_report.md` | Design report |
| `docs/database_structure_and_usage_report.md` | Structure and usage report |
| `src/auto_data_fetch/ingestion.py` | Incremental ingestion flow |
| `src/auto_data_fetch/quality.py` | Quality checks |
| `src/auto_data_fetch/db.py` | Database access layer |
