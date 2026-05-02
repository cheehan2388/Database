import { pool } from "./db";
import type { ChartResponse, DerivativesResponse, Exchange, Interval, StatusRow, WatchlistAsset } from "./types";

export type Selection = {
  asset: string;
  exchange: Exchange;
  interval: Interval;
};

const intervals = new Set(["1m", "5m", "1h"]);
const exchanges = new Set(["all", "binance", "bybit", "kucoinfutures", "coinbase"]);

type ResolvedMarket = {
  symbol: string;
  marketType: string;
};

type AggregateVolumeRow = {
  open_time: Date;
  volume: string;
};

type AggregateLineRow = {
  open_time: Date;
  value: string;
};

type AggregateExchangeLineRow = AggregateLineRow & {
  exchange: string;
};

type AggregateFundingRow = {
  funding_time: Date;
  funding_rate: string;
};

type AggregateExchangeFundingRow = AggregateFundingRow & {
  exchange: string;
};

type AggregateLiquidationRow = {
  bucket_time: Date;
  long_value: string;
  short_value: string;
  total_value: string;
  event_count: string;
};

type AggregateExchangeLiquidationRow = AggregateLiquidationRow & {
  exchange: string;
};

type TakerBuySellRow = {
  open_time: Date;
  buy_volume: string | null;
  sell_volume: string | null;
  buy_sell_ratio: string;
};

export function normalizeSelection(input?: Partial<Record<string, string | string[] | undefined>>): Selection {
  const rawAsset = Array.isArray(input?.asset) ? input?.asset[0] : input?.asset;
  const rawExchange = Array.isArray(input?.exchange) ? input?.exchange[0] : input?.exchange;
  const rawInterval = Array.isArray(input?.interval) ? input?.interval[0] : input?.interval;
  const asset = (rawAsset ?? "BTC").trim().toUpperCase();
  const exchange = (rawExchange ?? "binance").trim().toLowerCase();
  const interval = (rawInterval ?? "1h").trim();

  return {
    asset: /^[A-Z0-9]{2,15}$/.test(asset) ? asset : "BTC",
    exchange: exchanges.has(exchange) ? (exchange as Exchange) : "binance",
    interval: intervals.has(interval) ? (interval as Interval) : "1h"
  };
}

function toSeconds(value: Date | string) {
  const date = value instanceof Date ? value : new Date(value);
  return Math.floor(date.getTime() / 1000);
}

async function resolveMarket(selection: Selection): Promise<ResolvedMarket> {
  const { rows } = await pool.query<{
    symbol: string;
    market_type: string;
  }>(
    `
      SELECT symbol, market_type
      FROM market_data.asset_registry
      WHERE exchange = $1
        AND upper(base_asset) = $2
        AND is_active = TRUE
      ORDER BY
        CASE
          WHEN $1 = 'coinbase' AND market_type = 'spot' AND quote_asset = 'USD' THEN 0
          WHEN $1 = 'coinbase' AND market_type = 'spot' AND quote_asset = 'USDC' THEN 1
          WHEN $1 = 'coinbase' AND market_type = 'spot' AND quote_asset = 'USDT' THEN 2
          WHEN market_type = 'perpetual' AND quote_asset = 'USDT' THEN 0
          WHEN market_type = 'spot' AND quote_asset = 'USDT' THEN 3
          ELSE 9
        END,
        symbol
      LIMIT 1;
    `,
    [selection.exchange, selection.asset]
  );

  if (rows[0]) {
    return {
      symbol: rows[0].symbol,
      marketType: rows[0].market_type
    };
  }

  return {
    symbol: `${selection.asset}/USDT:USDT`,
    marketType: "perpetual"
  };
}

async function resolveAggregateReferenceMarket(asset: string): Promise<ResolvedMarket & { exchange: string }> {
  const { rows } = await pool.query<{
    exchange: string;
    symbol: string;
    market_type: string;
  }>(
    `
      SELECT exchange, symbol, market_type
      FROM market_data.asset_registry
      WHERE upper(base_asset) = $1
        AND is_active = TRUE
      ORDER BY
        CASE
          WHEN exchange = 'binance' AND market_type = 'perpetual' AND quote_asset = 'USDT' THEN 0
          WHEN exchange = 'bybit' AND market_type = 'perpetual' AND quote_asset = 'USDT' THEN 1
          WHEN exchange = 'kucoinfutures' AND market_type = 'perpetual' THEN 2
          WHEN exchange = 'coinbase' AND market_type = 'spot' AND quote_asset = 'USD' THEN 3
          WHEN market_type = 'perpetual' THEN 4
          ELSE 9
        END,
        exchange,
        symbol
      LIMIT 1;
    `,
    [asset]
  );

  if (rows[0]) {
    return {
      exchange: rows[0].exchange,
      symbol: rows[0].symbol,
      marketType: rows[0].market_type
    };
  }

  return {
    exchange: "binance",
    symbol: `${asset}/USDT:USDT`,
    marketType: "perpetual"
  };
}

export async function getWatchlist(): Promise<WatchlistAsset[]> {
  const { rows } = await pool.query<{
    base_asset: string;
    display_order: number;
    exchanges: string[] | null;
  }>(`
    SELECT
      w.base_asset,
      w.display_order,
      COALESCE(
        ARRAY_AGG(DISTINCT s.exchange) FILTER (WHERE s.exchange IS NOT NULL),
        ARRAY[]::text[]
      ) AS exchanges
    FROM market_data.watchlist_asset w
    LEFT JOIN market_data.asset_registry a
      ON upper(a.base_asset) = w.base_asset
     AND a.is_active = TRUE
    LEFT JOIN market_data.v_dashboard_data_status s
      ON s.exchange = a.exchange
     AND s.symbol = a.symbol
     AND s.market_type = a.market_type
     AND s.source_dataset = 'kline'
     AND s.row_count > 0
    WHERE w.is_active = TRUE
    GROUP BY w.base_asset, w.display_order
    ORDER BY w.display_order, w.base_asset;
  `);

  return rows.map((row) => ({
    baseAsset: row.base_asset,
    displayOrder: row.display_order,
    exchanges: (row.exchanges ?? []) as Exchange[]
  }));
}

export async function getStatus(): Promise<StatusRow[]> {
  const { rows } = await pool.query(`
    SELECT
      exchange,
      symbol,
      market_type,
      bar_interval,
      source_dataset,
      first_time,
      last_time,
      row_count,
      last_ingested_at
    FROM market_data.v_dashboard_data_status
    ORDER BY exchange, symbol, source_dataset, bar_interval;
  `);

  return rows.map((row) => ({
    exchange: row.exchange,
    symbol: row.symbol,
    marketType: row.market_type,
    barInterval: row.bar_interval,
    sourceDataset: row.source_dataset,
    firstTime: row.first_time?.toISOString?.() ?? row.first_time,
    lastTime: row.last_time?.toISOString?.() ?? row.last_time,
    rowCount: Number(row.row_count ?? 0),
    lastIngestedAt: row.last_ingested_at?.toISOString?.() ?? row.last_ingested_at
  }));
}

async function fetchBars(exchange: string, symbol: string, marketType: string, interval: Interval, sourceDataset: string, limit = 600) {
  const { rows } = await pool.query<{
    open_time: Date;
    open: string;
    high: string;
    low: string;
    close: string;
    volume: string;
  }>(
    `
      SELECT open_time, open, high, low, close, volume
      FROM market_data.market_data_raw
      WHERE exchange = $1
        AND symbol = $2
        AND market_type = $3
        AND bar_interval = $4
        AND source_dataset = $5
      ORDER BY open_time DESC
      LIMIT $6;
    `,
    [exchange, symbol, marketType, interval, sourceDataset, limit]
  );

  return rows.reverse().map((row) => ({
    time: toSeconds(row.open_time),
    open: Number(row.open),
    high: Number(row.high),
    low: Number(row.low),
    close: Number(row.close),
    volume: Number(row.volume ?? 0)
  }));
}

async function fetchAggregateVolume(asset: string, interval: Interval, limit = 600) {
  const { rows } = await pool.query<AggregateVolumeRow>(
    `
      WITH preferred_markets AS (
        SELECT
          exchange,
          symbol,
          market_type,
          ROW_NUMBER() OVER (
            PARTITION BY exchange, upper(base_asset)
            ORDER BY
              CASE
                WHEN exchange = 'coinbase' AND market_type = 'spot' AND quote_asset = 'USD' THEN 0
                WHEN exchange = 'coinbase' AND market_type = 'spot' AND quote_asset = 'USDC' THEN 1
                WHEN exchange = 'coinbase' AND market_type = 'spot' AND quote_asset = 'USDT' THEN 2
                WHEN market_type = 'perpetual' AND quote_asset = 'USDT' THEN 0
                WHEN market_type = 'perpetual' THEN 1
                WHEN market_type = 'spot' AND quote_asset = 'USDT' THEN 4
                WHEN market_type = 'spot' AND quote_asset = 'USD' THEN 5
                WHEN market_type = 'spot' AND quote_asset = 'USDC' THEN 6
                ELSE 9
              END,
              symbol
          ) AS market_rank
        FROM market_data.asset_registry
        WHERE upper(base_asset) = $1
          AND is_active = TRUE
      )
      SELECT
        m.open_time,
        COALESCE(SUM(COALESCE(m.quote_volume, m.close * m.volume, m.volume, 0)), 0) AS volume
      FROM market_data.market_data_raw m
      JOIN preferred_markets p
        ON p.exchange = m.exchange
       AND p.symbol = m.symbol
       AND p.market_type = m.market_type
       AND p.market_rank = 1
      WHERE m.bar_interval = $2
        AND m.source_dataset = 'kline'
      GROUP BY m.open_time
      ORDER BY m.open_time DESC
      LIMIT $3;
    `,
    [asset, interval, limit]
  );

  return rows.reverse().map((row) => ({
    time: toSeconds(row.open_time),
    volume: Number(row.volume ?? 0)
  }));
}

async function fetchAggregateVolumeBreakdown(asset: string, interval: Interval, limit = 600) {
  const { rows } = await pool.query<AggregateExchangeLineRow>(
    `
      WITH preferred_markets AS (
        SELECT
          exchange,
          symbol,
          market_type,
          ROW_NUMBER() OVER (
            PARTITION BY exchange, upper(base_asset)
            ORDER BY
              CASE
                WHEN exchange = 'coinbase' AND market_type = 'spot' AND quote_asset = 'USD' THEN 0
                WHEN exchange = 'coinbase' AND market_type = 'spot' AND quote_asset = 'USDC' THEN 1
                WHEN exchange = 'coinbase' AND market_type = 'spot' AND quote_asset = 'USDT' THEN 2
                WHEN market_type = 'perpetual' AND quote_asset = 'USDT' THEN 0
                WHEN market_type = 'perpetual' THEN 1
                WHEN market_type = 'spot' AND quote_asset = 'USDT' THEN 4
                WHEN market_type = 'spot' AND quote_asset = 'USD' THEN 5
                WHEN market_type = 'spot' AND quote_asset = 'USDC' THEN 6
                ELSE 9
              END,
              symbol
          ) AS market_rank
        FROM market_data.asset_registry
        WHERE upper(base_asset) = $1
          AND is_active = TRUE
      ),
      exchange_volume AS (
        SELECT
          m.open_time,
          p.exchange,
          COALESCE(SUM(COALESCE(m.quote_volume, m.close * m.volume, m.volume, 0)), 0) AS value
        FROM market_data.market_data_raw m
        JOIN preferred_markets p
          ON p.exchange = m.exchange
         AND p.symbol = m.symbol
         AND p.market_type = m.market_type
         AND p.market_rank = 1
        WHERE m.bar_interval = $2
          AND m.source_dataset = 'kline'
        GROUP BY m.open_time, p.exchange
      ),
      latest_times AS (
        SELECT DISTINCT open_time
        FROM exchange_volume
        ORDER BY open_time DESC
        LIMIT $3
      )
      SELECT ev.open_time, ev.exchange, ev.value
      FROM exchange_volume ev
      JOIN latest_times lt ON lt.open_time = ev.open_time
      ORDER BY ev.open_time ASC, ev.exchange;
    `,
    [asset, interval, limit]
  );

  return rows.map((row) => ({
    time: toSeconds(row.open_time),
    exchange: row.exchange,
    value: Number(row.value ?? 0)
  }));
}

function chartLimit(interval: Interval) {
  if (interval === "1m") {
    return 1440;
  }
  if (interval === "5m") {
    return 1200;
  }
  return 1000;
}

export async function getChart(selection: Selection): Promise<ChartResponse> {
  if (selection.exchange === "all") {
    const reference = await resolveAggregateReferenceMarket(selection.asset);
    const limit = chartLimit(selection.interval);
    const [referenceCandles, aggregateVolumes, volumeBreakdown, markBars, indexBars] = await Promise.all([
      fetchBars(reference.exchange, reference.symbol, reference.marketType, selection.interval, "kline", limit),
      fetchAggregateVolume(selection.asset, selection.interval, limit),
      fetchAggregateVolumeBreakdown(selection.asset, selection.interval, limit),
      reference.marketType === "perpetual"
        ? fetchBars(reference.exchange, reference.symbol, reference.marketType, selection.interval, "mark_price_kline", limit)
        : Promise.resolve([]),
      reference.marketType === "perpetual"
        ? fetchBars(reference.exchange, reference.symbol, reference.marketType, selection.interval, "index_price_kline", limit)
        : Promise.resolve([])
    ]);
    const volumeByTime = new Map(aggregateVolumes.map((point) => [point.time, point.volume]));

    return {
      exchange: selection.exchange,
      symbol: `${selection.asset}/ALL`,
      marketType: "aggregate",
      interval: selection.interval,
      candles: referenceCandles.map((bar) => ({
        ...bar,
        volume: volumeByTime.get(bar.time) ?? bar.close * bar.volume
      })),
      markPrice: markBars.map((bar) => ({ time: bar.time, value: bar.close })),
      indexPrice: indexBars.map((bar) => ({ time: bar.time, value: bar.close })),
      volumeBreakdown
    };
  }

  const market = await resolveMarket(selection);
  const limit = chartLimit(selection.interval);
  const [candles, markBars, indexBars] = await Promise.all([
    fetchBars(selection.exchange, market.symbol, market.marketType, selection.interval, "kline", limit),
    market.marketType === "perpetual"
      ? fetchBars(selection.exchange, market.symbol, market.marketType, selection.interval, "mark_price_kline", limit)
      : Promise.resolve([]),
    market.marketType === "perpetual"
      ? fetchBars(selection.exchange, market.symbol, market.marketType, selection.interval, "index_price_kline", limit)
      : Promise.resolve([])
  ]);

  return {
    exchange: selection.exchange,
    symbol: market.symbol,
    marketType: market.marketType,
    interval: selection.interval,
    candles,
    markPrice: markBars.map((bar) => ({ time: bar.time, value: bar.close })),
    indexPrice: indexBars.map((bar) => ({ time: bar.time, value: bar.close })),
    volumeBreakdown: []
  };
}

function bucketExpression(interval: Interval) {
  if (interval === "1m") {
    return "date_trunc('minute', liquidation_time)";
  }
  if (interval === "5m") {
    return "date_trunc('hour', liquidation_time) + floor(extract(minute from liquidation_time) / 5) * interval '5 minutes'";
  }

  return "date_trunc('hour', liquidation_time)";
}

async function fetchAggregateOpenInterest(asset: string, interval: string) {
  const { rows } = await pool.query<AggregateLineRow>(
    `
      WITH preferred_markets AS (
        SELECT
          exchange,
          symbol,
          market_type,
          ROW_NUMBER() OVER (
            PARTITION BY exchange, upper(base_asset)
            ORDER BY
              CASE
                WHEN quote_asset = 'USDT' THEN 0
                WHEN quote_asset = 'USD' THEN 1
                WHEN quote_asset = 'USDC' THEN 2
                ELSE 9
              END,
              symbol
          ) AS market_rank
        FROM market_data.asset_registry
        WHERE upper(base_asset) = $1
          AND market_type = 'perpetual'
          AND is_active = TRUE
      )
      SELECT
        o.open_time,
        COALESCE(SUM(COALESCE(o.open_interest_value, o.open_interest_amount, 0)), 0) AS value
      FROM market_data.open_interest_history o
      JOIN preferred_markets p
        ON p.exchange = o.exchange
       AND p.symbol = o.symbol
       AND p.market_type = o.market_type
       AND p.market_rank = 1
      WHERE o.market_type = 'perpetual'
        AND o.bar_interval = $2
      GROUP BY o.open_time
      ORDER BY o.open_time DESC
      LIMIT 600;
    `,
    [asset, interval]
  );

  return rows.reverse().map((row) => ({
    time: toSeconds(row.open_time),
    value: Number(row.value ?? 0)
  }));
}

async function fetchAggregateOpenInterestBreakdown(asset: string, interval: string) {
  const { rows } = await pool.query<AggregateExchangeLineRow>(
    `
      WITH preferred_markets AS (
        SELECT
          exchange,
          symbol,
          market_type,
          ROW_NUMBER() OVER (
            PARTITION BY exchange, upper(base_asset)
            ORDER BY
              CASE
                WHEN quote_asset = 'USDT' THEN 0
                WHEN quote_asset = 'USD' THEN 1
                WHEN quote_asset = 'USDC' THEN 2
                ELSE 9
              END,
              symbol
          ) AS market_rank
        FROM market_data.asset_registry
        WHERE upper(base_asset) = $1
          AND market_type = 'perpetual'
          AND is_active = TRUE
      ),
      exchange_oi AS (
        SELECT
          o.open_time,
          p.exchange,
          COALESCE(SUM(COALESCE(o.open_interest_value, o.open_interest_amount, 0)), 0) AS value
        FROM market_data.open_interest_history o
        JOIN preferred_markets p
          ON p.exchange = o.exchange
         AND p.symbol = o.symbol
         AND p.market_type = o.market_type
         AND p.market_rank = 1
        WHERE o.market_type = 'perpetual'
          AND o.bar_interval = $2
        GROUP BY o.open_time, p.exchange
      ),
      latest_times AS (
        SELECT DISTINCT open_time
        FROM exchange_oi
        ORDER BY open_time DESC
        LIMIT 600
      )
      SELECT eo.open_time, eo.exchange, eo.value
      FROM exchange_oi eo
      JOIN latest_times lt ON lt.open_time = eo.open_time
      ORDER BY eo.open_time ASC, eo.exchange;
    `,
    [asset, interval]
  );

  return rows.map((row) => ({
    time: toSeconds(row.open_time),
    exchange: row.exchange,
    value: Number(row.value ?? 0)
  }));
}

async function fetchAggregateFundingRate(asset: string) {
  const { rows } = await pool.query<AggregateFundingRow>(
    `
      WITH preferred_markets AS (
        SELECT
          exchange,
          symbol,
          market_type,
          ROW_NUMBER() OVER (
            PARTITION BY exchange, upper(base_asset)
            ORDER BY
              CASE
                WHEN quote_asset = 'USDT' THEN 0
                WHEN quote_asset = 'USD' THEN 1
                WHEN quote_asset = 'USDC' THEN 2
                ELSE 9
              END,
              symbol
          ) AS market_rank
        FROM market_data.asset_registry
        WHERE upper(base_asset) = $1
          AND market_type = 'perpetual'
          AND is_active = TRUE
      )
      SELECT
        date_trunc('second', f.funding_time) AS funding_time,
        AVG(f.funding_rate) AS funding_rate
      FROM market_data.funding_rate_history f
      JOIN preferred_markets p
        ON p.exchange = f.exchange
       AND p.symbol = f.symbol
       AND p.market_type = f.market_type
       AND p.market_rank = 1
      WHERE f.market_type = 'perpetual'
      GROUP BY date_trunc('second', f.funding_time)
      ORDER BY funding_time DESC
      LIMIT 240;
    `,
    [asset]
  );

  return rows.reverse().map((row) => ({
    time: toSeconds(row.funding_time),
    value: Number(row.funding_rate)
  }));
}

async function fetchAggregateFundingRateBreakdown(asset: string) {
  const { rows } = await pool.query<AggregateExchangeFundingRow>(
    `
      WITH preferred_markets AS (
        SELECT
          exchange,
          symbol,
          market_type,
          ROW_NUMBER() OVER (
            PARTITION BY exchange, upper(base_asset)
            ORDER BY
              CASE
                WHEN quote_asset = 'USDT' THEN 0
                WHEN quote_asset = 'USD' THEN 1
                WHEN quote_asset = 'USDC' THEN 2
                ELSE 9
              END,
              symbol
          ) AS market_rank
        FROM market_data.asset_registry
        WHERE upper(base_asset) = $1
          AND market_type = 'perpetual'
          AND is_active = TRUE
      ),
      exchange_funding AS (
        SELECT
          date_trunc('second', f.funding_time) AS funding_time,
          p.exchange,
          AVG(f.funding_rate) AS funding_rate
        FROM market_data.funding_rate_history f
        JOIN preferred_markets p
          ON p.exchange = f.exchange
         AND p.symbol = f.symbol
         AND p.market_type = f.market_type
         AND p.market_rank = 1
        WHERE f.market_type = 'perpetual'
        GROUP BY date_trunc('second', f.funding_time), p.exchange
      ),
      latest_times AS (
        SELECT DISTINCT funding_time
        FROM exchange_funding
        ORDER BY funding_time DESC
        LIMIT 240
      )
      SELECT ef.funding_time, ef.exchange, ef.funding_rate
      FROM exchange_funding ef
      JOIN latest_times lt ON lt.funding_time = ef.funding_time
      ORDER BY ef.funding_time ASC, ef.exchange;
    `,
    [asset]
  );

  return rows.map((row) => ({
    time: toSeconds(row.funding_time),
    exchange: row.exchange,
    value: Number(row.funding_rate)
  }));
}

async function fetchBinanceLongShortRatio(asset: string, interval: string) {
  const { rows } = await pool.query<AggregateLineRow>(
    `
      SELECT l.open_time, l.long_short_ratio AS value
      FROM market_data.long_short_ratio_history l
      JOIN market_data.asset_registry a
        ON a.exchange = l.exchange
       AND a.symbol = l.symbol
       AND a.market_type = l.market_type
       AND a.is_active = TRUE
      WHERE l.exchange = 'binance'
        AND l.market_type = 'perpetual'
        AND l.bar_interval = $2
        AND upper(a.base_asset) = $1
      ORDER BY l.open_time DESC
      LIMIT 600;
    `,
    [asset, interval]
  );

  return rows.reverse().map((row) => ({
    time: toSeconds(row.open_time),
    value: Number(row.value ?? 0)
  }));
}

async function fetchBinanceLongShortRatioBreakdown(asset: string, interval: string) {
  const rows = await fetchBinanceLongShortRatio(asset, interval);
  return rows.map((row) => ({
    ...row,
    exchange: "binance"
  }));
}

async function fetchBinanceTakerBuySell(asset: string, interval: string) {
  const { rows } = await pool.query<TakerBuySellRow>(
    `
      SELECT
        t.open_time,
        t.buy_volume,
        t.sell_volume,
        t.buy_sell_ratio
      FROM market_data.taker_buy_sell_volume_history t
      JOIN market_data.asset_registry a
        ON a.exchange = t.exchange
       AND a.symbol = t.symbol
       AND a.market_type = t.market_type
       AND a.is_active = TRUE
      WHERE t.exchange = 'binance'
        AND t.market_type = 'perpetual'
        AND t.bar_interval = $2
        AND upper(a.base_asset) = $1
      ORDER BY t.open_time DESC
      LIMIT 600;
    `,
    [asset, interval]
  );

  return rows.reverse().map((row) => ({
    time: toSeconds(row.open_time),
    buyVolume: Number(row.buy_volume ?? 0),
    sellVolume: Number(row.sell_volume ?? 0),
    buySellRatio: Number(row.buy_sell_ratio)
  }));
}

async function fetchBinanceTakerBuySellBreakdown(asset: string, interval: string) {
  const rows = await fetchBinanceTakerBuySell(asset, interval);
  return rows.map((row) => ({
    ...row,
    exchange: "binance"
  }));
}

async function fetchAggregateLiquidations(asset: string, interval: Interval) {
  const liquidationBucket = bucketExpression(interval);
  const { rows } = await pool.query<AggregateLiquidationRow>(
    `
      SELECT
        ${liquidationBucket} AS bucket_time,
        COALESCE(SUM(CASE WHEN lower(l.side) IN ('sell', 'long') THEN COALESCE(l.cost, l.price * l.amount, 0) ELSE 0 END), 0) AS long_value,
        COALESCE(SUM(CASE WHEN lower(l.side) IN ('buy', 'short') THEN COALESCE(l.cost, l.price * l.amount, 0) ELSE 0 END), 0) AS short_value,
        COALESCE(SUM(COALESCE(l.cost, l.price * l.amount, 0)), 0) AS total_value,
        COUNT(*) AS event_count
      FROM market_data.liquidation_event l
      JOIN market_data.asset_registry a
        ON a.exchange = l.exchange
       AND a.symbol = l.symbol
       AND a.market_type = l.market_type
       AND a.is_active = TRUE
      WHERE upper(a.base_asset) = $1
        AND l.market_type = 'perpetual'
      GROUP BY bucket_time
      ORDER BY bucket_time DESC
      LIMIT 600;
    `,
    [asset]
  );

  return rows.reverse().map((row) => ({
    time: toSeconds(row.bucket_time),
    longValue: Number(row.long_value),
    shortValue: Number(row.short_value),
    totalValue: Number(row.total_value),
    count: Number(row.event_count)
  }));
}

async function fetchAggregateLiquidationBreakdown(asset: string, interval: Interval) {
  const liquidationBucket = bucketExpression(interval);
  const { rows } = await pool.query<AggregateExchangeLiquidationRow>(
    `
      WITH exchange_liquidations AS (
        SELECT
          ${liquidationBucket} AS bucket_time,
          l.exchange,
          COALESCE(SUM(CASE WHEN lower(l.side) IN ('sell', 'long') THEN COALESCE(l.cost, l.price * l.amount, 0) ELSE 0 END), 0) AS long_value,
          COALESCE(SUM(CASE WHEN lower(l.side) IN ('buy', 'short') THEN COALESCE(l.cost, l.price * l.amount, 0) ELSE 0 END), 0) AS short_value,
          COALESCE(SUM(COALESCE(l.cost, l.price * l.amount, 0)), 0) AS total_value,
          COUNT(*) AS event_count
        FROM market_data.liquidation_event l
        JOIN market_data.asset_registry a
          ON a.exchange = l.exchange
         AND a.symbol = l.symbol
         AND a.market_type = l.market_type
         AND a.is_active = TRUE
        WHERE upper(a.base_asset) = $1
          AND l.market_type = 'perpetual'
        GROUP BY bucket_time, l.exchange
      ),
      latest_times AS (
        SELECT DISTINCT bucket_time
        FROM exchange_liquidations
        ORDER BY bucket_time DESC
        LIMIT 600
      )
      SELECT
        el.bucket_time,
        el.exchange,
        el.long_value,
        el.short_value,
        el.total_value,
        el.event_count
      FROM exchange_liquidations el
      JOIN latest_times lt ON lt.bucket_time = el.bucket_time
      ORDER BY el.bucket_time ASC, el.exchange;
    `,
    [asset]
  );

  return rows.map((row) => ({
    time: toSeconds(row.bucket_time),
    exchange: row.exchange,
    longValue: Number(row.long_value),
    shortValue: Number(row.short_value),
    totalValue: Number(row.total_value),
    count: Number(row.event_count)
  }));
}

export async function getDerivatives(selection: Selection): Promise<DerivativesResponse> {
  if (selection.exchange === "all") {
    const oiInterval = selection.interval === "1m" ? "5m" : selection.interval;
    const longShortInterval = selection.interval === "1m" ? "5m" : selection.interval;
    const takerBuySellInterval = selection.interval === "1m" ? "5m" : selection.interval;
    const [
      openInterest,
      openInterestBreakdown,
      longShortRatio,
      longShortRatioBreakdown,
      takerBuySell,
      takerBuySellBreakdown,
      fundingRate,
      fundingRateBreakdown,
      liquidations,
      liquidationBreakdown
    ] = await Promise.all([
      fetchAggregateOpenInterest(selection.asset, oiInterval),
      fetchAggregateOpenInterestBreakdown(selection.asset, oiInterval),
      fetchBinanceLongShortRatio(selection.asset, longShortInterval),
      fetchBinanceLongShortRatioBreakdown(selection.asset, longShortInterval),
      fetchBinanceTakerBuySell(selection.asset, takerBuySellInterval),
      fetchBinanceTakerBuySellBreakdown(selection.asset, takerBuySellInterval),
      fetchAggregateFundingRate(selection.asset),
      fetchAggregateFundingRateBreakdown(selection.asset),
      fetchAggregateLiquidations(selection.asset, selection.interval),
      fetchAggregateLiquidationBreakdown(selection.asset, selection.interval)
    ]);

    return {
      openInterest,
      longShortRatio,
      takerBuySell,
      fundingRate,
      liquidations,
      openInterestBreakdown,
      longShortRatioBreakdown,
      takerBuySellBreakdown,
      fundingRateBreakdown,
      liquidationBreakdown
    };
  }

  const market = await resolveMarket(selection);
  if (market.marketType !== "perpetual") {
    return {
      openInterest: [],
      longShortRatio: [],
      takerBuySell: [],
      fundingRate: [],
      liquidations: [],
      openInterestBreakdown: [],
      longShortRatioBreakdown: [],
      takerBuySellBreakdown: [],
      fundingRateBreakdown: [],
      liquidationBreakdown: []
    };
  }
  const symbol = market.symbol;
  const oiInterval = selection.interval === "1m" ? "5m" : selection.interval;
  const longShortInterval = selection.interval === "1m" ? "5m" : selection.interval;
  const takerBuySellInterval = selection.interval === "1m" ? "5m" : selection.interval;
  const liquidationBucket = bucketExpression(selection.interval);

  const [openInterest, longShortRatio, takerBuySell, fundingRate, liquidations] = await Promise.all([
    pool.query<{
      open_time: Date;
      open_interest_amount: string | null;
      open_interest_value: string | null;
    }>(
      `
        SELECT open_time, open_interest_amount, open_interest_value
        FROM market_data.open_interest_history
        WHERE exchange = $1
          AND symbol = $2
          AND market_type = 'perpetual'
          AND bar_interval = $3
        ORDER BY open_time DESC
        LIMIT 600;
      `,
      [selection.exchange, symbol, oiInterval]
    ),
    selection.exchange === "binance"
      ? pool.query<{
          open_time: Date;
          long_short_ratio: string;
        }>(
          `
            SELECT open_time, long_short_ratio
            FROM market_data.long_short_ratio_history
            WHERE exchange = $1
              AND symbol = $2
              AND market_type = 'perpetual'
              AND bar_interval = $3
            ORDER BY open_time DESC
            LIMIT 600;
          `,
          [selection.exchange, symbol, longShortInterval]
        )
      : Promise.resolve({ rows: [] }),
    selection.exchange === "binance"
      ? pool.query<TakerBuySellRow>(
          `
            SELECT open_time, buy_volume, sell_volume, buy_sell_ratio
            FROM market_data.taker_buy_sell_volume_history
            WHERE exchange = $1
              AND symbol = $2
              AND market_type = 'perpetual'
              AND bar_interval = $3
            ORDER BY open_time DESC
            LIMIT 600;
          `,
          [selection.exchange, symbol, takerBuySellInterval]
        )
      : Promise.resolve({ rows: [] }),
    pool.query<{
      funding_time: Date;
      funding_rate: string;
    }>(
      `
        SELECT funding_time, funding_rate
        FROM market_data.funding_rate_history
        WHERE exchange = $1
          AND symbol = $2
          AND market_type = 'perpetual'
        ORDER BY funding_time DESC
        LIMIT 240;
      `,
      [selection.exchange, symbol]
    ),
    pool.query<{
      bucket_time: Date;
      long_value: string;
      short_value: string;
      total_value: string;
      event_count: string;
    }>(
      `
        SELECT
          ${liquidationBucket} AS bucket_time,
          COALESCE(SUM(CASE WHEN lower(side) IN ('sell', 'long') THEN COALESCE(cost, price * amount, 0) ELSE 0 END), 0) AS long_value,
          COALESCE(SUM(CASE WHEN lower(side) IN ('buy', 'short') THEN COALESCE(cost, price * amount, 0) ELSE 0 END), 0) AS short_value,
          COALESCE(SUM(COALESCE(cost, price * amount, 0)), 0) AS total_value,
          COUNT(*) AS event_count
        FROM market_data.liquidation_event
        WHERE exchange = $1
          AND symbol = $2
          AND market_type = 'perpetual'
        GROUP BY bucket_time
        ORDER BY bucket_time DESC
        LIMIT 600;
      `,
      [selection.exchange, symbol]
    )
  ]);

  return {
    openInterest: openInterest.rows.reverse().map((row) => ({
      time: toSeconds(row.open_time),
      value: Number(row.open_interest_value ?? row.open_interest_amount ?? 0)
    })),
    longShortRatio: longShortRatio.rows.reverse().map((row) => ({
      time: toSeconds(row.open_time),
      value: Number(row.long_short_ratio)
    })),
    takerBuySell: takerBuySell.rows.reverse().map((row) => ({
      time: toSeconds(row.open_time),
      buyVolume: Number(row.buy_volume ?? 0),
      sellVolume: Number(row.sell_volume ?? 0),
      buySellRatio: Number(row.buy_sell_ratio)
    })),
    fundingRate: fundingRate.rows.reverse().map((row) => ({
      time: toSeconds(row.funding_time),
      value: Number(row.funding_rate)
    })),
    liquidations: liquidations.rows.reverse().map((row) => ({
      time: toSeconds(row.bucket_time),
      longValue: Number(row.long_value),
      shortValue: Number(row.short_value),
      totalValue: Number(row.total_value),
      count: Number(row.event_count)
    })),
    openInterestBreakdown: [],
    longShortRatioBreakdown: [],
    takerBuySellBreakdown: [],
    fundingRateBreakdown: [],
    liquidationBreakdown: []
  };
}
