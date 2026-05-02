"use client";

import { useEffect, useMemo, useState } from "react";
import MarketChart from "./MarketChart";
import type { ChartResponse, DerivativesResponse, Exchange, Interval, StatusRow, WatchlistAsset } from "@/lib/types";
import type { Selection } from "@/lib/market-data";

const exchanges: Exchange[] = ["all", "binance", "bybit", "kucoinfutures", "coinbase"];
const intervals: Interval[] = ["1m", "5m", "1h"];
const autoRefreshMs = 30_000;
const exchangeLabels: Record<Exchange, string> = {
  all: "All",
  binance: "Binance",
  bybit: "Bybit",
  kucoinfutures: "KuCoin",
  coinbase: "Coinbase"
};
type LoadState = "idle" | "loading" | "error" | "ready";

const taipeiTimeZone = "Asia/Taipei";

function compactNumber(value: number | null | undefined, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }

  return Intl.NumberFormat("en", {
    notation: "compact",
    maximumFractionDigits: digits
  }).format(value);
}

function priceNumber(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }

  return Intl.NumberFormat("en", {
    maximumFractionDigits: value > 100 ? 2 : 6
  }).format(value);
}

function pctNumber(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }

  return `${(value * 100).toFixed(4)}%`;
}

function ratioNumber(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value) || !Number.isFinite(value)) {
    return "-";
  }

  return Intl.NumberFormat("en", {
    notation: Math.abs(value) >= 1_000_000 ? "compact" : "standard",
    maximumFractionDigits: Math.abs(value) >= 100 ? 2 : 4
  }).format(value);
}

function shortTime(value: string | null | undefined) {
  if (!value) {
    return "-";
  }

  return new Intl.DateTimeFormat("en", {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    timeZone: taipeiTimeZone,
    timeZoneName: "short"
  }).format(new Date(value));
}

type DashboardProps = {
  initialAssets: WatchlistAsset[];
  initialStatus: StatusRow[];
  initialChart: ChartResponse;
  initialDerivatives: DerivativesResponse;
  initialSelection: Selection;
};

export default function Dashboard({
  initialAssets,
  initialStatus,
  initialChart,
  initialDerivatives,
  initialSelection
}: DashboardProps) {
  const [assets, setAssets] = useState<WatchlistAsset[]>(initialAssets);
  const [asset, setAsset] = useState(initialSelection.asset);
  const [exchange, setExchange] = useState<Exchange>(initialSelection.exchange);
  const [interval, setInterval] = useState<Interval>(initialSelection.interval);
  const [chart, setChart] = useState<ChartResponse | null>(initialChart);
  const [derivatives, setDerivatives] = useState<DerivativesResponse | null>(initialDerivatives);
  const [status, setStatus] = useState<StatusRow[]>(initialStatus);
  const [loadState, setLoadState] = useState<LoadState>("ready");
  const [error, setError] = useState<string | null>(null);
  const [lastRefreshAt, setLastRefreshAt] = useState(() => new Date().toISOString());
  const [watchlistCollapsed, setWatchlistCollapsed] = useState(false);

  useEffect(() => {
    async function loadStaticData() {
      const [watchlistResponse, statusResponse] = await Promise.all([
        fetch("/api/watchlist"),
        fetch("/api/status")
      ]);
      const watchlistData = await watchlistResponse.json();
      const statusData = await statusResponse.json();
      setAssets(watchlistData.assets);
      setStatus(statusData.status);
    }

    loadStaticData().catch((err) => {
      setError(err instanceof Error ? err.message : "Failed to load watchlist.");
      setLoadState("error");
    });
  }, []);

  useEffect(() => {
    let cancelled = false;

    async function loadSelection(showLoading: boolean) {
      if (showLoading) {
        setLoadState("loading");
      }
      setError(null);

      const query = new URLSearchParams({ asset, exchange, interval });
      const [chartResponse, derivativesResponse, statusResponse] = await Promise.all([
        fetch(`/api/chart?${query.toString()}`),
        fetch(`/api/derivatives?${query.toString()}`),
        fetch("/api/status")
      ]);

      if (!chartResponse.ok) {
        throw new Error(`Chart API failed: ${chartResponse.status}`);
      }
      if (!derivativesResponse.ok) {
        throw new Error(`Derivatives API failed: ${derivativesResponse.status}`);
      }

      const [chartData, derivativesData, statusData] = await Promise.all([
        chartResponse.json(),
        derivativesResponse.json(),
        statusResponse.json()
      ]);

      if (cancelled) {
        return;
      }

      setChart(chartData);
      setDerivatives(derivativesData);
      setStatus(statusData.status);
      setLastRefreshAt(new Date().toISOString());
      setLoadState("ready");
    }

    const handleError = (err: unknown) => {
      if (cancelled) {
        return;
      }
      setError(err instanceof Error ? err.message : "Failed to load dashboard data.");
      setLoadState("error");
    };

    loadSelection(true).catch(handleError);
    const intervalId = window.setInterval(() => {
      if (document.visibilityState === "visible") {
        loadSelection(false).catch(handleError);
      }
    }, autoRefreshMs);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
    };
  }, [asset, exchange, interval]);

  const selectedSymbol = chart?.symbol ?? `${asset}/USDT:USDT`;
  const selectedMarketType = chart?.marketType ?? "perpetual";

  const selectedStatus = useMemo(
    () =>
      status.filter((row) => {
        if (exchange === "all") {
          return row.symbol.split("/")[0]?.toUpperCase() === asset.toUpperCase();
        }

        return (
          row.exchange === exchange &&
          row.symbol === selectedSymbol &&
          row.marketType === selectedMarketType
        );
      }),
    [asset, exchange, selectedMarketType, selectedSymbol, status]
  );

  const latestCandle = chart?.candles.at(-1);
  const previousCandle = chart?.candles.at(-2);
  const latestMark = chart?.markPrice.at(-1)?.value;
  const latestIndex = chart?.indexPrice.at(-1)?.value;
  const latestOi = derivatives?.openInterest.at(-1)?.value;
  const latestLongShortRatio = derivatives?.longShortRatio.at(-1)?.value;
  const latestTakerBuySell = derivatives?.takerBuySell.at(-1);
  const latestFunding = derivatives?.fundingRate.at(-1)?.value;
  const latestLiquidation = derivatives?.liquidations.at(-1);
  const basis = latestMark && latestIndex ? (latestMark - latestIndex) / latestIndex : null;
  const isAggregateExchange = exchange === "all";
  const oiVolumeRatio =
    latestOi !== null &&
    latestOi !== undefined &&
    latestCandle?.volume !== null &&
    latestCandle?.volume !== undefined &&
    latestCandle.volume > 0
      ? latestOi / latestCandle.volume
      : null;
  const move =
    latestCandle && previousCandle
      ? (latestCandle.close - previousCandle.close) / previousCandle.close
      : null;
  const oiNote = isAggregateExchange ? "all exchanges" : interval === "1m" ? "5m fallback" : interval;
  const longShortNote = exchange === "binance" || isAggregateExchange ? "Binance global accounts" : "Binance only";
  const takerNote = exchange === "binance" || isAggregateExchange ? "Binance taker volume" : "Binance only";
  const volumeNote = isAggregateExchange ? "all exchange notional" : "price-row bar";
  const liquidationNote = isAggregateExchange ? "all exchanges quote notional" : "latest bucket quote notional";
  const fundingNote = isAggregateExchange ? "average print" : "latest print";

  return (
    <main className="relative mx-auto flex min-h-screen max-w-[1520px] flex-col gap-6 px-4 py-5 font-body text-ink sm:px-6 lg:px-8">
      <section className="grain lift-in panel rounded-[2rem] px-5 py-5 sm:px-7">
        <div className="relative z-10 flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <p className="text-xs font-bold uppercase tracking-[0.36em] text-moss/60">Crypto Market Intelligence</p>
            <h1 className="font-display text-4xl font-semibold tracking-[-0.04em] text-ink sm:text-6xl">
              Market watchtower
            </h1>
            <p className="mt-3 max-w-2xl text-sm leading-6 text-moss/75">
              Coinglass-style stacked panels on one synchronized time axis: spot or perpetual price
              first, then open interest, taker flow, liquidation, and funding rate when available.
            </p>
          </div>

          <div className="grid grid-cols-3 gap-2 rounded-2xl border border-moss/10 bg-cream/50 p-2">
            {intervals.map((item) => (
              <button
                key={item}
                onClick={() => setInterval(item)}
                className={`rounded-xl px-4 py-2 text-sm font-bold transition ${
                  interval === item ? "bg-ink text-cream" : "text-moss hover:bg-moss/10"
                }`}
              >
                {item}
              </button>
            ))}
          </div>
          <div className="rounded-2xl border border-moss/10 bg-cream/50 px-4 py-3 text-xs font-bold uppercase tracking-[0.18em] text-moss/65">
            Auto refresh 30s<br />
            <span className="font-normal tracking-normal text-moss/55">Taipei: {shortTime(lastRefreshAt)}</span>
          </div>
        </div>
      </section>

      <section
        className={`grid gap-4 lg:gap-5 ${
          watchlistCollapsed
            ? "lg:grid-cols-[86px_minmax(0,1fr)_250px]"
            : "lg:grid-cols-[260px_minmax(0,1fr)_250px]"
        }`}
      >
        <aside className="lift-in panel rounded-[2rem] p-3 [animation-delay:80ms]">
          <div className={`mb-3 flex items-center ${watchlistCollapsed ? "justify-center" : "justify-between"}`}>
            {!watchlistCollapsed ? <h2 className="font-display text-xl font-semibold">Watchlist</h2> : null}
            <button
              type="button"
              onClick={() => setWatchlistCollapsed((value) => !value)}
              className="rounded-full bg-ink px-3 py-2 text-xs font-black uppercase tracking-[0.16em] text-cream transition hover:bg-moss"
              aria-label={watchlistCollapsed ? "Expand watchlist" : "Collapse watchlist"}
            >
              {watchlistCollapsed ? ">>" : "<<"}
            </button>
          </div>

          {!watchlistCollapsed ? (
            <div className="mb-3 rounded-2xl bg-moss/10 px-3 py-2 text-[10px] font-bold uppercase tracking-[0.22em] text-moss/70">
              live db / scrollable
            </div>
          ) : null}

          <div className={`${watchlistCollapsed ? "max-h-[66vh] space-y-2" : "max-h-[58vh] space-y-2 pr-1"} overflow-y-auto`}>
            {assets.map((item) => {
              const available = isAggregateExchange ? item.exchanges.length > 0 : item.exchanges.includes(exchange);
              return (
                <button
                  key={item.baseAsset}
                  onClick={() => setAsset(item.baseAsset)}
                  title={isAggregateExchange ? `${item.baseAsset} aggregate` : `${item.baseAsset}/USDT perpetual`}
                  className={`w-full rounded-2xl border text-left transition ${
                    asset === item.baseAsset
                      ? "border-ink bg-ink text-cream shadow-panel"
                      : "border-moss/10 bg-cream/45 text-ink hover:border-moss/30"
                  } ${watchlistCollapsed ? "px-2 py-3 text-center" : "px-3 py-3"}`}
                >
                  <div className={`flex items-center ${watchlistCollapsed ? "justify-center" : "justify-between"}`}>
                    <span className={watchlistCollapsed ? "text-sm font-black tracking-[-0.04em]" : "text-xl font-black tracking-[-0.04em]"}>
                      {item.baseAsset}
                    </span>
                    {!watchlistCollapsed ? (
                    <span
                      className={`rounded-full px-2.5 py-1 text-[11px] font-bold uppercase tracking-[0.18em] ${
                        available ? "bg-glade/18 text-glade" : "bg-ember/10 text-ember"
                      }`}
                    >
                      {available ? "ready" : "empty"}
                    </span>
                    ) : null}
                  </div>
                  {!watchlistCollapsed ? (
                    <p className="mt-1 text-[11px] opacity-70">
                      {isAggregateExchange ? "all exchange aggregate" : `${item.baseAsset}/USDT perpetual`}
                    </p>
                  ) : null}
                </button>
              );
            })}
          </div>

          <div className={watchlistCollapsed ? "mt-3 grid gap-2" : "mt-4 grid grid-cols-2 gap-2"}>
            {exchanges.map((item) => (
              <button
                key={item}
                onClick={() => setExchange(item)}
                className={`rounded-2xl font-bold uppercase transition ${
                  exchange === item ? "bg-brass text-ink" : "bg-moss/10 text-moss hover:bg-moss/15"
                } ${watchlistCollapsed ? "px-2 py-2 text-[10px] tracking-[0.08em]" : "px-2 py-2.5 text-[11px] tracking-[0.12em]"}`}
              >
                {watchlistCollapsed ? exchangeLabels[item].slice(0, 2) : exchangeLabels[item]}
              </button>
            ))}
          </div>
        </aside>

        <section className="lift-in panel rounded-[2rem] p-3 [animation-delay:140ms] sm:p-4">
          <div className="mb-3 flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
            <div>
              <p className="text-xs font-bold uppercase tracking-[0.3em] text-moss/55">
                {exchangeLabels[exchange]} / {selectedMarketType} / {interval}
              </p>
              <h2 className="font-display text-3xl font-semibold tracking-[-0.03em]">
                {selectedSymbol}
              </h2>
            </div>
            <div className="flex flex-wrap gap-2 text-xs font-bold uppercase tracking-[0.16em]">
              <span className="rounded-full bg-glade/15 px-3 py-1.5 text-glade">Price</span>
              <span className="rounded-full bg-moss/10 px-3 py-1.5 text-moss">Open Interest</span>
              <span className="rounded-full bg-brass/20 px-3 py-1.5 text-moss">OI / Volume</span>
              <span className="rounded-full bg-moss/10 px-3 py-1.5 text-moss">Long / Short</span>
              <span className="rounded-full bg-glade/15 px-3 py-1.5 text-glade">Taker Buy/Sell</span>
              <span className="rounded-full bg-ember/10 px-3 py-1.5 text-ember">Liquidation</span>
              <span className="rounded-full bg-brass/20 px-3 py-1.5 text-moss">Funding</span>
            </div>
          </div>

          {error ? (
            <div className="rounded-2xl border border-ember/20 bg-ember/10 p-4 text-sm text-ember">{error}</div>
          ) : null}

          <MarketChart
            candles={chart?.candles ?? []}
            markPrice={chart?.markPrice ?? []}
            indexPrice={chart?.indexPrice ?? []}
            openInterest={derivatives?.openInterest ?? []}
            longShortRatio={derivatives?.longShortRatio ?? []}
            takerBuySell={derivatives?.takerBuySell ?? []}
            liquidations={derivatives?.liquidations ?? []}
            fundingRate={derivatives?.fundingRate ?? []}
            volumeBreakdown={chart?.volumeBreakdown ?? []}
            openInterestBreakdown={derivatives?.openInterestBreakdown ?? []}
            longShortRatioBreakdown={derivatives?.longShortRatioBreakdown ?? []}
            takerBuySellBreakdown={derivatives?.takerBuySellBreakdown ?? []}
            liquidationBreakdown={derivatives?.liquidationBreakdown ?? []}
            fundingRateBreakdown={derivatives?.fundingRateBreakdown ?? []}
          />

          <div className="mt-3 grid gap-2 sm:grid-cols-4">
            <Metric label="Last close" value={priceNumber(latestCandle?.close)} note={pctNumber(move)} />
            <Metric label="Open interest" value={compactNumber(latestOi, 2)} note={oiNote} />
            <Metric label="OI / Volume" value={ratioNumber(oiVolumeRatio)} note="latest OI / volume" />
            <Metric label="Long / Short" value={ratioNumber(latestLongShortRatio)} note={longShortNote} />
          </div>

          <div className="mt-2 grid gap-2 sm:grid-cols-4">
            <Metric label="Liquidation quote" value={compactNumber(latestLiquidation?.totalValue, 2)} note={liquidationNote} />
            <Metric label="Funding" value={pctNumber(latestFunding)} note={fundingNote} />
            <Metric label="Volume" value={compactNumber(latestCandle?.volume, 1)} note={volumeNote} />
            <Metric label="Mark price" value={priceNumber(latestMark)} note="latest mark" />
          </div>

          <div className="mt-2 grid gap-2 sm:grid-cols-4">
            <Metric label="Index price" value={priceNumber(latestIndex)} note="latest index" />
            <Metric label="Mark basis" value={pctNumber(basis)} note="mark vs index" />
            <Metric label="State" value={loadState === "loading" ? "Loading" : "Ready"} note="API status" />
            <Metric label="Taker B/S" value={ratioNumber(latestTakerBuySell?.buySellRatio)} note={takerNote} />
          </div>

          <div className="mt-2 grid gap-2 sm:grid-cols-4">
            <Metric label="Taker buy" value={compactNumber(latestTakerBuySell?.buyVolume, 2)} note="base volume" />
            <Metric label="Taker sell" value={compactNumber(latestTakerBuySell?.sellVolume, 2)} note="base volume" />
            <Metric label="OI points" value={compactNumber(derivatives?.openInterest.length, 0)} note="history row" />
            <Metric label="L/S points" value={compactNumber(derivatives?.longShortRatio.length, 0)} note="history row" />
            <Metric label="Taker points" value={compactNumber(derivatives?.takerBuySell.length, 0)} note="history row" />
          </div>
        </section>

        <aside className="lift-in space-y-4 [animation-delay:200ms]">
          <section className="panel rounded-[2rem] p-4">
            <h2 className="font-display text-xl font-semibold">Derivatives</h2>
            <div className="mt-3 space-y-2">
              <Metric label="Open interest" value={compactNumber(latestOi, 2)} note={oiNote} />
              <Metric label="OI / Volume" value={ratioNumber(oiVolumeRatio)} note="latest OI / volume" />
              <Metric label="Long / Short" value={ratioNumber(latestLongShortRatio)} note={longShortNote} />
              <Metric label="Taker buy/sell" value={ratioNumber(latestTakerBuySell?.buySellRatio)} note={takerNote} />
              <Metric label="Funding rate" value={pctNumber(latestFunding)} note={fundingNote} />
              <Metric label="Liquidations quote" value={compactNumber(latestLiquidation?.totalValue, 2)} note={`${derivatives?.liquidations.length ?? 0} buckets, quote notional`} />
            </div>
          </section>

          <section className="panel rounded-[2rem] p-4">
            <div className="flex items-center justify-between gap-3">
              <h2 className="font-display text-xl font-semibold">Data quality</h2>
              <span className="rounded-full bg-ink px-2 py-1 text-[10px] font-bold uppercase tracking-[0.12em] text-cream">
                {selectedStatus.length} streams
              </span>
            </div>

            <div className="mt-3 max-h-[390px] space-y-2 overflow-auto pr-1">
              {selectedStatus.map((row) => (
                <div
                  key={`${row.exchange}-${row.symbol}-${row.marketType}-${row.sourceDataset}-${row.barInterval}`}
                  className="rounded-2xl border border-moss/10 bg-cream/45 p-2.5"
                >
                  <div className="flex items-center justify-between gap-3">
                    <span className="text-xs font-bold">{row.sourceDataset}</span>
                    <span className="rounded-full bg-moss/10 px-2 py-1 text-[11px] font-bold text-moss/70">
                      {row.barInterval}
                    </span>
                  </div>
                  <div className="mt-2 flex items-center justify-between text-xs text-moss/70">
                    <span>{compactNumber(row.rowCount, 1)} rows</span>
                    <span>{shortTime(row.lastTime)}</span>
                  </div>
                </div>
              ))}
            </div>
          </section>
        </aside>
      </section>
    </main>
  );
}

function Metric({ label, value, note }: { label: string; value: string; note: string }) {
  return (
    <div className="rounded-2xl border border-moss/10 bg-cream/50 p-2.5">
      <p className="text-[10px] font-bold uppercase tracking-[0.18em] text-moss/55">{label}</p>
      <p className="mt-1 truncate font-display text-xl font-semibold tracking-[-0.04em] text-ink">{value}</p>
      <p className="mt-0.5 truncate text-[10px] text-moss/60">{note}</p>
    </div>
  );
}
