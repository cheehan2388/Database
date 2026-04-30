"use client";

import { useState } from "react";
import type { Candle, ExchangeLinePoint, ExchangeLiquidationPoint, LinePoint, LiquidationPoint } from "@/lib/types";

type MarketChartProps = {
  candles: Candle[];
  markPrice: LinePoint[];
  indexPrice: LinePoint[];
  openInterest: LinePoint[];
  liquidations: LiquidationPoint[];
  fundingRate: LinePoint[];
  volumeBreakdown: ExchangeLinePoint[];
  openInterestBreakdown: ExchangeLinePoint[];
  liquidationBreakdown: ExchangeLiquidationPoint[];
  fundingRateBreakdown: ExchangeLinePoint[];
};

type Domain = {
  min: number;
  max: number;
};

type HoverState = {
  x: number;
  y: number;
  candle: Candle;
  mark: LinePoint | null;
  index: LinePoint | null;
  openInterest: LinePoint | null;
  oiVolumeRatio: LinePoint | null;
  liquidation: LiquidationPoint | null;
  funding: LinePoint | null;
  exchangeBreakdown: ExchangeHoverRow[];
};

type ExchangeHoverRow = {
  exchange: string;
  volume: number | null;
  openInterest: number | null;
  oiVolumeRatio: number | null;
  liquidation: number | null;
  funding: number | null;
};

const width = 1240;
const left = 124;
const right = 68;
const plotWidth = width - left - right;
const panels = {
  price: { top: 22, height: 330, label: "PRICE", note: "Candles / Volume / Mark / Index" },
  openInterest: { top: 352, height: 130, label: "OPEN INTEREST", note: "OI value or amount" },
  oiVolumeRatio: { top: 482, height: 130, label: "OI / VOLUME", note: "Positioning intensity" },
  liquidation: { top: 612, height: 130, label: "LIQUIDATION", note: "Long below zero / Short above zero" },
  funding: { top: 742, height: 130, label: "FUNDING RATE", note: "Positive / Negative funding" }
};
const plotBottom = panels.funding.top + panels.funding.height;
const xAxisLabelY = plotBottom + 22;
const height = xAxisLabelY + 10;
const exchangeOrder = ["binance", "bybit", "kucoinfutures", "coinbase"];
const exchangeLabels: Record<string, string> = {
  binance: "Binance",
  bybit: "Bybit",
  kucoinfutures: "KuCoin",
  coinbase: "Coinbase"
};

function valueDomain(values: number[], symmetric = false): Domain {
  const clean = values.filter((value) => Number.isFinite(value));
  if (!clean.length) {
    return { min: 0, max: 1 };
  }

  if (symmetric) {
    const maxAbs = Math.max(...clean.map((value) => Math.abs(value)), 1e-9);
    return { min: -maxAbs * 1.15, max: maxAbs * 1.15 };
  }

  let min = Math.min(...clean);
  let max = Math.max(...clean);
  if (min === max) {
    min -= Math.abs(min || 1) * 0.01;
    max += Math.abs(max || 1) * 0.01;
  }
  const padding = (max - min) * 0.08;
  return { min: min - padding, max: max + padding };
}

function yScale(value: number, domain: Domain, top: number, panelHeight: number) {
  const usable = panelHeight - 22;
  const yTop = top + 10;
  const ratio = (value - domain.min) / (domain.max - domain.min || 1);
  return yTop + usable - ratio * usable;
}

function xScale(time: number, domain: Domain) {
  const ratio = (time - domain.min) / (domain.max - domain.min || 1);
  return left + ratio * plotWidth;
}

function linePath(points: LinePoint[], xDomain: Domain, yDomain: Domain, top: number, panelHeight: number) {
  const filtered = points.filter((point) => point.time >= xDomain.min && point.time <= xDomain.max);
  return filtered
    .map((point, index) => {
      const command = index === 0 ? "M" : "L";
      return `${command}${xScale(point.time, xDomain).toFixed(2)},${yScale(point.value, yDomain, top, panelHeight).toFixed(2)}`;
    })
    .join(" ");
}

function formatValue(value: number) {
  return Intl.NumberFormat("en", {
    notation: Math.abs(value) >= 1_000_000 ? "compact" : "standard",
    maximumFractionDigits: Math.abs(value) >= 100 ? 0 : 4
  }).format(value);
}

function formatCompactValue(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value) || !Number.isFinite(value)) {
    return "-";
  }

  return Intl.NumberFormat("en", {
    notation: Math.abs(value) >= 1_000_000 ? "compact" : "standard",
    maximumFractionDigits: Math.abs(value) >= 100 ? 2 : 4
  }).format(value);
}

function formatPreciseValue(value: number | null | undefined, digits = 8) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }

  return Intl.NumberFormat("en", {
    maximumFractionDigits: digits
  }).format(value);
}

function formatPercent(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }

  return `${(value * 100).toFixed(6)}%`;
}

const taipeiTimeZone = "Asia/Taipei";

function formatTime(seconds: number) {
  return new Intl.DateTimeFormat("en", {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    timeZone: taipeiTimeZone
  }).format(new Date(seconds * 1000));
}

function formatExactTime(seconds: number) {
  return new Intl.DateTimeFormat("en", {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    timeZone: taipeiTimeZone,
    timeZoneName: "short"
  }).format(new Date(seconds * 1000));
}

function ticks(domain: Domain, count: number) {
  if (domain.max <= domain.min) {
    return [domain.min];
  }
  return Array.from({ length: count }, (_, index) => domain.min + ((domain.max - domain.min) * index) / (count - 1));
}

function filteredPoints<T extends { time: number }>(points: T[], xDomain: Domain) {
  return points.filter((point) => point.time >= xDomain.min && point.time <= xDomain.max);
}

function nearestByTime<T extends { time: number }>(points: T[], time: number): T | null {
  if (!points.length) {
    return null;
  }

  let low = 0;
  let high = points.length - 1;
  while (low < high) {
    const mid = Math.floor((low + high) / 2);
    if (points[mid].time < time) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }

  const current = points[low];
  const previous = low > 0 ? points[low - 1] : null;
  if (!previous) {
    return current;
  }

  return Math.abs(current.time - time) < Math.abs(previous.time - time) ? current : previous;
}

function nearestByTimeWithin<T extends { time: number }>(points: T[], time: number, maxDistanceSeconds: number): T | null {
  const nearest = nearestByTime(points, time);
  if (!nearest || Math.abs(nearest.time - time) > maxDistanceSeconds) {
    return null;
  }

  return nearest;
}

function buildExchangeHoverRows({
  time,
  stepSeconds,
  volumeBreakdown,
  openInterestBreakdown,
  liquidationBreakdown,
  fundingRateBreakdown
}: {
  time: number;
  stepSeconds: number;
  volumeBreakdown: ExchangeLinePoint[];
  openInterestBreakdown: ExchangeLinePoint[];
  liquidationBreakdown: ExchangeLiquidationPoint[];
  fundingRateBreakdown: ExchangeLinePoint[];
}) {
  const observed = new Set([
    ...volumeBreakdown.map((point) => point.exchange),
    ...openInterestBreakdown.map((point) => point.exchange),
    ...liquidationBreakdown.map((point) => point.exchange),
    ...fundingRateBreakdown.map((point) => point.exchange)
  ]);
  const exchanges = [
    ...exchangeOrder.filter((exchange) => observed.has(exchange)),
    ...Array.from(observed).filter((exchange) => !exchangeOrder.includes(exchange)).sort()
  ];

  return exchanges
    .map((exchange) => {
      const volume = nearestByTimeWithin(
        volumeBreakdown.filter((point) => point.exchange === exchange),
        time,
        Math.max(stepSeconds * 0.75, 60)
      );
      const openInterest = nearestByTimeWithin(
        openInterestBreakdown.filter((point) => point.exchange === exchange),
        time,
        Math.max(stepSeconds * 1.2, 360)
      );
      const liquidation = nearestByTimeWithin(
        liquidationBreakdown.filter((point) => point.exchange === exchange),
        time,
        Math.max(stepSeconds * 0.75, 60)
      );
      const funding = nearestByTimeWithin(
        fundingRateBreakdown.filter((point) => point.exchange === exchange),
        time,
        8 * 60 * 60 + 60
      );
      const oiValue = openInterest?.value ?? null;
      const volumeValue = volume?.value ?? null;

      return {
        exchange,
        volume: volumeValue,
        openInterest: oiValue,
        oiVolumeRatio: oiValue !== null && volumeValue !== null && volumeValue > 0 ? oiValue / volumeValue : null,
        liquidation: liquidation?.totalValue ?? null,
        funding: funding?.value ?? null
      };
    })
    .filter((row) =>
      [row.volume, row.openInterest, row.oiVolumeRatio, row.liquidation, row.funding].some(
        (value) => value !== null && value !== undefined
      )
    );
}

function buildOiVolumeRatio(candles: Candle[], openInterest: LinePoint[]) {
  if (!candles.length || !openInterest.length) {
    return [];
  }

  return candles.flatMap((candle) => {
    const nearestOi = nearestByTime(openInterest, candle.time);
    if (!nearestOi || candle.volume <= 0) {
      return [];
    }

    return [{ time: candle.time, value: nearestOi.value / candle.volume }];
  });
}

export default function MarketChart({
  candles,
  markPrice,
  indexPrice,
  openInterest,
  liquidations,
  fundingRate,
  volumeBreakdown,
  openInterestBreakdown,
  liquidationBreakdown,
  fundingRateBreakdown
}: MarketChartProps) {
  const [hover, setHover] = useState<HoverState | null>(null);

  if (!candles.length) {
    return (
      <div className="flex h-[820px] items-center justify-center rounded-[1.5rem] border border-dashed border-moss/25 bg-cream/40 text-sm text-moss/70">
        No chart data for this selection.
      </div>
    );
  }

  const xDomain = {
    min: candles[0].time,
    max: candles[candles.length - 1].time
  };
  const visibleCandles = filteredPoints(candles, xDomain);
  const visibleMark = filteredPoints(markPrice, xDomain);
  const visibleIndex = filteredPoints(indexPrice, xDomain);
  const visibleOi = filteredPoints(openInterest, xDomain);
  const visibleOiVolumeRatio = filteredPoints(buildOiVolumeRatio(candles, openInterest), xDomain);
  const visibleLiquidations = filteredPoints(liquidations, xDomain);
  const visibleFunding = filteredPoints(fundingRate, xDomain);
  const visibleVolumeBreakdown = filteredPoints(volumeBreakdown, xDomain);
  const visibleOiBreakdown = filteredPoints(openInterestBreakdown, xDomain);
  const visibleLiquidationBreakdown = filteredPoints(liquidationBreakdown, xDomain);
  const visibleFundingBreakdown = filteredPoints(fundingRateBreakdown, xDomain);

  const priceDomain = valueDomain([
    ...visibleCandles.flatMap((bar) => [bar.high, bar.low]),
    ...visibleMark.map((point) => point.value),
    ...visibleIndex.map((point) => point.value)
  ]);
  const volumeDomain = valueDomain([0, ...visibleCandles.map((bar) => bar.volume)]);
  const oiDomain = valueDomain(visibleOi.map((point) => point.value));
  const oiVolumeDomain = valueDomain(visibleOiVolumeRatio.map((point) => point.value));
  const liquidationMax = Math.max(
    ...visibleLiquidations.flatMap((point) => [point.longValue, point.shortValue]),
    0
  );
  const liquidationDomain = { min: -Math.max(liquidationMax, 1), max: Math.max(liquidationMax, 1) };
  const fundingDomain = valueDomain(visibleFunding.map((point) => point.value), true);
  const candleWidth = Math.max(1, Math.min(8, (plotWidth / Math.max(visibleCandles.length, 1)) * 0.56));
  const stepSeconds = Math.max(
    60,
    Math.round((xDomain.max - xDomain.min) / Math.max(visibleCandles.length - 1, 1))
  );

  function buildHoverState(candle: Candle, y: number): HoverState {
    return {
      x: xScale(candle.time, xDomain),
      y,
      candle,
      mark: nearestByTime(visibleMark, candle.time),
      index: nearestByTime(visibleIndex, candle.time),
      openInterest: nearestByTime(visibleOi, candle.time),
      oiVolumeRatio: nearestByTime(visibleOiVolumeRatio, candle.time),
      liquidation: nearestByTime(visibleLiquidations, candle.time),
      funding: nearestByTime(visibleFunding, candle.time),
      exchangeBreakdown: buildExchangeHoverRows({
        time: candle.time,
        stepSeconds,
        volumeBreakdown: visibleVolumeBreakdown,
        openInterestBreakdown: visibleOiBreakdown,
        liquidationBreakdown: visibleLiquidationBreakdown,
        fundingRateBreakdown: visibleFundingBreakdown
      })
    };
  }

  function handlePointer(event: React.PointerEvent<SVGElement> | React.MouseEvent<SVGElement>) {
    const svgElement =
      event.currentTarget instanceof SVGSVGElement ? event.currentTarget : event.currentTarget.ownerSVGElement;
    if (!svgElement) {
      return;
    }

    const rect = svgElement.getBoundingClientRect();
    const svgX = ((event.clientX - rect.left) / rect.width) * width;
    const svgY = ((event.clientY - rect.top) / rect.height) * height;
    if (svgX < left || svgX > left + plotWidth || svgY < panels.price.top || svgY > plotBottom) {
      setHover(null);
      return;
    }

    const time = xDomain.min + ((svgX - left) / plotWidth) * (xDomain.max - xDomain.min);
    const candle = nearestByTime(visibleCandles, time);
    if (!candle) {
      setHover(null);
      return;
    }

    setHover(buildHoverState(candle, svgY));
  }

  const activeHover = hover ?? buildHoverState(visibleCandles[visibleCandles.length - 1], panels.price.top + 64);

  return (
    <div className="overflow-hidden rounded-[1.6rem] border border-moss/10 bg-cream/35">
      <svg
        role="img"
        aria-label="Stacked market chart"
        viewBox={`0 0 ${width} ${height}`}
        className="block h-auto w-full"
        preserveAspectRatio="none"
        onPointerMove={handlePointer}
        onPointerDown={handlePointer}
        onPointerLeave={() => setHover(null)}
        onMouseMove={handlePointer}
        onMouseDown={handlePointer}
        onClick={handlePointer}
        onMouseLeave={() => setHover(null)}
      >
        <defs>
          <linearGradient id="priceFill" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor="#708f63" stopOpacity="0.16" />
            <stop offset="100%" stopColor="#708f63" stopOpacity="0" />
          </linearGradient>
        </defs>

        <rect x="0" y="0" width={width} height={height} fill="rgba(255,250,236,0.28)" />
        {Object.values(panels).map((panel) => (
          <PanelFrame key={panel.label} {...panel} />
        ))}
        {ticks(xDomain, 7).map((tick, index) => (
          <g key={`x-${tick}-${index}`}>
            <line x1={xScale(tick, xDomain)} x2={xScale(tick, xDomain)} y1={panels.price.top} y2={plotBottom} stroke="rgba(36,49,38,0.07)" />
            <text x={xScale(tick, xDomain)} y={xAxisLabelY} textAnchor="middle" className="fill-moss/60 text-[12px]">
              {formatTime(tick)}
            </text>
          </g>
        ))}

        <PricePanel
          candles={visibleCandles}
          markPrice={visibleMark}
          indexPrice={visibleIndex}
          priceDomain={priceDomain}
          volumeDomain={volumeDomain}
          candleWidth={candleWidth}
          xDomain={xDomain}
        />
        <LinePanel
          points={visibleOi}
          xDomain={xDomain}
          yDomain={oiDomain}
          top={panels.openInterest.top}
          panelHeight={panels.openInterest.height}
          color="#243126"
          emptyLabel="No open interest history"
        />
        <LinePanel
          points={visibleOiVolumeRatio}
          xDomain={xDomain}
          yDomain={oiVolumeDomain}
          top={panels.oiVolumeRatio.top}
          panelHeight={panels.oiVolumeRatio.height}
          color="#c68e3f"
          emptyLabel="No OI / volume history"
        />
        <LiquidationPanel points={visibleLiquidations} xDomain={xDomain} yDomain={liquidationDomain} />
        <FundingPanel points={visibleFunding} xDomain={xDomain} yDomain={fundingDomain} />
        <rect
          data-testid="chart-interaction-layer"
          x={left}
          y={panels.price.top}
          width={plotWidth}
          height={plotBottom - panels.price.top}
          fill="#ffffff"
          opacity="0.001"
          pointerEvents="all"
          className="cursor-crosshair"
          onPointerMove={handlePointer}
          onPointerDown={handlePointer}
          onMouseMove={handlePointer}
          onMouseDown={handlePointer}
          onClick={handlePointer}
        />
        {activeHover ? (
          <CrosshairOverlay
            hover={activeHover}
            priceDomain={priceDomain}
            oiDomain={oiDomain}
            oiVolumeDomain={oiVolumeDomain}
            liquidationDomain={liquidationDomain}
            fundingDomain={fundingDomain}
          />
        ) : null}
      </svg>
    </div>
  );
}

function PanelFrame({ top, height: panelHeight, label, note }: { top: number; height: number; label: string; note: string }) {
  return (
    <g>
      <rect x="0" y={top} width={left} height={panelHeight} fill="rgba(36,49,38,0.035)" />
      <rect x={left} y={top} width={plotWidth} height={panelHeight} fill="transparent" />
      <line x1={left} x2={left} y1={top} y2={top + panelHeight} stroke="rgba(36,49,38,0.12)" />
      <line x1="0" x2={width} y1={top} y2={top} stroke="rgba(36,49,38,0.12)" />
      <text x="24" y={top + 34} className="fill-moss text-[18px] font-black tracking-[0.22em]">
        {label}
      </text>
      <text x="24" y={top + panelHeight - 36} className="fill-moss/55 text-[14px] font-bold">
        {note}
      </text>
    </g>
  );
}

function PricePanel({
  candles,
  markPrice,
  indexPrice,
  priceDomain,
  volumeDomain,
  candleWidth,
  xDomain
}: {
  candles: Candle[];
  markPrice: LinePoint[];
  indexPrice: LinePoint[];
  priceDomain: Domain;
  volumeDomain: Domain;
  candleWidth: number;
  xDomain: Domain;
}) {
  const panel = panels.price;
  const volumeTop = panel.top + panel.height * 0.74;
  const volumeHeight = panel.height * 0.22;

  return (
    <g>
      {ticks(priceDomain, 5).map((tick, index) => (
        <g key={`price-y-${tick}-${index}`}>
          <line x1={left} x2={left + plotWidth} y1={yScale(tick, priceDomain, panel.top, panel.height)} y2={yScale(tick, priceDomain, panel.top, panel.height)} stroke="rgba(36,49,38,0.07)" />
          <text x={width - 10} y={yScale(tick, priceDomain, panel.top, panel.height) + 4} textAnchor="end" className="fill-moss/55 text-[12px]">
            {formatValue(tick)}
          </text>
        </g>
      ))}
      {candles.map((bar, index) => {
        const x = xScale(bar.time, xDomain);
        const yHigh = yScale(bar.high, priceDomain, panel.top, panel.height);
        const yLow = yScale(bar.low, priceDomain, panel.top, panel.height);
        const yOpen = yScale(bar.open, priceDomain, panel.top, panel.height);
        const yClose = yScale(bar.close, priceDomain, panel.top, panel.height);
        const up = bar.close >= bar.open;
        const color = up ? "#2d7d55" : "#d9573f";
        const bodyTop = Math.min(yOpen, yClose);
        const bodyHeight = Math.max(1, Math.abs(yClose - yOpen));
        const volumeY = volumeTop + volumeHeight - ((bar.volume - volumeDomain.min) / (volumeDomain.max - volumeDomain.min || 1)) * volumeHeight;

        return (
          <g key={`candle-${bar.time}-${index}`}>
            <rect
              x={x - candleWidth / 2}
              y={volumeY}
              width={candleWidth}
              height={volumeTop + volumeHeight - volumeY}
              fill={up ? "rgba(45,125,85,0.22)" : "rgba(217,87,63,0.22)"}
            />
            <line x1={x} x2={x} y1={yHigh} y2={yLow} stroke={color} strokeWidth="1" />
            <rect x={x - candleWidth / 2} y={bodyTop} width={candleWidth} height={bodyHeight} fill={color} rx="0.6" />
          </g>
        );
      })}
      <path d={linePath(markPrice, xDomain, priceDomain, panel.top, panel.height)} fill="none" stroke="#c68e3f" strokeWidth="2" />
      <path d={linePath(indexPrice, xDomain, priceDomain, panel.top, panel.height)} fill="none" stroke="#4b6f92" strokeWidth="2" />
    </g>
  );
}

function LinePanel({
  points,
  xDomain,
  yDomain,
  top,
  panelHeight,
  color,
  emptyLabel
}: {
  points: LinePoint[];
  xDomain: Domain;
  yDomain: Domain;
  top: number;
  panelHeight: number;
  color: string;
  emptyLabel: string;
}) {
  if (!points.length) {
    return <EmptyLabel top={top} panelHeight={panelHeight} label={emptyLabel} />;
  }

  return (
    <g>
      {ticks(yDomain, 3).map((tick, index) => (
        <g key={`line-y-${top}-${tick}-${index}`}>
          <line x1={left} x2={left + plotWidth} y1={yScale(tick, yDomain, top, panelHeight)} y2={yScale(tick, yDomain, top, panelHeight)} stroke="rgba(36,49,38,0.07)" />
          <text x={width - 10} y={yScale(tick, yDomain, top, panelHeight) + 4} textAnchor="end" className="fill-moss/55 text-[12px]">
            {formatValue(tick)}
          </text>
        </g>
      ))}
      <path d={linePath(points, xDomain, yDomain, top, panelHeight)} fill="none" stroke={color} strokeWidth="2.2" />
    </g>
  );
}

function LiquidationPanel({ points, xDomain, yDomain }: { points: LiquidationPoint[]; xDomain: Domain; yDomain: Domain }) {
  const panel = panels.liquidation;
  const zeroY = yScale(0, yDomain, panel.top, panel.height);
  const barWidth = Math.max(1, Math.min(8, (plotWidth / Math.max(points.length, 1)) * 0.55));

  if (!points.length) {
    return <EmptyLabel top={panel.top} panelHeight={panel.height} label="No liquidation history in database yet" />;
  }

  return (
    <g>
      <line x1={left} x2={left + plotWidth} y1={zeroY} y2={zeroY} stroke="rgba(36,49,38,0.18)" />
      {points.map((point, index) => {
        const x = xScale(point.time, xDomain);
        const longY = yScale(-point.longValue, yDomain, panel.top, panel.height);
        const shortY = yScale(point.shortValue, yDomain, panel.top, panel.height);

        return (
          <g key={`liquidation-${point.time}-${index}`}>
            <rect x={x - barWidth / 2} y={zeroY} width={barWidth} height={longY - zeroY} fill="rgba(217,87,63,0.72)" />
            <rect x={x - barWidth / 2} y={shortY} width={barWidth} height={zeroY - shortY} fill="rgba(45,125,85,0.72)" />
          </g>
        );
      })}
    </g>
  );
}

function FundingPanel({ points, xDomain, yDomain }: { points: LinePoint[]; xDomain: Domain; yDomain: Domain }) {
  const panel = panels.funding;
  const zeroY = yScale(0, yDomain, panel.top, panel.height);
  const barWidth = Math.max(3, Math.min(12, (plotWidth / Math.max(points.length, 1)) * 0.45));

  if (!points.length) {
    return <EmptyLabel top={panel.top} panelHeight={panel.height} label="No funding history" />;
  }

  return (
    <g>
      <line x1={left} x2={left + plotWidth} y1={zeroY} y2={zeroY} stroke="rgba(36,49,38,0.18)" />
      {ticks(yDomain, 3).map((tick, index) => (
        <text key={`funding-y-${tick}-${index}`} x={width - 10} y={yScale(tick, yDomain, panel.top, panel.height) + 4} textAnchor="end" className="fill-moss/55 text-[12px]">
          {(tick * 100).toFixed(3)}%
        </text>
      ))}
      {points.map((point, index) => {
        const x = xScale(point.time, xDomain);
        const y = yScale(point.value, yDomain, panel.top, panel.height);
        const positive = point.value >= 0;

        return (
          <rect
            key={`funding-${point.time}-${index}`}
            x={x - barWidth / 2}
            y={positive ? y : zeroY}
            width={barWidth}
            height={Math.abs(zeroY - y)}
            fill={positive ? "rgba(45,125,85,0.66)" : "rgba(217,87,63,0.66)"}
            rx="1"
          />
        );
      })}
    </g>
  );
}

function EmptyLabel({ top, panelHeight, label }: { top: number; panelHeight: number; label: string }) {
  return (
    <text
      x={left + plotWidth / 2}
      y={top + panelHeight / 2 + 5}
      textAnchor="middle"
      className="fill-moss/45 text-[18px] font-black tracking-[0.24em]"
    >
      {label.toUpperCase()}
    </text>
  );
}

function CrosshairOverlay({
  hover,
  priceDomain,
  oiDomain,
  oiVolumeDomain,
  liquidationDomain,
  fundingDomain
}: {
  hover: HoverState;
  priceDomain: Domain;
  oiDomain: Domain;
  oiVolumeDomain: Domain;
  liquidationDomain: Domain;
  fundingDomain: Domain;
}) {
  const exchangeLines = hover.exchangeBreakdown.map((row) => {
    const label = exchangeLabels[row.exchange] ?? row.exchange;
    return `${label}: V ${formatCompactValue(row.volume)} | OI ${formatCompactValue(row.openInterest)} | OI/V ${formatCompactValue(row.oiVolumeRatio)} | Liq ${formatCompactValue(row.liquidation)} | F ${formatPercent(row.funding)}`;
  });
  const tooltipWidth = exchangeLines.length ? 560 : 330;
  const tooltipHeight = 264 + (exchangeLines.length ? 32 + exchangeLines.length * 18 : 0);
  const tooltipX = hover.x > width - tooltipWidth - 28 ? hover.x - tooltipWidth - 16 : hover.x + 16;
  const tooltipY = hover.y > height - tooltipHeight - 20 ? hover.y - tooltipHeight - 14 : hover.y + 14;
  const closeY = yScale(hover.candle.close, priceDomain, panels.price.top, panels.price.height);
  const oiY = hover.openInterest ? yScale(hover.openInterest.value, oiDomain, panels.openInterest.top, panels.openInterest.height) : null;
  const oiVolumeY = hover.oiVolumeRatio ? yScale(hover.oiVolumeRatio.value, oiVolumeDomain, panels.oiVolumeRatio.top, panels.oiVolumeRatio.height) : null;
  const liquidationY = hover.liquidation
    ? yScale(hover.liquidation.shortValue || -hover.liquidation.longValue || 0, liquidationDomain, panels.liquidation.top, panels.liquidation.height)
    : null;
  const fundingY = hover.funding ? yScale(hover.funding.value, fundingDomain, panels.funding.top, panels.funding.height) : null;
  const lines = [
    `Time: ${formatExactTime(hover.candle.time)}`,
    `O: ${formatPreciseValue(hover.candle.open, 8)}  H: ${formatPreciseValue(hover.candle.high, 8)}`,
    `L: ${formatPreciseValue(hover.candle.low, 8)}  C: ${formatPreciseValue(hover.candle.close, 8)}`,
    `Volume: ${formatPreciseValue(hover.candle.volume, 4)}`,
    `Mark: ${formatPreciseValue(hover.mark?.value, 8)}`,
    `Index: ${formatPreciseValue(hover.index?.value, 8)}`,
    `OI: ${formatPreciseValue(hover.openInterest?.value, 4)}`,
    `OI / Volume: ${formatPreciseValue(hover.oiVolumeRatio?.value, 4)}`,
    `Liq long: ${formatPreciseValue(hover.liquidation?.longValue, 4)}`,
    `Liq short: ${formatPreciseValue(hover.liquidation?.shortValue, 4)}`,
    `Funding: ${formatPercent(hover.funding?.value)}`
  ];

  return (
    <g pointerEvents="none">
      <line
        x1={hover.x}
        x2={hover.x}
        y1={panels.price.top}
        y2={plotBottom}
        stroke="rgba(16,20,16,0.62)"
        strokeDasharray="4 5"
        strokeWidth="1.5"
      />
      <line
        x1={left}
        x2={left + plotWidth}
        y1={closeY}
        y2={closeY}
        stroke="rgba(16,20,16,0.28)"
        strokeDasharray="3 5"
      />
      <circle cx={hover.x} cy={closeY} r="4.5" fill="#101410" stroke="#fff5df" strokeWidth="2" />
      {oiY !== null ? <circle cx={hover.x} cy={oiY} r="4" fill="#243126" stroke="#fff5df" strokeWidth="2" /> : null}
      {oiVolumeY !== null ? <circle cx={hover.x} cy={oiVolumeY} r="4" fill="#c68e3f" stroke="#fff5df" strokeWidth="2" /> : null}
      {liquidationY !== null ? <circle cx={hover.x} cy={liquidationY} r="4" fill="#d9573f" stroke="#fff5df" strokeWidth="2" /> : null}
      {fundingY !== null ? <circle cx={hover.x} cy={fundingY} r="4" fill="#c68e3f" stroke="#fff5df" strokeWidth="2" /> : null}
      <g>
        <rect
          x={tooltipX}
          y={tooltipY}
          width={tooltipWidth}
          height={tooltipHeight}
          rx="16"
          fill="rgba(16,20,16,0.92)"
          stroke="rgba(255,245,223,0.38)"
        />
        <text x={tooltipX + 16} y={tooltipY + 25} className="fill-cream text-[15px] font-black tracking-[0.14em]">
          POINT VALUES
        </text>
        {lines.map((line, index) => (
          <text
            key={`${index}-${line}`}
            x={tooltipX + 16}
            y={tooltipY + 52 + index * 18}
            className={index === 0 ? "fill-brass text-[13px] font-bold" : "fill-cream/90 text-[13px]"}
          >
            {line}
          </text>
        ))}
        {exchangeLines.length ? (
          <text x={tooltipX + 16} y={tooltipY + 262} className="fill-brass text-[12px] font-black tracking-[0.14em]">
            EXCHANGE BREAKDOWN
          </text>
        ) : null}
        {exchangeLines.map((line, index) => (
          <text
            key={`exchange-breakdown-${index}-${line}`}
            x={tooltipX + 16}
            y={tooltipY + 286 + index * 18}
            className="fill-cream/90 text-[12px]"
          >
            {line}
          </text>
        ))}
      </g>
    </g>
  );
}
