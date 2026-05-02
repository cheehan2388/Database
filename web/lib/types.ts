export type Interval = "1m" | "5m" | "1h";

export type Exchange = "all" | "binance" | "bybit" | "kucoinfutures" | "coinbase";

export type WatchlistAsset = {
  baseAsset: string;
  displayOrder: number;
  exchanges: Exchange[];
};

export type StatusRow = {
  exchange: string;
  symbol: string;
  marketType: string;
  barInterval: string;
  sourceDataset: string;
  firstTime: string | null;
  lastTime: string | null;
  rowCount: number;
  lastIngestedAt: string | null;
};

export type Candle = {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

export type LinePoint = {
  time: number;
  value: number;
};

export type ExchangeLinePoint = LinePoint & {
  exchange: string;
};

export type LiquidationPoint = {
  time: number;
  longValue: number;
  shortValue: number;
  totalValue: number;
  count: number;
};

export type ExchangeLiquidationPoint = LiquidationPoint & {
  exchange: string;
};

export type TakerBuySellPoint = {
  time: number;
  buyVolume: number;
  sellVolume: number;
  buySellRatio: number;
};

export type ExchangeTakerBuySellPoint = TakerBuySellPoint & {
  exchange: string;
};

export type ChartResponse = {
  exchange: string;
  symbol: string;
  marketType: string;
  interval: Interval;
  candles: Candle[];
  markPrice: LinePoint[];
  indexPrice: LinePoint[];
  volumeBreakdown?: ExchangeLinePoint[];
};

export type DerivativesResponse = {
  openInterest: LinePoint[];
  longShortRatio: LinePoint[];
  takerBuySell: TakerBuySellPoint[];
  fundingRate: LinePoint[];
  liquidations: LiquidationPoint[];
  openInterestBreakdown?: ExchangeLinePoint[];
  longShortRatioBreakdown?: ExchangeLinePoint[];
  takerBuySellBreakdown?: ExchangeTakerBuySellPoint[];
  fundingRateBreakdown?: ExchangeLinePoint[];
  liquidationBreakdown?: ExchangeLiquidationPoint[];
};
