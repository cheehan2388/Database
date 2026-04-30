import { NextResponse } from "next/server";
import { pool } from "@/lib/db";

export const dynamic = "force-dynamic";

export async function GET() {
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

  return NextResponse.json({
    status: rows.map((row) => ({
      exchange: row.exchange,
      symbol: row.symbol,
      marketType: row.market_type,
      barInterval: row.bar_interval,
      sourceDataset: row.source_dataset,
      firstTime: row.first_time?.toISOString?.() ?? row.first_time,
      lastTime: row.last_time?.toISOString?.() ?? row.last_time,
      rowCount: Number(row.row_count ?? 0),
      lastIngestedAt: row.last_ingested_at?.toISOString?.() ?? row.last_ingested_at
    }))
  });
}
