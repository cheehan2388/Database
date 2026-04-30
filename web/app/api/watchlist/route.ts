import { NextResponse } from "next/server";
import { getWatchlist } from "@/lib/market-data";

export const dynamic = "force-dynamic";

export async function GET() {
  return NextResponse.json({
    assets: await getWatchlist()
  });
}
