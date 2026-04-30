import { NextRequest, NextResponse } from "next/server";
import { getChart, normalizeSelection } from "@/lib/market-data";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  try {
    const selection = normalizeSelection(Object.fromEntries(request.nextUrl.searchParams.entries()));
    return NextResponse.json(await getChart(selection));
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Unknown chart error." },
      { status: 400 }
    );
  }
}
