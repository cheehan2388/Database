import { NextRequest, NextResponse } from "next/server";
import { getDerivatives, normalizeSelection } from "@/lib/market-data";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  try {
    const selection = normalizeSelection(Object.fromEntries(request.nextUrl.searchParams.entries()));
    return NextResponse.json(await getDerivatives(selection));
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Unknown derivatives error." },
      { status: 400 }
    );
  }
}
