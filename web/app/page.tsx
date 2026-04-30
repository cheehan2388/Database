import Dashboard from "@/components/Dashboard";
import { getChart, getDerivatives, getStatus, getWatchlist, normalizeSelection } from "@/lib/market-data";

export const dynamic = "force-dynamic";

type PageProps = {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
};

export default async function Home({ searchParams }: PageProps) {
  const selection = normalizeSelection(await searchParams);
  const [assets, status, chart, derivatives] = await Promise.all([
    getWatchlist(),
    getStatus(),
    getChart(selection),
    getDerivatives(selection)
  ]);

  return (
    <Dashboard
      initialAssets={assets}
      initialStatus={status}
      initialChart={chart}
      initialDerivatives={derivatives}
      initialSelection={selection}
    />
  );
}
