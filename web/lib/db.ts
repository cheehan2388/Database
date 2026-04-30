import { Pool, types } from "pg";
import { loadServerEnv } from "./env";

loadServerEnv();

const pgTimestampWithoutTimezoneOid = 1114;

types.setTypeParser(pgTimestampWithoutTimezoneOid, (value) => {
  // PostgreSQL stores project timestamps as UTC in timestamp-without-time-zone
  // columns. node-postgres otherwise interprets them as local time, which would
  // make 01:23 UTC display as 01:23 Taipei instead of 09:23 Taipei.
  return new Date(`${value.replace(" ", "T")}Z`);
});

const connectionString = process.env.DATABASE_URL?.trim();

if (!connectionString) {
  throw new Error("DATABASE_URL is required. Put it in ../.env or web/.env.local.");
}

const globalForPg = globalThis as unknown as {
  pgPool?: Pool;
};

export const pool =
  globalForPg.pgPool ??
  new Pool({
    connectionString,
    max: 8,
    idleTimeoutMillis: 30_000
  });

if (process.env.NODE_ENV !== "production") {
  globalForPg.pgPool = pool;
}
