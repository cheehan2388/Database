import path from "node:path";
import dotenv from "dotenv";

let loaded = false;

export function loadServerEnv() {
  if (loaded) {
    return;
  }

  dotenv.config({ path: path.resolve(process.cwd(), "..", ".env") });
  dotenv.config({ path: path.resolve(process.cwd(), ".env.local"), override: false });
  loaded = true;
}
