from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SCHEMA_PATH = PROJECT_ROOT / "sql" / "postgresql_schema.sql"
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"


def _strip_wrapping_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


def load_dotenv(env_path: Path = DEFAULT_ENV_PATH) -> None:
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = _strip_wrapping_quotes(value.strip())
        if key and key not in os.environ:
            os.environ[key] = value


def _read_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None or raw_value == "":
        return default
    return int(raw_value)


@dataclass(frozen=True)
class Settings:
    database_url: str
    fetch_limit: int
    ccxt_timeout_ms: int
    late_data_intervals: int
    schema_path: Path


def load_settings() -> Settings:
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL is required. Set it in PowerShell with "
            "$env:DATABASE_URL='postgresql://postgres:postgres@localhost:5432/market_data' "
            f"or create {DEFAULT_ENV_PATH} based on .env.example."
        )

    return Settings(
        database_url=database_url,
        fetch_limit=_read_int("FETCH_LIMIT", 500),
        ccxt_timeout_ms=_read_int("CCXT_TIMEOUT_MS", 30000),
        late_data_intervals=_read_int("LATE_DATA_INTERVALS", 2),
        schema_path=Path(os.getenv("SCHEMA_PATH", DEFAULT_SCHEMA_PATH)),
    )
