from __future__ import annotations

import os

DATABASE_URL_ENV_VAR = "GRID_PLATFORM_DATABASE_URL"
DEFAULT_DATABASE_URL = "postgresql+psycopg://grid:grid@127.0.0.1:5432/grid_platform"


def resolve_database_url() -> str:
    return str(os.environ.get(DATABASE_URL_ENV_VAR, DEFAULT_DATABASE_URL)).strip() or DEFAULT_DATABASE_URL
