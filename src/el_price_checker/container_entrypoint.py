from __future__ import annotations

import os
from pathlib import Path

import uvicorn

from .web import create_app


def main() -> None:
    host = os.getenv("ELPC_WEB_HOST", "0.0.0.0")
    port = int(os.getenv("ELPC_WEB_PORT", "8000"))
    db_path = Path(os.getenv("ELPC_DB", "/data/prices.sqlite3"))

    app = create_app(db_path)
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
