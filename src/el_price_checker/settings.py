from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from platformdirs import user_data_dir


@dataclass(frozen=True)
class Settings:
    app_name: str = "el-price-checker"

    def default_db_path(self) -> Path:
        base = Path(user_data_dir(self.app_name))
        base.mkdir(parents=True, exist_ok=True)
        return base / "prices.sqlite3"
