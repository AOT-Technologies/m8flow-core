#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#

from __future__ import annotations

from pathlib import Path

from m8flow_core.db import build_engine
from m8flow_core.models import Base
from m8flow_core.settings import Settings


def test_settings_default_to_sqlite() -> None:
    settings = Settings()

    assert settings.database_url.startswith("sqlite+pysqlite:///")
    assert settings.database_echo is False


def test_build_engine_accepts_in_memory_sqlite() -> None:
    engine = build_engine("sqlite+pysqlite:///:memory:")

    assert engine.url.drivername == "sqlite+pysqlite"
    engine.dispose()


def test_build_engine_enforces_foreign_keys_on_each_sqlite_connection(
    tmp_path: Path,
) -> None:
    engine = build_engine(f"sqlite+pysqlite:///{tmp_path / 'engine.sqlite'}")
    try:
        with engine.connect() as first, engine.connect() as second:
            assert first.connection.driver_connection is not (
                second.connection.driver_connection
            )
            assert first.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 1
            assert second.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 1
    finally:
        engine.dispose()


def test_base_uses_naming_convention() -> None:
    assert Base.metadata.naming_convention["pk"] == "pk_%(table_name)s"
