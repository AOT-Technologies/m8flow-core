from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from sqlite3 import Connection as SqliteConnection

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import ConnectionPoolEntry

from m8flow_core.models import Base
from m8flow_core.settings import get_settings


def build_engine(database_url: str | None = None, echo: bool | None = None) -> Engine:
    if not database_url or echo is None:
        settings = get_settings()
        database_url = database_url or settings.database_url
        echo = settings.database_echo if echo is None else echo
    engine = create_engine(database_url, echo=echo)
    if engine.dialect.name == "sqlite":
        event.listen(engine, "connect", _enable_sqlite_foreign_keys)
    return engine


def _enable_sqlite_foreign_keys(
    connection: SqliteConnection, _record: ConnectionPoolEntry
) -> None:
    # SQLite only changes this setting outside a transaction.
    autocommit = connection.autocommit
    connection.autocommit = True
    try:
        connection.execute("PRAGMA foreign_keys=ON").close()
    finally:
        connection.autocommit = autocommit


def build_session_factory(engine: Engine | None = None) -> sessionmaker[Session]:
    engine = engine or build_engine()
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def create_schema(engine: Engine | None = None) -> None:
    engine = engine or build_engine()
    Base.metadata.create_all(bind=engine)


@contextmanager
def session_scope(engine: Engine | None = None) -> Iterator[Session]:
    session_factory = build_session_factory(engine)
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
