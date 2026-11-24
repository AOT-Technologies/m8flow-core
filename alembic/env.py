#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#

from __future__ import annotations

import re
from typing import Any

import sqlalchemy as sa
from alembic import context
from sqlalchemy import Connection, UniqueConstraint, create_engine, pool

from m8flow_core.models import Base
from m8flow_core.settings import get_settings

target_metadata = Base.metadata


def include_object(
    obj: Any, name: str | None, type_: str, reflected: bool, compare_to: Any
) -> bool:
    """Skip unique constraints that PostgreSQL folds into the primary key.

    PostgreSQL doesn't create a UNIQUE constraint whose columns are exactly the
    primary key's, so comparing against a live database would always report it
    as missing.
    """
    if (
        type_ == "unique_constraint"
        and not reflected
        and isinstance(obj, UniqueConstraint)
        and context.get_context().dialect.name == "postgresql"
    ):
        primary_key = set(obj.table.primary_key.columns.keys())
        return set(obj.columns.keys()) != primary_key
    return True


def run_migrations_offline() -> None:
    settings = get_settings()
    context.configure(
        url=settings.database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
        include_object=include_object,
        render_as_batch=settings.database_url.startswith("sqlite"),
    )

    with context.begin_transaction():
        context.run_migrations()


def _require_legacy_schema(connection: Connection, purpose: str) -> None:
    names = (
        ("message_subscription", "message_delivery_receipt")
        if purpose == "message_correlation"
        else ("compensation_scope", "compensation_run", "compensation_record")
    )
    inspector = sa.inspect(connection)
    for name in names:
        expected = Base.metadata.tables[name]
        if not inspector.has_table(name):
            raise RuntimeError(f"Incompatible historical schema: missing table {name}")
        columns = {column["name"] for column in inspector.get_columns(name)}
        primary_key = set(inspector.get_pk_constraint(name)["constrained_columns"])
        keys = {
            (
                tuple(key["constrained_columns"]),
                key["referred_table"],
                tuple(key["referred_columns"]),
            )
            for key in inspector.get_foreign_keys(name)
            if key.get("referred_schema") in (None, inspector.default_schema_name)
        }
        expected_keys = {
            (
                tuple(key.column_keys),
                key.referred_table.name,
                tuple(element.column.name for element in key.elements),
            )
            for key in expected.foreign_key_constraints
        }
        if not (
            set(expected.columns.keys()) <= columns
            and primary_key == set(expected.primary_key.columns.keys())
            and expected_keys <= keys
        ):
            raise RuntimeError(f"Incompatible historical schema: {name}")


def _reconcile_legacy_revisions(connection: Connection) -> None:
    """Recognize supported historical purposes, without a ticket-key registry."""
    purposes = {
        "message_correlation": "message_correlation",
        "compensation": "compensation_journal",
    }
    if not sa.inspect(connection).has_table("alembic_version"):
        return
    version = sa.table("alembic_version", sa.column("version_num"))
    recorded = set(connection.scalars(sa.select(version.c.version_num)))
    aliases = {}
    for value in recorded:
        match = re.fullmatch(r"m8f_[0-9]+_([a-z_]+)", value)
        if match and match[1] in purposes:
            aliases[value] = purposes[match[1]]
    normalized = {aliases.get(value, value) for value in recorded}
    if len(normalized) != len(recorded):
        raise RuntimeError(
            "Both legacy and current core revision markers exist; "
            "reconcile migration state before upgrading."
        )
    for value in aliases:
        purpose = (
            "message_correlation"
            if aliases[value] == "message_correlation"
            else "compensation"
        )
        _require_legacy_schema(connection, purpose)
    for value, current in aliases.items():
        connection.execute(
            version.update()
            .where(version.c.version_num == value)
            .values(version_num=current)
        )


def run_migrations_online() -> None:
    settings = get_settings()
    connectable = create_engine(settings.database_url, poolclass=pool.NullPool)

    try:
        # The outer transaction also commits alias-only upgrades at head.
        with connectable.begin() as connection:
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                compare_type=True,
                compare_server_default=True,
                include_object=include_object,
                render_as_batch=connection.dialect.name == "sqlite",
            )

            with context.begin_transaction():
                _reconcile_legacy_revisions(connection)
                context.run_migrations()
    finally:
        connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
