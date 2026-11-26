#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command, op
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from alembic.util import CommandError
from sqlalchemy.orm import Session

from m8flow_core.db import build_engine
from m8flow_core.models import (
    Base,
    BpmnProcessDefinitionModel,
    BpmnProcessModel,
    CompensationRecordModel,
    CompensationRunModel,
    CompensationScopeModel,
    JsonDataModel,
    M8flowTenantModel,
    ProcessInstanceModel,
    SignalPublicationModel,
)
from m8flow_core.models.process_instance import WORKFLOW_STATE_JSON_DATA_KEY
from m8flow_core.models.user import UserModel
from m8flow_core.settings import get_settings

BASELINE_REVISION = "k2l3m4n5o6p7"
COLLABORATION_REVISION = "m8f_collaboration_01"
SIGNAL_REVISION = "l3m4n5o6p7q8"
COMPENSATION_REVISION = "compensation_journal"
MERGE_REVISION = "m8f_merge_runtime"
DEFINITION_TENANT_REVISION = "m8f_instance_definition_tenant"
PROCESS_TENANT_REVISION = "m8f_instance_process_tenant"
HEAD_REVISION = "message_correlation"

POSTGRES_URL = os.getenv("M8FLOW_POSTGRES_UPGRADE_DATABASE_URL", "")

NAMED_CHECK_CONSTRAINTS = {
    "permission_target": {
        "m8f_permission_target_resource_type_check",
        "m8f_permission_target_resource_pair_check",
    },
    "principal": {"m8f_principal_exactly_one_subject"},
    "process_instance_event": {"m8f_process_instance_event_category_check"},
    "signal_subscription": {"m8f_signal_subscription_references"},
    "message_delivery_receipt": {"m8f_msg_receipt_outcome"},
}


def _alembic_config() -> Config:
    repository_root = Path(__file__).resolve().parents[1]
    config = Config(str(repository_root / "alembic.ini"))
    config.set_main_option("script_location", str(repository_root / "alembic"))
    return config


@pytest.fixture()
def migration_database(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[str]:
    database_url: str = request.param or (
        f"sqlite+pysqlite:///{tmp_path / 'baseline.db'}"
    )
    monkeypatch.setenv("M8FLOW_DATABASE_URL", database_url)
    get_settings.cache_clear()
    try:
        yield database_url
    finally:
        get_settings.cache_clear()


def _duplicates_primary_key(diff: object) -> bool:
    # PostgreSQL folds a UNIQUE constraint over exactly the primary-key
    # columns into the primary key, so it never appears on reflection.
    # ``Base.metadata.create_all`` produces the same result.
    if not (isinstance(diff, tuple) and diff[0] == "add_constraint"):
        return False
    constraint = diff[1]
    if not isinstance(constraint, sa.UniqueConstraint):
        return False
    table = Base.metadata.tables[constraint.table.name]
    return set(constraint.columns.keys()) == set(table.primary_key.columns.keys())


def _schema_diff(database_url: str) -> list[object]:
    engine = sa.create_engine(database_url)
    try:
        with engine.connect() as connection:
            context = MigrationContext.configure(
                connection,
                opts={"compare_type": True, "compare_server_default": True},
            )
            diffs = compare_metadata(context, Base.metadata)
            if connection.dialect.name == "postgresql":
                diffs = [diff for diff in diffs if not _duplicates_primary_key(diff)]
            return list(diffs)
    finally:
        engine.dispose()


def _table_names(database_url: str) -> set[str]:
    engine = sa.create_engine(database_url)
    try:
        with engine.connect() as connection:
            return set(sa.inspect(connection).get_table_names())
    finally:
        engine.dispose()


def test_runtime_migrations_have_one_head_after_the_immutable_baseline() -> None:
    script = ScriptDirectory.from_config(_alembic_config())

    revisions = list(script.walk_revisions())

    assert {revision.revision: revision.down_revision for revision in revisions} == {
        HEAD_REVISION: PROCESS_TENANT_REVISION,
        PROCESS_TENANT_REVISION: DEFINITION_TENANT_REVISION,
        DEFINITION_TENANT_REVISION: MERGE_REVISION,
        MERGE_REVISION: (COLLABORATION_REVISION, COMPENSATION_REVISION),
        COLLABORATION_REVISION: BASELINE_REVISION,
        COMPENSATION_REVISION: SIGNAL_REVISION,
        SIGNAL_REVISION: BASELINE_REVISION,
        BASELINE_REVISION: None,
    }
    assert script.get_heads() == [HEAD_REVISION]


_DATABASES = [
    pytest.param(None, id="sqlite"),
    pytest.param(
        POSTGRES_URL,
        id="postgresql",
        marks=pytest.mark.skipif(
            not POSTGRES_URL.startswith("postgresql"),
            reason=(
                "set M8FLOW_POSTGRES_UPGRADE_DATABASE_URL to run the "
                "PostgreSQL baseline migration test"
            ),
        ),
    ),
]


@pytest.mark.parametrize("migration_database", _DATABASES, indirect=True)
@pytest.mark.parametrize(
    "existing_revision",
    (
        BASELINE_REVISION,
        COLLABORATION_REVISION,
        COMPENSATION_REVISION,
        MERGE_REVISION,
        DEFINITION_TENANT_REVISION,
    ),
)
def test_upgrade_from_existing_revision_matches_models_and_downgrade_removes_tables(
    migration_database: str,
    existing_revision: str,
) -> None:
    config = _alembic_config()

    command.upgrade(config, existing_revision)
    command.upgrade(config, "head")

    engine = sa.create_engine(migration_database)
    try:
        with engine.connect() as connection:
            assert (
                connection.scalar(sa.text("SELECT version_num FROM alembic_version"))
                == HEAD_REVISION
            )
            inspector = sa.inspect(connection)
            for table_name, expected in NAMED_CHECK_CONSTRAINTS.items():
                names = {
                    constraint["name"]
                    for constraint in inspector.get_check_constraints(table_name)
                }
                assert expected <= names, table_name
    finally:
        engine.dispose()
    assert _schema_diff(migration_database) == []
    command.check(config)

    command.downgrade(config, "base")

    remaining = _table_names(migration_database)
    assert remaining.isdisjoint(Base.metadata.tables), remaining


@pytest.mark.parametrize("migration_database", _DATABASES, indirect=True)
def test_baseline_schema_round_trips_post_2038_utc_timestamps(
    migration_database: str,
) -> None:
    config = _alembic_config()
    command.upgrade(config, "head")
    engine = sa.create_engine(migration_database)
    try:
        timestamp = datetime(2040, 1, 1, 12, 30, tzinfo=UTC)
        with Session(engine) as session:
            user = UserModel(
                username="baseline-post-2038",
                service="test",
                service_id="baseline-post-2038",
                created_at=timestamp,
                updated_at=timestamp,
            )
            session.add(user)
            session.commit()
            session.refresh(user)
            assert user.created_at == timestamp
            assert user.updated_at == timestamp
    finally:
        engine.dispose()
        command.downgrade(config, "base")


@pytest.mark.parametrize("migration_database", _DATABASES, indirect=True)
def test_additive_upgrade_preserves_baseline_data(migration_database: str) -> None:
    config = _alembic_config()
    command.upgrade(config, BASELINE_REVISION)
    engine = sa.create_engine(migration_database)
    try:
        with engine.begin() as connection:
            connection.execute(
                sa.text(
                    'INSERT INTO "user" (username, service, service_id) '
                    "VALUES ('existing-user', 'migration-test', 'existing-user')"
                )
            )
        command.upgrade(config, "head")
        assert _schema_diff(migration_database) == []
        with engine.connect() as connection:
            assert (
                connection.scalar(
                    sa.text('SELECT username FROM "user" WHERE service_id = :key'),
                    {"key": "existing-user"},
                )
                == "existing-user"
            )
        command.downgrade(config, BASELINE_REVISION)
        assert "compensation_record" not in _table_names(migration_database)
        assert "process_instance" in _table_names(migration_database)
    finally:
        engine.dispose()
        command.downgrade(config, "base")


@pytest.mark.parametrize("migration_database", _DATABASES, indirect=True)
def test_upgrade_from_signal_preserves_data_and_enforces_journal_tenant(
    migration_database: str,
) -> None:
    config = _alembic_config()
    command.upgrade(config, SIGNAL_REVISION)
    engine = build_engine(migration_database)
    tenant = M8flowTenantModel.__table__
    instance = ProcessInstanceModel.__table__
    publication = SignalPublicationModel.__table__
    now = datetime.now(UTC)
    journal_rows = (
        (
            CompensationScopeModel.__table__,
            {"activity_id": "saga", "completed": False, "data_hash": "scope-data"},
        ),
        (
            CompensationRunModel.__table__,
            {
                "run_id": "run",
                "throw_task_guid": "throw",
                "wait_for_completion": True,
                "status": "running",
            },
        ),
        (
            CompensationRecordModel.__table__,
            {
                "source_task_guid": "source",
                "activity_id": "shipment",
                "completion_sequence": 1,
                "status": "available",
                "attempts": 0,
                "data_hash": "record-data",
            },
        ),
    )
    try:
        with engine.begin() as connection:
            connection.execute(
                tenant.insert(),
                [{"id": name, "name": name, "slug": name} for name in ("one", "two")],
            )
            user_id = connection.scalar(
                UserModel.__table__.insert()
                .values(username="operator", service="test", service_id="operator")
                .returning(UserModel.__table__.c.id)
            )
            instance_id = connection.scalar(
                instance.insert()
                .values(
                    m8f_tenant_id="one",
                    process_model_identifier="saga",
                    process_model_display_name="Saga",
                    process_initiator_id=user_id,
                )
                .returning(instance.c.id)
            )
            connection.execute(
                publication.insert().values(
                    m8f_tenant_id="one",
                    source_process_instance_id=instance_id,
                    occurrence_id="existing-signal",
                    signal_name="Notice",
                    payload_json={"preserved": True},
                    published_at=now,
                )
            )

        command.upgrade(config, "head")
        assert _schema_diff(migration_database) == []
        with engine.begin() as connection:
            for table, values in journal_rows:
                insert = table.insert().values(
                    process_instance_id=instance_id,
                    scope_id="scope",
                    created_at=now,
                    updated_at=now,
                    **values,
                )
                connection.execute(insert.values(m8f_tenant_id="one"))
                with (
                    pytest.raises(sa.exc.IntegrityError, match=r"(?i)foreign key"),
                    connection.begin_nested(),
                ):
                    connection.execute(insert.values(m8f_tenant_id="two"))
                count = connection.scalar(sa.select(sa.func.count()).select_from(table))
                assert count == 1

        command.downgrade(config, SIGNAL_REVISION)
        assert {table.name for table, _ in journal_rows}.isdisjoint(
            _table_names(migration_database)
        )
        with engine.connect() as connection:
            assert connection.scalar(sa.select(publication.c.payload_json)) == {
                "preserved": True
            }
            assert connection.scalar(sa.select(instance.c.id)) == instance_id
    finally:
        engine.dispose()
        command.downgrade(config, "base")


@pytest.mark.parametrize("migration_database", _DATABASES, indirect=True)
@pytest.mark.parametrize(
    ("model", "relationship", "previous_revision"),
    [
        (BpmnProcessDefinitionModel, "bpmn_process_definition", MERGE_REVISION),
        (BpmnProcessModel, "bpmn_process", DEFINITION_TENANT_REVISION),
    ],
)
def test_instance_constraints_reject_cross_tenant_references(
    migration_database: str,
    model: type[BpmnProcessDefinitionModel] | type[BpmnProcessModel],
    relationship: str,
    previous_revision: str,
) -> None:
    config = _alembic_config()
    command.upgrade(config, previous_revision)
    engine = build_engine(migration_database)

    tenant = M8flowTenantModel.__table__
    definition = BpmnProcessDefinitionModel.__table__
    process = BpmnProcessModel.__table__
    instance = ProcessInstanceModel.__table__
    reference = instance.c[f"{relationship}_id"]
    try:
        with engine.begin() as connection:
            connection.execute(
                tenant.insert(),
                [{"id": name, "name": name, "slug": name} for name in ("one", "two")],
            )
            user = UserModel.__table__
            user_id = connection.scalar(
                user.insert()
                .values(username="operator", service="test", service_id="operator")
                .returning(user.c.id)
            )
            definition_id = connection.scalar(
                definition.insert()
                .values(
                    m8f_tenant_id="one",
                    bpmn_identifier="order",
                    process_xml_digest="order-digest",
                    properties_json={},
                )
                .returning(definition.c.id)
            )
            if model is BpmnProcessModel:
                connection.execute(
                    JsonDataModel.__table__.insert().values(
                        m8f_tenant_id="one",
                        hash="order-state",
                        data={WORKFLOW_STATE_JSON_DATA_KEY: "order-state"},
                    )
                )
                target_id = connection.scalar(
                    process.insert()
                    .values(
                        m8f_tenant_id="one",
                        bpmn_process_definition_id=definition_id,
                        properties_json={},
                        json_data_hash="order-state",
                    )
                    .returning(process.c.id)
                )
            else:
                target_id = definition_id
            insert = instance.insert().values(
                process_model_identifier="order",
                process_model_display_name="Order",
                process_initiator_id=user_id,
            )
            valid_id = connection.scalar(
                insert.values(
                    {instance.c.m8f_tenant_id: "one", reference: target_id}
                ).returning(instance.c.id)
            )
            invalid_id = connection.scalar(
                insert.values(
                    {instance.c.m8f_tenant_id: "two", reference: target_id}
                ).returning(instance.c.id)
            )

        if model is BpmnProcessModel:
            with Session(engine) as session:
                valid = session.get_one(ProcessInstanceModel, valid_id)
                assert valid.workflow_state_json == "order-state"
                invalid = session.get_one(ProcessInstanceModel, invalid_id)
                assert invalid.bpmn_process is None
                assert invalid.workflow_state_json is None

        with pytest.raises(
            RuntimeError, match=rf"Process instance {invalid_id} .*tenant"
        ):
            command.upgrade(config, "head")

        with engine.begin() as connection:
            assert (
                connection.scalar(
                    sa.select(reference).where(instance.c.id == invalid_id)
                )
                == target_id
            )
            connection.execute(
                instance.update()
                .where(instance.c.id == invalid_id)
                .values({reference: None})
            )

        command.upgrade(config, "head")
        with engine.begin() as connection:
            assert dict(
                connection.execute(sa.select(instance.c.id, reference)).all()
            ) == {valid_id: target_id, invalid_id: None}
            connection.execute(
                insert.values({instance.c.m8f_tenant_id: "one", reference: target_id})
            )
            connection.execute(insert.values(m8f_tenant_id="two"))
            with (
                pytest.raises(sa.exc.IntegrityError, match=r"(?i)foreign key"),
                connection.begin_nested(),
            ):
                connection.execute(
                    insert.values(
                        {instance.c.m8f_tenant_id: "two", reference: target_id}
                    )
                )
            with (
                pytest.raises(sa.exc.IntegrityError, match=r"(?i)foreign key"),
                connection.begin_nested(),
            ):
                connection.execute(
                    instance.update()
                    .where(instance.c.id == invalid_id)
                    .values({reference: target_id})
                )

        with Session(engine) as session:
            unlinked = session.get_one(ProcessInstanceModel, invalid_id)
            setattr(unlinked, relationship, session.get_one(model, target_id))
            with pytest.raises(sa.exc.IntegrityError, match=r"(?i)foreign key"):
                session.flush()
            session.rollback()
            assert unlinked.m8f_tenant_id == "two"

        command.downgrade(config, previous_revision)
        with engine.begin() as connection:
            assert (
                connection.scalar(sa.select(reference).where(instance.c.id == valid_id))
                == target_id
            )
            assert (
                connection.scalar(sa.select(sa.func.count()).select_from(instance)) == 4
            )
            connection.execute(
                insert.values({instance.c.m8f_tenant_id: "two", reference: target_id})
            )
    finally:
        engine.dispose()
        command.downgrade(config, "base")


# Synthetic examples exercise the historical format without production ticket IDs.
_HISTORICAL_NAMESPACE = "m8f_1"
_LEGACY_COMPENSATION_REVISION = f"{_HISTORICAL_NAMESPACE}_compensation"
_LEGACY_MESSAGE_REVISION = f"{_HISTORICAL_NAMESPACE}_message_correlation"

_LEGACY_REVISIONS = [
    pytest.param(
        COMPENSATION_REVISION, _LEGACY_COMPENSATION_REVISION, False, id="compensation"
    ),
    pytest.param(
        COMPENSATION_REVISION, _LEGACY_COMPENSATION_REVISION, True, id="parallel-heads"
    ),
    pytest.param(HEAD_REVISION, _LEGACY_MESSAGE_REVISION, False, id="message-head"),
]


def _revision_stamps(database_url: str) -> set[str]:
    engine = sa.create_engine(database_url)
    try:
        with engine.connect() as connection:
            return set(
                connection.scalars(sa.text("SELECT version_num FROM alembic_version"))
            )
    finally:
        engine.dispose()


@pytest.mark.parametrize("migration_database", _DATABASES, indirect=True)
@pytest.mark.parametrize("revision,legacy,parallel", _LEGACY_REVISIONS)
def test_legacy_stamps_preserve_journals_and_messages(
    migration_database: str,
    revision: str,
    legacy: str,
    parallel: bool,
) -> None:
    config = _alembic_config()
    command.upgrade(config, revision)
    if parallel:
        command.upgrade(config, COLLABORATION_REVISION)
    engine = build_engine(migration_database)
    now = datetime.now(UTC)
    try:
        with engine.begin() as connection:
            connection.execute(
                M8flowTenantModel.__table__.insert().values(
                    id="legacy",
                    name="Legacy",
                    slug="legacy",
                )
            )
            user = UserModel.__table__
            user_id = connection.scalar(
                user.insert()
                .values(
                    username="legacy",
                    service="test",
                    service_id="legacy",
                )
                .returning(user.c.id)
            )
            definition = BpmnProcessDefinitionModel.__table__
            definition_id = connection.scalar(
                definition.insert()
                .values(
                    m8f_tenant_id="legacy",
                    bpmn_identifier="saga",
                    process_xml_digest="digest",
                    properties_json={},
                )
                .returning(definition.c.id)
            )
            instance = ProcessInstanceModel.__table__
            instance_id = connection.scalar(
                instance.insert()
                .values(
                    m8f_tenant_id="legacy",
                    process_model_identifier="saga",
                    process_model_display_name="Saga",
                    process_initiator_id=user_id,
                )
                .returning(instance.c.id)
            )
            common = dict(
                m8f_tenant_id="legacy",
                process_instance_id=instance_id,
                scope_id="scope",
                created_at=now,
                updated_at=now,
            )
            connection.execute(
                CompensationScopeModel.__table__.insert().values(
                    **common,
                    activity_id="activity",
                    completed=True,
                    data_hash="scope",
                )
            )
            connection.execute(
                CompensationRunModel.__table__.insert().values(
                    **common,
                    run_id="run",
                    throw_task_guid="throw",
                    wait_for_completion=True,
                    status="running",
                )
            )
            connection.execute(
                CompensationRecordModel.__table__.insert().values(
                    **common,
                    source_task_guid="source",
                    activity_id="activity",
                    completion_sequence=1,
                    status="available",
                    attempts=0,
                    data_hash="record",
                )
            )
            tables = ["compensation_scope", "compensation_run", "compensation_record"]
            if revision == HEAD_REVISION:
                connection.execute(
                    sa.text(
                        "INSERT INTO message_subscription "
                        "(m8f_tenant_id, process_instance_id, task_guid, "
                        "process_definition_id, "
                        "message_name, property_paths, correlation_digest) "
                        "VALUES ('legacy', :instance, 'waiting', :definition, "
                        "'Payment', '[]', :digest)"
                    ),
                    {
                        "instance": instance_id,
                        "definition": definition_id,
                        "digest": "a" * 64,
                    },
                )
                connection.execute(
                    sa.text(
                        "INSERT INTO message_delivery_receipt "
                        "(m8f_tenant_id, delivery_id, request_digest, delivered_at) "
                        "VALUES ('legacy', 'receipt', :digest, :now)"
                    ),
                    {"digest": "b" * 64, "now": now},
                )
                tables += ["message_subscription", "message_delivery_receipt"]
            before = {
                name: connection.execute(sa.text(f"SELECT * FROM {name}")).all()
                for name in tables
            }
            connection.execute(
                sa.text(
                    "UPDATE alembic_version SET version_num = :legacy "
                    "WHERE version_num = :revision"
                ),
                {"legacy": legacy, "revision": revision},
            )
        command.upgrade(config, "head")
        command.upgrade(config, "head")
        assert _revision_stamps(migration_database) == {HEAD_REVISION}
        assert _schema_diff(migration_database) == []
        with engine.connect() as connection:
            for name, rows in before.items():
                assert (
                    connection.execute(sa.text(f"SELECT * FROM {name}")).all() == rows
                )
        command.downgrade(config, COMPENSATION_REVISION)
        command.upgrade(config, "head")
        with engine.connect() as connection:
            for name in (
                "compensation_scope",
                "compensation_run",
                "compensation_record",
            ):
                assert (
                    connection.execute(sa.text(f"SELECT * FROM {name}")).all()
                    == before[name]
                )
    finally:
        engine.dispose()
        command.downgrade(config, "base")


@pytest.mark.parametrize("migration_database", _DATABASES, indirect=True)
@pytest.mark.parametrize("revision,legacy,parallel", _LEGACY_REVISIONS)
def test_legacy_stamp_normalization_rolls_back_on_failure(
    migration_database: str,
    revision: str,
    legacy: str,
    parallel: bool,
) -> None:
    config = _alembic_config()
    command.upgrade(config, revision)
    if parallel:
        command.upgrade(config, COLLABORATION_REVISION)
    engine = sa.create_engine(migration_database)
    try:
        with engine.begin() as connection:
            connection.execute(
                sa.text(
                    "UPDATE alembic_version SET version_num = :legacy "
                    "WHERE version_num = :revision"
                ),
                {"legacy": legacy, "revision": revision},
            )
        before = _revision_stamps(migration_database)
        with pytest.raises(CommandError, match="Can't locate revision"):
            command.upgrade(config, "nonexistent_revision")
        assert _revision_stamps(migration_database) == before
    finally:
        engine.dispose()
        command.downgrade(config, "base")


@pytest.mark.parametrize("migration_database", _DATABASES, indirect=True)
def test_legacy_stamp_normalization_rolls_back_with_failed_schema_upgrade(
    migration_database: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _alembic_config()
    command.upgrade(config, COMPENSATION_REVISION)
    engine = sa.create_engine(migration_database)
    with engine.begin() as connection:
        connection.execute(
            sa.text("UPDATE alembic_version SET version_num = :legacy"),
            {"legacy": _LEGACY_COMPENSATION_REVISION},
        )
    create_table = op.create_table

    def fail_message_table(name: str, *args: object, **kwargs: object) -> object:
        if name == "message_subscription":
            raise RuntimeError("injected schema failure")
        return create_table(name, *args, **kwargs)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(op, "create_table", fail_message_table)
            with pytest.raises(RuntimeError, match="injected schema failure"):
                command.upgrade(config, "head")
        assert _revision_stamps(migration_database) == {_LEGACY_COMPENSATION_REVISION}
        assert "message_subscription" not in _table_names(migration_database)
        assert "collaboration_correlation_key" not in {
            col["name"] for col in sa.inspect(engine).get_columns("process_instance")
        }
        command.upgrade(config, "head")
    finally:
        engine.dispose()
        command.downgrade(config, "base")


@pytest.mark.parametrize("migration_database", _DATABASES, indirect=True)
@pytest.mark.parametrize(
    "revision,legacy",
    [
        (COMPENSATION_REVISION, _LEGACY_COMPENSATION_REVISION),
        (HEAD_REVISION, _LEGACY_MESSAGE_REVISION),
    ],
)
def test_legacy_and_current_revision_collision_is_rejected(
    migration_database: str,
    revision: str,
    legacy: str,
) -> None:
    config = _alembic_config()
    command.upgrade(config, revision)
    engine = sa.create_engine(migration_database)
    try:
        with engine.begin() as connection:
            connection.execute(
                sa.text("INSERT INTO alembic_version VALUES (:legacy)"),
                {"legacy": legacy},
            )
        with pytest.raises(RuntimeError, match="Both legacy and current"):
            command.upgrade(config, "head")
        assert _revision_stamps(migration_database) == {revision, legacy}
    finally:
        with engine.begin() as connection:
            connection.execute(
                sa.text("DELETE FROM alembic_version WHERE version_num = :legacy"),
                {"legacy": legacy},
            )
        engine.dispose()
        command.downgrade(config, "base")


@pytest.mark.parametrize("migration_database", _DATABASES, indirect=True)
@pytest.mark.parametrize(
    "marker",
    [
        "m8f_invalid_message_correlation",
        "m8f_1_unknown_schema",
        "m8f_1_message_correlation_extra",
        "m8f_1_compensation_journal",
    ],
)
def test_unsupported_historical_names_do_not_change_stamps(
    migration_database: str,
    marker: str,
) -> None:
    config = _alembic_config()
    command.upgrade(config, "head")
    engine = sa.create_engine(migration_database)
    try:
        with engine.begin() as connection:
            connection.execute(
                sa.text("UPDATE alembic_version SET version_num = :marker"),
                {"marker": marker},
            )
        with pytest.raises(CommandError, match="Can't locate revision"):
            command.upgrade(config, "head")
        assert _revision_stamps(migration_database) == {marker}
    finally:
        with engine.begin() as connection:
            connection.execute(
                sa.text("UPDATE alembic_version SET version_num = :head"),
                {"head": HEAD_REVISION},
            )
        engine.dispose()
        command.downgrade(config, "base")


@pytest.mark.parametrize("migration_database", _DATABASES, indirect=True)
@pytest.mark.parametrize(
    "revision,legacy",
    [
        (COMPENSATION_REVISION, _LEGACY_COMPENSATION_REVISION),
        (HEAD_REVISION, _LEGACY_MESSAGE_REVISION),
    ],
)
@pytest.mark.parametrize(
    "damage",
    [
        "table",
        "column",
        "foreign-key",
        "type",
        "length",
        "nullability",
        "missing-check",
        "altered-check",
        "unique",
        "cascade",
    ],
)
def test_historical_schema_is_validated_before_translation(
    migration_database: str,
    revision: str,
    legacy: str,
    damage: str,
) -> None:
    if (
        damage in {"missing-check", "altered-check", "cascade"}
        and revision != HEAD_REVISION
    ):
        pytest.skip("Message-specific constraint")
    if damage == "unique" and revision != COMPENSATION_REVISION:
        pytest.skip("Compensation-specific unique constraint")
    config = _alembic_config()
    command.upgrade(config, revision)
    table = (
        "compensation_scope"
        if revision == COMPENSATION_REVISION
        else "message_subscription"
    )
    if damage in {"missing-check", "altered-check"}:
        table = "message_delivery_receipt"
    engine = sa.create_engine(migration_database)
    try:
        with engine.begin() as connection:
            connection.execute(
                M8flowTenantModel.__table__.insert().values(
                    id="retained",
                    name="Retained",
                    slug="retained",
                )
            )
            connection.execute(
                sa.text("UPDATE alembic_version SET version_num = :legacy"),
                {"legacy": legacy},
            )
            if damage == "table":
                connection.execute(sa.DDL(f"DROP TABLE {table}"))
            elif damage == "column":
                column = (
                    "activity_id"
                    if revision == COMPENSATION_REVISION
                    else "property_paths"
                )
                connection.execute(sa.DDL(f"ALTER TABLE {table} DROP COLUMN {column}"))
            elif damage in {
                "type",
                "length",
                "nullability",
                "missing-check",
                "altered-check",
                "unique",
            }:
                with Operations.context(
                    MigrationContext.configure(connection)
                ) as operations:
                    with operations.batch_alter_table(table) as batch:
                        if damage in {"type", "length"}:
                            column = (
                                "activity_id"
                                if revision == COMPENSATION_REVISION
                                else "message_name"
                            )
                            batch.alter_column(
                                column,
                                type_=sa.Integer()
                                if damage == "type"
                                else sa.String(17),
                                postgresql_using=(
                                    f"{column}::integer"
                                    if damage == "type"
                                    else f"{column}::varchar(17)"
                                ),
                            )
                        elif damage == "nullability":
                            column = (
                                "activity_id"
                                if revision == COMPENSATION_REVISION
                                else "message_name"
                            )
                            batch.alter_column(column, nullable=True)
                        elif damage == "unique":
                            batch.drop_constraint(
                                "m8f_compensation_scope_occurrence", type_="unique"
                            )
                        else:
                            batch.drop_constraint(
                                "m8f_msg_receipt_outcome", type_="check"
                            )
                            if damage == "altered-check":
                                batch.create_check_constraint(
                                    "m8f_msg_receipt_outcome",
                                    "outcome IS NULL OR outcome IS NOT NULL",
                                )
            else:
                naming = {
                    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s"
                }
                key = next(
                    key
                    for key in sa.inspect(connection).get_foreign_keys(table)
                    if key["referred_table"]
                    == ("process_instance" if damage == "cascade" else "m8flow_tenant")
                )
                key_name = key["name"] or f"fk_{table}_m8f_tenant_id_m8flow_tenant"
                with Operations.context(
                    MigrationContext.configure(connection)
                ) as operations:
                    with operations.batch_alter_table(
                        table, naming_convention=naming
                    ) as batch:
                        batch.drop_constraint(
                            key_name,
                            type_="foreignkey",
                        )
                        if damage == "cascade":
                            batch.create_foreign_key(
                                key_name,
                                "process_instance",
                                key["constrained_columns"],
                                key["referred_columns"],
                            )
        with pytest.raises(RuntimeError, match="Incompatible historical schema"):
            command.upgrade(config, "head")
        assert _revision_stamps(migration_database) == {legacy}
        with engine.connect() as connection:
            assert (
                connection.scalar(
                    sa.select(M8flowTenantModel.__table__.c.slug).where(
                        M8flowTenantModel.__table__.c.id == "retained",
                    )
                )
                == "retained"
            )
            assert not any(
                name.startswith("legacy_schema_probe_")
                for name in sa.inspect(connection).get_table_names()
            )
    finally:
        # Deliberately damaged fixtures cannot use the normal downgrade chain.
        with engine.begin() as connection:
            metadata = sa.MetaData()
            metadata.reflect(connection)
            metadata.drop_all(connection)
        engine.dispose()


_LEGACY_LOOKUP_INDEXES = [
    (
        HEAD_REVISION,
        _LEGACY_MESSAGE_REVISION,
        "message_subscription",
        "m8f_msg_sub_match",
        ("m8f_tenant_id", "message_name", "correlation_digest"),
    ),
    (
        HEAD_REVISION,
        _LEGACY_MESSAGE_REVISION,
        "message_subscription",
        "m8f_msg_sub_definition",
        ("m8f_tenant_id", "message_name", "process_definition_id"),
    ),
    *[
        (
            COMPENSATION_REVISION,
            _LEGACY_COMPENSATION_REVISION,
            table,
            f"ix_{table}_{column}",
            (column,),
        )
        for table in ("compensation_scope", "compensation_run", "compensation_record")
        for column in ("m8f_tenant_id", "process_instance_id")
    ],
]


@pytest.mark.parametrize("migration_database", _DATABASES, indirect=True)
@pytest.mark.parametrize(
    "revision,legacy,table,index_name,columns,change",
    [(*index, "missing") for index in _LEGACY_LOOKUP_INDEXES]
    + [
        (*index, change)
        for index in _LEGACY_LOOKUP_INDEXES[:2]
        for change in ("columns", "order", "partial", "unique", "renamed")
    ],
)
def test_historical_lookup_indexes_are_validated_before_translation(
    migration_database: str,
    revision: str,
    legacy: str,
    table: str,
    index_name: str,
    columns: tuple[str, ...],
    change: str,
) -> None:
    config = _alembic_config()
    command.upgrade(config, revision)
    engine = sa.create_engine(migration_database)
    try:
        with engine.begin() as connection:
            connection.execute(
                M8flowTenantModel.__table__.insert().values(
                    id="retained", name="Retained", slug="retained"
                )
            )
            connection.execute(
                sa.text("UPDATE alembic_version SET version_num = :legacy"),
                {"legacy": legacy},
            )
            with Operations.context(
                MigrationContext.configure(connection)
            ) as operations:
                operations.drop_index(index_name, table_name=table)
                if change != "missing":
                    operations.create_index(
                        f"{index_name}_renamed" if change == "renamed" else index_name,
                        table,
                        ("task_guid",)
                        if change == "columns"
                        else (
                            tuple(reversed(columns)) if change == "order" else columns
                        ),
                        unique=change == "unique",
                        **(
                            {
                                "sqlite_where": sa.text("message_name = 'restricted'"),
                                "postgresql_where": sa.text(
                                    "message_name = 'restricted'"
                                ),
                            }
                            if change == "partial"
                            else {}
                        ),
                    )
        if change != "renamed":
            with pytest.raises(
                RuntimeError,
                match=f"Incompatible historical schema: {table}: .*index {index_name}",
            ):
                command.upgrade(config, "head")
            assert _revision_stamps(migration_database) == {legacy}
            with engine.begin() as connection:
                assert (
                    connection.scalar(
                        sa.select(M8flowTenantModel.__table__.c.slug).where(
                            M8flowTenantModel.__table__.c.id == "retained"
                        )
                    )
                    == "retained"
                )
                with Operations.context(
                    MigrationContext.configure(connection)
                ) as operations:
                    if change != "missing":
                        operations.drop_index(index_name, table_name=table)
                    operations.create_index(index_name, table, columns)
        command.upgrade(config, "head")
        command.upgrade(config, "head")
        assert _revision_stamps(migration_database) == {HEAD_REVISION}
        with engine.connect() as connection:
            assert (
                connection.scalar(
                    sa.select(M8flowTenantModel.__table__.c.slug).where(
                        M8flowTenantModel.__table__.c.id == "retained"
                    )
                )
                == "retained"
            )
    finally:
        with engine.begin() as connection:
            metadata = sa.MetaData()
            metadata.reflect(connection)
            metadata.drop_all(connection)
        engine.dispose()


@pytest.mark.parametrize("migration_database", _DATABASES, indirect=True)
def test_distinct_historical_names_cannot_map_to_the_same_revision(
    migration_database: str,
) -> None:
    config = _alembic_config()
    command.upgrade(config, COMPENSATION_REVISION)
    engine = sa.create_engine(migration_database)
    other = "m8f_2_compensation"
    try:
        with engine.begin() as connection:
            connection.execute(
                sa.text("UPDATE alembic_version SET version_num = :legacy"),
                {"legacy": _LEGACY_COMPENSATION_REVISION},
            )
            connection.execute(
                sa.text("INSERT INTO alembic_version VALUES (:other)"), {"other": other}
            )
        with pytest.raises(RuntimeError, match="Both legacy and current"):
            command.upgrade(config, "head")
        assert _revision_stamps(migration_database) == {
            _LEGACY_COMPENSATION_REVISION,
            other,
        }
    finally:
        with engine.begin() as connection:
            connection.execute(
                sa.text("DELETE FROM alembic_version WHERE version_num = :other"),
                {"other": other},
            )
        engine.dispose()
        command.downgrade(config, "base")
