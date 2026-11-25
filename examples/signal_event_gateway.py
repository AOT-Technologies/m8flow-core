#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#

"""Demonstrate tenant-scoped signal registry delivery to event-based gateways.

Run: uv run python examples/signal_event_gateway.py
Uses an in-memory SQLite database, with no external services.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from m8flow_core import api
from m8flow_core.db import build_engine, create_schema
from m8flow_core.models import (
    M8flowTenantModel,
    ProcessInstanceModel,
    SignalPublicationModel,
    UserModel,
)
from m8flow_core.services.authorization import ensure_v1_role

FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "tests"
    / "fixtures"
    / "signal_event_gateway.bpmn"
)


def add_tenant(session: Session, tenant_id: str, timestamp: datetime) -> int:
    user = UserModel(
        username=f"signal-admin-{tenant_id}",
        service=f"app/realms/{tenant_id}",
        service_id=f"signal-admin-{tenant_id}",
        created_at=timestamp,
        updated_at=timestamp,
    )
    session.add_all(
        [M8flowTenantModel(id=tenant_id, name=tenant_id, slug=tenant_id), user]
    )
    session.flush()
    ensure_v1_role(
        session,
        tenant_id=tenant_id,
        role_name="admin",
        user_ids=[user.id],
    )
    return user.id


def import_definition(session: Session, tenant_id: str, user_id: int):
    return api.execute_command(
        session,
        api.ImportBpmnProcessDefinitionCommand(
            tenant_id=tenant_id,
            bpmn_identifier="signal-event-gateway",
            user_id=user_id,
            source_bpmn_xml=FIXTURE.read_text(encoding="utf-8"),
        ),
    )


def start_instance(session: Session, tenant_id: str, user_id: int, definition_id: int):
    return api.execute_command(
        session,
        api.InitializeProcessInstanceFromDefinitionCommand(
            tenant_id=tenant_id,
            bpmn_process_definition_id=definition_id,
            process_initiator_id=user_id,
        ),
    )


def main() -> None:
    engine = build_engine("sqlite+pysqlite:///:memory:")
    create_schema(engine)
    timestamp = datetime.now(UTC)
    tenant_id = "signal-gateway-example"
    other_tenant_id = "signal-gateway-other"

    with Session(engine) as session:
        user_id = add_tenant(session, tenant_id, timestamp)
        other_user_id = add_tenant(session, other_tenant_id, timestamp)
        definition = import_definition(session, tenant_id, user_id)
        other_definition = import_definition(session, other_tenant_id, other_user_id)
        local = start_instance(session, tenant_id, user_id, definition.id)
        remote = start_instance(session, tenant_id, user_id, definition.id)
        foreign = start_instance(
            session, other_tenant_id, other_user_id, other_definition.id
        )
        local_id, remote_id, foreign_id = local.id, remote.id, foreign.id
        assert local.status == remote.status == foreign.status == "user_input_required"
        print(f"Local process instance: {local_id}")
        print(f"Same-tenant recipient: {remote_id}")
        print(f"Other-tenant listener: {foreign_id}")
        session.commit()

    # Completing Investigate publishes Notice. The local gateway catches it
    # immediately; the other instance receives a durable queued delivery.
    with Session(engine) as session:
        work_item = next(
            item
            for item in api.get_pending_tasks(
                session, tenant_id=tenant_id, user_id=user_id
            )
            if item.process_instance_id == local_id
        )
        api.execute_command(
            session, api.ClaimTaskCommand(tenant_id, work_item.id, user_id)
        )
        api.execute_command(
            session, api.CompleteTaskCommand(tenant_id, work_item.id, user_id)
        )
        assert session.get(ProcessInstanceModel, local_id).status == "complete"
        publication = session.scalars(
            select(SignalPublicationModel).where(
                SignalPublicationModel.m8f_tenant_id == tenant_id,
                SignalPublicationModel.source_process_instance_id == local_id,
            )
        ).one()
        receipts = api.list_signal_deliveries(
            session, tenant_id=tenant_id, occurrence_id=publication.occurrence_id
        )
        assert len(receipts) == 2
        assert sorted(receipt.status for receipt in receipts) == [
            "delivered",
            "pending",
        ]
        print("Notice was caught locally; one same-tenant delivery is pending.")
        print("No delivery was created for the other tenant.")
        session.commit()

    # Hosts run this public API periodically, once per tenant, and commit the
    # caller-owned transaction so receipt consumption and workflow changes persist.
    with Session(engine) as session:
        attempted = api.run_signal_deliveries(session, tenant_id=tenant_id)
        assert len(attempted) == 1 and attempted[0].status == "delivered"
        assert api.run_signal_deliveries(session, tenant_id=tenant_id) == []
        session.commit()
        print(
            f"Queued delivery advanced instance {remote_id}; a second poll did nothing."
        )

    with Session(engine) as session:
        assert api.run_signal_deliveries(session, tenant_id=other_tenant_id) == []
        assert (
            session.get(ProcessInstanceModel, foreign_id).status
            == "user_input_required"
        )
        print("The other tenant's gateway remains subscribed and untouched.")

    engine.dispose()


if __name__ == "__main__":
    main()
