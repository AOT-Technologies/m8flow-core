# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
"""Existing identifiers remain data, including inside serialized workflow state."""

from pathlib import Path

from sqlalchemy import text
from sqlalchemy.orm import Session

from m8flow_core import api
from m8flow_core.models import (
    BpmnProcessDefinitionModel,
    M8flowTenantModel,
    ProcessInstanceModel,
)
from m8flow_core.models.user import UserModel
from m8flow_core.services.authorization import ROLE_ADMIN, ensure_v1_role


def test_existing_identifiers_survive_session_reload_and_task_completion(
    session, engine
):
    legacy = "m8f640"
    tenant_id = f"tenant-{legacy}"
    tenant = M8flowTenantModel(id=tenant_id, name=legacy, slug=tenant_id)
    user = UserModel(
        username=legacy,
        service=f"http://localhost/realms/{tenant_id}",
        service_id=legacy,
    )
    session.add_all([tenant, user])
    session.flush()
    user_id = user.id
    ensure_v1_role(
        session, tenant_id=tenant_id, role_name=ROLE_ADMIN, user_ids=[user_id]
    )
    xml = (
        Path(__file__).parent / "fixtures" / "legacy_tracking_identifiers.bpmn"
    ).read_text(encoding="utf-8")
    definition = api.execute_command(
        session,
        api.ImportBpmnProcessDefinitionCommand(
            tenant_id=tenant_id,
            user_id=user_id,
            bpmn_identifier=legacy,
            source_bpmn_xml=xml,
        ),
    )
    instance = api.execute_command(
        session,
        api.InitializeProcessInstanceFromDefinitionCommand(
            tenant_id=tenant_id,
            bpmn_process_definition_id=definition.id,
            process_initiator_id=user_id,
        ),
    )
    definition_id, instance_id = definition.id, instance.id
    assert instance.status == api.ProcessInstanceStatus.user_input_required
    session.commit()
    session.expunge_all()

    with Session(engine) as restored:
        definition = restored.get(BpmnProcessDefinitionModel, definition_id)
        instance = api.execute_query(
            restored,
            api.GetProcessInstanceQuery(
                tenant_id=tenant_id,
                process_instance_id=instance_id,
            ),
        )
        assert definition.bpmn_identifier == legacy
        assert definition.source_bpmn_xml == xml
        assert instance.bpmn_process_definition_id == definition_id
        assert instance.m8f_tenant_id == definition.m8f_tenant_id == tenant_id
        assert legacy in instance.workflow_state_json
        (task,) = api.execute_query(
            restored, api.GetPendingTasksQuery(tenant_id=tenant_id, user_id=user_id)
        )
        assert task.process_instance_id == instance_id
        ids = {"tenant_id": tenant_id, "user_id": user_id, "work_item_id": task.id}
        api.execute_command(restored, api.ClaimTaskCommand(**ids))
        api.execute_command(
            restored, api.CompleteTaskCommand(**ids, task_payload={legacy: "kept"})
        )
        restored.commit()
        restored.expire_all()
        assert (
            restored.get(ProcessInstanceModel, instance_id).status
            == api.ProcessInstanceStatus.complete
        )
        assert (
            restored.get(BpmnProcessDefinitionModel, definition_id).bpmn_identifier
            == legacy
        )
        metadata = api.execute_query(
            restored,
            api.GetProcessInstanceMetadataQuery(
                tenant_id=tenant_id,
                process_instance_id=instance_id,
            ),
        )
        assert {row.key: row.value for row in metadata}[legacy] == "kept"
        if engine.dialect.name == "sqlite":
            assert restored.execute(text("PRAGMA foreign_key_check")).all() == []
