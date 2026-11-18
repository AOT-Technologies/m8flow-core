"""Reindex already-waiting message catches when upgrading from older snapshots."""

from __future__ import annotations

import argparse

from SpiffWorkflow.spiff.specs.event_definitions import MessageEventDefinition
from sqlalchemy import select
from sqlalchemy.orm import Session

from m8flow_core.db import build_engine
from m8flow_core.errors import ValidationError
from m8flow_core.models.bpmn_process_definition import (
    SOURCE_BPMN_XML_PROPERTY_KEY,
    BpmnProcessDefinitionModel,
)
from m8flow_core.models.process_instance import ProcessInstanceModel
from m8flow_core.services.message_model import read_message_catches
from m8flow_core.services.message_registry import sync_instance_messages
from m8flow_core.services.message_runtime import RuntimeMessageDefinition
from m8flow_core.services.signal_registry import lock_event_instance
from m8flow_core.services.workflow_engine import _restore_workflow
from m8flow_core.services.workflow_state import _persist_workflow_recovery_state


def reindex_message_subscriptions(session: Session, tenant_id: str) -> int:
    """Update compatible stored waits in a maintenance transaction."""
    instance_model = ProcessInstanceModel
    definition_model = BpmnProcessDefinitionModel
    instances = session.scalars(
        select(instance_model)
        .join(
            definition_model,
            (definition_model.id == instance_model.bpmn_process_definition_id)
            & (definition_model.m8f_tenant_id == instance_model.m8f_tenant_id),
        )
        .where(
            instance_model.m8f_tenant_id == tenant_id,
            instance_model.status.in_(
                ("waiting", "user_input_required", "running", "suspended")
            ),
            definition_model.properties_json[SOURCE_BPMN_XML_PROPERTY_KEY]
            .as_string()
            .contains("messageEventDefinition"),
        )
        .order_by(instance_model.id)
    ).all()
    count = 0
    for instance in instances:
        definition = instance.bpmn_process_definition
        if definition is None or definition.collaboration_metadata is not None:
            continue
        lock_event_instance(session, instance)
        state = instance.workflow_state_json
        definition = instance.bpmn_process_definition
        source = definition.source_bpmn_xml if definition is not None else None
        if not state or not source or instance.has_terminal_status():
            continue
        try:
            catches = read_message_catches(source)
        except ValidationError:
            continue
        workflow = _restore_workflow(state)
        changed = False
        for spec in [workflow.spec, *workflow.subprocess_specs.values()]:
            for task_spec in spec.task_specs.values():
                catch = catches.get((workflow.spec.name, task_spec.name))
                event = getattr(task_spec, "event_definition", None)
                if catch is not None and type(event) is MessageEventDefinition:
                    task_spec.event_definition = RuntimeMessageDefinition.from_catch(
                        catch
                    )
                    changed = True
        if changed:
            _persist_workflow_recovery_state(session, instance, workflow)
            count += 1
        sync_instance_messages(session, instance, workflow)
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tenant_id", help="Stored tenant ID to reindex")
    args = parser.parse_args()
    engine = build_engine()
    try:
        with Session(engine) as session, session.begin():
            count = reindex_message_subscriptions(session, args.tenant_id)
    finally:
        engine.dispose()
    print(f"Upgraded message waits in {count} stored workflow snapshots")


if __name__ == "__main__":
    main()
