"""Reconcile persisted subscriptions with waiting workflow tasks."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from typing import Any

from SpiffWorkflow.bpmn.workflow import BpmnWorkflow
from SpiffWorkflow.util.task import TaskState
from sqlalchemy import delete
from sqlalchemy.orm import Session

from m8flow_core.models.messages import MessageSubscriptionModel
from m8flow_core.models.process_instance import ProcessInstanceModel
from m8flow_core.services.message_runtime import RuntimeMessageDefinition
from m8flow_core.services.workflow_engine import _restore_workflow


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _key_digest(data: Mapping[str, Any], paths: dict[str, str]) -> str | None:
    values = {}
    for prop, path in paths.items():
        value: Any = data
        for part in path.split("/"):
            value = value.get(part) if isinstance(value, Mapping) else None
        if type(value) not in {str, bool, int, float}:
            return None
        if isinstance(value, float) and not math.isfinite(value):
            return None
        values[prop] = value
    return _digest(values)


def has_managed_messages(workflow: BpmnWorkflow) -> bool:
    return any(
        isinstance(
            getattr(task_spec, "event_definition", None), RuntimeMessageDefinition
        )
        for spec in [workflow.spec, *workflow.subprocess_specs.values()]
        for task_spec in spec.task_specs.values()
    )


def retire_instance_messages(session: Session, instance: ProcessInstanceModel) -> None:
    session.execute(
        delete(MessageSubscriptionModel).where(
            MessageSubscriptionModel.m8f_tenant_id == instance.m8f_tenant_id,
            MessageSubscriptionModel.process_instance_id == instance.id,
        )
    )


def sync_instance_messages(
    session: Session,
    instance: ProcessInstanceModel,
    workflow: BpmnWorkflow,
) -> None:
    if (
        not has_managed_messages(workflow)
        or instance.bpmn_process_definition_id is None
    ):
        return
    retire_instance_messages(session, instance)
    if (
        instance.has_terminal_status()
        or instance.status == "suspended"
        or workflow.completed
    ):
        return
    for task in workflow.event_manager.tasks.values():
        event = getattr(task.task_spec, "event_definition", None)
        if (
            not isinstance(event, RuntimeMessageDefinition)
            or task.state != TaskState.WAITING
        ):
            continue
        digest = _key_digest(task.data, event.binding_paths)
        if digest is not None:
            session.add(
                MessageSubscriptionModel(
                    m8f_tenant_id=instance.m8f_tenant_id,
                    process_instance_id=instance.id,
                    process_definition_id=instance.bpmn_process_definition_id,
                    task_guid=str(task.id),
                    message_name=event.name,
                    property_paths=_canonical_json(event.property_paths),
                    correlation_digest=digest,
                )
            )
    session.flush()


def refresh_instance_messages(session: Session, instance: ProcessInstanceModel) -> None:
    state = instance.workflow_state_json
    if state and "RuntimeMessageDefinition" in state:
        sync_instance_messages(session, instance, _restore_workflow(state))
