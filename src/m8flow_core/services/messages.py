"""Correlate a message with exactly one persisted, eligible runtime task."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import UUID

from SpiffWorkflow.bpmn.exceptions import WorkflowTaskException
from SpiffWorkflow.bpmn.util.event import BpmnEvent
from SpiffWorkflow.util.task import TaskState
from sqlalchemy import and_, or_, select, update
from sqlalchemy.orm import Session

from m8flow_core.application.commands import CorrelateMessageCommand
from m8flow_core.errors import (
    AmbiguousMessageCorrelationError,
    CompensationExecutionError,
    MessageDeliveryConflictError,
    NotFoundError,
    ServiceTaskExecutionError,
    ValidationError,
)
from m8flow_core.models.messages import (
    MessageDeliveryReceiptModel,
    MessageSubscriptionModel,
)
from m8flow_core.models.process_instance import ProcessInstanceModel
from m8flow_core.services import workflow_runtime as runtime
from m8flow_core.services.authorization import (
    MESSAGE_CORRELATE_COMMAND,
    _get_or_create,
    require_command_authorization,
)
from m8flow_core.services.message_registry import _canonical_json, _digest, _key_digest
from m8flow_core.services.message_runtime import RuntimeMessageDefinition
from m8flow_core.services.tenant_users import ensure_user_belongs_to_tenant

_ELIGIBLE_STATUSES = ("running", "waiting", "user_input_required")


def _matching_subscription(
    session: Session,
    command: CorrelateMessageCommand,
) -> tuple[int, str]:
    subscription = MessageSubscriptionModel
    instance = ProcessInstanceModel
    conditions = [
        subscription.m8f_tenant_id == command.tenant_id,
        subscription.message_name == command.message_name,
        instance.status.in_(_ELIGIBLE_STATUSES),
    ]
    if command.process_definition_id is not None:
        conditions.append(
            subscription.process_definition_id == command.process_definition_id
        )
    if command.process_instance_id is not None:
        conditions.append(
            subscription.process_instance_id == command.process_instance_id
        )
    ownership = and_(
        instance.m8f_tenant_id == subscription.m8f_tenant_id,
        instance.id == subscription.process_instance_id,
    )
    profiles = session.scalars(
        select(subscription.property_paths)
        .distinct()
        .join(instance, ownership)
        .where(*conditions)
    )
    keys = []
    for profile in profiles:
        digest = _key_digest(command.payload, json.loads(profile))
        if digest is not None:
            keys.append(
                and_(
                    subscription.property_paths == profile,
                    subscription.correlation_digest == digest,
                )
            )
    if not keys:
        raise NotFoundError("No waiting message subscription matches")
    rows = session.execute(
        select(subscription.process_instance_id, subscription.task_guid)
        .join(instance, ownership)
        .where(*conditions, or_(*keys))
        .limit(2)
    ).all()
    if len(rows) > 1:
        raise AmbiguousMessageCorrelationError(
            "More than one waiting message subscription matches"
        )
    if not rows:
        raise NotFoundError("No waiting message subscription matches")
    return rows[0].process_instance_id, rows[0].task_guid


def _request_digest(command: CorrelateMessageCommand) -> str:
    for value in (command.message_name, command.delivery_id):
        if (
            not isinstance(value, str)
            or not value.strip()
            or len(value) > 255
            or "\0" in value
        ):
            raise ValidationError(
                "Message name and delivery ID must be non-blank strings "
                "of at most 255 characters"
            )
    for selector in (command.process_definition_id, command.process_instance_id):
        if selector is not None and (
            type(selector) is not int or not 0 < selector <= 2**31 - 1
        ):
            raise ValidationError(
                "Process definition and instance IDs must be positive integers"
            )
    if not isinstance(command.payload, dict):
        raise ValidationError("Message payload must be a JSON object")
    try:
        normalized = json.loads(_canonical_json(command.payload))
        if normalized != command.payload:
            raise ValueError("payload changes when serialized")
        return _digest(
            [
                command.message_name,
                command.user_id,
                command.payload,
                command.process_definition_id,
                command.process_instance_id,
            ]
        )
    except (TypeError, ValueError, RecursionError) as exc:
        raise ValidationError("Message payload must be a JSON object") from exc


def correlate_message(
    session: Session,
    command: CorrelateMessageCommand,
) -> MessageDeliveryReceiptModel:
    request_digest = _request_digest(command)
    ensure_user_belongs_to_tenant(
        session, tenant_id=command.tenant_id, user_id=command.user_id
    )
    require_command_authorization(
        session,
        tenant_id=command.tenant_id,
        actor_user_id=command.user_id,
        command_key=MESSAGE_CORRELATE_COMMAND,
        resource_type="tenant",
        resource_id=command.tenant_id,
    )
    # SQLite ignores row locks. Acquire its writer lock and establish the
    # outer transaction before the savepoint, including in legacy driver mode.
    if session.get_bind().dialect.name == "sqlite":
        receipt_model = MessageDeliveryReceiptModel
        session.execute(
            update(receipt_model)
            .where(
                receipt_model.m8f_tenant_id == command.tenant_id,
                receipt_model.delivery_id == command.delivery_id,
            )
            .values(request_digest=receipt_model.request_digest)
        )
    with session.begin_nested():
        timestamp = datetime.now(UTC)
        identity = {
            "m8f_tenant_id": command.tenant_id,
            "delivery_id": command.delivery_id,
        }
        receipt = _get_or_create(
            session,
            MessageDeliveryReceiptModel,
            lookup=identity,
            values={
                **identity,
                "request_digest": request_digest,
                "_delivered_at": timestamp,
            },
            conflict_columns=["m8f_tenant_id", "delivery_id"],
        )
        if receipt.request_digest != request_digest:
            raise MessageDeliveryConflictError(
                "Delivery ID was already used with different inputs"
            )
        if receipt.outcome is not None:
            return receipt
        instance_id, task_guid = _matching_subscription(session, command)
        instance = session.scalar(
            select(ProcessInstanceModel)
            .where(
                ProcessInstanceModel.m8f_tenant_id == command.tenant_id,
                ProcessInstanceModel.id == instance_id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if (
            instance is None
            or instance.status not in _ELIGIBLE_STATUSES
            or _matching_subscription(session, command) != (instance_id, task_guid)
        ):
            raise NotFoundError(
                "The matching message subscription is no longer waiting"
            )
        if instance.bpmn_process is not None:
            session.refresh(instance.bpmn_process)
        receipt.outcome = _deliver(session, instance, task_guid, command, timestamp)
        receipt.process_instance_id = instance_id
        receipt.task_guid = task_guid
        session.flush()
        return receipt


def _deliver(
    session: Session,
    instance: ProcessInstanceModel,
    task_guid: str,
    command: CorrelateMessageCommand,
    timestamp: datetime,
) -> str:
    state = instance.workflow_state_json
    if state is None:
        raise NotFoundError("The matching message subscription is no longer waiting")
    workflow = runtime._restore_workflow(
        state,
        service_task_context=runtime._service_task_execution_context(
            tenant_id=command.tenant_id,
            process_instance_id=instance.id,
            process_definition_id=instance.bpmn_process_definition_id,
        ),
    )
    task = workflow.event_manager.tasks.get(UUID(task_guid))
    event = getattr(getattr(task, "task_spec", None), "event_definition", None)
    digest = (
        _key_digest(command.payload, event.property_paths)
        if isinstance(event, RuntimeMessageDefinition)
        else None
    )
    if (
        task is None
        or task.state != TaskState.WAITING
        or not isinstance(event, RuntimeMessageDefinition)
        or event.name != command.message_name
        or digest is None
        or _key_digest(task.data, event.binding_paths) != digest
    ):
        raise NotFoundError("The matching message subscription is no longer waiting")
    task.task_spec.catch(task, BpmnEvent(event, payload=command.payload))
    task.task_spec._update(task)
    try:
        runtime._run_workflow_with_service_task_failure_handling(
            session,
            tenant_id=command.tenant_id,
            process_instance=instance,
            workflow=workflow,
            occurred_at=timestamp,
        )
    except (
        ServiceTaskExecutionError,
        CompensationExecutionError,
        WorkflowTaskException,
    ):
        return "workflow_error"
    runtime._persist_workflow_state(session, instance, workflow, occurred_at=timestamp)
    runtime._sync_inactive_work_items(
        session, process_instance=instance, workflow=workflow, occurred_at=timestamp
    )
    runtime._materialize_ready_manual_tasks(
        session, process_instance=instance, workflow=workflow, occurred_at=timestamp
    )
    runtime._update_process_instance_status_from_workflow(
        instance, workflow, occurred_at=timestamp
    )
    return "delivered"
