#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from m8flow_core import api
from m8flow_core.models.bpmn_process import BpmnProcessModel
from m8flow_core.models.bpmn_process_definition import (
    BpmnProcessDefinitionModel,
)
from m8flow_core.models.future_task import FutureTaskModel
from m8flow_core.models.json_data import JsonDataModel
from m8flow_core.models.process_instance import (
    WORKFLOW_STATE_JSON_DATA_KEY,
    ProcessInstanceModel,
)
from m8flow_core.models.scheduler_job import SchedulerJobModel, SchedulerJobType
from m8flow_core.models.task import TaskModel
from m8flow_core.models.task_definition import TaskDefinitionModel
from m8flow_core.models.tenant import M8flowTenantModel
from m8flow_core.models.user import UserModel
from m8flow_core.models.work_item import WorkItemModel
from m8flow_core.models.work_item_user import WorkItemUserModel
from m8flow_core.services import scheduler_runtime
from m8flow_core.services.authorization import ROLE_ADMIN, ensure_v1_role
from m8flow_core.services.scheduler_jobs import (
    delete_scheduler_job,
    upsert_scheduler_job,
)

INTERMEDIATE_TIMER_BPMN = (
    """<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  id="Definitions_Timer_Runtime"
  targetNamespace="http://m8flow.example/timers">
  <bpmn:process id="Process_timer_runtime" isExecutable="true">
    <bpmn:startEvent id="StartEvent_1" />
    <bpmn:sequenceFlow id="Flow_1" sourceRef="StartEvent_1" targetRef="TimerCatch_1" />
    <bpmn:intermediateCatchEvent id="TimerCatch_1" name="TimerCatch_1">
      <bpmn:incoming>Flow_1</bpmn:incoming>
      <bpmn:outgoing>Flow_2</bpmn:outgoing>
      <bpmn:timerEventDefinition>
"""
    + (
        '        <bpmn:timeDate xsi:type="bpmn:tFormalExpression">'
        "'2099-01-01T00:00:00+00:00'</bpmn:timeDate>\n"
    )
    + """
      </bpmn:timerEventDefinition>
    </bpmn:intermediateCatchEvent>
    <bpmn:sequenceFlow id="Flow_2" sourceRef="TimerCatch_1" targetRef="Task_1" />
    <bpmn:userTask id="Task_1" name="Review Request" />
    <bpmn:sequenceFlow id="Flow_3" sourceRef="Task_1" targetRef="EndEvent_1" />
    <bpmn:endEvent id="EndEvent_1" />
  </bpmn:process>
</bpmn:definitions>
"""
)

TIMER_START_BPMN = (
    """<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  id="Definitions_Timer_Start_Runtime"
  targetNamespace="http://m8flow.example/timers">
  <bpmn:process id="Process_timer_start_runtime" isExecutable="true">
    <bpmn:startEvent id="StartEvent_timer_start" name="StartEvent_timer_start">
      <bpmn:timerEventDefinition>
"""
    + (
        '        <bpmn:timeDate xsi:type="bpmn:tFormalExpression">'
        "'2099-01-01T00:00:00+00:00'</bpmn:timeDate>\n"
    )
    + """
      </bpmn:timerEventDefinition>
    </bpmn:startEvent>
    <bpmn:sequenceFlow
      id="Flow_1"
      sourceRef="StartEvent_timer_start"
      targetRef="Task_1"
    />
    <bpmn:userTask id="Task_1" name="Review Request" />
    <bpmn:sequenceFlow id="Flow_2" sourceRef="Task_1" targetRef="EndEvent_1" />
    <bpmn:endEvent id="EndEvent_1" />
  </bpmn:process>
</bpmn:definitions>
"""
)

TIMER_START_LANE_BPMN = (
    """<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  id="Definitions_Timer_Start_Lane_Runtime"
  targetNamespace="http://m8flow.example/timers">
  <bpmn:process id="Process_timer_start_lane_runtime" isExecutable="true">
    <bpmn:laneSet id="LaneSet_1">
      <bpmn:lane id="Lane_Operations" name="Operations">
        <bpmn:flowNodeRef>Task_1</bpmn:flowNodeRef>
      </bpmn:lane>
    </bpmn:laneSet>
    <bpmn:startEvent id="StartEvent_timer_start" name="StartEvent_timer_start">
      <bpmn:timerEventDefinition>
"""
    + (
        '        <bpmn:timeDate xsi:type="bpmn:tFormalExpression">'
        "'2099-01-01T00:00:00+00:00'</bpmn:timeDate>\n"
    )
    + """
      </bpmn:timerEventDefinition>
    </bpmn:startEvent>
    <bpmn:sequenceFlow
      id="Flow_1"
      sourceRef="StartEvent_timer_start"
      targetRef="Task_1"
    />
    <bpmn:userTask id="Task_1" name="Review Request" />
    <bpmn:sequenceFlow id="Flow_2" sourceRef="Task_1" targetRef="EndEvent_1" />
    <bpmn:endEvent id="EndEvent_1" />
  </bpmn:process>
</bpmn:definitions>
"""
)

TIMER_START_CYCLE_BPMN = (
    """<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  id="Definitions_Timer_Start_Cycle_Runtime"
  targetNamespace="http://m8flow.example/timers">
  <bpmn:process id="Process_timer_start_cycle_runtime" isExecutable="true">
    <bpmn:startEvent id="StartEvent_timer_cycle" name="StartEvent_timer_cycle">
      <bpmn:timerEventDefinition>
"""
    + (
        '        <bpmn:timeCycle xsi:type="bpmn:tFormalExpression">'
        "'R3/PT5M'</bpmn:timeCycle>\n"
    )
    + """
      </bpmn:timerEventDefinition>
    </bpmn:startEvent>
    <bpmn:sequenceFlow
      id="Flow_1"
      sourceRef="StartEvent_timer_cycle"
      targetRef="Task_1"
    />
    <bpmn:userTask id="Task_1" name="Review Request" />
    <bpmn:sequenceFlow id="Flow_2" sourceRef="Task_1" targetRef="EndEvent_1" />
    <bpmn:endEvent id="EndEvent_1" />
  </bpmn:process>
</bpmn:definitions>
"""
)

TIMER_START_CYCLE_FAILING_SERVICE_BPMN = TIMER_START_CYCLE_BPMN.replace(
    'xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"',
    'xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"\n'
    '  xmlns:spiffworkflow="http://spiffworkflow.org/bpmn/schema/1.0/core"',
).replace(
    '<bpmn:userTask id="Task_1" name="Review Request" />',
    """<bpmn:serviceTask id="Task_1" name="Failing automatic task">
      <bpmn:extensionElements>
        <spiffworkflow:serviceTaskOperator id="timer-test/Fail" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>""",
)

TIMER_START_CYCLE_FAILING_SCRIPT_BPMN = TIMER_START_CYCLE_BPMN.replace(
    '<bpmn:userTask id="Task_1" name="Review Request" />',
    """<bpmn:scriptTask id="Task_1" name="Failing automatic script">
      <bpmn:script>raise Exception("timer automatic script failed")</bpmn:script>
    </bpmn:scriptTask>""",
)


class FailingTimerServiceTaskConnector:
    connector_key = "timer-test"

    def list_commands(self) -> tuple[api.ServiceTaskCommandDefinition, ...]:
        return (api.ServiceTaskCommandDefinition("timer-test", "Fail"),)

    def execute(self, request: api.ServiceTaskRequest) -> api.ServiceTaskResult:
        raise RuntimeError("automatic timer task failed")


BOUNDARY_TIMER_BPMN = (
    """<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  id="Definitions_Boundary_Timer_Runtime"
  targetNamespace="http://m8flow.example/timers">
  <bpmn:process id="Process_boundary_timer_runtime" isExecutable="true">
    <bpmn:laneSet id="LaneSet_1">
      <bpmn:lane id="Lane_Operations" name="Operations">
        <bpmn:flowNodeRef>Task_review</bpmn:flowNodeRef>
        <bpmn:flowNodeRef>Task_timeout</bpmn:flowNodeRef>
      </bpmn:lane>
    </bpmn:laneSet>
    <bpmn:startEvent id="StartEvent_1" />
    <bpmn:sequenceFlow id="Flow_1" sourceRef="StartEvent_1" targetRef="Task_review" />
    <bpmn:userTask id="Task_review" name="Review Request" />
    <bpmn:boundaryEvent
      id="BoundaryEvent_timeout"
      attachedToRef="Task_review"
      cancelActivity="true">
      <bpmn:timerEventDefinition>
"""
    + (
        '        <bpmn:timeDate xsi:type="bpmn:tFormalExpression">'
        "'2099-01-01T00:00:00+00:00'</bpmn:timeDate>\n"
    )
    + """
      </bpmn:timerEventDefinition>
    </bpmn:boundaryEvent>
    <bpmn:sequenceFlow
      id="Flow_timeout"
      sourceRef="BoundaryEvent_timeout"
      targetRef="Task_timeout"
    />
    <bpmn:userTask id="Task_timeout" name="Handle Timeout" />
    <bpmn:sequenceFlow
      id="Flow_2"
      sourceRef="Task_review"
      targetRef="EndEvent_review"
    />
    <bpmn:endEvent id="EndEvent_review" />
    <bpmn:sequenceFlow
      id="Flow_3"
      sourceRef="Task_timeout"
      targetRef="EndEvent_timeout"
    />
    <bpmn:endEvent id="EndEvent_timeout" />
  </bpmn:process>
</bpmn:definitions>
"""
)

TIMER_EVENT_SUBPROCESS_BPMN = (
    """<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  id="Definitions_Timer_Event_Subprocess"
  targetNamespace="http://m8flow.example/timers">
  <bpmn:process id="Process_timer_event_subprocess" isExecutable="true">
    <bpmn:startEvent id="StartEvent_1" />
    <bpmn:sequenceFlow id="Flow_1" sourceRef="StartEvent_1" targetRef="TimerCatch_1" />
    <bpmn:intermediateCatchEvent id="TimerCatch_1" name="TimerCatch_1">
      <bpmn:timerEventDefinition>
"""
    + (
        '        <bpmn:timeDate xsi:type="bpmn:tFormalExpression">'
        "'2099-01-01T00:00:00+00:00'</bpmn:timeDate>\n"
    )
    + """
      </bpmn:timerEventDefinition>
    </bpmn:intermediateCatchEvent>
    <bpmn:sequenceFlow id="Flow_2" sourceRef="TimerCatch_1" targetRef="EndEvent_1" />
    <bpmn:endEvent id="EndEvent_1" />
    <bpmn:subProcess id="Reminder" triggeredByEvent="true">
      <bpmn:startEvent id="ReminderStart" isInterrupting="false">
        <bpmn:timerEventDefinition>
"""
    + (
        '          <bpmn:timeDate xsi:type="bpmn:tFormalExpression">'
        "'2098-01-01T00:00:00+00:00'</bpmn:timeDate>\n"
    )
    + """
        </bpmn:timerEventDefinition>
      </bpmn:startEvent>
      <bpmn:sequenceFlow
        id="Flow_r1"
        sourceRef="ReminderStart"
        targetRef="Task_reminder"
      />
      <bpmn:scriptTask id="Task_reminder" name="Send Reminder">
        <bpmn:script>reminder_sent = True</bpmn:script>
      </bpmn:scriptTask>
      <bpmn:sequenceFlow
        id="Flow_r2"
        sourceRef="Task_reminder"
        targetRef="ReminderEnd"
      />
      <bpmn:endEvent id="ReminderEnd" />
    </bpmn:subProcess>
  </bpmn:process>
</bpmn:definitions>
"""
)


def test_run_due_scheduler_jobs_reschedules_stale_due_timer(
    session: Session,
) -> None:
    tenant, user = _seed_timer_actor(session)
    process_instance = _initialize_waiting_timer_process(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
    )
    scheduler_job = _load_scheduler_job(session, tenant_id=tenant.id)
    expected_run_at = scheduler_job.run_at
    scheduler_job.run_at = datetime.fromtimestamp(0, UTC)
    session.flush()

    processed_count = api.run_due_scheduler_jobs(
        session,
        worker_id="inline-timer-worker",
        tenant_id=tenant.id,
    )

    assert processed_count == 1
    session.expire_all()

    process_instance = api.execute_query(
        session,
        api.GetProcessInstanceQuery(
            tenant_id=tenant.id,
            process_instance_id=process_instance.id,
        ),
    )
    scheduler_job = _load_scheduler_job(session, tenant_id=tenant.id)
    assert process_instance.status == api.ProcessInstanceStatus.waiting.value
    assert scheduler_job.run_at == expected_run_at
    assert scheduler_job.locked_by is None
    assert scheduler_job.locked_at is None
    assert (
        api.execute_query(
            session,
            api.GetPendingTasksQuery(
                tenant_id=tenant.id,
                user_id=user.id,
            ),
        )
        == []
    )


def test_run_due_scheduler_jobs_advances_past_due_intermediate_timer(
    session: Session,
) -> None:
    tenant, user = _seed_timer_actor(session)
    process_instance = _initialize_waiting_timer_process(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
    )
    timer_task = next(
        task
        for task in process_instance.tasks
        if task.task_definition.bpmn_identifier == "TimerCatch_1"
    )
    _set_waiting_timer_event_value(
        session,
        process_instance=process_instance,
        task_guid=timer_task.guid,
        event_value="1970-01-01T00:00:00+00:00",
    )
    scheduler_job = _load_scheduler_job(session, tenant_id=tenant.id)
    scheduler_job.run_at = datetime.fromtimestamp(0, UTC)
    session.flush()

    processed_count = api.run_due_scheduler_jobs(
        session,
        worker_id="inline-timer-worker",
        tenant_id=tenant.id,
    )

    assert processed_count == 1
    session.expire_all()

    process_instance = api.execute_query(
        session,
        api.GetProcessInstanceQuery(
            tenant_id=tenant.id,
            process_instance_id=process_instance.id,
        ),
    )
    pending_tasks = api.execute_query(
        session,
        api.GetPendingTasksQuery(
            tenant_id=tenant.id,
            user_id=user.id,
        ),
    )
    scheduler_jobs = list(
        session.scalars(
            select(SchedulerJobModel).where(
                SchedulerJobModel.m8f_tenant_id == tenant.id
            )
        ).all()
    )

    assert process_instance.status == api.ProcessInstanceStatus.user_input_required
    assert len(pending_tasks) == 1
    assert pending_tasks[0].task_name == "Task_1"
    assert scheduler_jobs == []


def test_run_due_scheduler_jobs_executes_interrupting_boundary_timer(
    session: Session,
) -> None:
    tenant, user = _seed_timer_actor(session)
    definition = _import_process_definition(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
        bpmn_identifier="boundary-timer-runtime-process",
        bpmn_name="Boundary Timer Runtime Process",
        source_bpmn_xml=BOUNDARY_TIMER_BPMN,
        properties_json={
            "lane_owners": {
                "Operations": [user.username],
            }
        },
    )
    process_instance = api.execute_command(
        session,
        api.InitializeProcessInstanceFromDefinitionCommand(
            tenant_id=tenant.id,
            bpmn_process_definition_id=definition.id,
            process_initiator_id=user.id,
            started_at=datetime.fromtimestamp(20, UTC),
        ),
    )
    boundary_task = next(
        task
        for task in process_instance.tasks
        if task.task_definition.bpmn_identifier == "BoundaryEvent_timeout"
    )
    original_pending_tasks = api.execute_query(
        session,
        api.GetPendingTasksQuery(
            tenant_id=tenant.id,
            user_id=user.id,
        ),
    )
    original_review_human_task = next(
        task for task in original_pending_tasks if task.task_name == "Task_review"
    )
    _set_waiting_timer_event_value(
        session,
        process_instance=process_instance,
        task_guid=boundary_task.guid,
        event_value="1970-01-01T00:00:00+00:00",
    )
    scheduler_job = _load_scheduler_job(session, tenant_id=tenant.id)
    scheduler_job.run_at = datetime.fromtimestamp(0, UTC)
    session.flush()

    processed_count = api.run_due_scheduler_jobs(
        session,
        worker_id="inline-boundary-timer-worker",
        tenant_id=tenant.id,
    )

    assert processed_count == 1
    session.expire_all()

    process_instance = api.execute_query(
        session,
        api.GetProcessInstanceQuery(
            tenant_id=tenant.id,
            process_instance_id=process_instance.id,
        ),
    )
    pending_tasks = api.execute_query(
        session,
        api.GetPendingTasksQuery(
            tenant_id=tenant.id,
            user_id=user.id,
        ),
    )
    scheduler_jobs = list(
        session.scalars(
            select(SchedulerJobModel).where(
                SchedulerJobModel.m8f_tenant_id == tenant.id
            )
        ).all()
    )
    events = api.execute_query(
        session,
        api.GetProcessInstanceEventsQuery(
            tenant_id=tenant.id,
            process_instance_id=process_instance.id,
        ),
    )

    cancelled_review_human_task = session.get(
        WorkItemModel,
        original_review_human_task.id,
    )
    assert cancelled_review_human_task is not None

    assert (
        process_instance.status == api.ProcessInstanceStatus.user_input_required.value
    )
    assert [task.task_name for task in pending_tasks] == ["Task_timeout"]
    assert pending_tasks[0].task_status == "READY"
    assert scheduler_jobs == []
    assert cancelled_review_human_task.completed is True
    assert cancelled_review_human_task.task_status == "CANCELLED"
    assert [event.event_type for event in events] == [
        api.ProcessLifecycleEventType.process_instance_created.value,
        api.TaskEventType.task_cancelled.value,
    ]


def test_timer_event_subprocess_is_scheduled_as_an_intermediate_timer(
    session: Session,
) -> None:
    tenant, user = _seed_timer_actor(session)
    definition = _import_process_definition(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
        bpmn_identifier="timer-event-subprocess-process",
        bpmn_name="Timer Event Subprocess Process",
        source_bpmn_xml=TIMER_EVENT_SUBPROCESS_BPMN,
    )

    # The event subprocess start is not a process start: importing must not
    # create a timer_start job that would start new instances.
    assert (
        session.scalars(
            select(SchedulerJobModel).where(
                SchedulerJobModel.m8f_tenant_id == tenant.id
            )
        ).all()
        == []
    )

    process_instance = api.execute_command(
        session,
        api.InitializeProcessInstanceFromDefinitionCommand(
            tenant_id=tenant.id,
            bpmn_process_definition_id=definition.id,
            process_initiator_id=user.id,
            started_at=datetime.fromtimestamp(20, UTC),
        ),
    )

    scheduler_job = _load_scheduler_job(session, tenant_id=tenant.id)
    assert process_instance.status == api.ProcessInstanceStatus.waiting.value
    assert scheduler_job.job_type == "intermediate_timer"
    assert scheduler_job.run_at == datetime(2098, 1, 1, tzinfo=UTC)
    assert sorted(
        payload["task_spec_name"]
        for payload in scheduler_job.payload_json["timer_tasks"]
    ) == ["ReminderStart", "TimerCatch_1"]


def test_run_due_scheduler_jobs_fires_timer_event_subprocess(
    session: Session,
) -> None:
    tenant, user = _seed_timer_actor(session)
    definition = _import_process_definition(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
        bpmn_identifier="timer-event-subprocess-process",
        bpmn_name="Timer Event Subprocess Process",
        source_bpmn_xml=TIMER_EVENT_SUBPROCESS_BPMN,
    )
    process_instance = api.execute_command(
        session,
        api.InitializeProcessInstanceFromDefinitionCommand(
            tenant_id=tenant.id,
            bpmn_process_definition_id=definition.id,
            process_initiator_id=user.id,
            started_at=datetime.fromtimestamp(20, UTC),
        ),
    )
    scheduler_job = _load_scheduler_job(session, tenant_id=tenant.id)
    reminder_start_guid = next(
        payload["task_guid"]
        for payload in scheduler_job.payload_json["timer_tasks"]
        if payload["task_spec_name"] == "ReminderStart"
    )
    _set_waiting_timer_event_value(
        session,
        process_instance=process_instance,
        task_guid=reminder_start_guid,
        event_value="1970-01-01T00:00:00+00:00",
    )
    scheduler_job.run_at = datetime.fromtimestamp(0, UTC)
    session.flush()

    processed_count = api.run_due_scheduler_jobs(
        session,
        worker_id="inline-event-subprocess-worker",
        tenant_id=tenant.id,
    )

    assert processed_count == 1
    session.expire_all()

    process_instance = api.execute_query(
        session,
        api.GetProcessInstanceQuery(
            tenant_id=tenant.id,
            process_instance_id=process_instance.id,
        ),
    )
    scheduler_job = _load_scheduler_job(session, tenant_id=tenant.id)

    # Non-interrupting: the reminder runs to completion and only the main
    # timer is left waiting.
    assert process_instance.status == api.ProcessInstanceStatus.waiting.value
    assert scheduler_job.run_at == datetime(2099, 1, 1, tzinfo=UTC)
    assert [
        payload["task_spec_name"]
        for payload in scheduler_job.payload_json["timer_tasks"]
    ] == ["TimerCatch_1"]


def test_run_due_scheduler_jobs_executes_scheduled_process_retry(
    session: Session,
) -> None:
    tenant, user, process_instance, human_task = _seed_errored_retry_process(session)
    api.execute_command(
        session,
        api.ScheduleProcessInstanceRetryCommand(
            tenant_id=tenant.id,
            process_instance_id=process_instance.id,
            user_id=user.id,
            retry_at=datetime.fromtimestamp(200, UTC),
            scheduled_at=datetime.fromtimestamp(190, UTC),
        ),
    )

    processed_count = api.run_due_scheduler_jobs(
        session,
        now=datetime.fromtimestamp(200, UTC),
        worker_id="inline-retry-worker",
        tenant_id=tenant.id,
    )

    assert processed_count == 1
    session.expire_all()

    process_instance = api.execute_query(
        session,
        api.GetProcessInstanceQuery(
            tenant_id=tenant.id,
            process_instance_id=process_instance.id,
        ),
    )
    pending_tasks = api.execute_query(
        session,
        api.GetPendingTasksQuery(
            tenant_id=tenant.id,
            user_id=user.id,
        ),
    )
    scheduler_jobs = list(
        session.scalars(
            select(SchedulerJobModel).where(
                SchedulerJobModel.m8f_tenant_id == tenant.id
            )
        ).all()
    )
    events = api.execute_query(
        session,
        api.GetProcessInstanceEventsQuery(
            tenant_id=tenant.id,
            process_instance_id=process_instance.id,
        ),
    )

    assert process_instance.status == api.ProcessInstanceStatus.running.value
    assert process_instance.ended_at is None
    assert len(pending_tasks) == 1
    assert pending_tasks[0].id == human_task.id
    assert pending_tasks[0].task_status == "READY"
    assert scheduler_jobs == []
    assert [event.event_type for event in events] == [
        api.ProcessLifecycleEventType.process_instance_error.value,
        api.ProcessLifecycleEventType.process_instance_retried.value,
    ]


def test_run_due_scheduler_jobs_executes_timer_start_job(
    session: Session,
) -> None:
    tenant, user = _seed_timer_actor(session)
    definition = _import_process_definition(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
        bpmn_identifier="timer-start-runtime-process",
        bpmn_name="Timer Start Runtime Process",
        source_bpmn_xml=TIMER_START_BPMN,
    )
    scheduler_job = _load_scheduler_job(session, tenant_id=tenant.id)
    scheduler_job.run_at = datetime.fromtimestamp(0, UTC)
    session.flush()

    scheduled_count = api.run_due_scheduler_jobs(
        session,
        worker_id="inline-timer-start-worker",
        tenant_id=tenant.id,
    )
    processed_count = api.run_due_scheduler_jobs(
        session,
        worker_id="inline-timer-start-worker",
        tenant_id=tenant.id,
    )

    assert scheduled_count == 1
    assert processed_count == 1
    session.expire_all()

    process_instances = api.execute_query(
        session,
        api.ListProcessInstancesQuery(tenant_id=tenant.id),
    )
    scheduler_jobs = list(
        session.scalars(
            select(SchedulerJobModel).where(
                SchedulerJobModel.m8f_tenant_id == tenant.id
            )
        ).all()
    )

    assert len(process_instances) == 1
    assert process_instances[0].bpmn_process_definition_id == definition.id
    assert process_instances[0].process_initiator_id != user.id
    assert process_instances[0].status == api.ProcessInstanceStatus.user_input_required
    assert len(process_instances[0].work_items) == 1
    assert process_instances[0].work_items[0].task_status == "READY"
    assert process_instances[0].work_items[0].lane_assignment_id is None
    assert [
        (assignment.user_id, assignment.added_by)
        for assignment in process_instances[0].work_items[0].work_item_users
    ] == [(process_instances[0].process_initiator_id, "process_initiator")]
    assert scheduler_jobs == []


def test_run_due_scheduler_jobs_uses_definition_lane_owners_for_timer_start(
    session: Session,
) -> None:
    tenant, user = _seed_timer_actor(session)
    definition = _import_process_definition(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
        bpmn_identifier="timer-start-lane-runtime-process",
        bpmn_name="Timer Start Lane Runtime Process",
        source_bpmn_xml=TIMER_START_LANE_BPMN,
        properties_json={
            "lane_owners": {
                "Operations": [user.username],
            }
        },
    )
    scheduler_job = _load_scheduler_job(session, tenant_id=tenant.id)
    scheduler_job.run_at = datetime.fromtimestamp(0, UTC)
    session.flush()

    scheduled_count = api.run_due_scheduler_jobs(
        session,
        worker_id="inline-timer-start-worker",
        tenant_id=tenant.id,
    )
    processed_count = api.run_due_scheduler_jobs(
        session,
        worker_id="inline-timer-start-worker",
        tenant_id=tenant.id,
    )

    assert scheduled_count == 1
    assert processed_count == 1
    session.expire_all()

    process_instances = api.execute_query(
        session,
        api.ListProcessInstancesQuery(tenant_id=tenant.id),
    )
    pending_tasks = api.execute_query(
        session,
        api.GetPendingTasksQuery(
            tenant_id=tenant.id,
            user_id=user.id,
        ),
    )

    assert len(process_instances) == 1
    assert process_instances[0].bpmn_process_definition_id == definition.id
    assert process_instances[0].work_items[0].lane_name == "Operations"
    assert process_instances[0].work_items[0].json_metadata["lane_owners"] == {
        "Operations": [user.username]
    }
    assert len(pending_tasks) == 1
    assert pending_tasks[0].process_instance_id == process_instances[0].id


def test_run_due_scheduler_jobs_executes_past_due_timer_start_definition(
    session: Session,
) -> None:
    tenant, user = _seed_timer_actor(session)
    definition = _import_process_definition(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
        bpmn_identifier="timer-start-past-due-runtime-process",
        bpmn_name="Timer Start Past Due Runtime Process",
        source_bpmn_xml=TIMER_START_LANE_BPMN,
        properties_json={
            "lane_owners": {
                "Operations": [user.username],
            }
        },
    )
    scheduler_job = _load_scheduler_job(session, tenant_id=tenant.id)
    scheduler_job.run_at = datetime.fromtimestamp(0, UTC)
    definition.source_bpmn_xml = TIMER_START_LANE_BPMN.replace(
        "2099-01-01T00:00:00+00:00",
        "1970-01-01T00:00:00+00:00",
    )
    session.flush()

    scheduled_count = api.run_due_scheduler_jobs(
        session,
        worker_id="inline-timer-start-worker",
        tenant_id=tenant.id,
    )
    processed_count = api.run_due_scheduler_jobs(
        session,
        worker_id="inline-timer-start-worker",
        tenant_id=tenant.id,
    )

    assert scheduled_count == 1
    assert processed_count == 1
    session.expire_all()

    process_instances = api.execute_query(
        session,
        api.ListProcessInstancesQuery(tenant_id=tenant.id),
    )
    pending_tasks = api.execute_query(
        session,
        api.GetPendingTasksQuery(
            tenant_id=tenant.id,
            user_id=user.id,
        ),
    )

    assert len(process_instances) == 1
    assert process_instances[0].bpmn_process_definition_id == definition.id
    assert process_instances[0].status == api.ProcessInstanceStatus.user_input_required
    assert len(process_instances[0].work_items) == 1
    assert len(pending_tasks) == 1
    assert pending_tasks[0].process_instance_id == process_instances[0].id


def test_run_due_scheduler_jobs_reschedules_recurring_timer_start_job(
    session: Session,
) -> None:
    tenant, user = _seed_timer_actor(session)
    _import_process_definition(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
        bpmn_identifier="timer-start-cycle-runtime-process",
        bpmn_name="Timer Start Cycle Runtime Process",
        source_bpmn_xml=TIMER_START_CYCLE_BPMN,
    )
    scheduler_job = _load_scheduler_job(session, tenant_id=tenant.id)
    scheduler_job.run_at = datetime.fromtimestamp(0, UTC)
    scheduler_job.payload_json = {
        "scheduled_from": "test",
        "timer_task": {
            "event_definition_type": "TimeCycleEventDefinition",
            "event_value": {
                "cycles": 3,
                "next": "1970-01-01T00:00:00+00:00",
                "duration": 300.0,
            },
            "run_at_in_seconds": 0,
            "task_guid": "timer-start-cycle-guid",
            "task_spec_name": "StartEvent_timer_cycle",
            "task_spec_type": "StartEvent",
        },
    }
    session.flush()

    scheduled_count = api.run_due_scheduler_jobs(
        session,
        now=datetime.fromtimestamp(0, UTC),
        worker_id="inline-timer-start-worker",
        tenant_id=tenant.id,
    )
    assert scheduled_count == 1
    assert (
        api.execute_query(
            session,
            api.ListProcessInstancesQuery(tenant_id=tenant.id),
        )
        == []
    )

    queued_count = api.run_due_scheduler_jobs(
        session,
        now=datetime.fromtimestamp(0, UTC),
        worker_id="inline-timer-start-worker",
        tenant_id=tenant.id,
    )

    assert queued_count == 1
    session.expire_all()

    process_instances = api.execute_query(
        session,
        api.ListProcessInstancesQuery(tenant_id=tenant.id),
    )
    scheduler_job = _load_scheduler_job(
        session,
        tenant_id=tenant.id,
        job_type="timer_start",
    )
    human_tasks = list(
        session.scalars(select(WorkItemModel).order_by(WorkItemModel.id)).all()
    )

    assert len(process_instances) == 1
    assert len(human_tasks) == 1
    assert human_tasks[0].process_instance_id == process_instances[0].id
    assert human_tasks[0].task_name == "Task_1"
    assert scheduler_job.job_type == "timer_start"
    assert scheduler_job.run_at.timestamp() == 300
    assert scheduler_job.payload_json == {
        "scheduled_from": "timer_start_runtime",
        "timer_task": {
            "event_definition_type": "TimeCycleEventDefinition",
            "event_value": {
                "cycles": 2,
                "duration": 300.0,
                "next": "1970-01-01T00:05:00+00:00",
            },
            "run_at_in_seconds": 300,
            "task_guid": "timer-start-cycle-guid",
            "task_spec_name": "StartEvent_timer_cycle",
            "task_spec_type": "StartEvent",
        },
    }


def test_unbounded_timer_start_cycle_keeps_rescheduling_after_multiple_fires(
    session: Session,
) -> None:
    tenant, user = _seed_timer_actor(session)
    source = TIMER_START_CYCLE_BPMN.replace("'R3/PT5M'", "'R/PT5M'")
    _import_process_definition(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
        bpmn_identifier="timer-start-unbounded-cycle",
        bpmn_name="Timer Start Unbounded Cycle",
        source_bpmn_xml=source,
    )
    timer_job = _load_scheduler_job(session, tenant_id=tenant.id)
    timer_task = dict(timer_job.payload_json["timer_task"])
    event_value = dict(timer_task["event_value"])
    assert event_value["cycles"] == -1
    event_value["next"] = "1970-01-01T00:00:00+00:00"
    timer_task["event_value"] = event_value
    timer_task["run_at_in_seconds"] = 0
    timer_job.payload_json = {
        **timer_job.payload_json,
        "timer_task": timer_task,
    }
    timer_job.run_at = datetime.fromtimestamp(0, UTC)
    session.commit()

    for occurrence_number, now_in_seconds in enumerate((0, 300, 600), start=1):
        assert (
            api.run_due_scheduler_jobs(
                session,
                now=datetime.fromtimestamp(now_in_seconds, UTC),
                tenant_id=tenant.id,
            )
            == 1
        )
        session.commit()

        timer_job = _load_scheduler_job(
            session,
            tenant_id=tenant.id,
            job_type=SchedulerJobType.timer_start.value,
        )
        assert timer_job.run_at.timestamp() == now_in_seconds + 300
        next_event_value = timer_job.payload_json["timer_task"]["event_value"]
        assert (
            next_event_value["next"]
            == datetime.fromtimestamp(now_in_seconds + 300, UTC).isoformat()
        )

        assert (
            api.run_due_scheduler_jobs(
                session,
                now=datetime.fromtimestamp(now_in_seconds, UTC),
                tenant_id=tenant.id,
            )
            == 1
        )
        session.commit()
        instances = api.execute_query(
            session,
            api.ListProcessInstancesQuery(tenant_id=tenant.id),
        )
        assert len(instances) == occurrence_number


def test_recurring_timer_creates_later_occurrence_while_first_instance_waits(
    session: Session,
) -> None:
    tenant, user = _seed_timer_actor(session)
    _import_process_definition(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
        bpmn_identifier="timer-start-overlapping-occurrences",
        bpmn_name="Timer Start Overlapping Occurrences",
        source_bpmn_xml=TIMER_START_CYCLE_BPMN,
    )
    timer_job = _load_scheduler_job(session, tenant_id=tenant.id)
    timer_job.run_at = datetime.fromtimestamp(0, UTC)
    timer_job.payload_json = {
        "scheduled_from": "test",
        "timer_task": {
            "event_definition_type": "TimeCycleEventDefinition",
            "event_value": {
                "cycles": 3,
                "next": "1970-01-01T00:00:00+00:00",
                "duration": 300.0,
            },
            "run_at_in_seconds": 0,
            "task_guid": "timer-start-cycle-guid",
            "task_spec_name": "StartEvent_timer_cycle",
            "task_spec_type": "StartEvent",
        },
    }
    session.flush()

    assert (
        api.run_due_scheduler_jobs(
            session, now=datetime.fromtimestamp(0, UTC), tenant_id=tenant.id
        )
        == 1
    )
    assert (
        api.execute_query(
            session,
            api.ListProcessInstancesQuery(tenant_id=tenant.id),
        )
        == []
    )
    assert (
        api.run_due_scheduler_jobs(
            session, now=datetime.fromtimestamp(0, UTC), tenant_id=tenant.id
        )
        == 1
    )
    first_instances = api.execute_query(
        session,
        api.ListProcessInstancesQuery(tenant_id=tenant.id),
    )
    assert len(first_instances) == 1
    assert first_instances[0].status == api.ProcessInstanceStatus.user_input_required
    recurring_job = _load_scheduler_job(
        session, tenant_id=tenant.id, job_type="timer_start"
    )
    assert recurring_job.run_at.timestamp() == 300
    assert recurring_job.locked_by is None

    assert (
        api.run_due_scheduler_jobs(
            session, now=datetime.fromtimestamp(350, UTC), tenant_id=tenant.id
        )
        == 1
    )
    assert (
        _load_scheduler_job(
            session, tenant_id=tenant.id, job_type="timer_start"
        ).run_at.timestamp()
        == 600
    )
    assert (
        len(
            api.execute_query(
                session,
                api.ListProcessInstancesQuery(tenant_id=tenant.id),
            )
        )
        == 1
    )
    assert (
        api.run_due_scheduler_jobs(
            session, now=datetime.fromtimestamp(350, UTC), tenant_id=tenant.id
        )
        == 1
    )

    process_instances = api.execute_query(
        session,
        api.ListProcessInstancesQuery(tenant_id=tenant.id),
    )
    assert len(process_instances) == 2
    assert all(
        instance.status == api.ProcessInstanceStatus.user_input_required
        for instance in process_instances
    )


def test_overdue_timer_and_occurrences_both_make_progress(session: Session) -> None:
    tenant, user = _seed_timer_actor(session)
    _import_process_definition(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
        bpmn_identifier="timer-start-overdue-progress",
        bpmn_name="Timer Start Overdue Progress",
        source_bpmn_xml=TIMER_START_CYCLE_BPMN,
    )
    timer_job = _load_scheduler_job(session, tenant_id=tenant.id)
    timer_job.run_at = datetime.fromtimestamp(0, UTC)
    timer_job.payload_json = {
        "scheduled_from": "test",
        "timer_task": {
            "event_definition_type": "TimeCycleEventDefinition",
            "event_value": {
                "cycles": 3,
                "next": "1970-01-01T00:00:00+00:00",
                "duration": 300.0,
            },
            "run_at_in_seconds": 0,
            "task_guid": "timer-start-overdue-progress-guid",
            "task_spec_name": "StartEvent_timer_cycle",
            "task_spec_type": "StartEvent",
        },
    }
    session.flush()

    # Queue the first occurrence, then poll at t=600 with both it and the
    # next cadence due. Repeated late polls must run occurrences and advance
    # cadence rather than letting either class starve the other.
    assert (
        api.run_due_scheduler_jobs(
            session, now=datetime.fromtimestamp(0, UTC), tenant_id=tenant.id
        )
        == 1
    )
    for _ in range(5):
        assert (
            api.run_due_scheduler_jobs(
                session, now=datetime.fromtimestamp(600, UTC), tenant_id=tenant.id
            )
            == 1
        )

    instances = api.execute_query(
        session,
        api.ListProcessInstancesQuery(tenant_id=tenant.id),
    )
    assert len(instances) == 3
    assert all(
        instance.status == api.ProcessInstanceStatus.user_input_required
        for instance in instances
    )
    assert (
        session.scalar(
            select(SchedulerJobModel.id).where(
                SchedulerJobModel.m8f_tenant_id == tenant.id,
                SchedulerJobModel.job_type == "timer_start",
            )
        )
        is None
    )
    assert (
        session.scalar(
            select(SchedulerJobModel.id).where(
                SchedulerJobModel.m8f_tenant_id == tenant.id,
                SchedulerJobModel.job_type == "timer_start_occurrence",
            )
        )
        is None
    )


def test_scheduler_claim_survives_database_truncating_lock_timestamp(
    session: Session,
) -> None:
    if session.get_bind().dialect.name != "sqlite":
        pytest.skip(
            "The trigger simulates a database that truncates fractional seconds"
        )
    tenant, _ = _seed_timer_actor(session)
    now = datetime.fromtimestamp(10.123456, UTC)
    upsert_scheduler_job(
        session,
        tenant_id=tenant.id,
        job_key="test|q:truncated-lock-time",
        job_type=SchedulerJobType.process_retry,
        run_at=datetime.fromtimestamp(10, UTC),
        payload_json={},
        updated_at=datetime.fromtimestamp(1, UTC),
        created_at=datetime.fromtimestamp(1, UTC),
    )
    session.execute(
        text(
            """
            CREATE TRIGGER truncate_scheduler_locked_at
            AFTER UPDATE OF locked_at ON scheduler_job
            WHEN NEW.locked_at IS NOT NULL
            BEGIN
              UPDATE scheduler_job
              SET locked_at = substr(NEW.locked_at, 1, 19)
              WHERE id = NEW.id;
            END
            """
        )
    )
    session.flush()

    claimed = scheduler_runtime.claim_due_scheduler_jobs(
        session,
        now=now,
        worker_id="precision-worker",
        tenant_id=tenant.id,
    )

    assert len(claimed) == 1
    assert claimed[0].locked_by == "precision-worker"
    persisted = session.get(SchedulerJobModel, claimed[0].id)
    assert persisted is not None
    assert persisted.locked_at == datetime.fromtimestamp(10, UTC)


def test_persistently_overdue_cadence_alternates_with_execution_jobs(
    session: Session,
) -> None:
    tenant, user = _seed_timer_actor(session)
    other_tenant = M8flowTenantModel(
        id="tenant-scheduler-overdue-other",
        name="Other Scheduler Tenant",
        slug="tenant-scheduler-overdue-other",
    )
    other_user = UserModel(
        username="other-scheduler-overdue-admin",
        email="other-scheduler-overdue-admin@example.com",
        service="http://localhost:7002/realms/tenant-scheduler-overdue-other",
        service_id="other-scheduler-overdue-admin-keycloak",
        display_name="Other Scheduler Admin",
        created_at=datetime.fromtimestamp(1, UTC),
        updated_at=datetime.fromtimestamp(1, UTC),
    )
    session.add_all([other_tenant, other_user])
    session.flush()
    ensure_v1_role(
        session,
        tenant_id=other_tenant.id,
        role_name=ROLE_ADMIN,
        user_ids=[other_user.id],
    )
    _import_process_definition(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
        bpmn_identifier="timer-start-persistently-overdue",
        bpmn_name="Timer Start Persistently Overdue",
        source_bpmn_xml=TIMER_START_CYCLE_BPMN,
    )
    timer_job = _load_scheduler_job(session, tenant_id=tenant.id)
    timer_job.run_at = datetime.fromtimestamp(0, UTC)
    timer_job.payload_json = {
        "scheduled_from": "test",
        "timer_task": {
            "event_definition_type": "TimeCycleEventDefinition",
            "event_value": {
                "cycles": 99,
                "next": "1970-01-01T00:00:00+00:00",
                "duration": 1.0,
            },
            "run_at_in_seconds": 0,
            "task_guid": "timer-start-overdue-guid",
            "task_spec_name": "StartEvent_timer_cycle",
            "task_spec_type": "StartEvent",
        },
    }
    failed_instance = _create_stub_process_instance(
        session,
        tenant_id=other_tenant.id,
        user_id=other_user.id,
        process_model_identifier="scheduler-overdue-intermediate-timer",
    )
    upsert_scheduler_job(
        session,
        tenant_id=other_tenant.id,
        job_key=f"intermediate_timer|pi:{failed_instance.id}|q:overdue",
        job_type=SchedulerJobType.intermediate_timer,
        process_instance_id=failed_instance.id,
        run_at=datetime.fromtimestamp(0, UTC),
        updated_at=datetime.fromtimestamp(0, UTC),
        created_at=datetime.fromtimestamp(0, UTC),
    )
    session.commit()

    assert api.run_due_scheduler_jobs(session, now=datetime.fromtimestamp(0, UTC)) == 1
    session.commit()
    assert api.run_due_scheduler_jobs(session, now=datetime.fromtimestamp(2, UTC)) == 2
    session.commit()
    assert api.run_due_scheduler_jobs(session, now=datetime.fromtimestamp(4, UTC)) == 1
    session.commit()
    assert api.run_due_scheduler_jobs(session, now=datetime.fromtimestamp(6, UTC)) == 1
    session.commit()
    assert api.run_due_scheduler_jobs(session, now=datetime.fromtimestamp(8, UTC)) == 1
    session.commit()
    assert api.run_due_scheduler_jobs(session, now=datetime.fromtimestamp(10, UTC)) == 1
    session.commit()

    instances = api.execute_query(
        session,
        api.ListProcessInstancesQuery(tenant_id=tenant.id),
    )
    assert len(instances) == 3
    assert all(
        instance.status == api.ProcessInstanceStatus.user_input_required
        for instance in instances
    )
    assert failed_instance.status == api.ProcessInstanceStatus.error
    assert session.get(ProcessInstanceModel, failed_instance.id) is failed_instance
    assert (
        session.scalar(
            select(SchedulerJobModel.id).where(
                SchedulerJobModel.m8f_tenant_id == other_tenant.id,
                SchedulerJobModel.job_type == SchedulerJobType.intermediate_timer,
            )
        )
        is None
    )
    recurring_job = _load_scheduler_job(
        session, tenant_id=tenant.id, job_type=SchedulerJobType.timer_start.value
    )
    assert recurring_job.run_at.timestamp() == 3


def test_due_cadence_poll_keeps_all_execution_job_types_eligible(
    session: Session, monkeypatch
) -> None:
    tenant, user = _seed_timer_actor(session)
    definition = _import_process_definition(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
        bpmn_identifier="timer-start-execution-job-fairness",
        bpmn_name="Timer Start Execution Job Fairness",
        source_bpmn_xml=TIMER_START_CYCLE_BPMN,
    )
    timer_job = _load_scheduler_job(session, tenant_id=tenant.id)
    timer_job.run_at = datetime.fromtimestamp(10, UTC)
    failed_instance = _create_stub_process_instance(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
        process_model_identifier="execution-job-fairness",
    )
    run_at = datetime.fromtimestamp(0, UTC)
    execution_jobs = (
        (
            "timer_start_occurrence|test:fairness",
            SchedulerJobType.timer_start_occurrence,
        ),
        ("intermediate_timer|test:fairness", SchedulerJobType.intermediate_timer),
        ("process_retry|test:fairness", SchedulerJobType.process_retry),
        ("collaboration_message|test:fairness", SchedulerJobType.collaboration_message),
    )
    for job_key, job_type in execution_jobs:
        upsert_scheduler_job(
            session,
            tenant_id=tenant.id,
            job_key=job_key,
            job_type=job_type,
            process_instance_id=(
                failed_instance.id
                if job_type
                in {
                    SchedulerJobType.intermediate_timer,
                    SchedulerJobType.process_retry,
                }
                else None
            ),
            bpmn_process_definition_id=(
                definition.id
                if job_type
                in {
                    SchedulerJobType.timer_start_occurrence,
                    SchedulerJobType.collaboration_message,
                }
                else None
            ),
            run_at=run_at,
            payload_json={},
            updated_at=run_at,
            created_at=run_at,
        )
    session.flush()

    processed_types: list[str] = []

    def process_and_delete(session, *, job, occurred_at):
        processed_types.append(job.job_type)
        delete_scheduler_job(
            session,
            tenant_id=job.m8f_tenant_id,
            job_key=job.job_key,
        )
        return True

    monkeypatch.setattr(
        scheduler_runtime, "_execute_claimed_scheduler_job", process_and_delete
    )

    assert api.run_due_scheduler_jobs(
        session,
        now=datetime.fromtimestamp(10, UTC),
        tenant_id=tenant.id,
    ) == len(execution_jobs)

    assert set(processed_types) == {job_type.value for _, job_type in execution_jobs}
    assert timer_job.job_type == SchedulerJobType.timer_start.value
    assert timer_job.run_at.timestamp() == 10


def test_repeated_execution_failure_cannot_starve_overdue_timer_cadence(
    session: Session, monkeypatch
) -> None:
    tenant, user = _seed_timer_actor(session)
    _import_process_definition(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
        bpmn_identifier="timer-start-failing-retry-fairness",
        bpmn_name="Timer Start Failing Retry Fairness",
        source_bpmn_xml=TIMER_START_CYCLE_BPMN,
    )
    timer_job = _load_scheduler_job(session, tenant_id=tenant.id)
    timer_job.run_at = datetime.fromtimestamp(0, UTC)
    timer_job.payload_json = {
        "scheduled_from": "test",
        "timer_task": {
            "event_definition_type": "TimeCycleEventDefinition",
            "event_value": {
                "cycles": 99,
                "next": "1970-01-01T00:00:00+00:00",
                "duration": 1.0,
            },
            "run_at_in_seconds": 0,
            "task_guid": "timer-start-failing-retry-guid",
            "task_spec_name": "StartEvent_timer_cycle",
            "task_spec_type": "StartEvent",
        },
    }
    failed_instance = _create_stub_process_instance(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
        process_model_identifier="repeatedly-failing-scheduler-retry",
    )
    retry_job = upsert_scheduler_job(
        session,
        tenant_id=tenant.id,
        job_key=f"process_retry|pi:{failed_instance.id}|q:repeat",
        job_type=SchedulerJobType.process_retry,
        process_instance_id=failed_instance.id,
        run_at=datetime.fromtimestamp(-1, UTC),
        payload_json={"requested_by_user_id": user.id},
        updated_at=datetime.fromtimestamp(0, UTC),
        created_at=datetime.fromtimestamp(0, UTC),
    )
    session.commit()

    execute_job = scheduler_runtime._execute_claimed_scheduler_job

    def fail_retry(session, *, job, occurred_at):
        if job.job_type == SchedulerJobType.process_retry.value:
            raise api.ValidationError("retry keeps failing")
        return execute_job(session, job=job, occurred_at=occurred_at)

    monkeypatch.setattr(scheduler_runtime, "_execute_claimed_scheduler_job", fail_retry)

    with pytest.raises(api.ValidationError, match="retry keeps failing"):
        api.run_due_scheduler_jobs(
            session, now=datetime.fromtimestamp(2, UTC), tenant_id=tenant.id
        )
    session.commit()
    session.expire_all()
    retry_job = session.get(SchedulerJobModel, retry_job.id)
    assert retry_job is not None
    assert retry_job.run_at == datetime.fromtimestamp(3, UTC)
    assert retry_job.locked_by is None

    assert (
        api.run_due_scheduler_jobs(
            session, now=datetime.fromtimestamp(4, UTC), tenant_id=tenant.id
        )
        == 1
    )
    session.commit()
    for now_in_seconds in (6, 10, 14):
        with pytest.raises(api.ValidationError, match="retry keeps failing"):
            api.run_due_scheduler_jobs(
                session,
                now=datetime.fromtimestamp(now_in_seconds, UTC),
                tenant_id=tenant.id,
            )
        session.commit()
        if now_in_seconds < 14:
            assert (
                api.run_due_scheduler_jobs(
                    session,
                    now=datetime.fromtimestamp(now_in_seconds + 2, UTC),
                    tenant_id=tenant.id,
                )
                == 1
            )
            session.commit()

    session.expire_all()
    recurring_job = _load_scheduler_job(
        session, tenant_id=tenant.id, job_type=SchedulerJobType.timer_start.value
    )
    assert recurring_job.run_at == datetime.fromtimestamp(3, UTC)
    retry_job = session.get(SchedulerJobModel, retry_job.id)
    assert retry_job is not None
    assert retry_job.run_at == datetime.fromtimestamp(15, UTC)
    instances = api.execute_query(
        session,
        api.ListProcessInstancesQuery(tenant_id=tenant.id),
    )
    assert (
        sum(
            instance.status == api.ProcessInstanceStatus.user_input_required
            for instance in instances
        )
        == 3
    )


def test_failed_automatic_timer_task_does_not_block_later_cycles(
    session: Session,
) -> None:
    tenant, user = _seed_timer_actor(session)
    _import_process_definition(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
        bpmn_identifier="timer-start-failing-automatic-task",
        bpmn_name="Timer Start Failing Automatic Task",
        source_bpmn_xml=TIMER_START_CYCLE_FAILING_SERVICE_BPMN,
    )
    timer_job = _load_scheduler_job(session, tenant_id=tenant.id)
    timer_job.run_at = datetime.fromtimestamp(0, UTC)
    timer_job.payload_json = {
        "scheduled_from": "test",
        "timer_task": {
            "event_definition_type": "TimeCycleEventDefinition",
            "event_value": {
                "cycles": 3,
                "next": "1970-01-01T00:00:00+00:00",
                "duration": 300.0,
            },
            "run_at_in_seconds": 0,
            "task_guid": "timer-start-cycle-guid",
            "task_spec_name": "StartEvent_timer_cycle",
            "task_spec_type": "StartEvent",
        },
    }
    session.flush()

    with api.service_task_registry_scope(
        api.ServiceTaskRegistry(connectors=(FailingTimerServiceTaskConnector(),))
    ):
        assert (
            api.run_due_scheduler_jobs(
                session, now=datetime.fromtimestamp(0, UTC), tenant_id=tenant.id
            )
            == 1
        )
        assert (
            api.run_due_scheduler_jobs(
                session, now=datetime.fromtimestamp(0, UTC), tenant_id=tenant.id
            )
            == 1
        )
        first_instances = api.execute_query(
            session,
            api.ListProcessInstancesQuery(tenant_id=tenant.id),
        )
        assert len(first_instances) == 1
        assert first_instances[0].status == api.ProcessInstanceStatus.error

        assert (
            api.run_due_scheduler_jobs(
                session, now=datetime.fromtimestamp(350, UTC), tenant_id=tenant.id
            )
            == 1
        )
        assert (
            api.run_due_scheduler_jobs(
                session, now=datetime.fromtimestamp(350, UTC), tenant_id=tenant.id
            )
            == 1
        )

    process_instances = api.execute_query(
        session,
        api.ListProcessInstancesQuery(tenant_id=tenant.id),
    )
    assert len(process_instances) == 2
    assert all(
        instance.status == api.ProcessInstanceStatus.error
        for instance in process_instances
    )
    assert (
        _load_scheduler_job(
            session,
            tenant_id=tenant.id,
            job_type="timer_start",
        ).run_at.timestamp()
        == 600
    )


def test_failed_automatic_timer_script_is_consumed_and_later_cycles_run(
    session: Session,
) -> None:
    tenant, user = _seed_timer_actor(session)
    _import_process_definition(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
        bpmn_identifier="timer-start-failing-automatic-script",
        bpmn_name="Timer Start Failing Automatic Script",
        source_bpmn_xml=TIMER_START_CYCLE_FAILING_SCRIPT_BPMN,
    )
    timer_job = _load_scheduler_job(session, tenant_id=tenant.id)
    timer_job.run_at = datetime.fromtimestamp(0, UTC)
    timer_job.payload_json = {
        "scheduled_from": "test",
        "timer_task": {
            "event_definition_type": "TimeCycleEventDefinition",
            "event_value": {
                "cycles": 3,
                "next": "1970-01-01T00:00:00+00:00",
                "duration": 300.0,
            },
            "run_at_in_seconds": 0,
            "task_guid": "timer-start-cycle-guid",
            "task_spec_name": "StartEvent_timer_cycle",
            "task_spec_type": "StartEvent",
        },
    }
    session.flush()

    assert (
        api.run_due_scheduler_jobs(
            session, now=datetime.fromtimestamp(0, UTC), tenant_id=tenant.id
        )
        == 1
    )
    assert (
        api.run_due_scheduler_jobs(
            session, now=datetime.fromtimestamp(0, UTC), tenant_id=tenant.id
        )
        == 1
    )
    first_instances = api.execute_query(
        session,
        api.ListProcessInstancesQuery(tenant_id=tenant.id),
    )
    assert len(first_instances) == 1
    assert first_instances[0].status == api.ProcessInstanceStatus.error
    failed_instance = session.scalar(
        select(ProcessInstanceModel).where(
            ProcessInstanceModel.m8f_tenant_id == tenant.id,
            ProcessInstanceModel.id == first_instances[0].id,
        )
    )
    assert failed_instance is not None
    assert failed_instance.workflow_state_json is not None
    assert (
        session.scalar(
            select(SchedulerJobModel.id).where(
                SchedulerJobModel.m8f_tenant_id == tenant.id,
                SchedulerJobModel.job_type == "timer_start_occurrence",
            )
        )
        is None
    )

    assert (
        api.run_due_scheduler_jobs(
            session, now=datetime.fromtimestamp(350, UTC), tenant_id=tenant.id
        )
        == 1
    )
    assert (
        api.run_due_scheduler_jobs(
            session, now=datetime.fromtimestamp(350, UTC), tenant_id=tenant.id
        )
        == 1
    )

    process_instances = api.execute_query(
        session,
        api.ListProcessInstancesQuery(tenant_id=tenant.id),
    )
    assert len(process_instances) == 2
    assert all(
        instance.status == api.ProcessInstanceStatus.error
        for instance in process_instances
    )
    assert (
        _load_scheduler_job(
            session,
            tenant_id=tenant.id,
            job_type="timer_start",
        ).run_at.timestamp()
        == 600
    )
    assert (
        session.scalar(
            select(SchedulerJobModel.id).where(
                SchedulerJobModel.m8f_tenant_id == tenant.id,
                SchedulerJobModel.job_type == "timer_start_occurrence",
            )
        )
        is None
    )


def test_timer_start_occurrence_claim_and_poll_retry_is_idempotent(
    session: Session,
) -> None:
    tenant, user = _seed_timer_actor(session)
    _import_process_definition(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
        bpmn_identifier="timer-start-poll-retry",
        bpmn_name="Timer Start Poll Retry",
        source_bpmn_xml=TIMER_START_BPMN,
    )
    timer_job = _load_scheduler_job(session, tenant_id=tenant.id)
    timer_job.run_at = datetime.fromtimestamp(0, UTC)
    session.flush()

    first_claim = scheduler_runtime.claim_due_scheduler_jobs(
        session,
        now=datetime.fromtimestamp(0, UTC),
        worker_id="timer-worker-one",
        tenant_id=tenant.id,
    )
    competing_claim = scheduler_runtime.claim_due_scheduler_jobs(
        session,
        now=datetime.fromtimestamp(0, UTC),
        worker_id="timer-worker-two",
        tenant_id=tenant.id,
    )
    assert len(first_claim) == 1
    assert competing_claim == []

    timer_job.locked_by = None
    timer_job.locked_at = None
    session.flush()
    assert (
        api.run_due_scheduler_jobs(
            session, now=datetime.fromtimestamp(0, UTC), tenant_id=tenant.id
        )
        == 1
    )

    occurrence_attempt = session.begin_nested()
    assert (
        api.run_due_scheduler_jobs(
            session, now=datetime.fromtimestamp(0, UTC), tenant_id=tenant.id
        )
        == 1
    )
    occurrence_attempt.rollback()
    session.expunge_all()
    assert (
        api.execute_query(
            session,
            api.ListProcessInstancesQuery(tenant_id=tenant.id),
        )
        == []
    )
    assert (
        session.scalar(
            select(SchedulerJobModel.id).where(
                SchedulerJobModel.m8f_tenant_id == tenant.id,
                SchedulerJobModel.job_type == "timer_start_occurrence",
            )
        )
        is not None
    )

    assert (
        api.run_due_scheduler_jobs(
            session, now=datetime.fromtimestamp(0, UTC), tenant_id=tenant.id
        )
        == 1
    )
    assert (
        api.run_due_scheduler_jobs(
            session, now=datetime.fromtimestamp(0, UTC), tenant_id=tenant.id
        )
        == 0
    )
    assert (
        len(
            api.execute_query(
                session,
                api.ListProcessInstancesQuery(tenant_id=tenant.id),
            )
        )
        == 1
    )


def test_due_occurrence_is_claimed_separately_from_due_timer_start(
    session: Session,
) -> None:
    tenant, user = _seed_timer_actor(session)
    definition = _import_process_definition(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
        bpmn_identifier="timer-start-separate-cadence-claim",
        bpmn_name="Timer Start Separate Cadence Claim",
        source_bpmn_xml=TIMER_START_CYCLE_BPMN,
    )
    timer_job = _load_scheduler_job(session, tenant_id=tenant.id)
    timer_job.run_at = datetime.fromtimestamp(300, UTC)
    upsert_scheduler_job(
        session,
        tenant_id=tenant.id,
        job_key="timer_start_occurrence|test:due-at-zero",
        job_type="timer_start_occurrence",
        bpmn_process_definition_id=definition.id,
        run_at=datetime.fromtimestamp(0, UTC),
        payload_json={},
        updated_at=datetime.fromtimestamp(0, UTC),
    )
    session.flush()

    claimed_jobs = scheduler_runtime.claim_due_scheduler_jobs(
        session,
        now=datetime.fromtimestamp(350, UTC),
        worker_id="timer-cadence-worker",
        tenant_id=tenant.id,
    )

    assert [job.job_type for job in claimed_jobs] == ["timer_start_occurrence"]
    occurrence_job = session.scalar(
        select(SchedulerJobModel).where(
            SchedulerJobModel.m8f_tenant_id == tenant.id,
            SchedulerJobModel.job_type == "timer_start_occurrence",
        )
    )
    assert occurrence_job is not None
    assert occurrence_job.locked_by == "timer-cadence-worker"
    assert timer_job.locked_by is None


def test_due_timer_and_occurrence_are_processed_in_separate_polls(
    session: Session,
) -> None:
    tenant, user = _seed_timer_actor(session)
    definition = _import_process_definition(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
        bpmn_identifier="timer-start-due-cadence-and-occurrence",
        bpmn_name="Timer Start Due Cadence and Occurrence",
        source_bpmn_xml=TIMER_START_CYCLE_BPMN,
    )
    timer_task_payload = {
        "event_definition_type": "TimeCycleEventDefinition",
        "event_value": {
            "cycles": 2,
            "next": "1970-01-01T00:05:00+00:00",
            "duration": 300.0,
        },
        "run_at_in_seconds": 300,
        "task_guid": "timer-start-cycle-guid",
        "task_spec_name": "StartEvent_timer_cycle",
        "task_spec_type": "StartEvent",
    }
    timer_job = _load_scheduler_job(session, tenant_id=tenant.id)
    timer_job.run_at = datetime.fromtimestamp(300, UTC)
    timer_job.payload_json = {"timer_task": timer_task_payload}
    upsert_scheduler_job(
        session,
        tenant_id=tenant.id,
        job_key="timer_start_occurrence|test:due-at-zero",
        job_type="timer_start_occurrence",
        bpmn_process_definition_id=definition.id,
        run_at=datetime.fromtimestamp(0, UTC),
        payload_json={
            "occurrence_at": "1970-01-01T00:00:00+00:00",
            "timer_task": {
                **timer_task_payload,
                "event_value": {
                    "cycles": 3,
                    "next": "1970-01-01T00:00:00+00:00",
                    "duration": 300.0,
                },
                "run_at_in_seconds": 0,
            },
        },
        updated_at=datetime.fromtimestamp(0, UTC),
    )
    session.flush()

    assert (
        api.run_due_scheduler_jobs(
            session,
            now=datetime.fromtimestamp(350, UTC),
            tenant_id=tenant.id,
        )
        == 1
    )

    process_instances = api.execute_query(
        session,
        api.ListProcessInstancesQuery(tenant_id=tenant.id),
    )
    assert len(process_instances) == 1
    assert process_instances[0].status == api.ProcessInstanceStatus.user_input_required
    assert (
        _load_scheduler_job(
            session,
            tenant_id=tenant.id,
            job_type="timer_start",
        ).run_at.timestamp()
        == 300
    )
    assert (
        session.scalar(
            select(SchedulerJobModel.id).where(
                SchedulerJobModel.m8f_tenant_id == tenant.id,
                SchedulerJobModel.job_type == "timer_start_occurrence",
            )
        )
        is None
    )

    assert (
        api.run_due_scheduler_jobs(
            session,
            now=datetime.fromtimestamp(350, UTC),
            tenant_id=tenant.id,
        )
        == 1
    )

    assert (
        _load_scheduler_job(
            session,
            tenant_id=tenant.id,
            job_type="timer_start",
        ).run_at.timestamp()
        == 600
    )
    occurrence_jobs = list(
        session.scalars(
            select(SchedulerJobModel)
            .where(
                SchedulerJobModel.m8f_tenant_id == tenant.id,
                SchedulerJobModel.job_type == "timer_start_occurrence",
            )
            .order_by(SchedulerJobModel.run_at)
        ).all()
    )
    assert [job.run_at.timestamp() for job in occurrence_jobs] == [300]
    assert all(job.locked_by is None for job in occurrence_jobs)


def test_run_due_scheduler_jobs_continues_batch_after_job_error(
    session: Session,
    monkeypatch,
) -> None:
    tenant, user = _seed_timer_actor(session)
    first_process_instance = _create_stub_process_instance(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
        process_model_identifier="scheduler-runtime-first",
    )
    first_job_key = f"process_retry|pi:{first_process_instance.id}|q:first"
    second_process_instance = _create_stub_process_instance(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
        process_model_identifier="scheduler-runtime-second",
    )
    second_job_key = f"process_retry|pi:{second_process_instance.id}|q:second"
    first_job = upsert_scheduler_job(
        session,
        tenant_id=tenant.id,
        job_key=first_job_key,
        job_type="process_retry",
        process_instance_id=first_process_instance.id,
        run_at=datetime.fromtimestamp(10, UTC),
        payload_json={"requested_by_user_id": user.id},
        updated_at=datetime.fromtimestamp(10, UTC),
        created_at=datetime.fromtimestamp(10, UTC),
    )
    second_job = upsert_scheduler_job(
        session,
        tenant_id=tenant.id,
        job_key=second_job_key,
        job_type="process_retry",
        process_instance_id=second_process_instance.id,
        run_at=datetime.fromtimestamp(10, UTC),
        payload_json={"requested_by_user_id": user.id},
        updated_at=datetime.fromtimestamp(10, UTC),
        created_at=datetime.fromtimestamp(10, UTC),
    )

    executed_job_keys: list[str] = []

    def _execute_with_first_failure(session, *, job, occurred_at) -> None:
        executed_job_keys.append(job.job_key)
        if job.id == first_job.id:
            raise api.ValidationError("boom")
        delete_scheduler_job(
            session,
            tenant_id=job.m8f_tenant_id,
            job_key=job.job_key,
        )

    monkeypatch.setattr(
        scheduler_runtime,
        "_execute_claimed_scheduler_job",
        _execute_with_first_failure,
    )

    with pytest.raises(api.ValidationError, match="boom"):
        api.run_due_scheduler_jobs(
            session,
            now=datetime.fromtimestamp(10, UTC),
            worker_id="inline-failure-worker",
            tenant_id=tenant.id,
        )

    session.expire_all()
    first_job = session.get(SchedulerJobModel, first_job.id)
    second_job = session.get(SchedulerJobModel, second_job.id)
    assert first_job is not None
    assert second_job is None
    assert executed_job_keys == [first_job_key, second_job_key]
    assert first_job.locked_by is None
    assert first_job.locked_at is None


def test_run_due_scheduler_jobs_raises_summary_for_multiple_job_errors(
    session: Session,
    monkeypatch,
) -> None:
    tenant, user = _seed_timer_actor(session)
    first_process_instance = _create_stub_process_instance(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
        process_model_identifier="scheduler-runtime-summary-first",
    )
    first_job_key = f"process_retry|pi:{first_process_instance.id}|q:first"
    second_process_instance = _create_stub_process_instance(
        session,
        tenant_id=tenant.id,
        user_id=user.id,
        process_model_identifier="scheduler-runtime-summary-second",
    )
    second_job_key = f"process_retry|pi:{second_process_instance.id}|q:second"
    first_job = upsert_scheduler_job(
        session,
        tenant_id=tenant.id,
        job_key=first_job_key,
        job_type="process_retry",
        process_instance_id=first_process_instance.id,
        run_at=datetime.fromtimestamp(10, UTC),
        payload_json={"requested_by_user_id": user.id},
        updated_at=datetime.fromtimestamp(10, UTC),
        created_at=datetime.fromtimestamp(10, UTC),
    )
    second_job = upsert_scheduler_job(
        session,
        tenant_id=tenant.id,
        job_key=second_job_key,
        job_type="process_retry",
        process_instance_id=second_process_instance.id,
        run_at=datetime.fromtimestamp(10, UTC),
        payload_json={"requested_by_user_id": user.id},
        updated_at=datetime.fromtimestamp(10, UTC),
        created_at=datetime.fromtimestamp(10, UTC),
    )

    executed_job_keys: list[str] = []

    def _fail_both_jobs(session, *, job, occurred_at) -> None:
        executed_job_keys.append(job.job_key)
        if job.id == first_job.id:
            raise api.ValidationError("first failure")
        if job.id == second_job.id:
            raise api.NotFoundError("second failure")

    monkeypatch.setattr(
        scheduler_runtime,
        "_execute_claimed_scheduler_job",
        _fail_both_jobs,
    )

    with pytest.raises(api.BpmnCoreError) as exc_info:
        api.run_due_scheduler_jobs(
            session,
            now=datetime.fromtimestamp(10, UTC),
            worker_id="inline-failure-worker",
            tenant_id=tenant.id,
        )

    session.expire_all()
    first_job = session.get(SchedulerJobModel, first_job.id)
    second_job = session.get(SchedulerJobModel, second_job.id)
    assert first_job is not None
    assert second_job is not None
    assert executed_job_keys == [first_job_key, second_job_key]
    assert first_job.locked_by is None
    assert first_job.locked_at is None
    assert second_job.locked_by is None
    assert second_job.locked_at is None
    assert (
        str(exc_info.value) == "2 scheduler jobs failed in one batch: "
        f"{first_job_key}: ValidationError: first failure, "
        f"{second_job_key}: NotFoundError: second failure"
    )
    assert isinstance(exc_info.value.__cause__, api.ValidationError)


def _seed_timer_actor(
    session: Session,
) -> tuple[M8flowTenantModel, UserModel]:
    tenant = M8flowTenantModel(
        id="tenant-scheduler-runtime",
        name="Tenant Scheduler Runtime",
        slug="tenant-scheduler-runtime",
    )
    user = UserModel(
        username="timer-runtime-admin",
        email="timer-runtime-admin@example.com",
        service="http://localhost:7002/realms/tenant-scheduler-runtime",
        service_id="timer-runtime-admin-keycloak",
        display_name="Timer Runtime Admin",
        created_at=datetime.fromtimestamp(1, UTC),
        updated_at=datetime.fromtimestamp(1, UTC),
    )
    session.add_all([tenant, user])
    session.flush()
    ensure_v1_role(
        session,
        tenant_id=tenant.id,
        role_name=ROLE_ADMIN,
        user_ids=[user.id],
    )
    return tenant, user


def _create_stub_process_instance(
    session: Session,
    *,
    tenant_id: str,
    user_id: int,
    process_model_identifier: str,
) -> ProcessInstanceModel:
    process_instance = ProcessInstanceModel(
        m8f_tenant_id=tenant_id,
        process_model_identifier=process_model_identifier,
        process_model_display_name=process_model_identifier,
        process_initiator_id=user_id,
        status=api.ProcessInstanceStatus.error.value,
        started_at=datetime.fromtimestamp(10, UTC),
        ended_at=datetime.fromtimestamp(10, UTC),
        task_updated_at=datetime.fromtimestamp(10, UTC),
        created_at=datetime.fromtimestamp(10, UTC),
        updated_at=datetime.fromtimestamp(10, UTC),
    )
    session.add(process_instance)
    session.flush()
    return process_instance


def _initialize_waiting_timer_process(
    session: Session,
    *,
    tenant_id: str,
    user_id: int,
):
    definition = api.execute_command(
        session,
        api.ImportBpmnProcessDefinitionCommand(
            tenant_id=tenant_id,
            bpmn_identifier="timer-runtime-process",
            user_id=user_id,
            bpmn_name="Timer Runtime Process",
            source_bpmn_xml=INTERMEDIATE_TIMER_BPMN,
            created_at=datetime.fromtimestamp(10, UTC),
            updated_at=datetime.fromtimestamp(10, UTC),
        ),
    )
    return api.execute_command(
        session,
        api.InitializeProcessInstanceFromDefinitionCommand(
            tenant_id=tenant_id,
            bpmn_process_definition_id=definition.id,
            process_initiator_id=user_id,
            started_at=datetime.fromtimestamp(20, UTC),
        ),
    )


def _import_process_definition(
    session: Session,
    *,
    tenant_id: str,
    user_id: int,
    bpmn_identifier: str,
    bpmn_name: str,
    source_bpmn_xml: str,
    properties_json: dict[str, object] | None = None,
) -> BpmnProcessDefinitionModel:
    return api.execute_command(
        session,
        api.ImportBpmnProcessDefinitionCommand(
            tenant_id=tenant_id,
            bpmn_identifier=bpmn_identifier,
            user_id=user_id,
            bpmn_name=bpmn_name,
            source_bpmn_xml=source_bpmn_xml,
            properties_json=properties_json,
            created_at=datetime.fromtimestamp(10, UTC),
            updated_at=datetime.fromtimestamp(10, UTC),
        ),
    )


def _load_scheduler_job(
    session: Session,
    *,
    tenant_id: str,
    job_type: str | None = None,
) -> SchedulerJobModel:
    stmt = select(SchedulerJobModel).where(SchedulerJobModel.m8f_tenant_id == tenant_id)
    if job_type is not None:
        stmt = stmt.where(SchedulerJobModel.job_type == job_type)
    scheduler_job = session.scalar(stmt)
    assert scheduler_job is not None
    return scheduler_job


def _set_waiting_timer_event_value(
    session: Session,
    *,
    process_instance,
    task_guid: str,
    event_value: str,
) -> None:
    if process_instance.bpmn_process is None:
        raise AssertionError("Process instance is missing its BPMN process")

    json_data = session.get(
        JsonDataModel,
        (
            process_instance.bpmn_process.m8f_tenant_id,
            process_instance.bpmn_process.json_data_hash,
        ),
    )
    assert json_data is not None
    payload = dict(json_data.data)
    serialized_workflow = json.loads(payload[WORKFLOW_STATE_JSON_DATA_KEY])
    # Event subprocess tasks live in their subprocess, not the top level.
    tasks = next(
        workflow["tasks"]
        for workflow in [
            serialized_workflow,
            *serialized_workflow.get("subprocesses", {}).values(),
        ]
        if task_guid in workflow["tasks"]
    )
    tasks[task_guid]["internal_data"]["event_value"] = event_value
    payload[WORKFLOW_STATE_JSON_DATA_KEY] = json.dumps(serialized_workflow)
    process_instance.bpmn_process.json_data_hash = (
        JsonDataModel.create_or_update_from_payload(
            session, process_instance.m8f_tenant_id, payload
        )
    )
    session.flush()


def _seed_errored_retry_process(
    session: Session,
) -> tuple[M8flowTenantModel, UserModel, ProcessInstanceModel, WorkItemModel]:
    tenant = M8flowTenantModel(
        id="tenant-scheduler-retry-runtime",
        name="Tenant Scheduler Retry Runtime",
        slug="tenant-scheduler-retry-runtime",
    )
    user = UserModel(
        username="retry-runtime-admin",
        email="retry-runtime-admin@example.com",
        service="http://localhost:7002/realms/tenant-scheduler-retry-runtime",
        service_id="retry-runtime-admin-keycloak",
        display_name="Retry Runtime Admin",
        created_at=datetime.fromtimestamp(1, UTC),
        updated_at=datetime.fromtimestamp(1, UTC),
    )
    session.add_all([tenant, user])
    session.flush()
    ensure_v1_role(
        session,
        tenant_id=tenant.id,
        role_name=ROLE_ADMIN,
        user_ids=[user.id],
    )

    definition = BpmnProcessDefinitionModel(
        m8f_tenant_id=tenant.id,
        process_xml_digest="test-definition-digest",
        bpmn_identifier="retry-runtime-process",
        bpmn_name="Retry Runtime Process",
        properties_json={},
        created_at=datetime.fromtimestamp(10, UTC),
        updated_at=datetime.fromtimestamp(10, UTC),
    )
    session.add(definition)
    session.flush()

    bpmn_process = BpmnProcessModel(
        m8f_tenant_id=tenant.id,
        guid="retry-runtime-process-guid",
        bpmn_process_definition_id=definition.id,
        top_level_process_id=None,
        direct_parent_process_id=None,
        properties_json={"root": "retry-runtime-root"},
        json_data_hash="retry-runtime-json",
        started_at=datetime.fromtimestamp(20.0, UTC),
        ended_at=datetime.fromtimestamp(130.0, UTC),
    )
    session.add(bpmn_process)
    session.flush()

    task_definition = TaskDefinitionModel(
        m8f_tenant_id=tenant.id,
        bpmn_process_definition_id=definition.id,
        bpmn_identifier="Task_retry_runtime",
        bpmn_name="Retry Runtime Task",
        typename="UserTask",
        properties_json={"allowGuest": False},
        created_at=datetime.fromtimestamp(30, UTC),
        updated_at=datetime.fromtimestamp(30, UTC),
    )
    session.add(task_definition)
    session.flush()

    process_instance = ProcessInstanceModel(
        m8f_tenant_id=tenant.id,
        process_model_identifier="retry-runtime-process",
        process_model_display_name="retry-runtime-process",
        process_initiator_id=user.id,
        bpmn_process_definition_id=definition.id,
        bpmn_process_id=bpmn_process.id,
        status="error",
        started_at=datetime.fromtimestamp(20, UTC),
        ended_at=datetime.fromtimestamp(130, UTC),
        task_updated_at=datetime.fromtimestamp(130, UTC),
        created_at=datetime.fromtimestamp(20, UTC),
        updated_at=datetime.fromtimestamp(130, UTC),
    )
    session.add(process_instance)
    session.flush()

    task = TaskModel(
        m8f_tenant_id=tenant.id,
        guid="retry-runtime-task-guid",
        bpmn_process_id=bpmn_process.id,
        process_instance_id=process_instance.id,
        task_definition_id=task_definition.id,
        state="TERMINATED",
        properties_json={"task_spec": "Retry Runtime Task"},
        json_data_hash="retry-runtime-task-json",
        python_env_data_hash="retry-runtime-task-env",
        started_at=datetime.fromtimestamp(20.0, UTC),
        ended_at=datetime.fromtimestamp(130.0, UTC),
    )
    session.add(task)
    session.flush()

    future_task = FutureTaskModel(
        m8f_tenant_id=tenant.id,
        guid=task.guid,
        run_at=datetime.fromtimestamp(130, UTC),
        queued_to_run_at=datetime.fromtimestamp(130, UTC),
        completed=True,
        archived_for_process_instance_status=True,
        updated_at=datetime.fromtimestamp(130, UTC),
    )
    session.add(future_task)
    session.flush()

    human_task = WorkItemModel(
        m8f_tenant_id=tenant.id,
        process_instance_id=process_instance.id,
        task_guid=task.guid,
        lane_assignment_id=None,
        completed_by_user_id=user.id,
        actual_owner_id=user.id,
        task_status="TERMINATED",
        completed=True,
        updated_at=datetime.fromtimestamp(130, UTC),
        created_at=datetime.fromtimestamp(20, UTC),
    )
    session.add(human_task)
    session.flush()

    session.add(
        WorkItemUserModel(
            m8f_tenant_id=tenant.id,
            work_item_id=human_task.id,
            user_id=user.id,
            added_by=None,
        )
    )
    session.flush()

    api.record_process_instance_event(
        session,
        tenant_id=tenant.id,
        process_instance_id=process_instance.id,
        event_type=api.ProcessLifecycleEventType.process_instance_error,
        occurred_at=datetime.fromtimestamp(130.0, UTC),
        user_id=user.id,
    )
    session.flush()

    return tenant, user, process_instance, human_task
