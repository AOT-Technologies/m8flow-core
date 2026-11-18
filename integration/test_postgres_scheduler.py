from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from threading import Event, Lock
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from m8flow_core import api
from m8flow_core.models.bpmn_process_definition import BpmnProcessDefinitionModel
from m8flow_core.models.process_instance import ProcessInstanceStatus
from m8flow_core.models.scheduler_job import SchedulerJobModel
from m8flow_core.models.tenant import M8flowTenantModel
from m8flow_core.models.user import UserModel
from m8flow_core.services import scheduler_runtime
from m8flow_core.services.authorization import ROLE_ADMIN, ensure_v1_role
from m8flow_core.services.collaborations import lock_collaboration_definition

_CYCLE_BPMN_PATH = (
    Path(__file__).resolve().parents[1]
    / "tests"
    / "fixtures"
    / "scheduled_cycle_timer_poc.bpmn"
)
_COLLABORATION_BPMN_PATH = (
    Path(__file__).resolve().parents[1]
    / "tests"
    / "fixtures"
    / "order-fulfilment-collaboration.bpmn"
)
_EPOCH = datetime.fromtimestamp(0, UTC)


def test_postgres_overlapping_workers_advance_timer_cadence_once(
    postgres_engine: Engine,
    monkeypatch,
) -> None:
    tenant_id = f"postgres-timer-{uuid4().hex}"
    with Session(bind=postgres_engine) as session:
        tenant = M8flowTenantModel(
            id=tenant_id,
            name="Postgres Timer Test",
            slug=tenant_id,
        )
        user = UserModel(
            username=f"timer-admin-{uuid4().hex}",
            email=f"timer-admin-{uuid4().hex}@example.com",
            service=f"http://localhost:7002/realms/{tenant_id}",
            service_id=f"timer-admin-{uuid4().hex}",
            display_name="Timer Admin",
            created_at=_EPOCH,
            updated_at=_EPOCH,
        )
        session.add_all([tenant, user])
        session.flush()
        ensure_v1_role(
            session,
            tenant_id=tenant.id,
            role_name=ROLE_ADMIN,
            user_ids=[user.id],
        )
        bpmn_xml = _CYCLE_BPMN_PATH.read_text(encoding="utf-8").replace(
            "__TIMER_CYCLE__", "R3/PT5M"
        )
        api.execute_command(
            session,
            api.ImportBpmnProcessDefinitionCommand(
                tenant_id=tenant.id,
                bpmn_identifier="postgres-recurring-timer",
                user_id=user.id,
                bpmn_name="Postgres Recurring Timer",
                source_bpmn_xml=bpmn_xml,
                created_at=_EPOCH,
                updated_at=_EPOCH,
            ),
        )
        timer_job = session.scalar(
            select(SchedulerJobModel).where(
                SchedulerJobModel.m8f_tenant_id == tenant.id,
                SchedulerJobModel.job_type == "timer_start",
            )
        )
        assert timer_job is not None
        timer_job.run_at = _EPOCH
        timer_job.payload_json = {
            "scheduled_from": "postgres_timer_test",
            "timer_task": {
                "event_definition_type": "TimeCycleEventDefinition",
                "event_value": {
                    "cycles": 3,
                    "next": _EPOCH.isoformat(),
                    "duration": 300.0,
                },
                "run_at_in_seconds": 0,
                "task_guid": "postgres-timer-start-guid",
                "task_spec_name": "StartEvent_timer_cycle",
                "task_spec_type": "StartEvent",
            },
        }
        session.commit()

    with Session(bind=postgres_engine) as session:
        assert api.run_due_scheduler_jobs(
            session,
            now=_EPOCH,
            tenant_id=tenant_id,
            worker_id="timer-scheduler",
        ) == 1
        session.commit()

    original_initialize = (
        scheduler_runtime._initialize_process_instance_from_timer_start_definition
    )
    initialization_entered = Event()
    release_initialization = Event()
    initialized_once = False
    initialization_guard = Lock()

    def hold_first_initialization(session: Session, **kwargs):
        nonlocal initialized_once
        with initialization_guard:
            should_hold = not initialized_once
            initialized_once = True
        if should_hold:
            initialization_entered.set()
            if not release_initialization.wait(timeout=20):
                raise TimeoutError("Timed out waiting to release timer startup")
        return original_initialize(session, **kwargs)

    monkeypatch.setattr(
        scheduler_runtime,
        "_initialize_process_instance_from_timer_start_definition",
        hold_first_initialization,
    )

    def run_worker(worker_id: str, now: datetime) -> int:
        with Session(bind=postgres_engine) as session:
            processed = api.run_due_scheduler_jobs(
                session,
                now=now,
                tenant_id=tenant_id,
                worker_id=worker_id,
            )
            session.commit()
            return processed

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_worker = executor.submit(
            run_worker, "timer-worker-one", datetime.fromtimestamp(350, UTC)
        )
        assert initialization_entered.wait(timeout=10)
        try:
            second_worker = executor.submit(
                run_worker,
                "timer-worker-two",
                datetime.fromtimestamp(650, UTC),
            )
            assert second_worker.result(timeout=10) == 1
            with Session(bind=postgres_engine) as session:
                assert api.execute_query(
                    session,
                    api.ListProcessInstancesQuery(tenant_id=tenant_id),
                ) == []
                next_schedule = session.scalar(
                    select(SchedulerJobModel).where(
                        SchedulerJobModel.m8f_tenant_id == tenant_id,
                        SchedulerJobModel.job_type == "timer_start",
                    )
                )
                assert next_schedule is not None
                assert next_schedule.run_at.timestamp() == 600
        finally:
            release_initialization.set()
        assert first_worker.result(timeout=30) == 1

    assert run_worker(
        "timer-worker-three", datetime.fromtimestamp(650, UTC)
    ) == 1
    assert run_worker(
        "timer-worker-four", datetime.fromtimestamp(650, UTC)
    ) == 1
    assert run_worker(
        "timer-worker-five", datetime.fromtimestamp(650, UTC)
    ) == 1
    with Session(bind=postgres_engine) as session:
        process_instances = api.execute_query(
            session,
            api.ListProcessInstancesQuery(tenant_id=tenant_id),
        )
        assert len(process_instances) == 3
        assert all(
            process_instance.status
            == ProcessInstanceStatus.user_input_required.value
            for process_instance in process_instances
        )
        next_schedule = session.scalar(
            select(SchedulerJobModel).where(
                SchedulerJobModel.m8f_tenant_id == tenant_id,
                SchedulerJobModel.job_type == "timer_start",
            )
        )
        assert next_schedule is None


def test_postgres_collaboration_timer_cadence_advances_while_start_is_blocked(
    postgres_engine: Engine, monkeypatch
) -> None:
    tenant_id = f"postgres-collaboration-timer-{uuid4().hex}"
    with Session(bind=postgres_engine) as session:
        tenant = M8flowTenantModel(
            id=tenant_id,
            name="Postgres Collaboration Timer Test",
            slug=tenant_id,
        )
        user = UserModel(
            username=f"timer-admin-{uuid4().hex}",
            email=f"timer-admin-{uuid4().hex}@example.com",
            service=f"http://localhost:7002/realms/{tenant_id}",
            service_id=f"timer-admin-{uuid4().hex}",
            display_name="Timer Admin",
            created_at=_EPOCH,
            updated_at=_EPOCH,
        )
        session.add_all([tenant, user])
        session.flush()
        ensure_v1_role(
            session,
            tenant_id=tenant.id,
            role_name=ROLE_ADMIN,
            user_ids=[user.id],
        )
        bpmn_xml = _COLLABORATION_BPMN_PATH.read_text(encoding="utf-8").replace(
            '<bpmn:startEvent id="OrderStart"/>',
            '''<bpmn:startEvent id="OrderStart"/>
            <bpmn:startEvent id="OrderTimer">
              <bpmn:timerEventDefinition>
                <bpmn:timeCycle>R3/PT5M</bpmn:timeCycle>
              </bpmn:timerEventDefinition>
            </bpmn:startEvent>
            <bpmn:sequenceFlow id="OrderTimerFlow" sourceRef="OrderTimer"
                               targetRef="OrderEnd"/>''',
        )
        api.execute_command(
            session,
            api.ImportBpmnProcessDefinitionCommand(
                tenant_id=tenant.id,
                bpmn_identifier="postgres-collaboration-timer",
                user_id=user.id,
                bpmn_name="Postgres Collaboration Timer",
                source_bpmn_xml=bpmn_xml,
                created_at=_EPOCH,
                updated_at=_EPOCH,
            ),
        )
        timer_job = session.scalar(
            select(SchedulerJobModel).where(
                SchedulerJobModel.m8f_tenant_id == tenant.id,
                SchedulerJobModel.job_type == "timer_start",
            )
        )
        assert timer_job is not None
        timer_job.run_at = _EPOCH
        timer_job.payload_json = {
            "scheduled_from": "postgres_collaboration_timer_test",
            "timer_task": {
                "event_definition_type": "TimeCycleEventDefinition",
                "event_value": {
                    "cycles": 3,
                    "next": _EPOCH.isoformat(),
                    "duration": 300.0,
                },
                "run_at_in_seconds": 0,
                "task_guid": "postgres-collaboration-timer-guid",
                "task_spec_name": "OrderTimer",
                "task_spec_type": "StartEvent",
                "bpmn_process_id": "Order",
            },
        }
        session.commit()

    with Session(bind=postgres_engine) as session:
        assert api.run_due_scheduler_jobs(
            session,
            now=datetime.fromtimestamp(50, UTC),
            tenant_id=tenant_id,
            worker_id="collaboration-cadence-seed",
        ) == 1
        session.commit()

    from m8flow_core.services import workflow_runtime

    participant_start = workflow_runtime._start_collaboration_participant
    start_entered = Event()
    release_start = Event()

    def hold_collaboration_start(*args, **kwargs):
        start_entered.set()
        if not release_start.wait(timeout=20):
            raise TimeoutError("Timed out waiting to release collaboration timer start")
        return participant_start(*args, **kwargs)

    monkeypatch.setattr(
        workflow_runtime, "_start_collaboration_participant", hold_collaboration_start
    )

    def run_worker(worker_id: str, now: datetime) -> int:
        with Session(bind=postgres_engine) as session:
            processed = api.run_due_scheduler_jobs(
                session,
                now=now,
                tenant_id=tenant_id,
                worker_id=worker_id,
            )
            session.commit()
            return processed

    with ThreadPoolExecutor(max_workers=2) as executor:
        occurrence_worker = executor.submit(
            run_worker, "collaboration-occurrence", datetime.fromtimestamp(50, UTC)
        )
        assert start_entered.wait(timeout=10)
        try:
            cadence_worker = executor.submit(
                run_worker,
                "collaboration-cadence",
                datetime.fromtimestamp(350, UTC),
            )
            assert cadence_worker.result(timeout=10) == 1
        finally:
            release_start.set()
        assert occurrence_worker.result(timeout=30) == 1

    with Session(bind=postgres_engine) as session:
        due_occurrences = list(
            session.scalars(
                select(SchedulerJobModel)
                .where(
                    SchedulerJobModel.m8f_tenant_id == tenant_id,
                    SchedulerJobModel.job_type == "timer_start_occurrence",
                )
                .order_by(SchedulerJobModel.run_at)
            ).all()
        )
        assert [job.run_at.timestamp() for job in due_occurrences] == [300]
        next_schedule = session.scalar(
            select(SchedulerJobModel).where(
                SchedulerJobModel.m8f_tenant_id == tenant_id,
                SchedulerJobModel.job_type == "timer_start",
            )
        )
        assert next_schedule is not None
        assert next_schedule.run_at.timestamp() == 600


def test_postgres_collaboration_definition_lock_still_serializes_transitions(
    postgres_engine: Engine,
) -> None:
    tenant_id = f"postgres-collaboration-lock-{uuid4().hex}"
    with Session(bind=postgres_engine) as session:
        tenant = M8flowTenantModel(
            id=tenant_id,
            name="Postgres Collaboration Lock Test",
            slug=tenant_id,
        )
        user = UserModel(
            username=f"collaboration-admin-{uuid4().hex}",
            email=f"collaboration-admin-{uuid4().hex}@example.com",
            service=f"http://localhost:7002/realms/{tenant_id}",
            service_id=f"collaboration-admin-{uuid4().hex}",
            display_name="Collaboration Admin",
            created_at=_EPOCH,
            updated_at=_EPOCH,
        )
        session.add_all([tenant, user])
        session.flush()
        ensure_v1_role(
            session,
            tenant_id=tenant.id,
            role_name=ROLE_ADMIN,
            user_ids=[user.id],
        )
        api.execute_command(
            session,
            api.ImportBpmnProcessDefinitionCommand(
                tenant_id=tenant.id,
                bpmn_identifier="postgres-collaboration-lock",
                user_id=user.id,
                bpmn_name="Postgres Collaboration Lock",
                source_bpmn_xml=_COLLABORATION_BPMN_PATH.read_text(
                    encoding="utf-8"
                ),
                created_at=_EPOCH,
                updated_at=_EPOCH,
            ),
        )
        definition_id = session.scalar(
            select(BpmnProcessDefinitionModel.id).where(
                BpmnProcessDefinitionModel.m8f_tenant_id == tenant_id
            )
        )
        assert definition_id is not None
        session.commit()

    first_lock_acquired = Event()
    release_first_lock = Event()
    second_lock_attempted = Event()
    second_lock_acquired = Event()

    def hold_first_lock() -> None:
        with Session(bind=postgres_engine) as session:
            definition = session.get(
                BpmnProcessDefinitionModel, definition_id
            )
            assert definition is not None
            lock_collaboration_definition(session, definition)
            first_lock_acquired.set()
            if not release_first_lock.wait(timeout=20):
                raise TimeoutError("Timed out waiting to release collaboration lock")
            session.commit()

    def acquire_second_lock() -> None:
        with Session(bind=postgres_engine) as session:
            definition = session.get(
                BpmnProcessDefinitionModel, definition_id
            )
            assert definition is not None
            second_lock_attempted.set()
            lock_collaboration_definition(session, definition)
            second_lock_acquired.set()
            session.commit()

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_worker = executor.submit(hold_first_lock)
        assert first_lock_acquired.wait(timeout=10)
        second_worker = executor.submit(acquire_second_lock)
        assert second_lock_attempted.wait(timeout=10)
        try:
            assert not second_lock_acquired.wait(timeout=0.5)
        finally:
            release_first_lock.set()
        first_worker.result(timeout=20)
        second_worker.result(timeout=20)
    assert second_lock_acquired.is_set()
