from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from m8flow_core import api
from m8flow_core.models.tenant import M8flowTenantModel
from m8flow_core.models.user import UserModel
from m8flow_core.services.authorization import ROLE_ADMIN, ensure_v1_role

FIXTURES = Path(__file__).with_name("fixtures")


# Each fixture runs a sequential then a parallel script-task loop, and its
# final script raises unless both output collections hold [10, 20, 30]. A
# complete instance proves each loop ran once per instance and mapped its
# outputs (M8F-668).
@pytest.mark.parametrize(
    "fixture",
    [
        # Collection-driven: loopDataInputRef over [1, 2, 3].
        "simple-multi-instance.bpmn",
        # loopCardinality: a literal 3 (sequential) and an expression (parallel);
        # each instance gets its index in inputDataItem.
        "multi-instance-cardinality.bpmn",
    ],
)
def test_sequential_and_parallel_multi_instance_complete(
    session: Session, fixture: str
) -> None:
    tenant = M8flowTenantModel(id="mi-tenant", name="Loops", slug="mi-tenant")
    user = UserModel(
        username="looper",
        email="looper@example.com",
        service="http://localhost/realms/mi-tenant",
        service_id="looper",
        display_name="Looper",
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    session.add_all([tenant, user])
    session.flush()
    ensure_v1_role(
        session, tenant_id=tenant.id, role_name=ROLE_ADMIN, user_ids=[user.id]
    )

    definition = api.execute_command(
        session,
        api.ImportBpmnProcessDefinitionCommand(
            tenant_id=tenant.id,
            bpmn_identifier="multi-instance-model",
            user_id=user.id,
            source_bpmn_xml=(FIXTURES / fixture).read_text(encoding="utf-8"),
        ),
    )
    instance = api.execute_command(
        session,
        api.InitializeProcessInstanceFromDefinitionCommand(
            tenant_id=tenant.id,
            bpmn_process_definition_id=definition.id,
            process_initiator_id=user.id,
        ),
    )

    assert instance.status == api.ProcessInstanceStatus.complete


# --- M8F-640: iteration-local data, runtime counters, output order -----------

_DEFINITIONS = (
    '<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" '
    'id="Defs_m8f640" targetNamespace="http://bpmn.io/schema/bpmn">'
    '<bpmn:process id="Process_m8f640" isExecutable="true">{body}</bpmn:process>'
    "</bpmn:definitions>"
)
# Names that belong to one iteration and must never reach the process scope.
_LOCAL_NAMES = (
    "it",
    "res",
    "loopCounter",
    "nrOfInstances",
    "nrOfActiveInstances",
    "nrOfCompletedInstances",
)
_NOTHING_LEAKED = (
    f"for name in {_LOCAL_NAMES!r}:\n"
    "    if name in locals(): raise Exception(name + ' leaked')"
)


def _loop(
    sequential: bool, *, condition: str | None = None, mapped: bool = True
) -> str:
    inner = "<bpmn:loopDataInputRef>items</bpmn:loopDataInputRef>"
    if mapped:
        inner += "<bpmn:loopDataOutputRef>out</bpmn:loopDataOutputRef>"
    inner += '<bpmn:inputDataItem id="it" name="it"/>'
    if mapped:
        inner += '<bpmn:outputDataItem id="res" name="res"/>'
    if condition is not None:
        inner += f"<bpmn:completionCondition>{condition}</bpmn:completionCondition>"
    flag = ' isSequential="true"' if sequential else ""
    return (
        f"<bpmn:multiInstanceLoopCharacteristics{flag}>{inner}"
        "</bpmn:multiInstanceLoopCharacteristics>"
    )


def _script(task_id: str, script: str, loop: str = "") -> str:
    return (
        f'<bpmn:scriptTask id="{task_id}">{loop}'
        f"<bpmn:script>{script}</bpmn:script></bpmn:scriptTask>"
    )


def _chain(*tasks: str) -> str:
    ids = ["StartEvent_1"]
    ids += [task.split('id="', 1)[1].split('"', 1)[0] for task in tasks]
    ids.append("EndEvent_1")
    flows = "".join(
        f'<bpmn:sequenceFlow id="f{n}" sourceRef="{a}" targetRef="{b}"/>'
        for n, (a, b) in enumerate(zip(ids, ids[1:], strict=False))
    )
    body = (
        '<bpmn:startEvent id="StartEvent_1"/>'
        + "".join(tasks)
        + '<bpmn:endEvent id="EndEvent_1"/>'
        + flows
    )
    return _DEFINITIONS.format(body=body)


def _run(session: Session, xml: str) -> str:
    """Import and start ``xml`` as an admin; return the instance status."""
    tenant = M8flowTenantModel(id="mi-640", name="Loops", slug="mi-640")
    user = UserModel(
        username="looper640",
        email="looper640@example.com",
        service="http://localhost/realms/mi-640",
        service_id="looper640",
        display_name="Looper",
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    session.add_all([tenant, user])
    session.flush()
    ensure_v1_role(
        session, tenant_id=tenant.id, role_name=ROLE_ADMIN, user_ids=[user.id]
    )
    definition = api.execute_command(
        session,
        api.ImportBpmnProcessDefinitionCommand(
            tenant_id=tenant.id,
            bpmn_identifier="m8f640",
            user_id=user.id,
            source_bpmn_xml=xml,
        ),
    )
    instance = api.execute_command(
        session,
        api.InitializeProcessInstanceFromDefinitionCommand(
            tenant_id=tenant.id,
            bpmn_process_definition_id=definition.id,
            process_initiator_id=user.id,
        ),
    )
    return str(instance.status)


_PREP = _script("prep", "items = ['a', 'b', 'c']")


@pytest.mark.parametrize(
    ("sequential", "expected"),
    [
        (True, [[0, 3, 1, 0], [1, 3, 1, 1], [2, 3, 1, 2]]),
        (False, [[0, 3, 3, 0], [1, 3, 2, 1], [2, 3, 1, 2]]),
    ],
    ids=["sequential", "parallel"],
)
def test_each_iteration_sees_its_runtime_counters(
    session: Session, sequential: bool, expected: list[list[int]]
) -> None:
    work = _script(
        "work",
        "res = [loopCounter, nrOfInstances,"
        " nrOfActiveInstances, nrOfCompletedInstances]",
        _loop(sequential),
    )
    check = _script(
        "chk",
        f"if out != {expected!r}: raise Exception('out was ' + repr(out))\n"
        + _NOTHING_LEAKED,
    )
    assert _run(session, _chain(_PREP, work, check)) == "complete"


@pytest.mark.parametrize("sequential", [True, False], ids=["sequential", "parallel"])
def test_unmapped_loop_keeps_the_input_item_local(
    session: Session, sequential: bool
) -> None:
    # Without an output mapping, other variables still flow back (last write
    # wins), but the input item and counters stay in their iteration.
    work = _script("work", "seen = it", _loop(sequential, mapped=False))
    check = _script(
        "chk",
        "if seen != 'c': raise Exception('seen was ' + repr(seen))\n" + _NOTHING_LEAKED,
    )
    assert _run(session, _chain(_PREP, work, check)) == "complete"


def test_parallel_completion_condition_reads_the_counters(session: Session) -> None:
    work = _script(
        "work",
        "res = it.upper()",
        _loop(False, condition="nrOfCompletedInstances &gt;= 2"),
    )
    check = _script(
        "chk", "if out != ['A', 'B']: raise Exception('out was ' + repr(out))"
    )
    assert _run(session, _chain(_PREP, work, check)) == "complete"


def test_cardinality_loop_replaces_a_stale_output_list(session: Session) -> None:
    loops = [
        _script(
            f"work_{kind}",
            "res = idx * 10",
            f"<bpmn:multiInstanceLoopCharacteristics{flag}>"
            "<bpmn:loopCardinality>3</bpmn:loopCardinality>"
            f"<bpmn:loopDataOutputRef>out_{kind}</bpmn:loopDataOutputRef>"
            '<bpmn:inputDataItem id="idx" name="idx"/>'
            '<bpmn:outputDataItem id="res" name="res"/>'
            "</bpmn:multiInstanceLoopCharacteristics>",
        )
        for kind, flag in (("seq", ' isSequential="true"'), ("par", ""))
    ]
    stale = _script("stale", "out_seq = ['stale']\nout_par = ['stale']")
    check = _script(
        "chk",
        "if (out_seq, out_par) != ([0, 10, 20], [0, 10, 20]):"
        " raise Exception(repr((out_seq, out_par)))",
    )
    assert _run(session, _chain(stale, *loops, check)) == "complete"


def test_reentered_activity_starts_a_new_output_collection(session: Session) -> None:
    body = (
        '<bpmn:startEvent id="StartEvent_1"/>'
        + _script("prep", "items = ['a', 'b']\nrounds = 0")
        + _script("work", "res = it.upper() + str(rounds)", _loop(True))
        + _script("count", "rounds = rounds + 1")
        + '<bpmn:exclusiveGateway id="again" default="f_done"/>'
        + _script(
            "chk", "if out != ['A1', 'B1']: raise Exception('out was ' + repr(out))"
        )
        + '<bpmn:endEvent id="EndEvent_1"/>'
        '<bpmn:sequenceFlow id="f0" sourceRef="StartEvent_1" targetRef="prep"/>'
        '<bpmn:sequenceFlow id="f1" sourceRef="prep" targetRef="work"/>'
        '<bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="count"/>'
        '<bpmn:sequenceFlow id="f3" sourceRef="count" targetRef="again"/>'
        '<bpmn:sequenceFlow id="f_loop" sourceRef="again" targetRef="work">'
        "<bpmn:conditionExpression>rounds &lt; 2</bpmn:conditionExpression>"
        "</bpmn:sequenceFlow>"
        '<bpmn:sequenceFlow id="f_done" sourceRef="again" targetRef="chk"/>'
        '<bpmn:sequenceFlow id="f4" sourceRef="chk" targetRef="EndEvent_1"/>'
    )
    assert _run(session, _DEFINITIONS.format(body=body)) == "complete"


def test_nested_loops_keep_their_own_counters(session: Session) -> None:
    inner = (
        "<bpmn:multiInstanceLoopCharacteristics>"
        "<bpmn:loopDataInputRef>row</bpmn:loopDataInputRef>"
        "<bpmn:loopDataOutputRef>row_out</bpmn:loopDataOutputRef>"
        '<bpmn:inputDataItem id="cell" name="cell"/>'
        '<bpmn:outputDataItem id="cell_out" name="cell_out"/>'
        "</bpmn:multiInstanceLoopCharacteristics>"
    )
    outer = (
        '<bpmn:multiInstanceLoopCharacteristics isSequential="true">'
        "<bpmn:loopDataInputRef>grid</bpmn:loopDataInputRef>"
        "<bpmn:loopDataOutputRef>grid_out</bpmn:loopDataOutputRef>"
        '<bpmn:inputDataItem id="row" name="row"/>'
        '<bpmn:outputDataItem id="row_summary" name="row_summary"/>'
        "</bpmn:multiInstanceLoopCharacteristics>"
    )
    expected = [
        [0, 2, [[0, 2, 1], [1, 2, 2]]],
        [1, 2, [[0, 3, 3], [1, 3, 4], [2, 3, 5]]],
    ]
    body = (
        '<bpmn:startEvent id="StartEvent_1"/>'
        + _script("prep", "grid = [[1, 2], [3, 4, 5]]")
        + f'<bpmn:subProcess id="per_row">{outer}'
        '<bpmn:startEvent id="row_start"/>'
        + _script("per_cell", "cell_out = [loopCounter, nrOfInstances, cell]", inner)
        + _script("summ", "row_summary = [loopCounter, nrOfInstances, row_out]")
        + '<bpmn:endEvent id="row_end"/>'
        '<bpmn:sequenceFlow id="r0" sourceRef="row_start" targetRef="per_cell"/>'
        '<bpmn:sequenceFlow id="r1" sourceRef="per_cell" targetRef="summ"/>'
        '<bpmn:sequenceFlow id="r2" sourceRef="summ" targetRef="row_end"/>'
        "</bpmn:subProcess>"
        + _script(
            "chk", f"if grid_out != {expected!r}: raise Exception(repr(grid_out))"
        )
        + '<bpmn:endEvent id="EndEvent_1"/>'
        '<bpmn:sequenceFlow id="f0" sourceRef="StartEvent_1" targetRef="prep"/>'
        '<bpmn:sequenceFlow id="f1" sourceRef="prep" targetRef="per_row"/>'
        '<bpmn:sequenceFlow id="f2" sourceRef="per_row" targetRef="chk"/>'
        '<bpmn:sequenceFlow id="f3" sourceRef="chk" targetRef="EndEvent_1"/>'
    )
    assert _run(session, _DEFINITIONS.format(body=body)) == "complete"


def test_saved_state_keeps_spiff_typenames(session: Session) -> None:
    # Snapshots written before M8F-640 must keep restoring, and new ones must
    # stay readable by the same names.
    from m8flow_core.models.process_instance import ProcessInstanceModel

    review = '<bpmn:userTask id="review">' + _loop(False) + "</bpmn:userTask>"
    assert _run(session, _chain(_PREP, review)) == "user_input_required"
    (instance,) = session.query(ProcessInstanceModel).all()
    state = instance.workflow_state_json or ""
    assert '"typename": "ParallelMultiInstanceTask"' in state
    assert "RuntimeParallelMultiInstanceTask" not in state


def test_reentered_activity_with_new_keys_starts_a_new_mapping_output(
    session: Session,
) -> None:
    # A mapping output reused across activations would keep the first run's keys.
    body = (
        '<bpmn:startEvent id="StartEvent_1"/>'
        + _script("prep", "items = {'x': 1, 'y': 2}\nrounds = 0")
        + _script("work", "res = it * 10", _loop(False))
        + _script("count", "rounds = rounds + 1\nitems = {'z': 3}")
        + '<bpmn:exclusiveGateway id="again" default="f_done"/>'
        + _script("chk", "if out != {'z': 30}: raise Exception('out was ' + repr(out))")
        + '<bpmn:endEvent id="EndEvent_1"/>'
        '<bpmn:sequenceFlow id="f0" sourceRef="StartEvent_1" targetRef="prep"/>'
        '<bpmn:sequenceFlow id="f1" sourceRef="prep" targetRef="work"/>'
        '<bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="count"/>'
        '<bpmn:sequenceFlow id="f3" sourceRef="count" targetRef="again"/>'
        '<bpmn:sequenceFlow id="f_loop" sourceRef="again" targetRef="work">'
        "<bpmn:conditionExpression>rounds &lt; 2</bpmn:conditionExpression>"
        "</bpmn:sequenceFlow>"
        '<bpmn:sequenceFlow id="f_done" sourceRef="again" targetRef="chk"/>'
        '<bpmn:sequenceFlow id="f4" sourceRef="chk" targetRef="EndEvent_1"/>'
    )
    assert _run(session, _DEFINITIONS.format(body=body)) == "complete"


@pytest.mark.parametrize("sequential", [True, False], ids=["sequential", "parallel"])
def test_loop_over_an_iterable_without_a_length(sequential: bool) -> None:
    # Spiff accepts any iterable input. One without a length can't be saved in
    # task data, so this runs the engine without persisting.
    from m8flow_core.services.workflow_engine import _build_workflow

    work = _script("work", "res = [it, loopCounter, nrOfInstances]", _loop(sequential))
    workflow = _build_workflow(
        bpmn_xml=_chain(_script("prep", "items = iter(['a', 'b', 'c'])"), work),
        dmn_xml=None,
        bpmn_process_id=None,
    )
    workflow.run_all(halt_on_manual=True)

    assert workflow.completed
    assert workflow.data["out"] == [["a", 0, 3], ["b", 1, 3], ["c", 2, 3]]


def test_snapshot_saved_before_m8f640_resumes_mid_loop() -> None:
    """A parallel review saved by the unpatched engine finishes on this one."""
    from SpiffWorkflow.bpmn.serializer.workflow import BpmnWorkflowSerializer
    from SpiffWorkflow.bpmn.workflow import BpmnWorkflow
    from SpiffWorkflow.spiff.parser.process import SpiffBpmnParser
    from SpiffWorkflow.spiff.serializer.config import SPIFF_CONFIG
    from SpiffWorkflow.util.task import TaskState

    from m8flow_core.services.workflow_engine import (
        _WORKFLOW_STATE_SERIALIZER_VERSION,
        _restore_workflow,
    )

    def finish(workflow: BpmnWorkflow, item: str) -> None:
        (task,) = [
            task
            for task in workflow.get_tasks(state=TaskState.READY, manual=True)
            if task.data["it"] == item
        ]
        task.data["res"] = item.upper()
        task.run()
        workflow.run_all(halt_on_manual=True)

    review = '<bpmn:userTask id="review">' + _loop(False) + "</bpmn:userTask>"
    parser = SpiffBpmnParser(validator=None)
    parser.add_bpmn_str(_chain(_PREP, review).encode())
    before = BpmnWorkflow(
        parser.get_spec("Process_m8f640"),
        parser.get_subprocess_specs("Process_m8f640"),
    )
    before.run_all(halt_on_manual=True)
    finish(before, "c")
    # What the engine wrote before M8F-640: Spiff's own classes and converters.
    stock = BpmnWorkflowSerializer(
        registry=BpmnWorkflowSerializer.configure(SPIFF_CONFIG),
        version=_WORKFLOW_STATE_SERIALIZER_VERSION,
    )

    restored = _restore_workflow(stock.serialize_json(before))
    for item in ("b", "a"):
        finish(restored, item)

    assert restored.completed
    assert restored.data["out"] == ["A", "B", "C"]


def test_parallel_subprocess_iterations_start_with_the_final_total(
    session: Session,
) -> None:
    # Each iteration's subprocess copies its data when the child is created, so
    # the total must be known before the first child exists.
    loop = _loop(False).replace(
        '<bpmn:outputDataItem id="res" name="res"/>',
        '<bpmn:outputDataItem id="seen" name="seen"/>',
    )
    body = (
        '<bpmn:startEvent id="StartEvent_1"/>'
        + _script("prep", "items = ['a', 'b', 'c']")
        + f'<bpmn:subProcess id="per_item">{loop}'
        '<bpmn:startEvent id="item_start"/>'
        + _script("record", "seen = [loopCounter, nrOfInstances]")
        + '<bpmn:endEvent id="item_end"/>'
        '<bpmn:sequenceFlow id="i0" sourceRef="item_start" targetRef="record"/>'
        '<bpmn:sequenceFlow id="i1" sourceRef="record" targetRef="item_end"/>'
        "</bpmn:subProcess>"
        + _script(
            "chk",
            "if out != [[0, 3], [1, 3], [2, 3]]:"
            " raise Exception('out was ' + repr(out))",
        )
        + '<bpmn:endEvent id="EndEvent_1"/>'
        '<bpmn:sequenceFlow id="f0" sourceRef="StartEvent_1" targetRef="prep"/>'
        '<bpmn:sequenceFlow id="f1" sourceRef="prep" targetRef="per_item"/>'
        '<bpmn:sequenceFlow id="f2" sourceRef="per_item" targetRef="chk"/>'
        '<bpmn:sequenceFlow id="f3" sourceRef="chk" targetRef="EndEvent_1"/>'
    )
    assert _run(session, _DEFINITIONS.format(body=body)) == "complete"
