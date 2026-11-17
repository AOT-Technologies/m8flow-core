from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC
from pathlib import Path
from threading import Barrier

import pytest
from SpiffWorkflow.spiff.specs.event_definitions import MessageEventDefinition
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from m8flow_core import api
from m8flow_core.message_upgrade import reindex_message_subscriptions
from m8flow_core.models import MessageSubscriptionModel, ProcessInstanceModel
from m8flow_core.services.workflow_engine import _restore_workflow
from m8flow_core.services.workflow_state import _persist_workflow_recovery_state
from test_signal_publication import deploy, seed_actor

BPMN = (Path(__file__).parent / "fixtures" / "message_waiting.bpmn").read_text()


@pytest.fixture
def actor(session):
    return seed_actor(session, "messages")


def start(session, actor, definition, case_number, region="west"):
    return api.execute_command(
        session,
        api.InitializeProcessInstanceFromDefinitionCommand(
            tenant_id=actor[0],
            bpmn_process_definition_id=definition.id,
            process_initiator_id=actor[1],
            submission_metadata={"case_number": str(case_number), "region": region},
        ),
    )


def message(actor, delivery_id, case_number, region="west", **kwargs):
    return api.CorrelateMessageCommand(
        actor[0],
        "Payment received",
        actor[1],
        delivery_id,
        {"payment": {"caseNumber": case_number, "region": region}},
        **kwargs,
    )


def test_message_selects_exact_composite_key_after_restart(engine, session, actor):
    definition = deploy(session, actor, BPMN)
    west = start(session, actor, definition, 17)
    east = start(session, actor, definition, 17, region="east")
    west_id, east_id = west.id, east.id
    session.commit()
    with Session(engine) as restarted:
        request = message(actor, "pay-west", 17)
        receipt = api.execute_command(restarted, request)
        assert receipt.process_instance_id == west_id
        assert receipt.delivered_at.tzinfo is UTC
        assert receipt.outcome == "delivered"
        assert restarted.get(ProcessInstanceModel, west_id).status == "complete"
        assert restarted.get(ProcessInstanceModel, east_id).status == "waiting"
        restarted.commit()
    with Session(engine) as restarted:
        replay = api.execute_command(restarted, request)
        assert replay.process_instance_id == west_id
        assert replay.delivered_at.tzinfo is UTC
        receipt = api.execute_command(restarted, message(actor, "pay-east", 17, "east"))
        assert receipt.process_instance_id == east_id
        assert restarted.get(ProcessInstanceModel, east_id).status == "complete"


def test_no_match_and_ambiguity_leave_id_available(session, actor):
    request = message(actor, "payment", 17)
    with pytest.raises(api.NotFoundError):
        api.execute_command(session, request)
    definition = deploy(session, actor, BPMN)
    one = start(session, actor, definition, 17)
    two = start(session, actor, definition, 17)
    with pytest.raises(api.AmbiguousMessageCorrelationError):
        api.execute_command(session, request)
    assert one.status == two.status == "waiting"
    with pytest.raises(api.NotFoundError):
        api.execute_command(
            session, replace(request, process_definition_id=definition.id + 1)
        )
    targeted = replace(
        request, process_definition_id=definition.id, process_instance_id=two.id
    )
    assert api.execute_command(session, targeted).process_instance_id == two.id
    assert one.status == "waiting"
    with pytest.raises(api.MessageDeliveryConflictError):
        api.execute_command(session, request)
    with pytest.raises(api.NotFoundError):
        api.execute_command(session, message(actor, "as-string", "17"))


def test_lifecycle_and_tenant_isolation(session, actor):
    definition = deploy(session, actor, BPMN)
    one = start(session, actor, definition, 17)
    two = start(session, actor, definition, 18)
    other_actor = seed_actor(session, "other")
    other = start(session, other_actor, deploy(session, other_actor, BPMN), 17)
    ids = {"tenant_id": actor[0], "user_id": actor[1], "process_instance_id": one.id}
    api.execute_command(session, api.SuspendProcessInstanceCommand(**ids))
    with pytest.raises(api.NotFoundError):
        api.execute_command(session, message(actor, "one", 17))
    assert (
        api.execute_command(
            session, message(other_actor, "one", 17)
        ).process_instance_id
        == other.id
    )
    api.execute_command(session, api.ResumeProcessInstanceCommand(**ids))
    assert (
        api.execute_command(session, message(actor, "one", 17)).process_instance_id
        == one.id
    )
    api.execute_command(
        session,
        api.TerminateProcessInstanceCommand(**{**ids, "process_instance_id": two.id}),
    )
    with pytest.raises(api.NotFoundError):
        api.execute_command(session, message(actor, "terminated", 18))


def test_connection_rollback_restores_waiting_subscription(engine, session, actor):
    definition = deploy(session, actor, BPMN)
    instance_id = start(session, actor, definition, 17).id
    session.commit()
    request = message(actor, "rollback", 17)
    with engine.connect() as connection:
        transaction = connection.begin()
        receipt = api.execute_command(connection, request)
        assert receipt.process_instance_id == instance_id
        transaction.rollback()
    with Session(engine) as restarted:
        assert restarted.get(ProcessInstanceModel, instance_id).status == "waiting"
        assert (
            api.execute_command(restarted, request).process_instance_id == instance_id
        )


@pytest.mark.parametrize("same_id", [True, False])
def test_concurrent_delivery_cannot_consume_the_same_activation(
    engine, session, actor, same_id
):
    definition = deploy(session, actor, BPMN)
    instance_id = start(session, actor, definition, 17).id
    session.commit()
    ready = Barrier(2)

    def deliver(delivery_id):
        with Session(engine) as worker, worker.begin():
            ready.wait(timeout=10)
            try:
                receipt = api.execute_command(worker, message(actor, delivery_id, 17))
                return (receipt.process_instance_id, receipt.delivered_at)
            except api.NotFoundError:
                return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(deliver, "concurrent")
        second = pool.submit(deliver, "concurrent" if same_id else "other")
        results = [first.result(timeout=20), second.result(timeout=20)]
    accepted = [result for result in results if result is not None]
    assert len(accepted) == (2 if same_id else 1)
    assert {result[0] for result in accepted} == {instance_id}
    assert len(set(accepted)) == 1


def test_legacy_wait_reindexed_without_advancing(session, actor):
    instance = start(session, actor, deploy(session, actor, BPMN), 17)
    workflow = _restore_workflow(instance.workflow_state_json)
    workflow.spec.task_specs["payment_catch"].event_definition = MessageEventDefinition(
        "Payment received"
    )
    _persist_workflow_recovery_state(session, instance, workflow)
    session.execute(delete(MessageSubscriptionModel))
    assert reindex_message_subscriptions(session, actor[0]) == 1
    assert reindex_message_subscriptions(session, actor[0]) == 0
    assert instance.status == "waiting"
    assert len(session.scalars(select(MessageSubscriptionModel)).all()) == 1
    assert (
        api.execute_command(session, message(actor, "legacy", 17)).process_instance_id
        == instance.id
    )


def test_downstream_script_failure_keeps_receipt_and_can_retry(session, actor):
    xml = BPMN.replace(
        '<sequenceFlow id="from_payment" sourceRef="payment_catch" targetRef="end"/>',
        '<scriptTask id="verify"><script>assert approved</script></scriptTask>'
        '<sequenceFlow id="from_payment" sourceRef="payment_catch" targetRef="verify"/>'
        '<sequenceFlow id="from_verify" sourceRef="verify" targetRef="end"/>',
    )
    instance = start(session, actor, deploy(session, actor, xml), 17)
    request = message(actor, "script-failure", 17)
    assert api.execute_command(session, request).outcome == "workflow_error"
    assert instance.status == "error"
    assert api.execute_command(session, request).outcome == "workflow_error"
    api.execute_command(
        session,
        api.UpsertProcessInstanceMetadataCommand(
            tenant_id=actor[0],
            process_instance_id=instance.id,
            key="approved",
            value="true",
            updated_at=api.get_process_instance(
                session, tenant_id=actor[0], process_instance_id=instance.id
            ).updated_at,
        ),
    )
    api.execute_command(
        session, api.RetryProcessInstanceCommand(actor[0], instance.id, actor[1])
    )
    assert instance.status == "complete"


def test_embedded_catch_receives_the_optional_message_variable(session, actor):
    xml = BPMN.replace(
        'name="Payment received"/>',
        'name="event_fired"><extensionElements>'
        "<spiffworkflow:messageVariable xmlns:spiffworkflow="
        '"http://spiffworkflow.org/bpmn/schema/1.0/core">'
        "received</spiffworkflow:messageVariable></extensionElements></message>",
    )
    start_tag = xml.index('    <startEvent id="start"/>')
    end_tag = xml.index("    <correlationSubscription")
    embedded = xml[start_tag:end_tag].replace(
        'targetRef="end"/>',
        'targetRef="verify"/>'
        '<scriptTask id="verify"><script>'
        "assert received['payment']['caseNumber'] == 17"
        "</script></scriptTask>"
        '<sequenceFlow id="to_inner_end" sourceRef="verify" targetRef="end"/>',
    )
    xml = (
        xml[:start_tag]
        + '<startEvent id="outer_start"/>'
        + '<subProcess id="embedded">'
        + embedded
        + "</subProcess>"
        + '<endEvent id="outer_end"/>'
        + '<sequenceFlow id="to_embedded" '
        'sourceRef="outer_start" targetRef="embedded"/>'
        + '<sequenceFlow id="from_embedded" '
        'sourceRef="embedded" targetRef="outer_end"/>'
        + xml[end_tag:]
    )
    instance = start(session, actor, deploy(session, actor, xml), 17)
    request = replace(message(actor, "embedded", 17), message_name="event_fired")
    assert api.execute_command(session, request).outcome == "delivered"
    assert instance.status == "complete"


@pytest.mark.parametrize(
    "invalid",
    [
        BPMN.replace("tns:payment_message", "tns:missing"),
        BPMN.replace("payment/caseNumber", "payment/*"),
        BPMN.replace("<dataPath>region</dataPath>", ""),
        BPMN.replace("<intermediateCatchEvent", "<startEvent").replace(
            "</intermediateCatchEvent", "</startEvent"
        ),
    ],
)
def test_invalid_message_model_rejected_on_import(session, actor, invalid):
    with pytest.raises(api.ValidationError):
        deploy(session, actor, invalid)
