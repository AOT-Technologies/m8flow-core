"""Read the supported context-based correlation declarations from BPMN."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from lxml import etree

from m8flow_core.errors import ValidationError

BPMN = "{http://www.omg.org/spec/BPMN/20100524/MODEL}"
SPIFF = "{http://spiffworkflow.org/bpmn/schema/1.0/core}"
_XPATH = "http://www.w3.org/1999/XPath"
_FIELD_PATH = re.compile(r"(?:\./)?[A-Za-z_][\w.-]*(?:/[A-Za-z_][\w.-]*)*", re.ASCII)


@dataclass(frozen=True, slots=True)
class MessageCatch:
    name: str
    property_paths: dict[str, str]
    binding_paths: dict[str, str]
    message_var: str | None


def _ref(node: etree._Element, value: str | None) -> str:
    if not value or not value.strip():
        raise ValidationError("A BPMN message correlation reference is missing")
    prefix, separator, identifier = value.strip().rpartition(":")
    if separator and (
        prefix not in node.nsmap
        or node.nsmap[prefix] != node.getroottree().getroot().get("targetNamespace")
    ):
        raise ValidationError(f"Unresolved BPMN message reference {value!r}")
    return identifier


def _path(node: etree._Element, tag: str) -> str:
    expressions = node.findall(BPMN + tag)
    if len(expressions) != 1:
        raise ValidationError(f"BPMN message correlation requires one {tag}")
    expression = expressions[0]
    default_language = node.getroottree().getroot().get("expressionLanguage", _XPATH)
    value = (expression.text or "").strip()
    if (
        len(expression)
        or expression.get("language", default_language) != _XPATH
        or not _FIELD_PATH.fullmatch(value)
    ):
        raise ValidationError(f"BPMN message {tag} must be a relative field path")
    return value.removeprefix("./")


def _index(nodes: list[etree._Element]) -> dict[str, etree._Element]:
    indexed: dict[str, etree._Element] = {}
    for node in nodes:
        identifier = node.get("id")
        if not identifier or identifier in indexed:
            raise ValidationError("BPMN message declarations require unique IDs")
        indexed[identifier] = node
    return indexed


def _subscriptions(
    root: etree._Element,
    process: etree._Element,
    properties: dict[str, dict[str, str]],
) -> list[tuple[str, dict[str, str]]]:
    keys: list[etree._Element] = []
    for collaboration in root.findall(BPMN + "collaboration"):
        if any(
            participant.get("processRef")
            and _ref(participant, participant.get("processRef")) == process.get("id")
            for participant in collaboration.findall(BPMN + "participant")
        ):
            keys.extend(collaboration.findall(BPMN + "correlationKey"))
            keys.extend(
                collaboration.findall(f".//{BPMN}conversation/{BPMN}correlationKey")
            )
    indexed_keys = _index(keys)
    subscriptions = []
    used_keys = set()
    for subscription in process.findall(BPMN + "correlationSubscription"):
        key_id = _ref(subscription, subscription.get("correlationKeyRef"))
        key = indexed_keys.get(key_id)
        if key is None or key_id in used_keys:
            raise ValidationError(
                f"BPMN correlation key {key_id!r} is missing or duplicated"
            )
        used_keys.add(key_id)
        refs = [
            _ref(ref, ref.text) for ref in key.findall(BPMN + "correlationPropertyRef")
        ]
        bindings: dict[str, str] = {}
        for binding in subscription.findall(BPMN + "correlationPropertyBinding"):
            prop = _ref(binding, binding.get("correlationPropertyRef"))
            if prop in bindings:
                raise ValidationError(
                    f"Duplicate BPMN correlation property binding {prop!r}"
                )
            bindings[prop] = _path(binding, "dataPath")
        if (
            not refs
            or len(refs) != len(set(refs))
            or set(refs) != bindings.keys()
            or not bindings.keys() <= properties.keys()
        ):
            raise ValidationError(
                f"BPMN correlation key {key_id!r} requires complete bindings"
            )
        subscriptions.append((key_id, bindings))
    return subscriptions


def compile_message_catches(
    root: etree._Element,
) -> dict[tuple[str, str], MessageCatch]:
    events = list(root.iter(BPMN + "messageEventDefinition"))
    if not events:
        return {}

    message_nodes = root.findall(BPMN + "message")
    messages = _index(message_nodes)
    properties: dict[str, dict[str, str]] = {}
    for identifier, node in _index(root.findall(BPMN + "correlationProperty")).items():
        paths: dict[str, str] = {}
        for retrieval in node.findall(BPMN + "correlationPropertyRetrievalExpression"):
            message_ref = _ref(retrieval, retrieval.get("messageRef"))
            if message_ref not in messages or message_ref in paths:
                raise ValidationError(
                    f"Invalid BPMN correlation retrieval for {message_ref!r}"
                )
            paths[message_ref] = _path(retrieval, "messagePath")
        if not paths:
            raise ValidationError(
                f"BPMN correlation property {identifier!r} requires a retrieval"
            )
        properties[identifier] = paths

    catches: dict[tuple[str, str], MessageCatch] = {}
    process_subscriptions: dict[str, list[tuple[str, dict[str, str]]]] = {}
    gateways = {item.get("id") for item in root.iter(BPMN + "eventBasedGateway")}
    gateway_targets = {
        _ref(flow, flow.get("targetRef"))
        for flow in root.iter(BPMN + "sequenceFlow")
        if flow.get("sourceRef") and _ref(flow, flow.get("sourceRef")) in gateways
    }
    for event in events:
        catch = event.getparent()
        if catch is None or catch.tag != BPMN + "intermediateCatchEvent":
            raise ValidationError(
                "Only inline Intermediate Message Catch Events are supported"
            )
        event_elements = [
            child
            for child in catch
            if isinstance(child.tag, str)
            and (
                child.tag.endswith("EventDefinition")
                or child.tag == BPMN + "eventDefinitionRef"
            )
        ]
        if catch.get("id") in gateway_targets:
            raise ValidationError(
                "Event-based gateways cannot target a Message Catch Event"
            )
        if len(event_elements) != 1:
            raise ValidationError(
                "A Message Catch Event must have a single event definition"
            )
        scope = catch.getparent()
        while scope is not None and scope.tag == BPMN + "subProcess":
            if scope.get("triggeredByEvent") in {"true", "1"}:
                raise ValidationError(
                    "Message catches require an ordinary process scope"
                )
            scope = scope.getparent()
        if scope is None or scope.tag != BPMN + "process":
            raise ValidationError("Message catches require an ordinary process scope")
        process_id, catch_id = scope.get("id"), catch.get("id")
        if not process_id or not catch_id or (process_id, catch_id) in catches:
            raise ValidationError("Message catches require unique BPMN IDs")
        message_ref = _ref(event, event.get("messageRef"))
        message = messages.get(message_ref)
        if message is None:
            raise ValidationError(f"Unknown BPMN message {message_ref!r}")
        name = message.get("name")
        if name is None or not name.strip() or len(name) > 255:
            raise ValidationError("Message Catch Events require a named BPMN message")
        if process_id not in process_subscriptions:
            process_subscriptions[process_id] = _subscriptions(root, scope, properties)
        keys = [
            bindings
            for _, bindings in process_subscriptions[process_id]
            if all(message_ref in properties[prop] for prop in bindings)
        ]
        if len(keys) != 1:
            raise ValidationError(
                f"Message catch {catch_id!r} requires one complete correlation key"
            )
        bindings = keys[0]
        message_var = event.findtext(f".//{SPIFF}messageVariable") or message.findtext(
            f".//{SPIFF}messageVariable"
        )
        catches[(process_id, catch_id)] = MessageCatch(
            name,
            {prop: properties[prop][message_ref] for prop in bindings},
            bindings,
            message_var,
        )
    return catches


def read_message_catches(
    source_bpmn_xml: str | bytes,
) -> dict[tuple[str, str], MessageCatch]:
    if isinstance(source_bpmn_xml, str):
        source_bpmn_xml = source_bpmn_xml.encode("utf-8")
    try:
        root = etree.fromstring(
            source_bpmn_xml,
            parser=etree.XMLParser(resolve_entities=False, no_network=True),
        )
    except (etree.XMLSyntaxError, ValueError) as exc:
        raise ValidationError("BPMN XML is malformed") from exc
    if root.getroottree().docinfo.doctype:
        raise ValidationError("BPMN XML must not contain a DOCTYPE")
    return compile_message_catches(root)


def validate_message_bpmn(source_bpmn_xml: str | bytes) -> None:
    """Validate the supported BPMN Message Catch Event and correlation subset."""
    read_message_catches(source_bpmn_xml)


def message_catch_for_node(
    catches: dict[tuple[str, str], MessageCatch],
    node: Any,
) -> MessageCatch | None:
    process = next(node.iterancestors(BPMN + "process"), None)
    if process is None:
        return None
    return catches.get((process.get("id"), node.get("id")))
