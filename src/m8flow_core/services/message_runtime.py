"""Adapt managed intermediate catches without invoking Spiff's message router."""

from __future__ import annotations

from typing import Any

from SpiffWorkflow.bpmn.specs.event_definitions.base import EventDefinition
from SpiffWorkflow.spiff.serializer.event_definition import (
    MessageEventDefinitionConverter,
)
from SpiffWorkflow.spiff.specs.event_definitions import MessageEventDefinition

from m8flow_core.services.message_model import MessageCatch, message_catch_for_node
from m8flow_core.services.signal_runtime import SignalCatchParser

_PAYLOAD_KEY = "m8f_message_payload"


class RuntimeMessageDefinition(MessageEventDefinition):
    def __init__(
        self,
        name: str,
        *,
        property_paths: dict[str, str],
        binding_paths: dict[str, str],
        **kwargs: Any,
    ) -> None:
        super().__init__(name, **kwargs)
        self.property_paths = property_paths
        self.binding_paths = binding_paths

    @classmethod
    def from_catch(cls, catch: MessageCatch) -> RuntimeMessageDefinition:
        return cls(
            catch.name,
            property_paths=catch.property_paths,
            binding_paths=catch.binding_paths,
            message_var=catch.message_var,
        )

    def catches(self, my_task: Any, event: Any) -> bool:
        return False

    def catch(self, my_task: Any, event: Any = None) -> None:
        if event is not None and self.message_var is not None:
            my_task.internal_data[_PAYLOAD_KEY] = event.payload
        EventDefinition.catch(self, my_task, event)

    def update_task_data(self, my_task: Any) -> None:
        if self.message_var is not None:
            my_task.data[self.message_var] = my_task.internal_data.pop(_PAYLOAD_KEY)

    def reset(self, my_task: Any) -> None:
        my_task.internal_data.pop(_PAYLOAD_KEY, None)
        EventDefinition.reset(self, my_task)


class RuntimeMessageConverter(MessageEventDefinitionConverter):
    def to_dict(self, event_definition: RuntimeMessageDefinition) -> dict[str, Any]:
        data: dict[str, Any] = super().to_dict(event_definition)
        data["property_paths"] = event_definition.property_paths
        data["binding_paths"] = event_definition.binding_paths
        return data


class MessageCatchParser(SignalCatchParser):
    def parse_message_event(self, node: Any) -> Any:
        catch = message_catch_for_node(
            self.process_parser.parser.message_catches,
            node.getparent(),
        )
        if catch is not None:
            return RuntimeMessageDefinition.from_catch(catch)
        return super().parse_message_event(node)
