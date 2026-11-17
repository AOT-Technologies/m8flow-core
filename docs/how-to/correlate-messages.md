# Correlate external BPMN messages

Model an Intermediate Message Catch Event with a `messageRef` to a named
BPMN message. Declare a `correlationKey` in the process's collaboration and
reference its properties with `correlationPropertyRef`. Bind every property
to process context through `correlationSubscription` and
`correlationPropertyBinding`. Declare the matching incoming `messagePath`
on each property's retrieval expression for that message.

The [payment BPMN fixture](../../tests/fixtures/message_waiting.bpmn) binds
`case_number` and `region` from the waiting task's data. The incoming message
supplies those values at `payment/caseNumber` and `payment/region`. Paths are
relative slash-separated field names. Values must be strings, finite numbers,
or booleans. Values and message names match exactly, including type and case.
For example, the number `17` and the string `"17"` are different keys. A
missing or non-scalar property makes that key ineligible.

Pass initial context through process submission metadata or set it inside
the workflow before the catch. A catch can optionally use Spiff's
`messageVariable` to expose the delivered JSON object to the continuing path.

## Deliver from an application

```python
from m8flow_core import api

with session_factory.begin() as session:
    receipt = api.execute_command(
        session,
        api.CorrelateMessageCommand(
            tenant_id=tenant_id,
            message_name="Payment received",
            user_id=integration_user_id,
            delivery_id="payment-17-west",
            payload={"payment": {"caseNumber": 17, "region": "west"}},
        ),
    )
```

Use a stable delivery ID across retries. An identical retry returns the
original receipt even after the instance advances. Altering the payload,
sender, message name, or target filters for that ID raises
`MessageDeliveryConflictError`. A request with no match or with multiple
matches leaves the ID available. Optional definition and instance IDs narrow
the same tenant-scoped lookup.

Commit before acknowledging delivery to the sender. Rolling back restores
the waiting task and releases the delivery ID. A receipt with outcome
`workflow_error` means the catch consumed the message and a later workflow
step failed. Recover the instance through the normal workflow retry API.

## Upgrade an existing core database

Run `alembic upgrade head` before starting workflow writers. If a tenant has
compatible legacy waits, run the maintenance command with the application
database configured and workflow writers stopped.

```sh
python -m m8flow_core.message_upgrade TENANT_ID
```

The command reindexes compatible waits using their stored model and workflow
snapshot. It is safe to rerun and does not execute workflow tasks.
