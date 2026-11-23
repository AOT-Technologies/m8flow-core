<!--
SPDX-FileCopyrightText: 2026 AOT Technologies

SPDX-License-Identifier: Apache-2.0
-->

# Changelog

All notable changes to m8flow-core are recorded in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Each release
lists its changes under **Added**, **Changed**, **Deprecated**, **Removed**,
**Fixed**, and **Security**. Versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html); the
[versioning policy](docs/reference/versioning.md) defines the public contract
and what each version part means.

## [Unreleased]

### Fixed

- Signal subscriptions now include signal definitions nested in event-based
  gateways, allowing local and queued publications to reach their catch events.
  Long signal names keep hashed subscription keys within the database column
  limit while preserving the catcher activation identity.
- Recurring timer start schedules now enqueue uniquely keyed occurrences and
  advance from the original `timeCycle` cadence independently of process
  execution. Process creation and occurrence completion share the caller's
  transaction, preventing duplicate instances on retries. Failed automatic
  script and service tasks consume their occurrence while preserving the
  errored instance.

### Added

- External correlation for Intermediate Message Catch Events through
  `CorrelateMessageCommand`. BPMN bindings and retrieval paths form composite
  keys. Delivery receipts are durable and idempotent within each tenant.
- BPMN Transaction cancellation stops active work and timers, runs recorded
  compensation handlers, and follows the attached cancel boundary event once.
  `validate_compensation_bpmn` lets hosts check supported forms before saving.
  See [Cancel a Transaction](docs/how-to/run-compensation.md#cancel-a-transaction).
- [Publish a release](docs/how-to/publish-a-release.md#withdraw-or-replace-a-release)
  now covers yanking a broken version from PyPI, marking it on GitHub and in
  the changelog, and replacing it with a patch release.
- A [FAQ](docs/reference/faq.md) with short answers and links to the full
  docs.
- CI uploads test coverage from `main` to Codecov, and the README shows a
  coverage badge. The PyPI badge now reads the latest version from PyPI.
- BPMN collaborations deploy multiple executable pools from one file. Modeled
  messages start or resume a participant through the tenant-scoped scheduler.
  Run `alembic upgrade head` before starting the updated runtime
  and scheduler pollers.
- Alembic revision `m8f_merge_runtime` joins the collaboration and compensation
  migration branches without changing either history.
- Alembic revision `m8f_instance_definition_tenant` prevents instances from
  referencing a BPMN definition owned by another tenant.
- Alembic revision `m8f_instance_process_tenant` prevents instances from
  referencing a BPMN process owned by another tenant.
- Task code can raise a BPMN error that an error boundary event catches, for
  models migrated from Camunda 7 (M8F-675). Scripts call
  `raise BpmnError("CODE")`. Connectors raise
  `ServiceTaskExecutionError(..., error_code="CODE")`, and the connector proxy
  client sets `error_code` from the proxy's reply, such as `HttpError404`. The
  nearest matching boundary cancels the task and takes over. Without a
  matching boundary the task fails as before, and other Python exceptions are
  never caught as BPMN errors.
- Multi-instance runtime values. Each iteration sees `loopCounter` (zero-based,
  as in Camunda 7), `nrOfInstances`, `nrOfActiveInstances` and
  `nrOfCompletedInstances`, and a `completionCondition` can use them.

### Changed

- CI runs the Python 3.13 and 3.14, macOS, and Windows test matrix on pull
  requests as well as on `main`.
- Release tags are GPG-signed. See
  [Set up tag signing](docs/how-to/publish-a-release.md#set-up-tag-signing).
- The Codecov upload runs in its own `main`-only job, so the job that runs
  pull request code no longer holds the `id-token: write` permission.
- The publish workflow attests build provenance in its own job, so the job
  that runs the tests no longer holds the `id-token: write` permission.
- SQLite engines created with `m8flow_core.db.build_engine()` enforce foreign
  keys on every connection. Process instance creation reports missing and
  out-of-tenant BPMN references as `NotFoundError`.
- Imports now reject BPMN collaborations with multiple pools but fewer than
  two distinct executable participant processes.
- Multi-instance iterations keep their input item, output item and runtime
  values to themselves; without an output mapping, their other variables still
  flow back. Parallel output collections now list results in input order, and
  an activity that runs again starts a new output collection instead of failing.
- `advance_process_instance_workflow` takes an optional `task_payload`, and
  `CompleteTaskCommand` passes it. A human task inside a multi-instance
  iteration applies its payload to that iteration only instead of recording it
  as process instance metadata, so one iteration's form values can't reach
  another. Other tasks record their payload as before.

### Fixed

- A `loopCardinality` multi-instance loop no longer fails with `NameError` when
  its output collection already exists (a SpiffWorkflow 3.2.0 bug).
- Error and escalation events now go to the nearest matching catcher (M8F-665).
  The walk starts in the thrower's scope and moves out one scope at a time. In
  each scope its event subprocesses come first, then boundary events on the
  activity that hosts the scope. Before, SpiffWorkflow broadcast the event to
  every matching catcher in the process, so with matching boundaries at two
  levels the outer one won.
- Event subprocesses with an error or escalation start event now run only when
  a matching event is thrown (M8F-665). SpiffWorkflow parsed those start events
  as none starts, so the handler ran every time its scope started and caught
  nothing. The handler starts from the thrower's data. An interrupting one,
  the BPMN default, cancels the rest of its scope, including waiting user
  tasks; one with `isInterrupting="false"` runs beside the main path.
- An escalation that nothing catches is ignored and the flow continues, and an
  error end event that nothing catches ends its path like a plain end event.
  Neither puts the instance into an error state (M8F-665).

### Security

- Connector proxy calls and the Keycloak provisioning helper no longer follow
  an HTTP redirect to a non-http(s) URL such as `ftp://`. They also no longer
  carry handlers for `ftp:`, `file:`, or `data:` URLs.
- A malformed connector proxy or Keycloak URL such as `http://[::1` or
  `http://example.com:bad` now raises
  `ValidationError` or `KeycloakProvisioningError` instead of a bare
  `ValueError`.

## [1.0.0] - 2026-11-12

The first stable release, and the first under the name m8flow-core. Versions
0.1.x to 0.2.1 were untagged development builds named `m8flow-bpmn-core`, and
`1.0.0b1` to `1.0.0b3` were pre-releases on PyPI. This release changes the
package name, the public API, and the database schema, so upgrade the library
and m8flow together. To move from a 0.x build or a beta, follow
[Upgrade to 1.0](docs/how-to/upgrade-to-1.0.md), which lists every renamed and
removed name.

### Added

- Public in-process API in `m8flow_core.api`: separate commands and queries,
  `execute_command` and `execute_query` dispatchers that accept a
  caller-owned `Session` or `Connection`, and the `BpmnCoreError` error
  hierarchy.
- BPMN and DMN import, process instances, human task worklists with claim and
  completion, process metadata, event history, and lifecycle commands to
  suspend, resume, error, retry, and terminate.
- Tenant-aware command authorization with built-in `user`, `manager`, and
  `admin` roles and pluggable policies.
- Persisted scheduler jobs for timer start events, `timeCycle` timers,
  intermediate and boundary timers, and scheduled retries, run through
  `run_due_scheduler_jobs`.
- Synchronous BPMN service tasks through pluggable connectors, including an
  adapter for `m8flow-connector-proxy`.
- BPMN call activities through the tenant-aware `CallActivityResolver` API,
  and sequential and parallel multi-instance tasks.
- Durable, tenant-scoped BPMN signals. Intermediate and end throw events
  broadcast to waiting intermediate catch events, boundary events, event
  subprocesses, and signal start events, with a subscription index and
  idempotent delivery receipts. New names: `PublishSignalCommand`,
  `RunSignalDeliveriesCommand`, `GetSignalPublicationQuery`,
  `ListSignalDeliveriesQuery`, `run_signal_deliveries`, the matching service
  functions, and the `SIGNAL_PUBLISH_COMMAND` permission, granted to managers
  and admins. Alembic revision `l3m4n5o6p7q8` adds the four signal registry
  tables. See [Publish signals](docs/how-to/publish-signals.md).
- BPMN compensation: compensation boundary events, task and subprocess
  handlers, intermediate throw and end events, targeted compensation, both
  wait modes, and durable reverse completion ordering with nested scope
  isolation. New names: `GetProcessInstanceCompensationsQuery`,
  `get_process_instance_compensations`, `CompensationRecordModel`,
  `CompensationStatus`, and `CompensationExecutionError`. Connectors get a
  stable idempotency key, and failed undo handlers can be retried. Alembic
  revision `m8f_671_compensation` adds the three compensation journal tables.
  See [Run compensation handlers](docs/how-to/run-compensation.md).
- The `work_item` table and `WorkItemModel` for human-task claim state, with
  claims that are safe under concurrency.
- `properties_json["initiator_lanes"]`, to route a lane's tasks to the user
  who started the instance, and `assign_pending_tasks_for_user`, to add
  potential owners after a directory sync.
- Support for Python 3.12, 3.13, and 3.14.
- A sample host application in `sample_app/` and runnable examples, including
  interactive signal broadcast and compensation walkthroughs.
- Apache License 2.0 with [REUSE](https://reuse.software/) 3.3 compliance:
  every file declares its copyright and license, and the wheel and sdist ship
  the license texts.
- A [versioning policy](docs/reference/versioning.md) and a
  [performance baseline](docs/reference/performance-baseline.md).
- PyPI releases with PyPI and GitHub build provenance attestations. See
  [Publish a release](docs/how-to/publish-a-release.md#signing-and-provenance).

### Changed

- **Breaking:** renamed the distribution from `m8flow-bpmn-core` to
  `m8flow-core`, and the import package from `m8flow_bpmn_core` to
  `m8flow_core`.
- **Breaking:** the Alembic history starts again at a single baseline,
  `k2l3m4n5o6p7`, which creates the 1.0 schema on an empty database. Convert
  0.x databases yourself, then run `alembic stamp k2l3m4n5o6p7`.
- **Breaking:** every timestamp is a timezone-aware UTC `datetime`, in public
  parameters and in database columns. `*_in_seconds` names are gone.
- **Breaking:** `ClaimTaskCommand` and `CompleteTaskCommand` take
  `work_item_id` and return `WorkItemModel`, and `GetPendingTasksQuery`
  returns work items.
- **Breaking:** permission targets are exact `(resource_type, resource_id)`
  pairs instead of URI patterns.
- **Breaking:** user identity, group, serializer-version, and digest columns
  were renamed, and constraints and indexes use the `m8f_*` namespace.
- **Breaking:** a lane goes to the process initiator only when listed in
  `initiator_lanes`, not because its name looks like "process initiator".
- Lane owners that match no user in the tenant are skipped with a warning
  instead of failing the process.
- The minimum supported SpiffWorkflow version is 3.2.0.

### Removed

- **Breaking:** `HumanTaskModel` and the `human_task` tables, replaced by
  `work_item`.
- **Breaking:** `ProcessInstanceEventType`, replaced by
  `ProcessLifecycleEventType`, `TaskEventType`, and
  `ProcessInstanceEventCategory`.
- **Breaking:** several `ProcessInstanceModel` status helpers,
  `FutureTaskModel.insert_or_update`, the `added_by` argument for claims, and
  event types and `WorkItemUserAddedBy` labels the library never records.

### Fixed

- A timer start event inside an event subprocess fires within the running
  instance instead of starting extra instances.
- Race conditions when two requests create the same permission rows at once.

### Security

- Process and task payloads are isolated per tenant. Before, two tenants with
  equal payload content shared one `json_data` row.
- BPMN source XML is parsed with `defusedxml`.
- The connector proxy and Keycloak HTTP helpers reject URLs that aren't
  absolute `http` or `https` URLs, so a misconfigured base URL can't read
  local files through `file://`.

[Unreleased]: https://github.com/AOT-Technologies/m8flow-core/compare/1.0.0...HEAD
[1.0.0]: https://github.com/AOT-Technologies/m8flow-core/releases/tag/1.0.0
