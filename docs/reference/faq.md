<!--
SPDX-FileCopyrightText: 2026 AOT Technologies

SPDX-License-Identifier: Apache-2.0
-->

# Frequently asked questions

Short answers to common questions about m8flow-core, each with a link to the
page that covers it in full. For an error or a stuck process, see
[Troubleshoot common problems](../how-to/troubleshoot.md) instead.

## Is m8flow-core a server I deploy?

No. It's a Python library that runs inside your application's process. You
call `execute_command` and `execute_query` directly; there's no HTTP API. See
[About the architecture](../explanation/architecture.md).

## Do I need m8flow or SpiffArena to use it?

No. Any Python application can embed it. m8flow is one host, and the
[sample app](../../sample_app/README.md) is another. See
[Embed the library in your application](../how-to/embed-in-your-application.md).

## Which Python versions are supported?

Python 3.12 or later. CI tests 3.12, 3.13, and 3.14 on Linux, macOS, and
Windows. See [Versioning policy](versioning.md#supported-python-versions).

## Which databases can I use?

SQLite works with no extra install. For PostgreSQL or MySQL, install the
`postgresql` or `mysql` extra and set `M8FLOW_DATABASE_URL`. See
[Configuration](configuration.md).

## Who commits the transaction?

You do. Pass a `Session` you already manage, or pass a `Connection` and the
library opens a temporary session on it that never commits or rolls back. See
[Embed the library in your application](../how-to/embed-in-your-application.md#choose-a-session-or-a-connection).

## Which BPMN elements work?

See [BPMN support](bpmn-support.md) for what's tested, and
[Limitations](limitations.md#advanced-bpmn-coverage) for what isn't.

## Does it support BPMN messages?

No. Signals are supported, but message events and correlation aren't. See
[Limitations](limitations.md#messaging-and-correlation).

## How do timers run?

Your application runs a scheduler loop that calls `run_due_scheduler_jobs`,
inline or from Celery. Run one loop per database or per tenant, because
several workers claiming the same jobs isn't safe yet. See
[Run timers and scheduled retries](../how-to/run-timers-and-retries.md).

## Can a service task wait for a callback?

No. Connectors run synchronously and must finish within the call. See
[Call external services from service tasks](../how-to/call-external-services.md)
and [Limitations](limitations.md#service-tasks).

## Does it manage users and tenants?

No. It checks tenant membership and roles, but creating users and tenants is
up to your application. See
[About authorization and tenancy](../explanation/authorization-and-tenancy.md).

## How do I upgrade from `m8flow-bpmn-core` 0.x?

Follow [Upgrade to 1.0](../how-to/upgrade-to-1.0.md), which lists every
renamed and removed name.

## Will a minor release break my code?

No. m8flow-core follows semantic versioning, so breaking changes to the
public API or database schema ship only in a major release. The one exception
is a security fix that can't be made any other way. See
[Versioning policy](versioning.md).
