# FerricStore Elixir SDK Documentation

Start here if you are using FerricStore from Elixir.

## Read first

- [Quickstart](quickstart.md): install, start Docker, create queue/workflow work.
- [Client API](client.md): what each module exposes today.
- [Workflow and queue APIs](workflow.md): durable queues and explicit state machines.
- [Data, attributes, and value refs](data.md): payload, indexed attributes, named values.
- [Use cases](use-cases.md): sagas, IoT fanout, AI orchestration, batch imports.
- [Web and serverless usage](web.md): Phoenix/API producers and worker services.

## Production

- [Configuration](configuration.md): URLs, TLS, auth, timeouts, codecs.
- [Production readiness](production.md): deployment shape, leases, shutdown, idempotency.
- [Testing](testing.md): unit tests, Docker integration, fake clients.
- [Troubleshooting](troubleshooting.md): common errors and fixes.

## Maintainers

- [Benchmark notes](benchmark.md): local source-server benchmark shapes and results.
- [Development checks](development.md): quality gates, architecture tests, release checks.

## Current SDK scope

The Elixir SDK is native-protocol first. It exposes:

| Area | Modules |
| --- | --- |
| Connection and KV | `FerricStore`, `FerricStore.Client`, `FerricStore.Protocol` |
| Topology-aware SDK | `FerricStore.SDK`, `FerricStore.SDK.KV`, `FerricStore.SDK.Flow` |
| Management helpers | `FerricStore.SDK.Management` |
| Flow commands | `FerricStore.Flow` |
| Queue helper | `FerricStore.Queue` |
| Workflow helper | `FerricStore.Workflow` |
| Codecs | `FerricStore.Codec.Raw`, `FerricStore.Codec.Term` |

The Python SDK currently has a richer worker runtime and more governance/admin
helpers. The Elixir SDK focuses on the current stable native protocol surface and
keeps unsupported server commands available through `FerricStore.command/4`.
