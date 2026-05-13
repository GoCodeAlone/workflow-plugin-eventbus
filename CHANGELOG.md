# Changelog

## v0.3.1 — 2026-05-13

### Fixed

- `trigger.eventbus.subscribe` no longer fails engine build with `config.name is required` when the pipeline supplies the BMW-style `consumer` and `bus` keys instead of the proto-canonical `name` and `broker_ref`. The plugin's `CreateTrigger` legacy-map handler now treats `consumer` as an alias for `name` and `bus` as an alias for `broker_ref`; canonical keys win when both are supplied. When only the consumer name is supplied, `stream_name` (and `broker_ref` if unset) are inherited from the matching `infra.eventbus.consumer` module's registered `ConsumerConfig` via `eventbus.GetConsumerByName`. Unblocks 7 BMW pipelines (`bmw-status-poller`, `bmw-contributor-notifier`, `bmw-settlement-runner`, `bmw-audit-appender`, `bmw-recipient-notifier`, `bmw-financial-health`, `bmw-fulfillment-dispatcher`) against workflow v0.51.5. Proto schema unchanged.
- `plugin.json` version bumped from the stale `0.1.0` to `0.3.1`; download URLs updated to match. Previous `v0.2.0` / `v0.3.0` releases shipped with the v0.1.0 manifest version inadvertently.

### Added

- CI workflow `.github/workflows/workflow-compat.yml` that builds the plugin and runs `wfctl validate` against a minimal `trigger.eventbus.subscribe` config that omits `name` (uses `consumer` alias), guarding against regressions of the BMW compatibility path.

## v0.2.0 — 2026-05-11

### BREAKING

- Module types renamed: `infra.eventbus*` → `eventbus.*`. The `infra.` prefix is workflow's IaC-convention prefix; eventbus modules are runtime modules, not IaC. Migration: see `MIGRATION.md`.
- `ClusterConfig` requires `broker_target` for pgchannel provider (`in_process`) and `deploy_target` for nats/kafka/kinesis providers (existing rules).
- `StreamConfig.broker_ref` and `ConsumerConfig.broker_ref` are now load-bearing — point at the broker module instance name; required when the plugin is used with the new RuntimeBroker dispatch path. Legacy single-broker deploys continue to work via the ambiguity-detecting fallback in `LookupRuntimeWithFallback`.

### Added

- New `pgchannel` provider: Postgres LISTEN/NOTIFY + polling fallback + per-consumer advisory locks + max_deliver enforcement via `eventbus_event_deliveries` tracking. Useful for low-traffic deployments wanting zero broker infrastructure.
- `providers.RuntimeBroker` abstraction (Connect / EnsureStream / EnsureConsumer / Publish / Consume / Subscribe / Ack); NATS and pgchannel both implement.
- `ClusterConfig.dsn`, `poll_interval`, `broker_target`, `max_conns` (proto fields 10-13).
- Stream + consumer modules' `Start()` now idempotently ensures their JetStream/Postgres resources via the runtime abstraction. Init() still registers config for legacy compat.
- `module.LookupRuntime(brokerRef)` + `RegisterBrokerInstance` registry for cross-module dispatch.
- pgchannel migrations are NOT shipped with the plugin — consumer applications add the three SQL migration files (`eventbus_streams`, `eventbus_events` with headers JSONB + correlation_id TEXT, `eventbus_consumers`, `eventbus_event_deliveries`) to their migration pipeline. Reference schema: `providers/pgchannel/internal/testutil/schema.sql`.

### Internal

- NATS publish/subscribe/ack code extracted from step factories + trigger module into `providers/nats/runtime.go`. No behavior change for existing NATS consumers.
- Step factories + trigger module now dispatch through `RuntimeBroker`.
- Legacy NATS-only helpers (`DefaultBusConn`, `GetOrDialNATSConn`, `Register/Get/UnregisterNATSConn`) retained as `// Deprecated:` for source-compat; removal slated for v1.0.0.
- `clusterModule.runtime`/`conn` fields guarded by mutex (`sync.RWMutex`) for concurrent Start/Stop/LookupRuntime safety.
- Stream name validation at `EnsureStream` boundary (`[a-zA-Z0-9_]+`) for pgchannel.
- AckToken format for pgchannel changed to `<stream>:<consumer>:<id>` (3-part) to prevent cross-stream pollution.

### Co-released

- workflow-registry manifest update (separate PR): module type names match v0.2.0 plugin.json.

**Full changelog:** https://github.com/GoCodeAlone/workflow-plugin-eventbus/compare/v0.1.0...v0.2.0
