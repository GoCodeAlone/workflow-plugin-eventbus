# Migration: v0.1.0 → v0.2.0

## TL;DR

1. Bump pin: `.wfctl-lock.yaml` → `workflow-plugin-eventbus@v0.2.0`.
2. Rename module types in YAML configs:
   - `infra.eventbus` → `eventbus.broker`
   - `infra.eventbus.stream` → `eventbus.stream`
   - `infra.eventbus.consumer` → `eventbus.consumer`
3. If you used `infra.eventbus*` blocks in `infra.yaml` (the wfctl IaC config): MOVE them to `app.yaml`. They're runtime modules, not IaC resources.
4. Add `broker_ref: <broker-module-name>` to each stream + consumer module config (point at the broker instance — same name you put on the broker module's `name:` field).
5. New: if you want zero NATS infrastructure, switch broker `provider: nats` → `provider: pgchannel` and add `broker_target: in_process` + `dsn: ${DATABASE_URL}` + `max_conns: 32` (sized for your consumer count: 2*N + 4 where N is consumer module count).
6. For pgchannel users: apply the 3 reference migrations from `providers/pgchannel/internal/testutil/schema.sql` to your Postgres (numbered per your project's migration convention).

## Sed-friendly rename

```bash
# Inside your project root:
find . -name '*.yaml' -type f -exec sed -i.bak \
    -e 's|infra\.eventbus\.consumer|eventbus.consumer|g' \
    -e 's|infra\.eventbus\.stream|eventbus.stream|g' \
    -e 's|infra\.eventbus|eventbus.broker|g' \
    {} +
# Review the .bak files, then `find . -name '*.bak' -delete`.
```

NOTE: order matters — replace `infra.eventbus.consumer` and `infra.eventbus.stream` BEFORE `infra.eventbus` to avoid eating the trailing path.

## Why the rename

The `infra.*` prefix is workflow's IaC convention. wfctl filters config blocks by it when running plan/apply. Eventbus modules are runtime connectivity + JetStream/Postgres state managers, NOT IaC. Marking them `infra.*` caused wfctl to try planning them as cloud resources (no plugin realized them → 8 errors at apply time). The `eventbus.*` name corrects the category error.

## Broker_ref required

Pre-v0.2.0, stream and consumer modules looked up their NATS connection by global registry first-broker-wins. v0.2.0 introduces a typed RuntimeBroker abstraction that supports multiple broker instances per process; each stream and consumer must declare which broker it binds to.

Configs with single broker can omit `broker_ref` and rely on the auto-fallback (warning logged if multiple brokers registered without explicit broker_ref).

## pgchannel provider

For low-traffic deployments, the new pgchannel provider eliminates the NATS broker entirely — it implements pub/sub atop Postgres LISTEN/NOTIFY + polling. Tradeoff: higher latency, lower throughput, but no broker infrastructure to provision. When traffic justifies NATS, swap `provider: pgchannel` → `provider: nats`; the call sites (step.eventbus.publish/consume/ack + trigger.eventbus.subscribe) remain unchanged.

Reference schema migrations live at `providers/pgchannel/internal/testutil/schema.sql`. Consumer projects ship these directly in their own migration pipeline.

## Legacy NATS helpers

`DefaultBusConn`, `GetOrDialNATSConn`, `Register/Get/UnregisterNATSConn` are deprecated but functional. They issue runtime warnings via godoc-style `// Deprecated:` comments. New code should use `module.LookupRuntime(brokerRef)` instead. Helpers slated for removal in v1.0.0.
