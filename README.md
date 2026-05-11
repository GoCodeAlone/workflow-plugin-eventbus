# workflow-plugin-eventbus

A [workflow](https://github.com/GoCodeAlone/workflow) external plugin that provisions durable event-bus clusters and exposes typed pipeline steps for publish/consume operations.

> **v0.2.0 module-type rename:** `infra.eventbus*` → `eventbus.*`. The `infra.` prefix is reserved for IaC modules; eventbus modules are runtime modules. If you're upgrading from v0.1.0, see [MIGRATION.md](MIGRATION.md).

## Providers

| Provider | Deploy targets | Notes |
|---|---|---|
| `nats` | DO App Platform, AWS ECS/EKS, Kubernetes StatefulSet | JetStream-backed; durable streams + consumers |
| `pgchannel` | `in_process` | Postgres LISTEN/NOTIFY + polling fallback; no broker infrastructure |
| `kafka` | DO Managed Kafka, AWS MSK, Kubernetes (Strimzi) | scaffold |
| `kinesis` | AWS (Kinesis Data Streams) | scaffold |

### pgchannel — Postgres-backed broker

For low-traffic deployments, the `pgchannel` provider eliminates the need for a NATS/Kafka broker by implementing pub/sub atop Postgres LISTEN/NOTIFY with a polling fallback for guaranteed delivery. Per-consumer advisory locks + a `eventbus_event_deliveries` tracking table enforce `max_deliver` semantics.

Tradeoffs: higher latency, lower throughput than NATS. When traffic justifies a real broker, swap `provider: pgchannel` → `provider: nats`; pipeline call sites (`step.eventbus.publish` / `step.eventbus.consume` / `step.eventbus.ack` / `trigger.eventbus.subscribe`) remain unchanged.

Reference schema migrations live at `providers/pgchannel/internal/testutil/schema.sql`. Consumer projects ship these directly in their own migration pipeline — the plugin does NOT embed them.

## Usage

### Declare a cluster (NATS)

```yaml
modules:
  - name: my-events
    type: eventbus.broker
    config:
      provider: nats
      deploy_target: digitalocean.app_platform
      version: "2.10"
      replicas: 2
      jetstream:
        enabled: true
        max_storage_bytes: 53687091200  # 50 GB
```

### Declare a cluster (pgchannel)

```yaml
modules:
  - name: my-events
    type: eventbus.broker
    config:
      provider: pgchannel
      broker_target: in_process
      dsn: ${DATABASE_URL}
      poll_interval: 5s
      max_conns: 32  # size as 2*N + 4 where N = consumer module count
```

### Declare streams and consumers

```yaml
  - name: my-stream
    type: eventbus.stream
    config:
      broker_ref: my-events       # points at the broker module name above
      name: MY_EVENTS
      subjects: ["events.>"]
      retention_policy: RETENTION_POLICY_LIMITS
      max_bytes: 10737418240  # 10 GB

  - name: my-consumer
    type: eventbus.consumer
    config:
      broker_ref: my-events       # points at the broker module name above
      stream_name: MY_EVENTS
      name: my-handler
      filter_subject: "events.>"
      ack_policy: ACK_POLICY_EXPLICIT
      max_deliver: 5
```

`broker_ref` is required when multiple broker instances are registered in the same process. Single-broker deployments may omit it (the runtime falls back to the sole registered broker and logs a warning if ambiguity is detected).

### Publish from a pipeline step

```yaml
steps:
  - name: publish
    type: step.eventbus.publish
    config:
      subject: events.created
      payload: '{{ toJson .input }}'
```

### Subscribe trigger

```yaml
my-handler:
  trigger:
    type: trigger.eventbus.subscribe
    config:
      stream_name: MY_EVENTS
      name: my-handler
      filter_subject: "events.>"
      ack_policy: ACK_POLICY_EXPLICIT
  steps:
    - name: ack
      type: step.eventbus.ack
      config:
        ack_token: '{{ .nats.message.ack_token }}'
```

## Upgrading from v0.1.0

See [MIGRATION.md](MIGRATION.md) for the full guide. Highlights:

- Rename `infra.eventbus*` module types to `eventbus.*` in YAML configs.
- Move eventbus blocks out of `infra.yaml` into `app.yaml` (runtime, not IaC).
- Add `broker_ref` to each stream and consumer config.
- Optionally adopt the new `pgchannel` provider for broker-free deployments.

## Development

```sh
# Regenerate proto bindings after editing proto/eventbus.proto
make proto-gen

# Build
make build

# Test
make test
```
