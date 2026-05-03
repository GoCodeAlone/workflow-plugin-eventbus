# workflow-plugin-eventbus

**Status: pre-pilot scaffold** — Provider implementations are in progress (BMW E2E fulfillment pilot, PR 5).

A [workflow](https://github.com/GoCodeAlone/workflow) external plugin that provisions durable event-bus clusters as IaC and exposes typed pipeline steps for publish/consume operations.

## Providers

| Provider | Deploy targets |
|---|---|
| `nats` | DO App Platform, AWS ECS/EKS, Kubernetes StatefulSet |
| `kafka` | DO Managed Kafka, AWS MSK, Kubernetes (Strimzi) |
| `kinesis` | AWS (Kinesis Data Streams) |

## Usage

### Declare a cluster

```yaml
modules:
  - name: my-events
    type: infra.eventbus
    config:
      provider: nats
      deploy_target: digitalocean.app_platform
      version: "2.10"
      replicas: 2
      jetstream:
        enabled: true
        max_storage_bytes: 53687091200  # 50 GB
```

### Declare streams and consumers

```yaml
  - name: my-stream
    type: infra.eventbus.stream
    config:
      name: MY_EVENTS
      subjects: ["events.>"]
      retention_policy: RETENTION_POLICY_LIMITS
      max_bytes: 10737418240  # 10 GB

  - name: my-consumer
    type: infra.eventbus.consumer
    config:
      stream_name: MY_EVENTS
      name: my-handler
      filter_subject: "events.>"
      ack_policy: ACK_POLICY_EXPLICIT
      max_deliver: 5
```

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

## Development

```sh
# Regenerate proto bindings after editing proto/eventbus.proto
make proto-gen

# Build
make build

# Test
make test
```

## Planned providers

- `nats` — NATS JetStream (in progress)
- `kafka` — stub (in progress)
- `kinesis` — stub (in progress)
