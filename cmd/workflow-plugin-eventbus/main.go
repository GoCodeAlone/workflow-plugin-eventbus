// Command workflow-plugin-eventbus is a workflow engine external plugin that
// provisions durable event-bus clusters (NATS / Kafka / Kinesis) as IaC and
// exposes typed pipeline steps for publish / consume operations.
package main

import sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"

// version is stamped at release time via goreleaser ldflags (-X main.version=<tag>).
var version = "dev"

func main() {
	sdk.Serve(&eventbusPlugin{})
}
