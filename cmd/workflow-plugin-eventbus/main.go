// Command workflow-plugin-eventbus is a workflow engine external plugin that
// provisions durable event-bus clusters (NATS / Kafka / Kinesis) as IaC and
// exposes typed pipeline steps for publish / consume operations.
//
// Status: pre-pilot scaffold — provider implementations are in progress.
package main

// version is stamped at release time via goreleaser ldflags (-X main.version=<tag>).
var version = "dev"

func main() {
	// TODO(Task 17): wire sdk.Serve(plugin.New()) once Provider interface is implemented.
	panic("workflow-plugin-eventbus: provider implementation pending — scaffold only; see github.com/GoCodeAlone/workflow-plugin-eventbus")
}
