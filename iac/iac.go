// Package iac defines typed IaC primitives emitted by eventbus Providers.
// These are consumed by downstream IaC provider plugins
// (workflow-plugin-digitalocean, workflow-plugin-aws, etc.) which realize
// them against the target cloud. Zero map[string]any — all fields are typed.
package iac

// Resource is a typed IaC resource declaration.
// The Kind field identifies which IaC provider plugin realizes this resource.
type Resource struct {
	// Kind is the resource type, e.g. "infra.app", "infra.ecs.task_definition",
	// "infra.k8s.statefulset", "infra.aws.kinesis_stream".
	Kind string
	// Name is the resource instance name, unique within the deployment environment.
	Name string
	// Properties carries resource-type-specific configuration key/value pairs.
	// Schema per Kind is defined by the downstream IaC plugin that realizes this resource.
	// Examples: image, replicas, storage_size, port, shard_count.
	Properties map[string]string
	// Labels are metadata tags attached to the resource (e.g. "env": "prod", "team": "bmw").
	// These are not config — they are used for filtering, cost allocation, and observability.
	Labels map[string]string
}

// Output is a single state value with explicit sensitivity marking.
// Sensitive outputs (connection strings, credentials, ARNs with embedded secrets)
// must be marked Sensitive: true to prevent accidental logging in plan output.
type Output struct {
	// Value is the resolved output string.
	Value string
	// Sensitive marks outputs that must not appear in plain-text plan/log output.
	// The workflow engine suppresses sensitive outputs unless --show-sensitive is passed.
	Sensitive bool
}

// State holds resolved outputs from previously provisioned resources.
// Each output is explicitly typed and sensitivity-marked.
type State struct {
	// Outputs maps output key to a typed, sensitivity-marked Output value.
	// Example: "uri" → Output{Value: "nats://host:4222", Sensitive: true}
	Outputs map[string]Output
}

// Output returns the value for key and whether it was present.
// The Sensitive flag is not surfaced here — callers that need it should read
// s.Outputs[key] directly.
func (s State) Output(key string) (string, bool) {
	out, ok := s.Outputs[key]
	return out.Value, ok
}
