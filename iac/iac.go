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
	// Labels are metadata key/value pairs attached to the resource (e.g. "env": "prod").
	Labels map[string]string
}

// State holds resolved outputs from previously provisioned resources.
// All values are typed strings (connection strings, ARNs, cluster IDs, etc.).
// Sensitive outputs (connection strings with credentials) are marked as such
// by convention: keys ending in "_uri" or "_dsn" are treated as sensitive.
type State struct {
	// Outputs maps output key to value, e.g. "uri" → "nats://host:4222".
	Outputs map[string]string
}

// Output returns the state output for key, and whether it was present.
func (s State) Output(key string) (string, bool) {
	v, ok := s.Outputs[key]
	return v, ok
}
