package providers

import "fmt"

// DeployTarget identifies the deployment platform for an event-bus cluster.
type DeployTarget string

const (
	// TargetDigitalOceanApp deploys to DigitalOcean App Platform.
	TargetDigitalOceanApp DeployTarget = "digitalocean.app_platform"
	// TargetDigitalOceanManagedKafka deploys to DigitalOcean Managed Kafka.
	TargetDigitalOceanManagedKafka DeployTarget = "digitalocean.managed_kafka"
	// TargetAWSECS deploys to AWS ECS (Elastic Container Service).
	TargetAWSECS DeployTarget = "aws.ecs"
	// TargetAWSEKS deploys to AWS EKS (Elastic Kubernetes Service).
	TargetAWSEKS DeployTarget = "aws.eks"
	// TargetAWSManagedKafka deploys to AWS MSK (Managed Streaming for Apache Kafka).
	TargetAWSManagedKafka DeployTarget = "aws.msk"
	// TargetAWSKinesis deploys to AWS Kinesis Data Streams.
	TargetAWSKinesis DeployTarget = "aws.kinesis"
	// TargetKubernetes deploys to a generic Kubernetes cluster.
	TargetKubernetes DeployTarget = "kubernetes"
	// TargetSelfHosted deploys to a self-managed host (Docker, bare metal).
	TargetSelfHosted DeployTarget = "self_hosted"
)

// supportedTargets encodes the provider × DeployTarget compatibility matrix.
// Each provider maps to the set of deploy targets it supports.
// Combos absent from this map are rejected at config-load time.
var supportedTargets = map[string]map[DeployTarget]bool{
	"nats": {
		TargetDigitalOceanApp: true,
		TargetAWSECS:          true,
		TargetAWSEKS:          true,
		TargetKubernetes:      true,
		TargetSelfHosted:      true,
	},
	"kafka": {
		TargetDigitalOceanManagedKafka: true,
		TargetAWSManagedKafka:          true,
		TargetKubernetes:               true,
		TargetSelfHosted:               true,
	},
	"kinesis": {
		TargetAWSKinesis: true,
	},
}

// ValidateProviderTarget returns an error if the provider × target combination
// is unsupported. It is called at config-load time so misconfigured deployments
// are rejected before any IaC resources are emitted.
func ValidateProviderTarget(provider string, target DeployTarget) error {
	targets, ok := supportedTargets[provider]
	if !ok {
		return fmt.Errorf("eventbus: unknown provider %q — supported providers are: nats, kafka, kinesis", provider)
	}
	if !targets[target] {
		return fmt.Errorf("eventbus: provider %q does not support deploy target %q", provider, target)
	}
	return nil
}
