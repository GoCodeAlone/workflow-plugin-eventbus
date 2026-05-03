package providers_test

import (
	"testing"

	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
)

// TestValidateProviderTarget_CompatibilityMatrix asserts the full provider×target
// compatibility matrix. Every supported combo must return nil; every unsupported
// combo must return a non-nil error.
//
// IMPORTANT: add a new row whenever a new DeployTarget or provider is added —
// this test gates the entire matrix, not just individual combos.
func TestValidateProviderTarget_CompatibilityMatrix(t *testing.T) {
	type combo struct {
		provider string
		target   providers.DeployTarget
		wantErr  bool
	}
	cases := []combo{
		// ── nats: supported targets ────────────────────────────────────────────
		{"nats", providers.TargetDigitalOceanApp, false},
		{"nats", providers.TargetAWSECS, false},
		{"nats", providers.TargetAWSEKS, false},
		{"nats", providers.TargetKubernetes, false},
		{"nats", providers.TargetSelfHosted, false},
		// ── nats: unsupported targets ──────────────────────────────────────────
		{"nats", providers.TargetDigitalOceanManagedKafka, true},
		{"nats", providers.TargetAWSManagedKafka, true},
		{"nats", providers.TargetAWSKinesis, true},

		// ── kafka: supported targets ───────────────────────────────────────────
		{"kafka", providers.TargetDigitalOceanManagedKafka, false},
		{"kafka", providers.TargetAWSManagedKafka, false},
		{"kafka", providers.TargetKubernetes, false},
		{"kafka", providers.TargetSelfHosted, false},
		// ── kafka: unsupported targets ─────────────────────────────────────────
		{"kafka", providers.TargetDigitalOceanApp, true},
		{"kafka", providers.TargetAWSECS, true},
		{"kafka", providers.TargetAWSEKS, true},
		{"kafka", providers.TargetAWSKinesis, true},

		// ── kinesis: supported targets ─────────────────────────────────────────
		{"kinesis", providers.TargetAWSKinesis, false},
		// ── kinesis: unsupported targets ───────────────────────────────────────
		{"kinesis", providers.TargetDigitalOceanApp, true},      // key case per task spec
		{"kinesis", providers.TargetDigitalOceanManagedKafka, true},
		{"kinesis", providers.TargetAWSECS, true},
		{"kinesis", providers.TargetAWSEKS, true},
		{"kinesis", providers.TargetAWSManagedKafka, true},
		{"kinesis", providers.TargetKubernetes, true},
		{"kinesis", providers.TargetSelfHosted, true},
	}

	for _, tc := range cases {
		t.Run(tc.provider+"×"+string(tc.target), func(t *testing.T) {
			err := providers.ValidateProviderTarget(tc.provider, tc.target)
			if tc.wantErr && err == nil {
				t.Errorf("expected validation error for %s × %s, got nil", tc.provider, tc.target)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error for %s × %s: %v", tc.provider, tc.target, err)
			}
		})
	}
}

// TestValidateProviderTarget_UnknownProvider asserts that an unrecognised
// provider name returns an error regardless of target.
func TestValidateProviderTarget_UnknownProvider(t *testing.T) {
	err := providers.ValidateProviderTarget("rabbitmq", providers.TargetDigitalOceanApp)
	if err == nil {
		t.Error("expected error for unknown provider 'rabbitmq', got nil")
	}
}
