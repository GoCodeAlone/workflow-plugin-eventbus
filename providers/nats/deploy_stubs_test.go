package nats_test

import (
	"strings"
	"testing"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers/nats"
)

// minimalCfg returns a minimal ClusterConfig suitable for stub target tests.
func minimalCfg() *eventbusv1.ClusterConfig {
	return &eventbusv1.ClusterConfig{Version: "2.10", Replicas: 1}
}

// TestNATSStub_ErrorBehavior asserts that every not-yet-activated deploy target
// (ECS, EKS, Kubernetes) and every default-arm target (SelfHosted) satisfy the
// stub contract:
//
//  1. Resources() returns a non-nil error.
//  2. The resource slice is nil (not empty).
//  3. The error contains "not implemented".
//  4. The error mentions the target name so callers can diagnose config mistakes.
func TestNATSStub_ErrorBehavior(t *testing.T) {
	cases := []struct {
		target  providers.DeployTarget
		mention string // expected substring in error message
	}{
		// Explicit stub files (Task 20).
		{providers.TargetAWSECS, "aws.ecs"},
		{providers.TargetAWSEKS, "aws.eks"},
		{providers.TargetKubernetes, "kubernetes"},
		// Default arm — no dedicated stub file; falls through to the default branch.
		{providers.TargetSelfHosted, "self_hosted"},
	}

	p := nats.New()
	for _, tc := range cases {
		t.Run(string(tc.target), func(t *testing.T) {
			res, err := p.Resources(minimalCfg(), tc.target)

			if err == nil {
				t.Fatalf("expected error for target %q, got nil", tc.target)
			}
			if res != nil {
				t.Errorf("expected nil resource slice for target %q, got %v", tc.target, res)
			}
			if !strings.Contains(err.Error(), "not implemented") {
				t.Errorf("error for %q does not contain 'not implemented': %v", tc.target, err)
			}
			if !strings.Contains(err.Error(), tc.mention) {
				t.Errorf("error for %q does not mention %q: %v", tc.target, tc.mention, err)
			}
		})
	}
}
