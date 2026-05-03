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

// ── AWS ECS ──────────────────────────────────────────────────────────────────

// TestNATSStub_AWSECS_ReturnsNotImplemented asserts that the AWS ECS deploy
// target returns a non-nil "not implemented" error (pilot stub).
func TestNATSStub_AWSECS_ReturnsNotImplemented(t *testing.T) {
	p := nats.New()
	_, err := p.Resources(minimalCfg(), providers.TargetAWSECS)
	if err == nil {
		t.Fatal("expected not-implemented error for aws.ecs, got nil")
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Errorf("error %q does not contain 'not implemented'", err.Error())
	}
}

// TestNATSStub_AWSECS_ErrorMentionsTarget asserts the error message names the target.
func TestNATSStub_AWSECS_ErrorMentionsTarget(t *testing.T) {
	p := nats.New()
	_, err := p.Resources(minimalCfg(), providers.TargetAWSECS)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "aws.ecs") {
		t.Errorf("error %q does not mention target 'aws.ecs'", err.Error())
	}
}

// ── AWS EKS ──────────────────────────────────────────────────────────────────

// TestNATSStub_AWSEKS_ReturnsNotImplemented asserts that the AWS EKS deploy
// target returns a non-nil "not implemented" error (pilot stub).
func TestNATSStub_AWSEKS_ReturnsNotImplemented(t *testing.T) {
	p := nats.New()
	_, err := p.Resources(minimalCfg(), providers.TargetAWSEKS)
	if err == nil {
		t.Fatal("expected not-implemented error for aws.eks, got nil")
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Errorf("error %q does not contain 'not implemented'", err.Error())
	}
}

// TestNATSStub_AWSEKS_ErrorMentionsTarget asserts the error message names the target.
func TestNATSStub_AWSEKS_ErrorMentionsTarget(t *testing.T) {
	p := nats.New()
	_, err := p.Resources(minimalCfg(), providers.TargetAWSEKS)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "aws.eks") {
		t.Errorf("error %q does not mention target 'aws.eks'", err.Error())
	}
}

// ── Kubernetes ───────────────────────────────────────────────────────────────

// TestNATSStub_Kubernetes_ReturnsNotImplemented asserts that the Kubernetes deploy
// target returns a non-nil "not implemented" error (pilot stub).
func TestNATSStub_Kubernetes_ReturnsNotImplemented(t *testing.T) {
	p := nats.New()
	_, err := p.Resources(minimalCfg(), providers.TargetKubernetes)
	if err == nil {
		t.Fatal("expected not-implemented error for kubernetes, got nil")
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Errorf("error %q does not contain 'not implemented'", err.Error())
	}
}

// TestNATSStub_Kubernetes_ErrorMentionsTarget asserts the error message names the target.
func TestNATSStub_Kubernetes_ErrorMentionsTarget(t *testing.T) {
	p := nats.New()
	_, err := p.Resources(minimalCfg(), providers.TargetKubernetes)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "kubernetes") {
		t.Errorf("error %q does not mention target 'kubernetes'", err.Error())
	}
}

// ── Stub return shape ─────────────────────────────────────────────────────────

// TestNATSStub_AllReturnNilResources asserts each stub target returns a nil
// (not empty) resource slice alongside the error.
func TestNATSStub_AllReturnNilResources(t *testing.T) {
	targets := []providers.DeployTarget{
		providers.TargetAWSECS,
		providers.TargetAWSEKS,
		providers.TargetKubernetes,
	}
	p := nats.New()
	for _, target := range targets {
		t.Run(string(target), func(t *testing.T) {
			res, err := p.Resources(minimalCfg(), target)
			if err == nil {
				t.Fatalf("expected error for %s, got nil", target)
			}
			if res != nil {
				t.Errorf("expected nil resource slice for stub target %s, got %v", target, res)
			}
		})
	}
}
