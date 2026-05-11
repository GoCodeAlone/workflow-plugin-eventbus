package providers_test

import (
	"testing"

	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
)

// brokerImplementations is populated by each provider sub-package's TestMain
// (or compile-time wiring) so this shared test can exercise every provider
// against a uniform conformance suite. Empty in this package — the suite
// itself runs from the provider sub-packages where concrete impls are wired.
var brokerImplementations []providers.RuntimeBroker

// TestRuntimeBroker_InterfaceCompiles is a compile-time check that the
// providers.RuntimeBroker interface exists and that brokerImplementations
// can hold values of it. The conformance suite (publish/subscribe round-trip
// against each provider) is added in subsequent tasks (Task 4 for NATS,
// Task 8 for pgchannel).
func TestRuntimeBroker_InterfaceCompiles(t *testing.T) {
	// Compile-time: brokerImplementations must be typed []providers.RuntimeBroker.
	// Runtime: list starts empty; sub-packages register via init() in later tasks.
	_ = brokerImplementations
	if brokerImplementations == nil {
		// Allowed; initialization is deferred to Task 4 / Task 8.
		t.Log("brokerImplementations not yet populated; conformance impls register in providers/{nats,pgchannel} packages")
	}
}

// Compile-time interface satisfaction checks live in the provider sub-packages
// (providers/nats/runtime.go, providers/pgchannel/runtime.go) where the concrete
// types are defined. This package only declares the interface.
