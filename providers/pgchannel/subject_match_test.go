package pgchannel_test

import (
	"reflect"
	"strings"
	"testing"

	pgchannel "github.com/GoCodeAlone/workflow-plugin-eventbus/providers/pgchannel"
)

// TestSubjectFilter_Literal verifies the literal-subject path: no wildcard,
// no comma → a parameterised equality predicate is returned with the subject
// preserved verbatim as the single arg.
func TestSubjectFilter_Literal(t *testing.T) {
	sql, args := pgchannel.GenFilter("bmw.fulfillment.created", "$1")
	if got, want := sql, "(subject = $1)"; got != want {
		t.Errorf("sql = %q, want %q", got, want)
	}
	if want := []any{"bmw.fulfillment.created"}; !reflect.DeepEqual(args, want) {
		t.Errorf("args = %#v, want %#v", args, want)
	}
}

// TestSubjectFilter_PrefixWildcard verifies that "<prefix>.>" maps to a LIKE
// predicate with the SQL wildcard '%' substituted for the ">" token.
func TestSubjectFilter_PrefixWildcard(t *testing.T) {
	sql, args := pgchannel.GenFilter("bmw.>", "$1")
	if got, want := sql, "(subject LIKE $1)"; got != want {
		t.Errorf("sql = %q, want %q", got, want)
	}
	if want := []any{"bmw.%"}; !reflect.DeepEqual(args, want) {
		t.Errorf("args = %#v, want %#v", args, want)
	}
}

// TestSubjectFilter_CommaList verifies that comma-separated subjects become
// a single ANY($N::text[]) predicate with a []string arg, and that each
// component is trimmed of surrounding whitespace.
func TestSubjectFilter_CommaList(t *testing.T) {
	sql, args := pgchannel.GenFilter("bmw.delivered, bmw.cancelled", "$1")
	if got, want := sql, "(subject = ANY($1::text[]))"; got != want {
		t.Errorf("sql = %q, want %q", got, want)
	}
	if want := []any{[]string{"bmw.delivered", "bmw.cancelled"}}; !reflect.DeepEqual(args, want) {
		t.Errorf("args = %#v, want %#v", args, want)
	}
}

// TestSubjectFilter_RejectsSingleTokenWildcard pins the v0.2.0 contract that
// per-segment "*" wildcards are unsupported and surface a clear error
// pointing to the supported alternatives.
func TestSubjectFilter_RejectsSingleTokenWildcard(t *testing.T) {
	_, _, err := pgchannel.GenFilterValidated("bmw.*", "$1")
	if err == nil {
		t.Fatal("expected error for single-token wildcard, got nil")
	}
	if !strings.Contains(err.Error(), "single-token wildcard") {
		t.Errorf("error %q should mention 'single-token wildcard'", err.Error())
	}
}

// TestSubjectFilterValidated_PassesThrough sanity-checks that the validated
// variant returns identical output to GenFilter for accepted input.
func TestSubjectFilterValidated_PassesThrough(t *testing.T) {
	sql, args, err := pgchannel.GenFilterValidated("bmw.fulfillment.created", "$1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, want := sql, "(subject = $1)"; got != want {
		t.Errorf("sql = %q, want %q", got, want)
	}
	if want := []any{"bmw.fulfillment.created"}; !reflect.DeepEqual(args, want) {
		t.Errorf("args = %#v, want %#v", args, want)
	}
}
