// Package pgchannel — providers.RuntimeBroker implementation backed by
// Postgres LISTEN/NOTIFY + polling fallback + advisory locks.
//
// This file holds the subject-matcher: converts the
// ConsumerConfig.filter_subject DSL into a parameterised SQL predicate
// applied inside the polling SELECT.
//
// Supported filter syntax (intentionally minimal for v0.2.0):
//
//   - literal:           "bmw.fulfillment.created"           → subject = $N
//   - prefix wildcard:   "bmw.>"                              → subject LIKE 'bmw.%'
//   - comma-separated:   "bmw.delivered,bmw.cancelled"        → subject = ANY($N::text[])
//
// Single-token wildcard (`bmw.*`) is REJECTED by GenFilterValidated; the
// pgchannel provider does not implement per-segment matching in v0.2.0.
// Use prefix-wildcard or comma-list instead.
package pgchannel

import (
	"fmt"
	"strings"
)

// GenFilter generates a parameterised SQL predicate fragment + arg slice for
// the given filter_subject. The caller supplies the placeholder string
// (e.g. "$1") that the returned SQL will reference; the caller is responsible
// for stitching args into the surrounding query.
//
// The returned sql is always wrapped in parentheses so it can be AND-ed into
// a larger WHERE clause without ambiguity.
//
// GenFilter does NOT validate against the single-token-wildcard "*" form;
// for input that may originate from user config use GenFilterValidated.
func GenFilter(filter, placeholder string) (sql string, args []any) {
	if strings.Contains(filter, ",") {
		parts := strings.Split(filter, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			out = append(out, strings.TrimSpace(p))
		}
		return fmt.Sprintf("(subject = ANY(%s::text[]))", placeholder), []any{out}
	}
	if strings.HasSuffix(filter, ".>") {
		prefix := strings.TrimSuffix(filter, ".>")
		return fmt.Sprintf("(subject LIKE %s)", placeholder), []any{prefix + ".%"}
	}
	return fmt.Sprintf("(subject = %s)", placeholder), []any{filter}
}

// GenFilterValidated wraps GenFilter with strict validation: rejects the
// single-token wildcard form ("bmw.*") with an error that points users to
// the supported alternatives.
//
// Callers that receive filter_subject from user config should call this
// variant; internal callers that have already validated their input may
// call GenFilter directly.
func GenFilterValidated(filter, placeholder string) (sql string, args []any, err error) {
	if strings.Contains(filter, "*") {
		return "", nil, fmt.Errorf(
			"subject filter %q: single-token wildcard (*) is not supported in pgchannel v0.2.0; "+
				"use prefix wildcard (pat.>) or comma-separated list (a,b,c) instead",
			filter,
		)
	}
	sql, args = GenFilter(filter, placeholder)
	return sql, args, nil
}
