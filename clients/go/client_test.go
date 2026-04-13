package chronos

import "testing"

func TestStaleRouteErrorFalseOnNil(t *testing.T) {
	if isStaleRouteError(nil) {
		t.Fatal("nil error should not be treated as stale route")
	}
}
