package chronos

import (
	"testing"

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

func TestStaleRouteErrorFalseOnNil(t *testing.T) {
	if isStaleRouteError(nil) {
		t.Fatal("nil error should not be treated as stale route")
	}
}

func TestStaleRouteErrorRequiresChronosRouteDetail(t *testing.T) {
	err := grpcstatus.Error(codes.FailedPrecondition, "non-route precondition failed")
	if isStaleRouteError(err) {
		t.Fatal("plain FAILED_PRECONDITION should not be treated as stale route")
	}
}

func TestRouteRecoveryIncludesUnavailableOnly(t *testing.T) {
	if !isRouteRecoveryError(grpcstatus.Error(codes.Unavailable, "owner unavailable")) {
		t.Fatal("UNAVAILABLE should trigger route recovery")
	}
	if isRouteRecoveryError(grpcstatus.Error(codes.DeadlineExceeded, "allocation timed out")) {
		t.Fatal("DEADLINE_EXCEEDED should not retry a possibly committed allocation")
	}
}

func TestTransportCredentialsDefaultToTLS(t *testing.T) {
	creds, err := transportCredentials(transportConfig{})
	if err != nil {
		t.Fatalf("transportCredentials returned error: %v", err)
	}
	if creds.Info().SecurityProtocol != "tls" {
		t.Fatalf("expected tls security protocol, got %q", creds.Info().SecurityProtocol)
	}
}

func TestTransportCredentialsAllowExplicitInsecureOptIn(t *testing.T) {
	creds, err := transportCredentials(transportConfig{insecure: true})
	if err != nil {
		t.Fatalf("transportCredentials returned error: %v", err)
	}
	if creds.Info().SecurityProtocol != "insecure" {
		t.Fatalf("expected insecure security protocol, got %q", creds.Info().SecurityProtocol)
	}
}

func TestTransportCredentialsRejectPartialClientIdentity(t *testing.T) {
	if _, err := transportCredentials(transportConfig{certPEM: []byte("cert")}); err == nil {
		t.Fatal("expected partial client identity to fail")
	}
}
