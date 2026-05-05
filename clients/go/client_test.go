package chronos

import "testing"

func TestStaleRouteErrorFalseOnNil(t *testing.T) {
	if isStaleRouteError(nil) {
		t.Fatal("nil error should not be treated as stale route")
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
