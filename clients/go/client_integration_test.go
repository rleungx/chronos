package chronos

import (
	"context"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	tsov1 "github.com/rleungx/chronos/gen/proto/tso/v1"
)

type fakeChronosServer struct {
	tsov1.UnimplementedTimelineRouteServiceServer
	tsov1.UnimplementedTimestampServiceServer

	mu              sync.Mutex
	route           *tsov1.TimelineRoute
	ensureCalls     int
	getRouteCalls   int
	allocateCalls   int
	staleOnAllocate bool
	nextStartTso    uint64
	lastRequestID   string
	returnedRequest []string
	lastEnsureTier  tsov1.ResourceTier
	lastTimeoutMs   uint32
}

type fakeRouteOnlyServer struct {
	tsov1.UnimplementedTimelineRouteServiceServer

	inner *fakeChronosServer
}

func (s *fakeRouteOnlyServer) EnsureTimeline(ctx context.Context, req *tsov1.EnsureTimelineRequest) (*tsov1.EnsureTimelineResponse, error) {
	return s.inner.EnsureTimeline(ctx, req)
}

func (s *fakeRouteOnlyServer) GetTimelineRoute(ctx context.Context, req *tsov1.GetTimelineRouteRequest) (*tsov1.GetTimelineRouteResponse, error) {
	return s.inner.GetTimelineRoute(ctx, req)
}

type fakeTimestampOnlyServer struct {
	tsov1.UnimplementedTimestampServiceServer

	inner *fakeChronosServer
}

func (s *fakeTimestampOnlyServer) AllocateTimestamps(ctx context.Context, req *tsov1.AllocateTimestampsRequest) (*tsov1.AllocateTimestampsResponse, error) {
	return s.inner.AllocateTimestamps(ctx, req)
}

func newFakeChronosServer(staleOnAllocate bool) *fakeChronosServer {
	return &fakeChronosServer{
		route: &tsov1.TimelineRoute{
			TimelineKey:         "orders.primary",
			GeneratorId:         7,
			OwnerWorkerEndpoint: "127.0.0.1:50051",
			Epoch:               3,
			RouteVersion:        11,
			ResourceTier:        tsov1.ResourceTier_RESOURCE_TIER_SHARED,
		},
		staleOnAllocate: staleOnAllocate,
		nextStartTso:    100,
	}
}

func (s *fakeChronosServer) EnsureTimeline(_ context.Context, req *tsov1.EnsureTimelineRequest) (*tsov1.EnsureTimelineResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureCalls++
	s.lastEnsureTier = req.DesiredResourceTier
	s.route.TimelineKey = req.TimelineKey
	return &tsov1.EnsureTimelineResponse{Route: cloneRoute(s.route)}, nil
}

func (s *fakeChronosServer) GetTimelineRoute(_ context.Context, req *tsov1.GetTimelineRouteRequest) (*tsov1.GetTimelineRouteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getRouteCalls++
	s.route.TimelineKey = req.TimelineKey
	return &tsov1.GetTimelineRouteResponse{Route: cloneRoute(s.route)}, nil
}

func (s *fakeChronosServer) AllocateTimestamps(_ context.Context, req *tsov1.AllocateTimestampsRequest) (*tsov1.AllocateTimestampsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.allocateCalls++
	s.lastRequestID = req.ClientRequestId
	s.returnedRequest = append(s.returnedRequest, req.ClientRequestId)
	s.lastTimeoutMs = req.RequestTimeoutMs

	if s.staleOnAllocate {
		s.route.RouteVersion++
		s.staleOnAllocate = false
		return nil, routeMismatchError()
	}

	if req.ExpectedEpoch != s.route.Epoch || req.ExpectedRouteVersion != s.route.RouteVersion {
		return nil, routeMismatchError()
	}

	start := s.nextStartTso
	end := start + uint64(req.Count) - 1
	s.nextStartTso = end + 1

	return &tsov1.AllocateTimestampsResponse{
		TimelineKey:  req.TimelineKey,
		GeneratorId:  s.route.GeneratorId,
		Epoch:        s.route.Epoch,
		RouteVersion: s.route.RouteVersion,
		Ranges: []*tsov1.TimestampRange{{
			StartTso: start,
			EndTso:   end,
		}},
	}, nil
}

func TestClientOptionsFlowIntoRequests(t *testing.T) {
	server, addr := startFakeChronosServer(t, false)

	client, err := NewWithOptions(
		context.Background(),
		addr,
		"orders.primary",
		WithInsecureTransport(),
		WithDesiredResourceTier(tsov1.ResourceTier_RESOURCE_TIER_WARM),
		WithRequestTimeoutMs(1500),
	)
	if err != nil {
		t.Fatalf("NewWithOptions returned error: %v", err)
	}
	defer client.Close()

	if _, err := client.AllocateTimestamps(context.Background(), 1); err != nil {
		t.Fatalf("AllocateTimestamps returned error: %v", err)
	}

	server.mu.Lock()
	defer server.mu.Unlock()
	if server.lastEnsureTier != tsov1.ResourceTier_RESOURCE_TIER_WARM {
		t.Fatalf("expected warm tier, got %v", server.lastEnsureTier)
	}
	if server.lastTimeoutMs != 1500 {
		t.Fatalf("expected timeout 1500, got %d", server.lastTimeoutMs)
	}
}

func TestClientNewAndAllocateTimestamps(t *testing.T) {
	server, addr := startFakeChronosServer(t, false)

	client, err := NewWithOptions(context.Background(), addr, "orders.primary", WithInsecureTransport())
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	defer client.Close()

	ranges, err := client.AllocateTimestamps(context.Background(), 2)
	if err != nil {
		t.Fatalf("AllocateTimestamps returned error: %v", err)
	}

	if len(ranges) != 1 {
		t.Fatalf("expected one range, got %d", len(ranges))
	}
	if ranges[0].StartTso != 100 || ranges[0].EndTso != 101 {
		t.Fatalf("unexpected range: %+v", ranges[0])
	}

	server.mu.Lock()
	defer server.mu.Unlock()
	if server.ensureCalls != 1 {
		t.Fatalf("expected one ensure call, got %d", server.ensureCalls)
	}
	if server.allocateCalls != 1 {
		t.Fatalf("expected one allocate call, got %d", server.allocateCalls)
	}
	if server.lastRequestID == "" {
		t.Fatal("expected non-empty client request id")
	}
}

func TestClientRefreshesStaleRouteAndRetries(t *testing.T) {
	server, addr := startFakeChronosServer(t, true)

	client, err := NewWithOptions(context.Background(), addr, "orders.primary", WithInsecureTransport())
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	defer client.Close()

	ranges, err := client.AllocateTimestamps(context.Background(), 1)
	if err != nil {
		t.Fatalf("AllocateTimestamps returned error: %v", err)
	}
	if len(ranges) != 1 || ranges[0].StartTso != 100 {
		t.Fatalf("unexpected ranges: %+v", ranges)
	}

	server.mu.Lock()
	defer server.mu.Unlock()
	if server.getRouteCalls < 1 {
		t.Fatalf("expected route refresh to happen, got %d get calls", server.getRouteCalls)
	}
	if server.allocateCalls != 2 {
		t.Fatalf("expected one failed allocate and one retry, got %d calls", server.allocateCalls)
	}
	if len(server.returnedRequest) != 2 || server.returnedRequest[0] != server.returnedRequest[1] {
		t.Fatalf("expected retry to reuse logical request id, got %v", server.returnedRequest)
	}
}

func TestClientAllocatesAgainstRouteOwnerEndpoint(t *testing.T) {
	owner := newFakeChronosServer(false)
	ownerAddr := startTimestampOnlyServer(t, owner)

	route := newFakeChronosServer(false)
	route.route.OwnerWorkerEndpoint = ownerAddr
	routeAddr := startRouteOnlyServer(t, route)

	client, err := NewWithOptions(context.Background(), routeAddr, "orders.primary", WithInsecureTransport())
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	defer client.Close()

	ranges, err := client.AllocateTimestamps(context.Background(), 1)
	if err != nil {
		t.Fatalf("AllocateTimestamps returned error: %v", err)
	}
	if len(ranges) != 1 || ranges[0].StartTso != 100 {
		t.Fatalf("unexpected ranges: %+v", ranges)
	}

	route.mu.Lock()
	defer route.mu.Unlock()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if route.allocateCalls != 0 {
		t.Fatalf("route server should not serve allocate, got %d calls", route.allocateCalls)
	}
	if owner.allocateCalls != 1 {
		t.Fatalf("owner server should serve allocate, got %d calls", owner.allocateCalls)
	}
}

func startFakeChronosServer(t *testing.T, staleOnce bool) (*fakeChronosServer, string) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}

	grpcServer := grpc.NewServer()
	server := newFakeChronosServer(staleOnce)
	server.route.OwnerWorkerEndpoint = lis.Addr().String()
	tsov1.RegisterTimelineRouteServiceServer(grpcServer, server)
	tsov1.RegisterTimestampServiceServer(grpcServer, server)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	t.Cleanup(func() {
		grpcServer.Stop()
		_ = lis.Close()
	})

	return server, lis.Addr().String()
}

func startRouteOnlyServer(t *testing.T, server *fakeChronosServer) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}

	grpcServer := grpc.NewServer()
	tsov1.RegisterTimelineRouteServiceServer(grpcServer, &fakeRouteOnlyServer{inner: server})

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	t.Cleanup(func() {
		grpcServer.Stop()
		_ = lis.Close()
	})

	return lis.Addr().String()
}

func startTimestampOnlyServer(t *testing.T, server *fakeChronosServer) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}

	grpcServer := grpc.NewServer()
	tsov1.RegisterTimestampServiceServer(grpcServer, &fakeTimestampOnlyServer{inner: server})

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	t.Cleanup(func() {
		grpcServer.Stop()
		_ = lis.Close()
	})

	return lis.Addr().String()
}

func cloneRoute(route *tsov1.TimelineRoute) *tsov1.TimelineRoute {
	return &tsov1.TimelineRoute{
		TimelineKey:         route.TimelineKey,
		GeneratorId:         route.GeneratorId,
		OwnerWorkerEndpoint: route.OwnerWorkerEndpoint,
		Epoch:               route.Epoch,
		RouteVersion:        route.RouteVersion,
		ResourceTier:        route.ResourceTier,
	}
}

func routeMismatchError() error {
	st, err := grpcstatus.New(codes.FailedPrecondition, "stale route").WithDetails(
		&tsov1.ErrorDetail{Code: tsov1.ErrorCode_ERROR_CODE_ROUTE_VERSION_MISMATCH},
	)
	if err != nil {
		return err
	}
	return st.Err()
}
