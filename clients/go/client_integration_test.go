package chronos

import (
	"context"
	"net"
	"strings"
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

	mu               sync.Mutex
	route            *tsov1.TimelineRoute
	ensureCalls      int
	getRouteCalls    int
	allocateCalls    int
	staleOnAllocate  bool
	nextStartTso     uint64
	lastRequestID    string
	returnedRequest  []string
	lastEnsureTier   tsov1.ResourceTier
	lastTimeoutMs    uint32
	ensureDeadline   bool
	allocateDeadline bool
	omitEnsureRoute  bool
	omitGetRoute     bool
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

func (s *fakeChronosServer) EnsureTimeline(ctx context.Context, req *tsov1.EnsureTimelineRequest) (*tsov1.EnsureTimelineResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureCalls++
	s.lastEnsureTier = req.DesiredResourceTier
	_, s.ensureDeadline = ctx.Deadline()
	if s.omitEnsureRoute {
		return &tsov1.EnsureTimelineResponse{}, nil
	}
	s.route.TimelineKey = req.TimelineKey
	return &tsov1.EnsureTimelineResponse{Route: cloneRoute(s.route)}, nil
}

func (s *fakeChronosServer) GetTimelineRoute(_ context.Context, req *tsov1.GetTimelineRouteRequest) (*tsov1.GetTimelineRouteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getRouteCalls++
	if s.omitGetRoute {
		return &tsov1.GetTimelineRouteResponse{}, nil
	}
	s.route.TimelineKey = req.TimelineKey
	return &tsov1.GetTimelineRouteResponse{Route: cloneRoute(s.route)}, nil
}

func (s *fakeChronosServer) AllocateTimestamps(ctx context.Context, req *tsov1.AllocateTimestampsRequest) (*tsov1.AllocateTimestampsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.allocateCalls++
	s.lastRequestID = req.ClientRequestId
	s.returnedRequest = append(s.returnedRequest, req.ClientRequestId)
	s.lastTimeoutMs = req.RequestTimeoutMs
	_, s.allocateDeadline = ctx.Deadline()

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
		WithIdempotency(true),
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
	if server.lastRequestID == "" {
		t.Fatal("expected idempotency option to send a client request id")
	}
	if !server.ensureDeadline {
		t.Fatal("expected request timeout option to set ensure RPC deadline")
	}
	if !server.allocateDeadline {
		t.Fatal("expected request timeout option to set allocate RPC deadline")
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
	if server.getRouteCalls != 0 {
		t.Fatalf("expected initial route from ensure response without extra get, got %d get calls", server.getRouteCalls)
	}
	if server.allocateCalls != 1 {
		t.Fatalf("expected one allocate call, got %d", server.allocateCalls)
	}
	if server.lastRequestID != "" {
		t.Fatalf("expected default allocation to omit client request id, got %q", server.lastRequestID)
	}
}

func TestClientRefreshesStaleRouteAndRetries(t *testing.T) {
	server, addr := startFakeChronosServer(t, true)

	client, err := NewWithOptions(
		context.Background(),
		addr,
		"orders.primary",
		WithInsecureTransport(),
		WithIdempotency(true),
	)
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
	if server.getRouteCalls != 1 {
		t.Fatalf("expected one route refresh after stale route, got %d get calls", server.getRouteCalls)
	}
	if server.allocateCalls != 2 {
		t.Fatalf("expected one failed allocate and one retry, got %d calls", server.allocateCalls)
	}
	if len(server.returnedRequest) != 2 || server.returnedRequest[0] == "" || server.returnedRequest[0] != server.returnedRequest[1] {
		t.Fatalf("expected retry to reuse logical request id, got %v", server.returnedRequest)
	}
}

func TestIdempotentClientsUseDistinctRequestIDs(t *testing.T) {
	server, addr := startFakeChronosServer(t, false)

	first, err := NewWithOptions(context.Background(), addr, "orders.primary", WithInsecureTransport(), WithIdempotency(true))
	if err != nil {
		t.Fatalf("first NewWithOptions returned error: %v", err)
	}
	defer first.Close()
	second, err := NewWithOptions(context.Background(), addr, "orders.primary", WithInsecureTransport(), WithIdempotency(true))
	if err != nil {
		t.Fatalf("second NewWithOptions returned error: %v", err)
	}
	defer second.Close()

	if _, err := first.AllocateTimestamps(context.Background(), 1); err != nil {
		t.Fatalf("first AllocateTimestamps returned error: %v", err)
	}
	if _, err := second.AllocateTimestamps(context.Background(), 1); err != nil {
		t.Fatalf("second AllocateTimestamps returned error: %v", err)
	}

	server.mu.Lock()
	defer server.mu.Unlock()
	if len(server.returnedRequest) != 2 {
		t.Fatalf("expected two request ids, got %v", server.returnedRequest)
	}
	if server.returnedRequest[0] == "" || server.returnedRequest[1] == "" {
		t.Fatalf("expected idempotent clients to send request ids, got %v", server.returnedRequest)
	}
	if server.returnedRequest[0] == server.returnedRequest[1] {
		t.Fatalf("expected independent clients to use distinct request ids, got %v", server.returnedRequest)
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

func TestClientBoundsRetainedStaleOwnerConnections(t *testing.T) {
	firstOwner := newFakeChronosServer(false)
	firstOwnerAddr := startTimestampOnlyServer(t, firstOwner)

	route := newFakeChronosServer(false)
	route.route.OwnerWorkerEndpoint = firstOwnerAddr
	routeAddr := startRouteOnlyServer(t, route)

	client, err := NewWithOptions(context.Background(), routeAddr, "orders.primary", WithInsecureTransport())
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	defer client.Close()

	for i := 0; i < maxRetainedStaleOwnerConns+3; i++ {
		owner := newFakeChronosServer(false)
		ownerAddr := startTimestampOnlyServer(t, owner)
		route.mu.Lock()
		route.route.OwnerWorkerEndpoint = ownerAddr
		route.route.RouteVersion++
		route.mu.Unlock()

		if _, err := client.refreshRoute(context.Background()); err != nil {
			t.Fatalf("refreshRoute returned error: %v", err)
		}
	}

	client.mu.RLock()
	staleConnCount := len(client.staleConns)
	client.mu.RUnlock()
	if staleConnCount != maxRetainedStaleOwnerConns {
		t.Fatalf("expected %d retained stale owner connections, got %d", maxRetainedStaleOwnerConns, staleConnCount)
	}
}

func TestConcurrentStaleAllocationsSingleflightRouteRefresh(t *testing.T) {
	server, addr := startFakeChronosServer(t, false)
	client, err := NewWithOptions(context.Background(), addr, "orders.primary", WithInsecureTransport())
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	defer client.Close()

	server.mu.Lock()
	server.route.RouteVersion++
	server.mu.Unlock()

	const callers = 32
	start := make(chan struct{})
	errCh := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, allocateErr := client.AllocateTimestamps(context.Background(), 1)
			errCh <- allocateErr
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for allocateErr := range errCh {
		if allocateErr != nil {
			t.Fatalf("concurrent allocation returned error: %v", allocateErr)
		}
	}

	server.mu.Lock()
	getRouteCalls := server.getRouteCalls
	server.mu.Unlock()
	if getRouteCalls != 1 {
		t.Fatalf("expected one singleflight route refresh, got %d", getRouteCalls)
	}
}

func TestClientRejectsMissingEnsureRoute(t *testing.T) {
	server, addr := startFakeChronosServer(t, false)
	server.mu.Lock()
	server.omitEnsureRoute = true
	server.mu.Unlock()

	_, err := NewWithOptions(context.Background(), addr, "orders.primary", WithInsecureTransport())
	if err == nil {
		t.Fatal("expected missing ensure route to fail")
	}
	if !strings.Contains(err.Error(), "chronos returned no route from ensure_timeline") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClientRejectsMissingRefreshedRoute(t *testing.T) {
	server, addr := startFakeChronosServer(t, true)
	server.mu.Lock()
	server.omitGetRoute = true
	server.mu.Unlock()

	client, err := NewWithOptions(context.Background(), addr, "orders.primary", WithInsecureTransport())
	if err != nil {
		t.Fatalf("NewWithOptions returned error: %v", err)
	}
	defer client.Close()

	_, err = client.AllocateTimestamps(context.Background(), 1)
	if err == nil {
		t.Fatal("expected missing refreshed route to fail after stale route")
	}
	if !strings.Contains(err.Error(), "chronos returned no route from get_timeline_route") {
		t.Fatalf("unexpected error: %v", err)
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
