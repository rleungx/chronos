package chronos

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	tsov1 "github.com/rleungx/chronos/gen/proto/tso/v1"
)

type Option func(*config)

type config struct {
	desiredResourceTier tsov1.ResourceTier
	requestTimeoutMs    uint32
}

func defaultConfig() config {
	return config{
		desiredResourceTier: tsov1.ResourceTier_RESOURCE_TIER_SHARED,
		requestTimeoutMs:    0,
	}
}

func WithDesiredResourceTier(tier tsov1.ResourceTier) Option {
	return func(cfg *config) {
		cfg.desiredResourceTier = tier
	}
}

func WithRequestTimeoutMs(timeoutMs uint32) Option {
	return func(cfg *config) {
		cfg.requestTimeoutMs = timeoutMs
	}
}

type Client struct {
	routeConn   *grpc.ClientConn
	tsoConn     *grpc.ClientConn
	staleConns  []*grpc.ClientConn
	routeClient tsov1.TimelineRouteServiceClient
	tsoClient   tsov1.TimestampServiceClient
	cache       map[string]*tsov1.TimelineRoute
	timelineKey string
	ownerAddr   string
	mu          sync.RWMutex
	requestID   atomic.Uint64
	config      config
}

func New(ctx context.Context, addr string, timelineKey string) (*Client, error) {
	return NewWithOptions(ctx, addr, timelineKey)
}

func NewWithOptions(ctx context.Context, addr string, timelineKey string, opts ...Option) (*Client, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	conn, err := grpc.DialContext(
		ctx,
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, err
	}
	client := &Client{
		routeConn:   conn,
		routeClient: tsov1.NewTimelineRouteServiceClient(conn),
		cache:       map[string]*tsov1.TimelineRoute{},
		timelineKey: timelineKey,
		config:      cfg,
	}
	if _, err := client.ensureRoute(ctx); err != nil {
		if client.tsoConn != nil {
			_ = client.tsoConn.Close()
		}
		_ = conn.Close()
		return nil, err
	}
	return client, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.staleConns {
		_ = conn.Close()
	}
	if c.tsoConn != nil {
		_ = c.tsoConn.Close()
	}
	return c.routeConn.Close()
}

func (c *Client) AllocateTimestamps(ctx context.Context, count uint32) ([]*tsov1.TimestampRange, error) {
	route, err := c.ensureRoute(ctx)
	if err != nil {
		return nil, err
	}

	ranges, err := c.allocateOnce(ctx, route, count)
	if err == nil {
		return ranges, nil
	}
	if !isStaleRouteError(err) {
		return nil, err
	}

	route, err = c.refreshRoute(ctx)
	if err != nil {
		return nil, err
	}
	return c.allocateOnce(ctx, route, count)
}

func (c *Client) ensureRoute(ctx context.Context) (*tsov1.TimelineRoute, error) {
	c.mu.RLock()
	route, ok := c.cache[c.timelineKey]
	c.mu.RUnlock()
	if ok {
		return route, nil
	}

	_, err := c.routeClient.EnsureTimeline(ctx, &tsov1.EnsureTimelineRequest{
		TimelineKey:         c.timelineKey,
		DesiredResourceTier: c.config.desiredResourceTier,
	})
	if err != nil {
		return nil, err
	}

	return c.refreshRoute(ctx)
}

func (c *Client) refreshRoute(ctx context.Context) (*tsov1.TimelineRoute, error) {
	resp, err := c.routeClient.GetTimelineRoute(ctx, &tsov1.GetTimelineRouteRequest{
		TimelineKey: c.timelineKey,
	})
	if err != nil {
		return nil, err
	}
	if err := c.ensureOwnerClient(ctx, resp.Route.OwnerWorkerEndpoint); err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.cache[c.timelineKey] = resp.Route
	c.mu.Unlock()
	return resp.Route, nil
}

func (c *Client) ensureOwnerClient(ctx context.Context, ownerAddr string) error {
	c.mu.Lock()
	if c.ownerAddr == ownerAddr && c.tsoConn != nil {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	conn, err := grpc.DialContext(
		ctx,
		ownerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tsoConn != nil {
		c.staleConns = append(c.staleConns, c.tsoConn)
	}
	c.tsoConn = conn
	c.tsoClient = tsov1.NewTimestampServiceClient(conn)
	c.ownerAddr = ownerAddr
	return nil
}

func (c *Client) allocateOnce(ctx context.Context, route *tsov1.TimelineRoute, count uint32) ([]*tsov1.TimestampRange, error) {
	c.mu.RLock()
	tsoClient := c.tsoClient
	c.mu.RUnlock()

	resp, err := tsoClient.AllocateTimestamps(ctx, &tsov1.AllocateTimestampsRequest{
		TimelineKey:          route.TimelineKey,
		Count:                count,
		ExpectedEpoch:        route.Epoch,
		ExpectedRouteVersion: route.RouteVersion,
		ClientRequestId:      fmt.Sprintf("%s-%d", route.TimelineKey, c.requestID.Add(1)),
		RequestTimeoutMs:     c.config.requestTimeoutMs,
	})
	if err != nil {
		return nil, err
	}

	return resp.Ranges, nil
}

func isStaleRouteError(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}

	for _, detail := range st.Details() {
		errorDetail, ok := detail.(*tsov1.ErrorDetail)
		if !ok {
			continue
		}

		switch errorDetail.Code {
		case tsov1.ErrorCode_ERROR_CODE_NOT_TIMELINE_OWNER,
			tsov1.ErrorCode_ERROR_CODE_ROUTE_VERSION_MISMATCH,
			tsov1.ErrorCode_ERROR_CODE_EPOCH_MISMATCH:
			return true
		}
	}

	return false
}
