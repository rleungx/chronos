package chronos.client;

import com.chronos.tso.v1.AllocateTimestampsRequest;
import com.chronos.tso.v1.AllocateTimestampsResponse;
import com.chronos.tso.v1.EnsureTimelineRequest;
import com.chronos.tso.v1.ErrorCode;
import com.chronos.tso.v1.ErrorDetail;
import com.chronos.tso.v1.GetTimelineRouteRequest;
import com.chronos.tso.v1.ResourceTier;
import com.chronos.tso.v1.TimelineRoute;
import com.chronos.tso.v1.TimestampRange;
import com.chronos.tso.v1.TimelineRouteServiceGrpc;
import com.chronos.tso.v1.TimestampServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.protobuf.StatusProto;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLException;

/** Thread-safe application client for allocating timestamp ranges from one Chronos timeline. */
public class Client implements AutoCloseable {
  private static final int MAX_RETAINED_STALE_OWNER_CHANNELS = 16;
  private static final int DEFAULT_REQUEST_TIMEOUT_MS = 250;
  private static final int DEFAULT_STALE_ROUTE_RETRY_ATTEMPTS = 100;
  private static final long DEFAULT_STALE_ROUTE_RETRY_BACKOFF_MS = 50;

  /** Immutable TLS and plaintext transport configuration. */
  public static final class TransportConfig {
    private final boolean plaintext;
    private final byte[] trustedCaPem;
    private final byte[] clientCertPem;
    private final byte[] clientKeyPem;
    private final String authorityOverride;

    private TransportConfig(
        boolean plaintext,
        byte[] trustedCaPem,
        byte[] clientCertPem,
        byte[] clientKeyPem,
        String authorityOverride) {
      this.plaintext = plaintext;
      this.trustedCaPem = trustedCaPem;
      this.clientCertPem = clientCertPem;
      this.clientKeyPem = clientKeyPem;
      this.authorityOverride = authorityOverride;
    }

    /**
     * Returns a TLS configuration that uses the platform trust roots.
     *
     * @return secure transport configuration
     */
    public static TransportConfig secure() {
      return new TransportConfig(false, null, null, null, null);
    }

    /**
     * Returns a copy with plaintext transport enabled or disabled.
     *
     * @param plaintext whether to disable TLS
     * @return updated immutable configuration
     */
    public TransportConfig withPlaintext(boolean plaintext) {
      return new TransportConfig(
          plaintext, copy(trustedCaPem), copy(clientCertPem), copy(clientKeyPem), authorityOverride);
    }

    /**
     * Returns a copy using the supplied PEM-encoded CA bundle.
     *
     * @param trustedCaPem PEM-encoded trusted certificate authorities
     * @return updated immutable configuration
     */
    public TransportConfig withTrustedCaPem(byte[] trustedCaPem) {
      return new TransportConfig(
          plaintext, copy(trustedCaPem), copy(clientCertPem), copy(clientKeyPem), authorityOverride);
    }

    /**
     * Returns a copy using the supplied PEM-encoded mTLS client identity.
     *
     * @param clientCertPem PEM-encoded client certificate chain
     * @param clientKeyPem PEM-encoded client private key
     * @return updated immutable configuration
     */
    public TransportConfig withClientIdentityPem(byte[] clientCertPem, byte[] clientKeyPem) {
      return new TransportConfig(
          plaintext, copy(trustedCaPem), copy(clientCertPem), copy(clientKeyPem), authorityOverride);
    }

    /**
     * Returns a copy overriding the TLS authority used for certificate verification.
     *
     * @param authorityOverride expected TLS server authority
     * @return updated immutable configuration
     */
    public TransportConfig withAuthorityOverride(String authorityOverride) {
      return new TransportConfig(
          plaintext,
          copy(trustedCaPem),
          copy(clientCertPem),
          copy(clientKeyPem),
          authorityOverride);
    }

    private static byte[] copy(byte[] value) {
      return value == null ? null : value.clone();
    }
  }

  /** Immutable allocation, retry, idempotency, and transport configuration. */
  public static final class Config {
    private final ResourceTier desiredResourceTier;
    private final int requestTimeoutMs;
    private final int staleRouteRetryAttempts;
    private final long staleRouteRetryBackoffMs;
    private final boolean idempotencyEnabled;
    private final TransportConfig transport;

    private Config(
        ResourceTier desiredResourceTier,
        int requestTimeoutMs,
        int staleRouteRetryAttempts,
        long staleRouteRetryBackoffMs,
        boolean idempotencyEnabled,
        TransportConfig transport) {
      this.desiredResourceTier = desiredResourceTier;
      this.requestTimeoutMs = requestTimeoutMs;
      this.staleRouteRetryAttempts = staleRouteRetryAttempts;
      this.staleRouteRetryBackoffMs = staleRouteRetryBackoffMs;
      this.idempotencyEnabled = idempotencyEnabled;
      this.transport = transport;
    }

    /**
     * Returns the production-oriented default client configuration.
     *
     * @return default immutable configuration
     */
    public static Config defaults() {
      return new Config(
          ResourceTier.RESOURCE_TIER_SHARED,
          DEFAULT_REQUEST_TIMEOUT_MS,
          DEFAULT_STALE_ROUTE_RETRY_ATTEMPTS,
          DEFAULT_STALE_ROUTE_RETRY_BACKOFF_MS,
          false,
          TransportConfig.secure());
    }

    /**
     * Returns a copy requesting the given resource tier when ensuring the timeline.
     *
     * @param desiredResourceTier desired Chronos resource tier
     * @return updated immutable configuration
     */
    public Config withDesiredResourceTier(ResourceTier desiredResourceTier) {
      return new Config(
          Objects.requireNonNull(desiredResourceTier, "desiredResourceTier"),
          requestTimeoutMs,
          staleRouteRetryAttempts,
          staleRouteRetryBackoffMs,
          idempotencyEnabled,
          transport);
    }

    /**
     * Returns a copy with the per-RPC timeout in milliseconds; zero disables the deadline.
     *
     * @param requestTimeoutMs non-negative timeout in milliseconds
     * @return updated immutable configuration
     * @throws IllegalArgumentException if the timeout is negative
     */
    public Config withRequestTimeoutMs(int requestTimeoutMs) {
      if (requestTimeoutMs < 0) {
        throw new IllegalArgumentException("Chronos request timeout must be >= 0");
      }
      return new Config(
          desiredResourceTier,
          requestTimeoutMs,
          staleRouteRetryAttempts,
          staleRouteRetryBackoffMs,
          idempotencyEnabled,
          transport);
    }

    /**
     * Returns a copy with the route-recovery retry budget.
     *
     * @param staleRouteRetryAttempts non-negative number of recovery retries
     * @return updated immutable configuration
     * @throws IllegalArgumentException if the retry count is negative
     */
    public Config withStaleRouteRetryAttempts(int staleRouteRetryAttempts) {
      if (staleRouteRetryAttempts < 0) {
        throw new IllegalArgumentException("Chronos stale-route retry attempts must be >= 0");
      }
      return new Config(
          desiredResourceTier,
          requestTimeoutMs,
          staleRouteRetryAttempts,
          staleRouteRetryBackoffMs,
          idempotencyEnabled,
          transport);
    }

    /**
     * Returns a copy with the delay between route-recovery attempts.
     *
     * @param staleRouteRetryBackoffMs non-negative delay in milliseconds
     * @return updated immutable configuration
     * @throws IllegalArgumentException if the delay is negative
     */
    public Config withStaleRouteRetryBackoffMs(long staleRouteRetryBackoffMs) {
      if (staleRouteRetryBackoffMs < 0) {
        throw new IllegalArgumentException("Chronos stale-route retry backoff must be >= 0");
      }
      return new Config(
          desiredResourceTier,
          requestTimeoutMs,
          staleRouteRetryAttempts,
          staleRouteRetryBackoffMs,
          idempotencyEnabled,
          transport);
    }

    /**
     * Returns a copy with request-record idempotency enabled or disabled.
     *
     * @param idempotencyEnabled whether allocations carry replay identifiers
     * @return updated immutable configuration
     */
    public Config withIdempotency(boolean idempotencyEnabled) {
      return new Config(
          desiredResourceTier,
          requestTimeoutMs,
          staleRouteRetryAttempts,
          staleRouteRetryBackoffMs,
          idempotencyEnabled,
          transport);
    }

    /**
     * Returns a copy using the supplied transport configuration.
     *
     * @param transport non-null transport configuration
     * @return updated immutable configuration
     */
    public Config withTransport(TransportConfig transport) {
      return new Config(
          desiredResourceTier,
          requestTimeoutMs,
          staleRouteRetryAttempts,
          staleRouteRetryBackoffMs,
          idempotencyEnabled,
          Objects.requireNonNull(transport, "transport"));
    }
  }

  @FunctionalInterface
  interface AllocationChannelFactory {
    ManagedChannel create(String ownerWorkerEndpoint);
  }

  private static final class RouteSnapshot {
    private final TimelineRoute route;
    private final ManagedChannel ownerChannel;
    private final TimestampServiceGrpc.TimestampServiceBlockingStub tsoStub;

    private RouteSnapshot(
        TimelineRoute route,
        ManagedChannel ownerChannel,
        TimestampServiceGrpc.TimestampServiceBlockingStub tsoStub) {
      this.route = route;
      this.ownerChannel = ownerChannel;
      this.tsoStub = tsoStub;
    }
  }

  private final ManagedChannel routeChannel;
  private final TimelineRouteServiceGrpc.TimelineRouteServiceBlockingStub routeStub;
  private final AtomicReference<RouteSnapshot> routeSnapshot = new AtomicReference<>();
  private final AtomicLong requestId = new AtomicLong(1);
  private final String timelineKey;
  private final AllocationChannelFactory allocationChannelFactory;
  private final Config config;
  private final String idempotencyScope;
  private final Object routeRefreshLock = new Object();
  private final Deque<ManagedChannel> staleOwnerChannels = new ArrayDeque<>();

  /**
   * Creates a client using the default secure configuration.
   *
   * @param addr stable Chronos control endpoint
   * @param timelineKey timeline bound to this client
   */
  public Client(String addr, String timelineKey) {
    this(addr, timelineKey, Config.defaults());
  }

  /**
   * Creates a client using custom transport settings.
   *
   * @param addr stable Chronos control endpoint
   * @param timelineKey timeline bound to this client
   * @param transportConfig TLS or plaintext transport settings
   */
  public Client(String addr, String timelineKey, TransportConfig transportConfig) {
    this(addr, timelineKey, Config.defaults().withTransport(transportConfig));
  }

  /**
   * Creates a client using custom transport and idempotency settings.
   *
   * @param addr stable Chronos control endpoint
   * @param timelineKey timeline bound to this client
   * @param transportConfig TLS or plaintext transport settings
   * @param idempotencyEnabled whether allocations carry replay identifiers
   */
  public Client(
      String addr,
      String timelineKey,
      TransportConfig transportConfig,
      boolean idempotencyEnabled) {
    this(
        addr,
        timelineKey,
        Config.defaults().withTransport(transportConfig).withIdempotency(idempotencyEnabled));
  }

  /**
   * Creates a client using the complete immutable configuration.
   *
   * @param addr stable Chronos control endpoint
   * @param timelineKey timeline bound to this client
   * @param config non-null client configuration
   */
  public Client(String addr, String timelineKey, Config config) {
    this(
        createManagedChannel(addr, requireConfig(config).transport),
        timelineKey,
        ownerWorkerEndpoint ->
            createManagedChannel(ownerWorkerEndpoint, requireConfig(config).transport),
        requireConfig(config));
  }

  Client(ManagedChannel routeChannel, String timelineKey) {
    this(
        routeChannel,
        timelineKey,
        ownerWorkerEndpoint ->
            ManagedChannelBuilder.forTarget(ownerWorkerEndpoint).usePlaintext().build(),
        Config.defaults());
  }

  Client(
      ManagedChannel routeChannel,
      String timelineKey,
      AllocationChannelFactory allocationChannelFactory) {
    this(routeChannel, timelineKey, allocationChannelFactory, Config.defaults());
  }

  Client(
      ManagedChannel routeChannel,
      String timelineKey,
      AllocationChannelFactory allocationChannelFactory,
      boolean idempotencyEnabled) {
    this(
        routeChannel,
        timelineKey,
        allocationChannelFactory,
        Config.defaults().withIdempotency(idempotencyEnabled));
  }

  Client(
      ManagedChannel routeChannel,
      String timelineKey,
      AllocationChannelFactory allocationChannelFactory,
      Config config) {
    this.routeChannel = routeChannel;
    this.routeStub = TimelineRouteServiceGrpc.newBlockingStub(routeChannel);
    this.timelineKey = timelineKey;
    this.allocationChannelFactory = allocationChannelFactory;
    this.config = requireConfig(config);
    this.idempotencyScope = UUID.randomUUID().toString();
    ensureRoute();
  }

  /**
   * Allocates timestamp ranges, transparently recovering stale routes and unavailable owners.
   *
   * @param count positive number of timestamps requested
   * @return one or more allocated timestamp ranges
   */
  public List<TimestampRange> allocateTimestamps(int count) {
    RouteSnapshot snapshot = ensureRoute();
    String clientRequestId = nextClientRequestId();
    int staleRetries = 0;

    while (true) {
      try {
        return allocateOnce(snapshot, count, clientRequestId).getRangesList();
      } catch (RuntimeException err) {
        if (!isRouteRecoveryError(err) || staleRetries >= config.staleRouteRetryAttempts) {
          throw err;
        }
        staleRetries++;
        try {
          snapshot = refreshRouteIfUnchanged(snapshot);
        } catch (RuntimeException refreshErr) {
          if (Status.fromThrowable(refreshErr).getCode() != Status.Code.UNAVAILABLE) {
            throw refreshErr;
          }
        }
        sleepBeforeStaleRouteRetry();
      }
    }
  }

  private void sleepBeforeStaleRouteRetry() {
    if (config.staleRouteRetryBackoffMs <= 0) {
      return;
    }
    try {
      Thread.sleep(config.staleRouteRetryBackoffMs);
    } catch (InterruptedException err) {
      Thread.currentThread().interrupt();
      throw Status.CANCELLED
          .withDescription("interrupted while waiting before stale-route retry")
          .asRuntimeException();
    }
  }

  private RouteSnapshot ensureRoute() {
    RouteSnapshot cached = routeSnapshot.get();
    if (cached != null) {
      return cached;
    }

    synchronized (routeRefreshLock) {
      cached = routeSnapshot.get();
      if (cached != null) {
        return cached;
      }

      var ensureResponse =
          routeStubWithDeadline().ensureTimeline(
              EnsureTimelineRequest.newBuilder()
                  .setTimelineKey(timelineKey)
                  .setDesiredResourceTier(config.desiredResourceTier)
                  .build());
      requireRoute("ensureTimeline", ensureResponse.hasRoute(), ensureResponse.getRoute());

      return installRouteLocked(ensureResponse.getRoute());
    }
  }

  private RouteSnapshot refreshRouteIfUnchanged(RouteSnapshot observed) {
    synchronized (routeRefreshLock) {
      RouteSnapshot current = routeSnapshot.get();
      if (current != null && !sameRouteIdentity(current.route, observed.route)) {
        return current;
      }
      return refreshRouteLocked();
    }
  }

  private RouteSnapshot refreshRouteLocked() {
    var response =
        routeStubWithDeadline()
            .getTimelineRoute(
                GetTimelineRouteRequest.newBuilder().setTimelineKey(timelineKey).build());
    TimelineRoute route = requireRoute("getTimelineRoute", response.hasRoute(), response.getRoute());
    return installRouteLocked(route);
  }

  private RouteSnapshot installRouteLocked(TimelineRoute route) {
    RouteSnapshot current = routeSnapshot.get();
    if (current != null
        && route.getOwnerWorkerEndpoint().equals(current.route.getOwnerWorkerEndpoint())) {
      RouteSnapshot next = new RouteSnapshot(route, current.ownerChannel, current.tsoStub);
      routeSnapshot.set(next);
      return next;
    }

    ManagedChannel nextChannel = allocationChannelFactory.create(route.getOwnerWorkerEndpoint());
    RouteSnapshot next =
        new RouteSnapshot(route, nextChannel, TimestampServiceGrpc.newBlockingStub(nextChannel));
    routeSnapshot.set(next);
    retainStaleOwnerChannelLocked(current == null ? null : current.ownerChannel);
    return next;
  }

  private void retainStaleOwnerChannelLocked(ManagedChannel previous) {
    if (previous == null) {
      return;
    }

    staleOwnerChannels.addLast(previous);
    if (staleOwnerChannels.size() > MAX_RETAINED_STALE_OWNER_CHANNELS) {
      staleOwnerChannels.removeFirst().shutdownNow();
    }
  }

  private static TimelineRoute requireRoute(String operation, boolean hasRoute, TimelineRoute route) {
    if (!hasRoute) {
      throw new IllegalStateException("Chronos returned no route from " + operation);
    }
    if (route.getOwnerWorkerEndpoint().isBlank()) {
      throw new IllegalStateException(
          "Chronos returned route with empty owner endpoint from " + operation);
    }
    return route;
  }

  private AllocateTimestampsResponse allocateOnce(
      RouteSnapshot snapshot, int count, String clientRequestId) {
    TimelineRoute route = snapshot.route;
    return stubWithDeadline(snapshot.tsoStub).allocateTimestamps(
        AllocateTimestampsRequest.newBuilder()
            .setTimelineKey(route.getTimelineKey())
            .setCount(count)
            .setExpectedEpoch(route.getEpoch())
            .setExpectedRouteVersion(route.getRouteVersion())
            .setClientRequestId(clientRequestId)
            .setRequestTimeoutMs(config.requestTimeoutMs)
            .build());
  }

  private TimelineRouteServiceGrpc.TimelineRouteServiceBlockingStub routeStubWithDeadline() {
    if (config.requestTimeoutMs <= 0) {
      return routeStub;
    }
    return routeStub.withDeadlineAfter(config.requestTimeoutMs, TimeUnit.MILLISECONDS);
  }

  private TimestampServiceGrpc.TimestampServiceBlockingStub stubWithDeadline(
      TimestampServiceGrpc.TimestampServiceBlockingStub stub) {
    if (config.requestTimeoutMs <= 0) {
      return stub;
    }
    return stub.withDeadlineAfter(config.requestTimeoutMs, TimeUnit.MILLISECONDS);
  }

  private String nextClientRequestId() {
    if (!config.idempotencyEnabled) {
      return "";
    }
    return idempotencyScope + "-" + requestId.getAndIncrement();
  }

  private static boolean sameRouteIdentity(TimelineRoute left, TimelineRoute right) {
    return left.getTimelineKey().equals(right.getTimelineKey())
        && left.getGeneratorId() == right.getGeneratorId()
        && left.getOwnerWorkerEndpoint().equals(right.getOwnerWorkerEndpoint())
        && left.getEpoch() == right.getEpoch()
        && left.getRouteVersion() == right.getRouteVersion()
        && left.getResourceTier() == right.getResourceTier();
  }

  private static boolean isStaleRouteError(RuntimeException err) {
    com.google.rpc.Status status = StatusProto.fromThrowable(err);
    if (status == null) {
      return false;
    }
    for (com.google.protobuf.Any detailAny : status.getDetailsList()) {
      if (!detailAny.is(ErrorDetail.class)) {
        continue;
      }
      try {
        ErrorDetail detail = detailAny.unpack(ErrorDetail.class);
        if (detail.getCode() == ErrorCode.ERROR_CODE_NOT_TIMELINE_OWNER
            || detail.getCode() == ErrorCode.ERROR_CODE_ROUTE_VERSION_MISMATCH
            || detail.getCode() == ErrorCode.ERROR_CODE_EPOCH_MISMATCH) {
          return true;
        }
      } catch (Exception unpackError) {
        continue;
      }
    }
    return false;
  }

  private static boolean isRouteRecoveryError(RuntimeException err) {
    Status.Code code = Status.fromThrowable(err).getCode();
    return isStaleRouteError(err) || code == Status.Code.UNAVAILABLE;
  }

  /** Closes the control and owner channels held by this client. */
  @Override
  public void close() {
    synchronized (routeRefreshLock) {
      for (ManagedChannel stale : staleOwnerChannels) {
        stale.shutdownNow();
      }
      staleOwnerChannels.clear();
      RouteSnapshot current = routeSnapshot.getAndSet(null);
      if (current != null) {
        current.ownerChannel.shutdownNow();
      }
      routeChannel.shutdownNow();
    }
  }

  private static ManagedChannel createManagedChannel(String target, TransportConfig transportConfig) {
    validateTransportConfig(transportConfig);
    if (transportConfig.plaintext) {
      return ManagedChannelBuilder.forTarget(target).usePlaintext().build();
    }

    NettyChannelBuilder builder = NettyChannelBuilder.forTarget(target);
    if (transportConfig.authorityOverride != null && !transportConfig.authorityOverride.isBlank()) {
      builder = builder.overrideAuthority(transportConfig.authorityOverride);
    }

    try {
      var sslContextBuilder = GrpcSslContexts.forClient();
      if (transportConfig.trustedCaPem != null) {
        sslContextBuilder =
            sslContextBuilder.trustManager(new ByteArrayInputStream(transportConfig.trustedCaPem));
      }
      if (transportConfig.clientCertPem != null && transportConfig.clientKeyPem != null) {
        InputStream certStream = new ByteArrayInputStream(transportConfig.clientCertPem);
        InputStream keyStream = new ByteArrayInputStream(transportConfig.clientKeyPem);
        sslContextBuilder = sslContextBuilder.keyManager(certStream, keyStream);
      }
      return builder.sslContext(sslContextBuilder.build()).build();
    } catch (SSLException err) {
      throw new IllegalArgumentException("invalid Chronos TLS transport configuration", err);
    }
  }

  private static Config requireConfig(Config config) {
    if (config == null) {
      throw new IllegalArgumentException("Chronos client config must not be null");
    }
    if (config.desiredResourceTier == null) {
      throw new IllegalArgumentException("Chronos desired resource tier must not be null");
    }
    if (config.requestTimeoutMs < 0) {
      throw new IllegalArgumentException("Chronos request timeout must be >= 0");
    }
    if (config.staleRouteRetryAttempts < 0) {
      throw new IllegalArgumentException("Chronos stale-route retry attempts must be >= 0");
    }
    if (config.staleRouteRetryBackoffMs < 0) {
      throw new IllegalArgumentException("Chronos stale-route retry backoff must be >= 0");
    }
    validateTransportConfig(config.transport);
    return config;
  }

  private static void validateTransportConfig(TransportConfig transportConfig) {
    if (transportConfig == null) {
      throw new IllegalArgumentException("Chronos transport config must not be null");
    }
    boolean hasClientCert = transportConfig.clientCertPem != null;
    boolean hasClientKey = transportConfig.clientKeyPem != null;
    if (hasClientCert != hasClientKey) {
      throw new IllegalArgumentException(
          "Chronos TLS client certificate and private key must be configured together");
    }
  }
}
