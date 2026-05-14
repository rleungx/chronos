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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLException;

public class Client implements AutoCloseable {
  private static final int MAX_RETAINED_STALE_OWNER_CHANNELS = 16;
  private static final int DEFAULT_STALE_ROUTE_RETRY_ATTEMPTS = 3;
  private static final long DEFAULT_STALE_ROUTE_RETRY_BACKOFF_MS = 5;

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

    public static TransportConfig secure() {
      return new TransportConfig(false, null, null, null, null);
    }

    public TransportConfig withPlaintext(boolean plaintext) {
      return new TransportConfig(
          plaintext, copy(trustedCaPem), copy(clientCertPem), copy(clientKeyPem), authorityOverride);
    }

    public TransportConfig withTrustedCaPem(byte[] trustedCaPem) {
      return new TransportConfig(
          plaintext, copy(trustedCaPem), copy(clientCertPem), copy(clientKeyPem), authorityOverride);
    }

    public TransportConfig withClientIdentityPem(byte[] clientCertPem, byte[] clientKeyPem) {
      return new TransportConfig(
          plaintext, copy(trustedCaPem), copy(clientCertPem), copy(clientKeyPem), authorityOverride);
    }

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

    public static Config defaults() {
      return new Config(
          ResourceTier.RESOURCE_TIER_SHARED,
          0,
          DEFAULT_STALE_ROUTE_RETRY_ATTEMPTS,
          DEFAULT_STALE_ROUTE_RETRY_BACKOFF_MS,
          false,
          TransportConfig.secure());
    }

    public Config withDesiredResourceTier(ResourceTier desiredResourceTier) {
      return new Config(
          Objects.requireNonNull(desiredResourceTier, "desiredResourceTier"),
          requestTimeoutMs,
          staleRouteRetryAttempts,
          staleRouteRetryBackoffMs,
          idempotencyEnabled,
          transport);
    }

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

    public Config withIdempotency(boolean idempotencyEnabled) {
      return new Config(
          desiredResourceTier,
          requestTimeoutMs,
          staleRouteRetryAttempts,
          staleRouteRetryBackoffMs,
          idempotencyEnabled,
          transport);
    }

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

  private final ManagedChannel routeChannel;
  private final TimelineRouteServiceGrpc.TimelineRouteServiceBlockingStub routeStub;
  private final AtomicReference<ManagedChannel> tsoChannel = new AtomicReference<>();
  private final AtomicReference<TimestampServiceGrpc.TimestampServiceBlockingStub> tsoStub =
      new AtomicReference<>();
  private final AtomicReference<TimelineRoute> route = new AtomicReference<>();
  private final AtomicLong requestId = new AtomicLong(1);
  private final String timelineKey;
  private final AllocationChannelFactory allocationChannelFactory;
  private final Config config;
  private final String idempotencyScope;
  private final Object routeRefreshLock = new Object();
  private final Deque<ManagedChannel> staleOwnerChannels = new ArrayDeque<>();
  private String ownerEndpoint = "";

  public Client(String addr, String timelineKey) {
    this(addr, timelineKey, Config.defaults());
  }

  public Client(String addr, String timelineKey, TransportConfig transportConfig) {
    this(addr, timelineKey, Config.defaults().withTransport(transportConfig));
  }

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

  public List<TimestampRange> allocateTimestamps(int count) {
    TimelineRoute route = ensureRoute();
    String clientRequestId = nextClientRequestId(route.getTimelineKey());
    int staleRetries = 0;

    while (true) {
      try {
        return allocateOnce(route, count, clientRequestId).getRangesList();
      } catch (RuntimeException err) {
        if (!isStaleRouteError(err) || staleRetries >= config.staleRouteRetryAttempts) {
          throw err;
        }
        staleRetries++;
        route = refreshRouteIfUnchanged(route);
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

  private TimelineRoute ensureRoute() {
    TimelineRoute cached = route.get();
    if (cached != null) {
      return cached;
    }

    synchronized (routeRefreshLock) {
      cached = route.get();
      if (cached != null) {
        return cached;
      }

      var ensureResponse =
          routeStub.ensureTimeline(
              EnsureTimelineRequest.newBuilder()
                  .setTimelineKey(timelineKey)
                  .setDesiredResourceTier(config.desiredResourceTier)
                  .build());
      requireRoute("ensureTimeline", ensureResponse.hasRoute(), ensureResponse.getRoute());

      return installRouteLocked(ensureResponse.getRoute());
    }
  }

  private TimelineRoute refreshRouteIfUnchanged(TimelineRoute observedRoute) {
    synchronized (routeRefreshLock) {
      TimelineRoute currentRoute = route.get();
      if (currentRoute != null && !sameRouteIdentity(currentRoute, observedRoute)) {
        ensureOwnerChannelLocked(currentRoute.getOwnerWorkerEndpoint());
        return currentRoute;
      }
      return refreshRouteLocked();
    }
  }

  private TimelineRoute refreshRouteLocked() {
    var response =
        routeStub
            .getTimelineRoute(
                GetTimelineRouteRequest.newBuilder().setTimelineKey(timelineKey).build());
    TimelineRoute route = requireRoute("getTimelineRoute", response.hasRoute(), response.getRoute());
    return installRouteLocked(route);
  }

  private TimelineRoute installRouteLocked(TimelineRoute route) {
    ensureOwnerChannelLocked(route.getOwnerWorkerEndpoint());
    this.route.set(route);
    return route;
  }

  private void ensureOwnerChannelLocked(String nextOwnerEndpoint) {
    if (nextOwnerEndpoint.equals(ownerEndpoint) && tsoStub.get() != null) {
      return;
    }

    ManagedChannel nextChannel = allocationChannelFactory.create(nextOwnerEndpoint);
    ManagedChannel previous = tsoChannel.getAndSet(nextChannel);
    tsoStub.set(TimestampServiceGrpc.newBlockingStub(nextChannel));
    ownerEndpoint = nextOwnerEndpoint;
    retainStaleOwnerChannelLocked(previous);
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
      TimelineRoute route, int count, String clientRequestId) {
    return tsoStub.get().allocateTimestamps(
        AllocateTimestampsRequest.newBuilder()
            .setTimelineKey(route.getTimelineKey())
            .setCount(count)
            .setExpectedEpoch(route.getEpoch())
            .setExpectedRouteVersion(route.getRouteVersion())
            .setClientRequestId(clientRequestId)
            .setRequestTimeoutMs(config.requestTimeoutMs)
            .build());
  }

  private String nextClientRequestId(String timelineKey) {
    if (!config.idempotencyEnabled) {
      return "";
    }
    return timelineKey + "-" + idempotencyScope + "-" + requestId.getAndIncrement();
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

  @Override
  public void close() {
    synchronized (routeRefreshLock) {
      for (ManagedChannel stale : staleOwnerChannels) {
        stale.shutdownNow();
      }
      staleOwnerChannels.clear();
      ManagedChannel currentTsoChannel = tsoChannel.getAndSet(null);
      if (currentTsoChannel != null) {
        currentTsoChannel.shutdownNow();
      }
      route.set(null);
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
