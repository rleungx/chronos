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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLException;

public class Client implements AutoCloseable {
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

  @FunctionalInterface
  interface AllocationChannelFactory {
    ManagedChannel create(String ownerWorkerEndpoint);
  }

  private final ManagedChannel routeChannel;
  private final TimelineRouteServiceGrpc.TimelineRouteServiceBlockingStub routeStub;
  private final AtomicReference<ManagedChannel> tsoChannel = new AtomicReference<>();
  private final AtomicReference<TimestampServiceGrpc.TimestampServiceBlockingStub> tsoStub =
      new AtomicReference<>();
  private final ConcurrentHashMap<String, TimelineRoute> cache = new ConcurrentHashMap<>();
  private final AtomicLong requestId = new AtomicLong(1);
  private final String timelineKey;
  private final AllocationChannelFactory allocationChannelFactory;

  public Client(String addr, String timelineKey) {
    this(addr, timelineKey, TransportConfig.secure());
  }

  public Client(String addr, String timelineKey, TransportConfig transportConfig) {
    this(
        createManagedChannel(addr, transportConfig),
        timelineKey,
        ownerWorkerEndpoint -> createManagedChannel(ownerWorkerEndpoint, transportConfig));
  }

  Client(ManagedChannel routeChannel, String timelineKey) {
    this(
        routeChannel,
        timelineKey,
        ownerWorkerEndpoint -> ManagedChannelBuilder.forTarget(ownerWorkerEndpoint).usePlaintext().build());
  }

  Client(
      ManagedChannel routeChannel,
      String timelineKey,
      AllocationChannelFactory allocationChannelFactory) {
    this.routeChannel = routeChannel;
    this.routeStub = TimelineRouteServiceGrpc.newBlockingStub(routeChannel);
    this.timelineKey = timelineKey;
    this.allocationChannelFactory = allocationChannelFactory;
    ensureRoute();
  }

  public synchronized List<TimestampRange> allocateTimestamps(int count) {
    TimelineRoute route = ensureRoute();
    String clientRequestId = nextClientRequestId(route.getTimelineKey());
    try {
      return allocateOnce(route, count, clientRequestId).getRangesList();
    } catch (RuntimeException err) {
      if (!isStaleRouteError(err)) {
        throw err;
      }
      route = refreshRoute();
      return allocateOnce(route, count, clientRequestId).getRangesList();
    }
  }

  private synchronized TimelineRoute ensureRoute() {
    TimelineRoute route = cache.get(timelineKey);
    if (route != null) {
      return route;
    }

    routeStub.ensureTimeline(
        EnsureTimelineRequest.newBuilder()
            .setTimelineKey(timelineKey)
            .setDesiredResourceTier(ResourceTier.RESOURCE_TIER_SHARED)
            .build());

    return refreshRoute();
  }

  private synchronized TimelineRoute refreshRoute() {
    TimelineRoute route =
        routeStub
            .getTimelineRoute(GetTimelineRouteRequest.newBuilder().setTimelineKey(timelineKey).build())
            .getRoute();
    ManagedChannel previous = tsoChannel.getAndSet(null);
    if (previous != null) {
      previous.shutdownNow();
    }
    ManagedChannel nextChannel = allocationChannelFactory.create(route.getOwnerWorkerEndpoint());
    tsoChannel.set(nextChannel);
    tsoStub.set(TimestampServiceGrpc.newBlockingStub(nextChannel));
    cache.put(timelineKey, route);
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
            .build());
  }

  private String nextClientRequestId(String timelineKey) {
    return timelineKey + "-" + requestId.getAndIncrement();
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
      } catch (Exception ignored) {
      }
    }
    return Status.fromThrowable(err).getCode() == Status.Code.FAILED_PRECONDITION;
  }

  @Override
  public synchronized void close() {
    ManagedChannel currentTsoChannel = tsoChannel.getAndSet(null);
    if (currentTsoChannel != null) {
      currentTsoChannel.shutdownNow();
    }
    routeChannel.shutdownNow();
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

  private static void validateTransportConfig(TransportConfig transportConfig) {
    boolean hasClientCert = transportConfig.clientCertPem != null;
    boolean hasClientKey = transportConfig.clientKeyPem != null;
    if (hasClientCert != hasClientKey) {
      throw new IllegalArgumentException(
          "Chronos TLS client certificate and private key must be configured together");
    }
  }
}
