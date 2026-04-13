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
import io.grpc.protobuf.StatusProto;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class Client implements AutoCloseable {
  private final ManagedChannel routeChannel;
  private ManagedChannel tsoChannel;
  private final TimelineRouteServiceGrpc.TimelineRouteServiceBlockingStub routeStub;
  private TimestampServiceGrpc.TimestampServiceBlockingStub tsoStub;
  private final ConcurrentHashMap<String, TimelineRoute> cache = new ConcurrentHashMap<>();
  private final AtomicLong requestId = new AtomicLong(1);
  private final String timelineKey;

  public Client(String addr, String timelineKey) {
    this.routeChannel = ManagedChannelBuilder.forTarget(addr).usePlaintext().build();
    this.routeStub = TimelineRouteServiceGrpc.newBlockingStub(routeChannel);
    this.timelineKey = timelineKey;
    ensureRoute();
  }

  public synchronized List<TimestampRange> allocateTimestamps(int count) {
    TimelineRoute route = ensureRoute();
    try {
      return allocateOnce(route, count).getRangesList();
    } catch (RuntimeException err) {
      if (!isStaleRouteError(err)) {
        throw err;
      }
      route = refreshRoute();
      return allocateOnce(route, count).getRangesList();
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
    if (tsoChannel != null) {
      tsoChannel.shutdownNow();
    }
    tsoChannel = ManagedChannelBuilder.forTarget(route.getOwnerWorkerEndpoint()).usePlaintext().build();
    tsoStub = TimestampServiceGrpc.newBlockingStub(tsoChannel);
    cache.put(timelineKey, route);
    return route;
  }

  private AllocateTimestampsResponse allocateOnce(TimelineRoute route, int count) {
    return tsoStub.allocateTimestamps(
        AllocateTimestampsRequest.newBuilder()
            .setTimelineKey(route.getTimelineKey())
            .setCount(count)
            .setExpectedEpoch(route.getEpoch())
            .setExpectedRouteVersion(route.getRouteVersion())
            .setClientRequestId(route.getTimelineKey() + "-" + requestId.getAndIncrement())
            .build());
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
    if (tsoChannel != null) {
      tsoChannel.shutdownNow();
    }
    routeChannel.shutdownNow();
  }
}
