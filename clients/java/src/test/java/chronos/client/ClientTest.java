package chronos.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.chronos.tso.v1.AllocateTimestampsRequest;
import com.chronos.tso.v1.AllocateTimestampsResponse;
import com.chronos.tso.v1.EnsureTimelineRequest;
import com.chronos.tso.v1.EnsureTimelineResponse;
import com.chronos.tso.v1.ErrorCode;
import com.chronos.tso.v1.ErrorDetail;
import com.chronos.tso.v1.GetTimelineRouteRequest;
import com.chronos.tso.v1.GetTimelineRouteResponse;
import com.chronos.tso.v1.ResourceTier;
import com.chronos.tso.v1.TimelineRoute;
import com.chronos.tso.v1.TimelineRouteServiceGrpc;
import com.chronos.tso.v1.TimestampRange;
import com.chronos.tso.v1.TimestampServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

final class ClientTest {
  @Test
  void allocateTimestampsUsesOwnerEndpointAndRefreshesStaleRoute() throws Exception {
    var staleOnFirstAllocate = new AtomicBoolean(true);
    var routeVersion = new AtomicInteger(11);
    var allocateCalls = new AtomicInteger();

    Server ownerServer =
        ServerBuilder.forPort(0)
            .addService(
                new TimestampServiceGrpc.TimestampServiceImplBase() {
                  @Override
                  public void allocateTimestamps(
                      AllocateTimestampsRequest request,
                      StreamObserver<AllocateTimestampsResponse> responseObserver) {
                    allocateCalls.incrementAndGet();
                    if (staleOnFirstAllocate.getAndSet(false)) {
                      routeVersion.incrementAndGet();
                      var status =
                          com.google.rpc.Status.newBuilder()
                              .setCode(Status.Code.FAILED_PRECONDITION.value())
                              .setMessage("stale route")
                              .addDetails(
                                  Any.pack(
                                      ErrorDetail.newBuilder()
                                          .setCode(ErrorCode.ERROR_CODE_ROUTE_VERSION_MISMATCH)
                                          .setCurrentRouteVersion(routeVersion.get())
                                          .build()))
                              .build();
                      responseObserver.onError(StatusProto.toStatusRuntimeException(status));
                      return;
                    }

                    responseObserver.onNext(
                        AllocateTimestampsResponse.newBuilder()
                            .setTimelineKey(request.getTimelineKey())
                            .setGeneratorId(7)
                            .setEpoch(3)
                            .setRouteVersion(routeVersion.get())
                            .addRanges(
                                TimestampRange.newBuilder().setStartTso(100).setEndTso(100).build())
                            .build());
                    responseObserver.onCompleted();
                  }
                })
            .build()
            .start();

    String ownerEndpoint = "127.0.0.1:" + ownerServer.getPort();

    Server routeServer =
        ServerBuilder.forPort(0)
            .addService(
                new TimelineRouteServiceGrpc.TimelineRouteServiceImplBase() {
                  @Override
                  public void ensureTimeline(
                      EnsureTimelineRequest request,
                      StreamObserver<EnsureTimelineResponse> responseObserver) {
                    responseObserver.onNext(
                        EnsureTimelineResponse.newBuilder()
                            .setRoute(
                                TimelineRoute.newBuilder()
                                    .setTimelineKey(request.getTimelineKey())
                                    .setGeneratorId(7)
                                    .setOwnerWorkerEndpoint(ownerEndpoint)
                                    .setEpoch(3)
                                    .setRouteVersion(routeVersion.get())
                                    .setResourceTier(ResourceTier.RESOURCE_TIER_SHARED)
                                    .build())
                            .build());
                    responseObserver.onCompleted();
                  }

                  @Override
                  public void getTimelineRoute(
                      GetTimelineRouteRequest request,
                      StreamObserver<GetTimelineRouteResponse> responseObserver) {
                    responseObserver.onNext(
                        GetTimelineRouteResponse.newBuilder()
                            .setRoute(
                                TimelineRoute.newBuilder()
                                    .setTimelineKey(request.getTimelineKey())
                                    .setGeneratorId(7)
                                    .setOwnerWorkerEndpoint(ownerEndpoint)
                                    .setEpoch(3)
                                    .setRouteVersion(routeVersion.get())
                                    .setResourceTier(ResourceTier.RESOURCE_TIER_SHARED)
                                    .build())
                            .build());
                    responseObserver.onCompleted();
                  }
                })
            .build()
            .start();

    String routeEndpoint = "127.0.0.1:" + routeServer.getPort();

    try (Client client = new Client(routeEndpoint, "orders.primary")) {
      List<TimestampRange> ranges = client.allocateTimestamps(1);
      assertEquals(1, ranges.size());
      assertEquals(100, ranges.get(0).getStartTso());
      assertEquals(2, allocateCalls.get());
      assertFalse(staleOnFirstAllocate.get());
    } finally {
      ownerServer.shutdownNow();
      routeServer.shutdownNow();
    }
  }
}
