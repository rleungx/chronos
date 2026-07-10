package chronos.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

final class ClientTest {
  @Test
  void allocateTimestampsUsesOwnerEndpointAndRefreshesStaleRoute() throws Exception {
    var staleOnFirstAllocate = new AtomicBoolean(true);
    var routeVersion = new AtomicInteger(11);
    var allocateCalls = new AtomicInteger();
    var getRouteCalls = new AtomicInteger();
    var firstRequestId = new AtomicReference<String>();
    var retryRequestId = new AtomicReference<String>();
    var ownerServerName = InProcessServerBuilder.generateName();
    var routeServerName = InProcessServerBuilder.generateName();

    Server ownerServer =
        InProcessServerBuilder.forName(ownerServerName)
            .directExecutor()
            .addService(
                new TimestampServiceGrpc.TimestampServiceImplBase() {
                  @Override
                  public void allocateTimestamps(
                      AllocateTimestampsRequest request,
                      StreamObserver<AllocateTimestampsResponse> responseObserver) {
                    int call = allocateCalls.incrementAndGet();
                    if (call == 1) {
                      firstRequestId.set(request.getClientRequestId());
                    } else {
                      retryRequestId.set(request.getClientRequestId());
                    }
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

    Server routeServer =
        InProcessServerBuilder.forName(routeServerName)
            .directExecutor()
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
                                    .setOwnerWorkerEndpoint(ownerServerName)
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
                    getRouteCalls.incrementAndGet();
                    responseObserver.onNext(
                        GetTimelineRouteResponse.newBuilder()
                            .setRoute(
                                TimelineRoute.newBuilder()
                                    .setTimelineKey(request.getTimelineKey())
                                    .setGeneratorId(7)
                                    .setOwnerWorkerEndpoint(ownerServerName)
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

    ManagedChannel routeChannel =
        InProcessChannelBuilder.forName(routeServerName).directExecutor().build();

    try (Client client = new Client(
        routeChannel,
        "orders.primary",
        inProcessOwnerChannelFactory(),
        true)) {
      List<TimestampRange> ranges = client.allocateTimestamps(1);
      assertEquals(1, ranges.size());
      assertEquals(100, ranges.get(0).getStartTso());
      assertEquals(2, allocateCalls.get());
      assertEquals(1, getRouteCalls.get());
      assertFalse(firstRequestId.get().isBlank());
      assertEquals(firstRequestId.get(), retryRequestId.get());
      assertFalse(staleOnFirstAllocate.get());
    } finally {
      routeChannel.shutdownNow();
      ownerServer.shutdownNow();
      routeServer.shutdownNow();
    }
  }

  @Test
  void defaultAllocationOmitsClientRequestId() throws Exception {
    var observedRequestId = new AtomicReference<String>("not-called");
    var ownerServerName = InProcessServerBuilder.generateName();
    var routeServerName = InProcessServerBuilder.generateName();

    Server ownerServer =
        InProcessServerBuilder.forName(ownerServerName)
            .directExecutor()
            .addService(
                new TimestampServiceGrpc.TimestampServiceImplBase() {
                  @Override
                  public void allocateTimestamps(
                      AllocateTimestampsRequest request,
                      StreamObserver<AllocateTimestampsResponse> responseObserver) {
                    observedRequestId.set(request.getClientRequestId());
                    responseObserver.onNext(
                        AllocateTimestampsResponse.newBuilder()
                            .setTimelineKey(request.getTimelineKey())
                            .setGeneratorId(7)
                            .setEpoch(3)
                            .setRouteVersion(11)
                            .addRanges(
                                TimestampRange.newBuilder().setStartTso(100).setEndTso(100).build())
                            .build());
                    responseObserver.onCompleted();
                  }
                })
            .build()
            .start();

    Server routeServer = routeServer(routeServerName, ownerServerName, 11);
    ManagedChannel routeChannel =
        InProcessChannelBuilder.forName(routeServerName).directExecutor().build();

    try (Client client = new Client(
        routeChannel,
        "orders.primary",
        inProcessOwnerChannelFactory())) {
      client.allocateTimestamps(1);
      assertEquals("", observedRequestId.get());
    } finally {
      routeChannel.shutdownNow();
      ownerServer.shutdownNow();
      routeServer.shutdownNow();
    }
  }

  @Test
  void failedPreconditionWithoutChronosRouteDetailIsNotRetried() throws Exception {
    var getRouteCalls = new AtomicInteger();
    var ownerServerName = InProcessServerBuilder.generateName();
    var routeServerName = InProcessServerBuilder.generateName();

    Server ownerServer =
        InProcessServerBuilder.forName(ownerServerName)
            .directExecutor()
            .addService(
                new TimestampServiceGrpc.TimestampServiceImplBase() {
                  @Override
                  public void allocateTimestamps(
                      AllocateTimestampsRequest request,
                      StreamObserver<AllocateTimestampsResponse> responseObserver) {
                    responseObserver.onError(
                        Status.FAILED_PRECONDITION
                            .withDescription("non-route precondition failed")
                            .asRuntimeException());
                  }
                })
            .build()
            .start();

    Server routeServer =
        InProcessServerBuilder.forName(routeServerName)
            .directExecutor()
            .addService(
                new TimelineRouteServiceGrpc.TimelineRouteServiceImplBase() {
                  @Override
                  public void ensureTimeline(
                      EnsureTimelineRequest request,
                      StreamObserver<EnsureTimelineResponse> responseObserver) {
                    responseObserver.onNext(
                        EnsureTimelineResponse.newBuilder()
                            .setRoute(route(request.getTimelineKey(), ownerServerName, 11))
                            .build());
                    responseObserver.onCompleted();
                  }

                  @Override
                  public void getTimelineRoute(
                      GetTimelineRouteRequest request,
                      StreamObserver<GetTimelineRouteResponse> responseObserver) {
                    getRouteCalls.incrementAndGet();
                    responseObserver.onNext(
                        GetTimelineRouteResponse.newBuilder()
                            .setRoute(route(request.getTimelineKey(), ownerServerName, 11))
                            .build());
                    responseObserver.onCompleted();
                  }
                })
            .build()
            .start();

    ManagedChannel routeChannel =
        InProcessChannelBuilder.forName(routeServerName).directExecutor().build();

    try (Client client = new Client(
        routeChannel,
        "orders.primary",
        inProcessOwnerChannelFactory())) {
      var err = assertThrows(RuntimeException.class, () -> client.allocateTimestamps(1));
      assertTrue(err.getMessage().contains("non-route precondition failed"));
      assertEquals(0, getRouteCalls.get());
    } finally {
      routeChannel.shutdownNow();
      ownerServer.shutdownNow();
      routeServer.shutdownNow();
    }
  }

  @Test
  void configFlowsIntoEnsureAndAllocateRequests() throws Exception {
    var observedTier = new AtomicReference<ResourceTier>();
    var observedTimeoutMs = new AtomicInteger(-1);
    var observedRequestId = new AtomicReference<String>("");
    var observedEnsureDeadline = new AtomicBoolean(false);
    var observedAllocateDeadline = new AtomicBoolean(false);
    var getRouteCalls = new AtomicInteger();
    var ownerServerName = InProcessServerBuilder.generateName();
    var routeServerName = InProcessServerBuilder.generateName();

    Server ownerServer =
        InProcessServerBuilder.forName(ownerServerName)
            .directExecutor()
            .addService(
                new TimestampServiceGrpc.TimestampServiceImplBase() {
                  @Override
                  public void allocateTimestamps(
                      AllocateTimestampsRequest request,
                      StreamObserver<AllocateTimestampsResponse> responseObserver) {
                    observedTimeoutMs.set(request.getRequestTimeoutMs());
                    observedRequestId.set(request.getClientRequestId());
                    observedAllocateDeadline.set(Context.current().getDeadline() != null);
                    responseObserver.onNext(
                        AllocateTimestampsResponse.newBuilder()
                            .setTimelineKey(request.getTimelineKey())
                            .setGeneratorId(7)
                            .setEpoch(3)
                            .setRouteVersion(11)
                            .addRanges(
                                TimestampRange.newBuilder().setStartTso(100).setEndTso(100).build())
                            .build());
                    responseObserver.onCompleted();
                  }
                })
            .build()
            .start();

    Server routeServer =
        InProcessServerBuilder.forName(routeServerName)
            .directExecutor()
            .addService(
                new TimelineRouteServiceGrpc.TimelineRouteServiceImplBase() {
                  @Override
                  public void ensureTimeline(
                      EnsureTimelineRequest request,
                      StreamObserver<EnsureTimelineResponse> responseObserver) {
                    observedTier.set(request.getDesiredResourceTier());
                    observedEnsureDeadline.set(Context.current().getDeadline() != null);
                    responseObserver.onNext(
                        EnsureTimelineResponse.newBuilder()
                            .setRoute(route(request.getTimelineKey(), ownerServerName, 11))
                            .build());
                    responseObserver.onCompleted();
                  }

                  @Override
                  public void getTimelineRoute(
                      GetTimelineRouteRequest request,
                      StreamObserver<GetTimelineRouteResponse> responseObserver) {
                    getRouteCalls.incrementAndGet();
                    responseObserver.onNext(
                        GetTimelineRouteResponse.newBuilder()
                            .setRoute(route(request.getTimelineKey(), ownerServerName, 11))
                            .build());
                    responseObserver.onCompleted();
                  }
                })
            .build()
            .start();

    ManagedChannel routeChannel =
        InProcessChannelBuilder.forName(routeServerName).directExecutor().build();

    try (Client client = new Client(
        routeChannel,
        "orders.primary",
        inProcessOwnerChannelFactory(),
        Client.Config.defaults()
            .withDesiredResourceTier(ResourceTier.RESOURCE_TIER_WARM)
            .withRequestTimeoutMs(1500)
            .withStaleRouteRetryAttempts(2)
            .withStaleRouteRetryBackoffMs(0)
            .withIdempotency(true))) {
      assertEquals(1, client.allocateTimestamps(1).size());
      assertEquals(ResourceTier.RESOURCE_TIER_WARM, observedTier.get());
      assertEquals(1500, observedTimeoutMs.get());
      assertFalse(observedRequestId.get().isBlank());
      assertTrue(observedEnsureDeadline.get());
      assertTrue(observedAllocateDeadline.get());
      assertEquals(0, getRouteCalls.get());
    } finally {
      routeChannel.shutdownNow();
      ownerServer.shutdownNow();
      routeServer.shutdownNow();
    }
  }

  @Test
  void idempotentClientsUseDistinctRequestIds() throws Exception {
    var requestIds = new CopyOnWriteArrayList<String>();
    var ownerServerName = InProcessServerBuilder.generateName();
    var routeServerName = InProcessServerBuilder.generateName();

    Server ownerServer =
        InProcessServerBuilder.forName(ownerServerName)
            .directExecutor()
            .addService(
                new TimestampServiceGrpc.TimestampServiceImplBase() {
                  @Override
                  public void allocateTimestamps(
                      AllocateTimestampsRequest request,
                      StreamObserver<AllocateTimestampsResponse> responseObserver) {
                    requestIds.add(request.getClientRequestId());
                    responseObserver.onNext(
                        AllocateTimestampsResponse.newBuilder()
                            .setTimelineKey(request.getTimelineKey())
                            .setGeneratorId(7)
                            .setEpoch(3)
                            .setRouteVersion(11)
                            .addRanges(
                                TimestampRange.newBuilder().setStartTso(100).setEndTso(100).build())
                            .build());
                    responseObserver.onCompleted();
                  }
                })
            .build()
            .start();

    Server routeServer = routeServer(routeServerName, ownerServerName, 11);
    ManagedChannel firstRouteChannel =
        InProcessChannelBuilder.forName(routeServerName).directExecutor().build();
    ManagedChannel secondRouteChannel =
        InProcessChannelBuilder.forName(routeServerName).directExecutor().build();

    try (Client first = new Client(
            firstRouteChannel,
            "orders.primary",
            inProcessOwnerChannelFactory(),
            true);
        Client second = new Client(
            secondRouteChannel,
            "orders.primary",
            inProcessOwnerChannelFactory(),
            true)) {
      assertEquals(1, first.allocateTimestamps(1).size());
      assertEquals(1, second.allocateTimestamps(1).size());
      assertEquals(2, requestIds.size());
      assertFalse(requestIds.get(0).isBlank());
      assertFalse(requestIds.get(1).isBlank());
      assertNotEquals(requestIds.get(0), requestIds.get(1));
    } finally {
      firstRouteChannel.shutdownNow();
      secondRouteChannel.shutdownNow();
      ownerServer.shutdownNow();
      routeServer.shutdownNow();
    }
  }

  @Test
  void concurrentAllocationsShareClientWithoutSerializingHotPath() throws Exception {
    var firstEntered = new CountDownLatch(1);
    var secondEntered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var allocateCalls = new AtomicInteger();
    var ownerServerName = InProcessServerBuilder.generateName();
    var routeServerName = InProcessServerBuilder.generateName();

    Server ownerServer =
        InProcessServerBuilder.forName(ownerServerName)
            .directExecutor()
            .addService(
                new TimestampServiceGrpc.TimestampServiceImplBase() {
                  @Override
                  public void allocateTimestamps(
                      AllocateTimestampsRequest request,
                      StreamObserver<AllocateTimestampsResponse> responseObserver) {
                    int call = allocateCalls.incrementAndGet();
                    if (call == 1) {
                      firstEntered.countDown();
                    } else if (call == 2) {
                      secondEntered.countDown();
                    }
                    try {
                      assertTrue(release.await(2, TimeUnit.SECONDS));
                    } catch (InterruptedException err) {
                      Thread.currentThread().interrupt();
                      responseObserver.onError(Status.CANCELLED.asRuntimeException());
                      return;
                    }
                    responseObserver.onNext(
                        AllocateTimestampsResponse.newBuilder()
                            .setTimelineKey(request.getTimelineKey())
                            .setGeneratorId(7)
                            .setEpoch(3)
                            .setRouteVersion(11)
                            .addRanges(
                                TimestampRange.newBuilder().setStartTso(100).setEndTso(100).build())
                            .build());
                    responseObserver.onCompleted();
                  }
                })
            .build()
            .start();

    Server routeServer = routeServer(routeServerName, ownerServerName, 11);
    ManagedChannel routeChannel =
        InProcessChannelBuilder.forName(routeServerName).directExecutor().build();
    var executor = Executors.newFixedThreadPool(2);

    try (Client client = new Client(
        routeChannel,
        "orders.primary",
        inProcessOwnerChannelFactory())) {
      var first = executor.submit(() -> client.allocateTimestamps(1));
      assertTrue(firstEntered.await(2, TimeUnit.SECONDS));
      var second = executor.submit(() -> client.allocateTimestamps(1));
      assertTrue(secondEntered.await(2, TimeUnit.SECONDS));
      release.countDown();
      assertEquals(1, first.get(2, TimeUnit.SECONDS).size());
      assertEquals(1, second.get(2, TimeUnit.SECONDS).size());
    } finally {
      executor.shutdownNow();
      routeChannel.shutdownNow();
      ownerServer.shutdownNow();
      routeServer.shutdownNow();
    }
  }

  @Test
  void concurrentStaleAllocationsSingleflightRouteRefresh() throws Exception {
    var routeVersion = new AtomicInteger(11);
    var getRouteCalls = new AtomicInteger();
    var ownerServerName = InProcessServerBuilder.generateName();
    var routeServerName = InProcessServerBuilder.generateName();

    Server ownerServer =
        InProcessServerBuilder.forName(ownerServerName)
            .directExecutor()
            .addService(
                new TimestampServiceGrpc.TimestampServiceImplBase() {
                  @Override
                  public void allocateTimestamps(
                      AllocateTimestampsRequest request,
                      StreamObserver<AllocateTimestampsResponse> responseObserver) {
                    if (request.getExpectedRouteVersion() != routeVersion.get()) {
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
    Server routeServer =
        InProcessServerBuilder.forName(routeServerName)
            .directExecutor()
            .addService(
                new TimelineRouteServiceGrpc.TimelineRouteServiceImplBase() {
                  @Override
                  public void ensureTimeline(
                      EnsureTimelineRequest request,
                      StreamObserver<EnsureTimelineResponse> responseObserver) {
                    responseObserver.onNext(
                        EnsureTimelineResponse.newBuilder()
                            .setRoute(
                                route(request.getTimelineKey(), ownerServerName, routeVersion.get()))
                            .build());
                    responseObserver.onCompleted();
                  }

                  @Override
                  public void getTimelineRoute(
                      GetTimelineRouteRequest request,
                      StreamObserver<GetTimelineRouteResponse> responseObserver) {
                    getRouteCalls.incrementAndGet();
                    responseObserver.onNext(
                        GetTimelineRouteResponse.newBuilder()
                            .setRoute(
                                route(request.getTimelineKey(), ownerServerName, routeVersion.get()))
                            .build());
                    responseObserver.onCompleted();
                  }
                })
            .build()
            .start();
    ManagedChannel routeChannel =
        InProcessChannelBuilder.forName(routeServerName).directExecutor().build();
    var executor = Executors.newFixedThreadPool(16);

    try (Client client =
        new Client(routeChannel, "orders.primary", inProcessOwnerChannelFactory())) {
      routeVersion.incrementAndGet();
      var start = new CountDownLatch(1);
      var futures = new ArrayList<java.util.concurrent.Future<List<TimestampRange>>>();
      for (int i = 0; i < 32; i++) {
        futures.add(
            executor.submit(
                () -> {
                  assertTrue(start.await(2, TimeUnit.SECONDS));
                  return client.allocateTimestamps(1);
                }));
      }
      start.countDown();
      for (var future : futures) {
        assertEquals(1, future.get(3, TimeUnit.SECONDS).size());
      }
      assertEquals(1, getRouteCalls.get());
    } finally {
      executor.shutdownNow();
      routeChannel.shutdownNow();
      ownerServer.shutdownNow();
      routeServer.shutdownNow();
    }
  }

  private static Server routeServer(
      String routeServerName, String ownerServerName, int routeVersion) throws Exception {
    return InProcessServerBuilder.forName(routeServerName)
        .directExecutor()
        .addService(
            new TimelineRouteServiceGrpc.TimelineRouteServiceImplBase() {
              @Override
              public void ensureTimeline(
                  EnsureTimelineRequest request,
                  StreamObserver<EnsureTimelineResponse> responseObserver) {
                responseObserver.onNext(
                    EnsureTimelineResponse.newBuilder()
                        .setRoute(route(request.getTimelineKey(), ownerServerName, routeVersion))
                        .build());
                responseObserver.onCompleted();
              }

              @Override
              public void getTimelineRoute(
                  GetTimelineRouteRequest request,
                  StreamObserver<GetTimelineRouteResponse> responseObserver) {
                responseObserver.onNext(
                    GetTimelineRouteResponse.newBuilder()
                        .setRoute(route(request.getTimelineKey(), ownerServerName, routeVersion))
                        .build());
                responseObserver.onCompleted();
              }
            })
        .build()
        .start();
  }

  private static TimelineRoute route(
      String timelineKey, String ownerWorkerEndpoint, int routeVersion) {
    return TimelineRoute.newBuilder()
        .setTimelineKey(timelineKey)
        .setGeneratorId(7)
        .setOwnerWorkerEndpoint(ownerWorkerEndpoint)
        .setEpoch(3)
        .setRouteVersion(routeVersion)
        .setResourceTier(ResourceTier.RESOURCE_TIER_SHARED)
        .build();
  }

  private static Client.AllocationChannelFactory inProcessOwnerChannelFactory() {
    return ownerWorkerEndpoint ->
        InProcessChannelBuilder.forName(ownerWorkerEndpoint).directExecutor().build();
  }

  @Test
  void transportConfigRequiresClientCertAndKeyTogether() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new Client(
                "dns:///chronos.internal:50051",
                "orders.primary",
                Client.TransportConfig.secure()
                    .withTrustedCaPem("ca".getBytes())
                    .withClientIdentityPem("cert".getBytes(), null)));
  }

  @Test
  void constructorRejectsMissingEnsureRoute() throws Exception {
    var routeServerName = InProcessServerBuilder.generateName();
    Server routeServer =
        InProcessServerBuilder.forName(routeServerName)
            .directExecutor()
            .addService(
                new TimelineRouteServiceGrpc.TimelineRouteServiceImplBase() {
                  @Override
                  public void ensureTimeline(
                      EnsureTimelineRequest request,
                      StreamObserver<EnsureTimelineResponse> responseObserver) {
                    responseObserver.onNext(EnsureTimelineResponse.newBuilder().build());
                    responseObserver.onCompleted();
                  }
                })
            .build()
            .start();
    ManagedChannel routeChannel =
        InProcessChannelBuilder.forName(routeServerName).directExecutor().build();

    try {
      var err =
          assertThrows(
              IllegalStateException.class,
              () ->
                  new Client(
                      routeChannel,
                      "orders.primary",
                      inProcessOwnerChannelFactory()));
      assertTrue(err.getMessage().contains("Chronos returned no route from ensureTimeline"));
    } finally {
      routeChannel.shutdownNow();
      routeServer.shutdownNow();
    }
  }

  @Test
  void allocateRejectsMissingRefreshedRoute() throws Exception {
    var staleOnFirstAllocate = new AtomicBoolean(true);
    var routeServerName = InProcessServerBuilder.generateName();
    var ownerServerName = InProcessServerBuilder.generateName();
    Server ownerServer =
        InProcessServerBuilder.forName(ownerServerName)
            .directExecutor()
            .addService(
                new TimestampServiceGrpc.TimestampServiceImplBase() {
                  @Override
                  public void allocateTimestamps(
                      AllocateTimestampsRequest request,
                      StreamObserver<AllocateTimestampsResponse> responseObserver) {
                    if (staleOnFirstAllocate.getAndSet(false)) {
                      var status =
                          com.google.rpc.Status.newBuilder()
                              .setCode(Status.Code.FAILED_PRECONDITION.value())
                              .setMessage("stale route")
                              .addDetails(
                                  Any.pack(
                                      ErrorDetail.newBuilder()
                                          .setCode(ErrorCode.ERROR_CODE_ROUTE_VERSION_MISMATCH)
                                          .build()))
                              .build();
                      responseObserver.onError(StatusProto.toStatusRuntimeException(status));
                      return;
                    }
                    responseObserver.onError(Status.INTERNAL.asRuntimeException());
                  }
                })
            .build()
            .start();
    Server routeServer =
        InProcessServerBuilder.forName(routeServerName)
            .directExecutor()
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
                                    .setOwnerWorkerEndpoint(ownerServerName)
                                    .setEpoch(3)
                                    .setRouteVersion(11)
                                    .setResourceTier(ResourceTier.RESOURCE_TIER_SHARED)
                                    .build())
                            .build());
                    responseObserver.onCompleted();
                  }

                  @Override
                  public void getTimelineRoute(
                      GetTimelineRouteRequest request,
                      StreamObserver<GetTimelineRouteResponse> responseObserver) {
                    responseObserver.onNext(GetTimelineRouteResponse.newBuilder().build());
                    responseObserver.onCompleted();
                  }
                })
            .build()
            .start();
    ManagedChannel routeChannel =
        InProcessChannelBuilder.forName(routeServerName).directExecutor().build();

    try {
      try (Client client =
          new Client(
              routeChannel,
              "orders.primary",
              inProcessOwnerChannelFactory())) {
        var err = assertThrows(IllegalStateException.class, () -> client.allocateTimestamps(1));
        assertTrue(err.getMessage().contains("Chronos returned no route from getTimelineRoute"));
      }
    } finally {
      routeChannel.shutdownNow();
      ownerServer.shutdownNow();
      routeServer.shutdownNow();
    }
  }
}
