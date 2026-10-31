package com.example.leaderboard;

import com.example.leaderboard.client.RetryConfig;
import com.example.leaderboard.grpc.v1.*;
import io.grpc.*;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Client-side retry policy and deadlines, against an in-process server (no Docker needed). */
class RetryAndDeadlineTest {
    private final List<Server> servers = new ArrayList<>();
    private final List<ManagedChannel> channels = new ArrayList<>();

    @AfterEach
    void tearDown() {
        channels.forEach(ManagedChannel::shutdownNow);
        servers.forEach(Server::shutdownNow);
    }

    private LeaderboardServiceGrpc.LeaderboardServiceBlockingStub stub(BindableService impl, boolean retries)
            throws Exception {
        String name = InProcessServerBuilder.generateName();
        servers.add(InProcessServerBuilder.forName(name).directExecutor().addService(impl).build().start());
        var b = InProcessChannelBuilder.forName(name).directExecutor();
        if (retries) b.defaultServiceConfig(RetryConfig.serviceConfig()).enableRetry();
        else b.disableRetry();
        var ch = b.build();
        channels.add(ch);
        return LeaderboardServiceGrpc.newBlockingStub(ch);
    }

    /** Fails with the given status `failures` times, then succeeds; records the idempotency key of every attempt. */
    private static class Flaky extends LeaderboardServiceGrpc.LeaderboardServiceImplBase {
        final AtomicInteger calls = new AtomicInteger();
        final List<String> keys = new ArrayList<>();
        final int failures;
        final Status status;

        Flaky(int failures, Status status) {
            this.failures = failures;
            this.status = status;
        }

        @Override
        public void submitScore(SubmitScoreRequest r, StreamObserver<SubmitScoreResponse> o) {
            keys.add(r.getIdempotencyKey());
            if (calls.incrementAndGet() <= failures) {
                o.onError(status.asRuntimeException());
                return;
            }
            o.onNext(SubmitScoreResponse.newBuilder().setBestScore(r.getScore()).build());
            o.onCompleted();
        }
    }

    private static SubmitScoreRequest req() {
        return SubmitScoreRequest.newBuilder().setScore(42).setIdempotencyKey("key-1").build();
    }

    @Test
    void retriesUnavailableThenSucceeds_withTheSameIdempotencyKey() throws Exception {
        var impl = new Flaky(2, Status.UNAVAILABLE);
        var resp = stub(impl, true).withDeadlineAfter(2, TimeUnit.SECONDS).submitScore(req());

        assertEquals(42, resp.getBestScore());
        assertEquals(3, impl.calls.get());                    // 2 failures + 1 success
        assertEquals(List.of("key-1", "key-1", "key-1"), impl.keys);
    }

    @Test
    void givesUpAfterMaxAttempts() throws Exception {
        var impl = new Flaky(100, Status.UNAVAILABLE);
        var e = assertThrows(StatusRuntimeException.class,
                () -> stub(impl, true).withDeadlineAfter(5, TimeUnit.SECONDS).submitScore(req()));

        assertEquals(Status.Code.UNAVAILABLE, e.getStatus().getCode());
        assertEquals(RetryConfig.MAX_ATTEMPTS, impl.calls.get());
    }

    @Test
    void doesNotRetryClientErrors() throws Exception {
        for (Status s : List.of(Status.INVALID_ARGUMENT, Status.NOT_FOUND, Status.ALREADY_EXISTS)) {
            var impl = new Flaky(1, s);
            var e = assertThrows(StatusRuntimeException.class,
                    () -> stub(impl, true).withDeadlineAfter(2, TimeUnit.SECONDS).submitScore(req()));
            assertEquals(s.getCode(), e.getStatus().getCode());
            assertEquals(1, impl.calls.get(), "must not retry " + s.getCode());
        }
    }

    @Test
    void withoutRetryPolicyTheFirstFailureSurfaces() throws Exception {
        var impl = new Flaky(1, Status.UNAVAILABLE);
        assertThrows(StatusRuntimeException.class,
                () -> stub(impl, false).withDeadlineAfter(2, TimeUnit.SECONDS).submitScore(req()));
        assertEquals(1, impl.calls.get());
    }

    @Test
    void deadlineExceededWhenServerIsSlow_andServerSeesTheDeadline() throws Exception {
        var serverDeadline = new java.util.concurrent.atomic.AtomicReference<Deadline>();
        var slow = new LeaderboardServiceGrpc.LeaderboardServiceImplBase() {
            @Override
            public void getTop(GetTopRequest r, StreamObserver<GetTopResponse> o) {
                serverDeadline.set(Context.current().getDeadline());   // propagated from the client
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ignored) {
                }
                o.onNext(GetTopResponse.getDefaultInstance());
                o.onCompleted();
            }
        };
        // a real executor (not direct) so the client's deadline timer can fire while the server sleeps
        String name = InProcessServerBuilder.generateName();
        servers.add(InProcessServerBuilder.forName(name).addService(slow).build().start());
        var ch = InProcessChannelBuilder.forName(name).defaultServiceConfig(RetryConfig.serviceConfig()).enableRetry().build();
        channels.add(ch);

        long t0 = System.nanoTime();
        var e = assertThrows(StatusRuntimeException.class, () -> LeaderboardServiceGrpc.newBlockingStub(ch)
                .withDeadlineAfter(100, TimeUnit.MILLISECONDS).getTop(GetTopRequest.getDefaultInstance()));
        long ms = (System.nanoTime() - t0) / 1_000_000;

        assertEquals(Status.Code.DEADLINE_EXCEEDED, e.getStatus().getCode());
        assertTrue(ms < 400, "call should fail at the deadline, took " + ms + " ms");
        assertNotNull(serverDeadline.get(), "server should see the client's deadline");
    }
}
