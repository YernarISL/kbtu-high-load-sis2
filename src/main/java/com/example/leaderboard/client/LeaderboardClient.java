package com.example.leaderboard.client;

import com.example.leaderboard.grpc.v1.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** gRPC client with per-call deadlines and channel-level retries. */
public final class LeaderboardClient implements AutoCloseable {
    public static final long WRITE_MS = 2000, READ_MS = 1000;   // total budget, retries included
    private final ManagedChannel channel;
    private final LeaderboardServiceGrpc.LeaderboardServiceBlockingStub stub;

    public LeaderboardClient(String target) {
        this(target, true);
    }

    public LeaderboardClient(String target, boolean retries) {
        var b = ManagedChannelBuilder.forTarget(target).usePlaintext().keepAliveTime(30, TimeUnit.SECONDS);
        if (retries) b.defaultServiceConfig(RetryConfig.serviceConfig()).enableRetry();
        else b.disableRetry();
        this.channel = b.build();
        this.stub = LeaderboardServiceGrpc.newBlockingStub(channel);
    }

    /** One logical write = one key, however many attempts it takes. */
    public SubmitScoreResponse submitScore(String lb, String user, long score, String matchHash) {
        return submitScore(SubmitScoreRequest.newBuilder().setLeaderboardId(lb).setUserId(user).setScore(score)
                .setMatchHash(matchHash).setIdempotencyKey(UUID.randomUUID().toString()).build());
    }

    public SubmitScoreResponse submitScore(SubmitScoreRequest req) {
        try {
            return stub.withDeadlineAfter(WRITE_MS, TimeUnit.MILLISECONDS).submitScore(req);
        } catch (StatusRuntimeException e) {
            // The channel never retries DEADLINE_EXCEEDED (the budget is spent) and the outcome is unknown.
            // The idempotency key makes one more attempt safe.
            if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED)
                return stub.withDeadlineAfter(WRITE_MS, TimeUnit.MILLISECONDS).submitScore(req);
            throw e;
        }
    }

    public GetTopResponse top(String lb, int limit) {
        return stub.withDeadlineAfter(READ_MS, TimeUnit.MILLISECONDS)
                .getTop(GetTopRequest.newBuilder().setLeaderboardId(lb).setLimit(limit).build());
    }

    public GetNeighborsResponse neighbors(String lb, String user, int radius) {
        return stub.withDeadlineAfter(READ_MS, TimeUnit.MILLISECONDS).getNeighbors(
                GetNeighborsRequest.newBuilder().setLeaderboardId(lb).setUserId(user).setRadius(radius).build());
    }

    public Entry userRank(String lb, String user) {
        return stub.withDeadlineAfter(READ_MS, TimeUnit.MILLISECONDS).getUserRank(
                GetUserRankRequest.newBuilder().setLeaderboardId(lb).setUserId(user).build());
    }

    @Override
    public void close() {
        channel.shutdown();
    }
}
