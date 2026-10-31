package com.example.leaderboard.grpc;

import com.example.leaderboard.core.LeaderboardCore;
import com.example.leaderboard.grpc.v1.*;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.springframework.grpc.server.service.GrpcService;

import java.util.UUID;

@GrpcService
public class LeaderboardGrpcService extends LeaderboardServiceGrpc.LeaderboardServiceImplBase {
    private final LeaderboardCore core;
    private final TopBroadcaster broadcaster;

    public LeaderboardGrpcService(LeaderboardCore core, TopBroadcaster broadcaster) {
        this.core = core;
        this.broadcaster = broadcaster;
    }

    @Override
    public void submitScore(SubmitScoreRequest r, StreamObserver<SubmitScoreResponse> out) {
        Deadlines.check();
        var res = core.submitScore(uuid(r.getLeaderboardId()), uuid(r.getUserId()),
                r.getScore(), r.getMatchHash(), r.getIdempotencyKey());
        out.onNext(SubmitScoreResponse.newBuilder().setBestScore(res.bestScore())
                .setRank(res.rank()).setReplayed(res.replayed()).build());
        out.onCompleted();
    }

    @Override
    public void getTop(GetTopRequest r, StreamObserver<GetTopResponse> out) {
        Deadlines.check();
        out.onNext(Pb.top(core.top(uuid(r.getLeaderboardId()), r.getLimit(), r.getOffset())));
        out.onCompleted();
    }

    @Override
    public void getNeighbors(GetNeighborsRequest r, StreamObserver<GetNeighborsResponse> out) {
        Deadlines.check();
        var b = GetNeighborsResponse.newBuilder();
        core.neighbors(uuid(r.getLeaderboardId()), uuid(r.getUserId()), r.getRadius())
                .forEach(e -> b.addEntries(Pb.entry(e)));
        out.onNext(b.build());
        out.onCompleted();
    }

    @Override
    public void getUserRank(GetUserRankRequest r, StreamObserver<Entry> out) {
        Deadlines.check();
        out.onNext(Pb.entry(core.userRank(uuid(r.getLeaderboardId()), uuid(r.getUserId()))));
        out.onCompleted();
    }

    @Override
    public void watchTop(WatchTopRequest r, StreamObserver<GetTopResponse> out) {
        broadcaster.register(uuid(r.getLeaderboardId()), r.getLimit(),
                (ServerCallStreamObserver<GetTopResponse>) out);
    }

    private static UUID uuid(String s) {
        try {
            return UUID.fromString(s);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("not a valid UUID: '" + s + "'");
        }
    }
}
