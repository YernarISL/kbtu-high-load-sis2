package com.example.leaderboard.grpc;

import com.example.leaderboard.core.Models;
import com.example.leaderboard.grpc.v1.Entry;
import com.example.leaderboard.grpc.v1.GetTopResponse;

import java.util.List;

/** Domain model to protobuf mapping. */
final class Pb {
    private Pb() {
    }

    static Entry entry(Models.Entry e) {
        return Entry.newBuilder().setUserId(e.userId().toString()).setUsername(e.username())
                .setRegion(e.region()).setRank(e.rank()).setScore(e.score()).build();
    }

    static GetTopResponse top(List<Models.Entry> es) {
        var b = GetTopResponse.newBuilder();
        es.forEach(e -> b.addEntries(entry(e)));
        return b.build();
    }
}
