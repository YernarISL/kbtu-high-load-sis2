package com.example.leaderboard.grpc;

import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.Status;

public final class Deadlines {
    private Deadlines() {
    }

    public static void check() {
        Context ctx = Context.current();
        if (ctx.isCancelled()) throw Status.CANCELLED.withDescription("client cancelled").asRuntimeException();
        Deadline d = ctx.getDeadline();
        if (d != null && d.isExpired()) throw Status.DEADLINE_EXCEEDED.asRuntimeException();
    }
}
