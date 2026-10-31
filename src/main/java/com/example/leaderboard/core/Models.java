package com.example.leaderboard.core;

import java.util.UUID;

public final class Models {
    public record Entry(UUID userId, String username, String region, int rank, long score) {
    }

    public record Profile(String username, String region) {
    }

    public record SubmitResult(long bestScore, long rank, boolean replayed) {
    }

    public static class NotFound extends RuntimeException {
        public NotFound(String m) {
            super(m);
        }
    }

    public static class IdempotencyConflict extends RuntimeException {
        public IdempotencyConflict(String m) {
            super(m);
        }
    }
}
