package com.example.leaderboard.client;

import java.util.List;
import java.util.Map;

/**
 * gRPC service config with a retry policy. Only UNAVAILABLE is retried: the request did not reach
 * business logic, or a dependency was down. Retrying SubmitScore is safe because every attempt
 * carries the same idempotency key.
 */
public final class RetryConfig {
    public static final int MAX_ATTEMPTS = 4;

    private RetryConfig() {
    }

    // Numbers in a service config map must be Doubles.
    public static Map<String, Object> serviceConfig() {
        Map<String, Object> retry = Map.of(
                "maxAttempts", (double) MAX_ATTEMPTS,
                "initialBackoff", "0.1s",
                "maxBackoff", "1s",
                "backoffMultiplier", 2.0,
                "retryableStatusCodes", List.of("UNAVAILABLE"));
        Map<String, Object> method = Map.of(
                "name", List.of(Map.of("service", "leaderboard.v1.LeaderboardService")),
                "retryPolicy", retry);
        return Map.of("methodConfig", List.of(method));
    }
}
