package com.example.leaderboard.client;

import com.example.leaderboard.rest.LeaderboardRestController.EntryDto;
import com.example.leaderboard.rest.LeaderboardRestController.SubmitBody;
import com.example.leaderboard.rest.LeaderboardRestController.SubmitResp;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * REST/JSON client with the same rules as the gRPC client: a total time budget per call,
 * retries on 503 and connection errors only (exponential backoff, full jitter, max 4 attempts),
 * and one Idempotency-Key reused on every attempt of a write.
 */
public final class RestLeaderboardClient {
    private static final TypeReference<List<EntryDto>> ENTRIES = new TypeReference<>() {
    };
    private final HttpClient http;
    private final JsonMapper json = JsonMapper.builder().build();
    private final String base;
    private final boolean retries;

    public static final class RestException extends RuntimeException {
        public final int status;

        RestException(int status, String body) {
            super("HTTP " + status + ": " + body);
            this.status = status;
        }
    }

    public RestLeaderboardClient(String baseUrl, boolean retries) {
        this.base = baseUrl;
        this.retries = retries;
        this.http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofMillis(500)).build();
    }

    public SubmitResp submitScore(String lb, String user, long score, String matchHash) {
        return submitScore(lb, user, score, matchHash, UUID.randomUUID().toString());
    }

    public SubmitResp submitScore(String lb, String user, long score, String matchHash, String idemKey) {
        var req = HttpRequest.newBuilder(URI.create(base + "/v1/leaderboards/" + lb + "/scores"))
                .header("Content-Type", "application/json").header("Idempotency-Key", idemKey)
                .POST(HttpRequest.BodyPublishers.ofByteArray(json.writeValueAsBytes(new SubmitBody(user, score, matchHash))));
        try {
            return json.readValue(send(req, LeaderboardClient.WRITE_MS), SubmitResp.class);
        } catch (Timeout t) {
            // Budget spent, outcome unknown: the idempotency key makes one more attempt safe.
            return json.readValue(send(req, LeaderboardClient.WRITE_MS), SubmitResp.class);
        }
    }

    public List<EntryDto> top(String lb, int limit) {
        return json.readValue(get("/v1/leaderboards/" + lb + "/top?limit=" + limit), ENTRIES);
    }

    public List<EntryDto> neighbors(String lb, String user, int radius) {
        return json.readValue(get("/v1/leaderboards/" + lb + "/users/" + user + "/neighbors?radius=" + radius), ENTRIES);
    }

    public EntryDto userRank(String lb, String user) {
        return json.readValue(get("/v1/leaderboards/" + lb + "/" + user), EntryDto.class);
    }

    private byte[] get(String path) {
        return send(HttpRequest.newBuilder(URI.create(base + path)).GET(), LeaderboardClient.READ_MS);
    }

    private static final class Timeout extends RuntimeException {
        Timeout() {
            super("deadline exceeded");
        }
    }

    private byte[] send(HttpRequest.Builder rb, long budgetMs) {
        long deadline = System.nanoTime() + budgetMs * 1_000_000;
        int maxAttempts = retries ? RetryConfig.MAX_ATTEMPTS : 1;
        for (int attempt = 1; ; attempt++) {
            long remaining = (deadline - System.nanoTime()) / 1_000_000;
            if (remaining <= 0) throw new Timeout();
            try {
                var resp = http.send(rb.timeout(Duration.ofMillis(remaining)).build(),
                        HttpResponse.BodyHandlers.ofByteArray());
                if (resp.statusCode() == 200) return resp.body();
                if (resp.statusCode() != 503 || attempt >= maxAttempts)
                    throw new RestException(resp.statusCode(), new String(resp.body()));
            } catch (HttpTimeoutException e) {
                throw new Timeout();
            } catch (IOException e) {                       // connection refused / reset: retryable
                if (attempt >= maxAttempts) throw new RestException(0, e.toString());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RestException(0, "interrupted");
            }
            backoff(attempt, deadline);
        }
    }

    /** Same schedule as the gRPC policy: random(0, min(1 s, 100 ms * 2^(n-1))). */
    private static void backoff(int attempt, long deadline) {
        long cap = Math.min(1000, 100L << (attempt - 1));
        long sleep = Math.min(ThreadLocalRandom.current().nextLong(cap + 1), (deadline - System.nanoTime()) / 1_000_000);
        if (sleep > 0) {
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
