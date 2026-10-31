package com.example.leaderboard;

import com.example.leaderboard.client.LeaderboardClient;
import com.example.leaderboard.client.RestLeaderboardClient;
import com.example.leaderboard.core.LeaderboardCore;
import com.example.leaderboard.core.Models;
import com.example.leaderboard.core.RankingStore;
import com.example.leaderboard.grpc.v1.SubmitScoreRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

/** Idempotency, error mapping and replay-repair against real PostgreSQL and Redis. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class LeaderboardServiceIntegrationTest {
    @Container
    static PostgreSQLContainer pg = new PostgreSQLContainer("postgres:16");
    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7").withExposedPorts(6379);
    static final int GRPC_PORT = freePort();

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", pg::getJdbcUrl);
        r.add("spring.datasource.username", pg::getUsername);
        r.add("spring.datasource.password", pg::getPassword);
        r.add("spring.data.redis.host", redis::getHost);
        r.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        r.add("spring.grpc.server.port", () -> GRPC_PORT);
    }

    static int freePort() {
        try (var s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Autowired
    LeaderboardCore core;
    @Autowired
    RankingStore ranking;
    @Autowired
    JdbcTemplate jdbc;
    @LocalServerPort
    int httpPort;

    static LeaderboardClient grpc;
    UUID lb = UUID.randomUUID(), user = UUID.randomUUID();

    @BeforeAll
    static void client() {
        grpc = new LeaderboardClient("localhost:" + GRPC_PORT);
    }

    @AfterAll
    static void closeClient() {
        grpc.close();
    }

    @BeforeEach
    void seed() {
        jdbc.update("INSERT INTO leaderboards(id, season) VALUES (?, 1)", lb);
        jdbc.update("INSERT INTO users(user_id, username, region) VALUES (?, ?, 'KZ')", user, "u-" + user);
    }

    // ------------------------------------------------------------ core idempotency

    @Test
    void sameKeyAppliesOnce_evenUnderConcurrency() throws Exception {
        var pool = Executors.newFixedThreadPool(16);
        List<Future<Models.SubmitResult>> fs = new ArrayList<>();
        for (int i = 0; i < 16; i++) fs.add(pool.submit(() -> core.submitScore(lb, user, 4500, "m1", "key-" + lb)));
        int replays = 0;
        for (var f : fs) {
            var r = f.get();
            assertEquals(4500, r.bestScore());
            assertEquals(1, r.rank());
            if (r.replayed()) replays++;
        }
        pool.shutdown();
        assertEquals(15, replays);
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM score_events WHERE leaderboard_id = ?", Integer.class, lb));
    }

    @Test
    void sameKeyDifferentPayload_isRejected() {
        core.submitScore(lb, user, 100, "m2", "k2-" + lb);
        assertThrows(Models.IdempotencyConflict.class, () -> core.submitScore(lb, user, 999, "m2", "k2-" + lb));
    }

    @Test
    void sameMatchUnderNewKey_isCountedOnce() {
        core.submitScore(lb, user, 300, "m-dup", "a-" + lb);
        var r = core.submitScore(lb, user, 900, "m-dup", "b-" + lb);   // new key, same match: not applied
        assertEquals(300, r.bestScore());
        assertFalse(r.replayed());
    }

    @Test
    void lowerScoreNeverReducesBest() {
        core.submitScore(lb, user, 500, "m3", "k3-" + lb);
        assertEquals(500, core.submitScore(lb, user, 100, "m4", "k4-" + lb).bestScore());
    }

    /**
     * Redis fails after the Postgres commit. The retry with the same key is recognised as a replay and repairs
     * Redis, so the write is applied exactly once and the ranking converges.
     */
    @Test
    void retryAfterRedisFailureReplaysAndRepairsRanking() {
        var docker = redis.getDockerClient();
        docker.pauseContainerCmd(redis.getContainerId()).exec();
        try {
            assertThrows(DataAccessException.class, () -> core.submitScore(lb, user, 777, "m5", "k5-" + lb));
        } finally {
            docker.unpauseContainerCmd(redis.getContainerId()).exec();
        }
        // committed in Postgres even though the call failed (the timed-out Redis command may or may not have run)
        assertEquals(777L, jdbc.queryForObject(
                "SELECT score FROM leaderboard_entries WHERE leaderboard_id = ? AND user_id = ?", Long.class, lb, user));

        var retry = core.submitScore(lb, user, 777, "m5", "k5-" + lb);

        assertTrue(retry.replayed());
        assertEquals(777, retry.bestScore());
        assertEquals(777.0, ranking.score(lb, user));
        assertEquals(1, retry.rank());
    }

    // ------------------------------------------------------------ gRPC end to end

    @Test
    void grpc_submitRetryIsReplayed_andReadsWork() {
        var req = SubmitScoreRequest.newBuilder().setLeaderboardId(lb.toString()).setUserId(user.toString())
                .setScore(1234).setMatchHash("g1").setIdempotencyKey("g1-" + lb).build();
        var first = grpc.submitScore(req);
        var again = grpc.submitScore(req);
        assertFalse(first.getReplayed());
        assertTrue(again.getReplayed());
        assertEquals(1234, again.getBestScore());

        assertEquals(1, grpc.top(lb.toString(), 10).getEntriesCount());
        var me = grpc.userRank(lb.toString(), user.toString());
        assertEquals("u-" + user, me.getUsername());
        assertEquals(1, me.getRank());
        assertEquals(1, grpc.neighbors(lb.toString(), user.toString(), 5).getEntriesCount());
    }

    @Test
    void grpc_statusCodes() {
        assertEquals(Status.Code.NOT_FOUND, grpcError(() -> grpc.userRank(lb.toString(), UUID.randomUUID().toString())));
        assertEquals(Status.Code.INVALID_ARGUMENT, grpcError(() -> grpc.top("not-a-uuid", 10)));
        assertEquals(Status.Code.INVALID_ARGUMENT,          // unknown user violates the foreign key
                grpcError(() -> grpc.submitScore(lb.toString(), UUID.randomUUID().toString(), 1, "x")));
        var req = SubmitScoreRequest.newBuilder().setLeaderboardId(lb.toString()).setUserId(user.toString())
                .setScore(1).setMatchHash("g2").setIdempotencyKey("g2-" + lb).build();
        grpc.submitScore(req);
        assertEquals(Status.Code.ALREADY_EXISTS, grpcError(() -> grpc.submitScore(req.toBuilder().setScore(2).build())));
    }

    static Status.Code grpcError(Runnable r) {
        return assertThrows(StatusRuntimeException.class, r::run).getStatus().getCode();
    }

    // ------------------------------------------------------------ REST baseline

    @Test
    void rest_sameDataAndSameSemantics() {
        var rest = new RestLeaderboardClient("http://localhost:" + httpPort, true);
        var first = rest.submitScore(lb.toString(), user.toString(), 555, "r1", "r1-" + lb);
        var again = rest.submitScore(lb.toString(), user.toString(), 555, "r1", "r1-" + lb);
        assertFalse(first.replayed());
        assertTrue(again.replayed());

        var restMe = rest.userRank(lb.toString(), user.toString());
        var grpcMe = grpc.userRank(lb.toString(), user.toString());
        assertEquals(grpcMe.getScore(), restMe.score());
        assertEquals(grpcMe.getRank(), restMe.rank());
        assertEquals(grpcMe.getUsername(), restMe.username());

        assertEquals(409, restError(() -> rest.submitScore(lb.toString(), user.toString(), 1, "r1", "r1-" + lb)));
        assertEquals(404, restError(() -> rest.userRank(lb.toString(), UUID.randomUUID().toString())));
        assertEquals(400, restError(() -> rest.top("not-a-uuid", 10)));
    }

    static int restError(Runnable r) {
        return assertThrows(RestLeaderboardClient.RestException.class, r::run).status;
    }
}
