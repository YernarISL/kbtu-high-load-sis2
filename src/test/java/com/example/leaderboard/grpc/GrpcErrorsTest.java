package com.example.leaderboard.grpc;

import com.example.leaderboard.core.Models;
import io.grpc.Status;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.transaction.CannotCreateTransactionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The status code decides whether the client retries, so the mapping is pinned down here. */
class GrpcErrorsTest {

    static Status.Code code(Throwable e) {
        return GrpcErrors.status(e).getCode();
    }

    @Test
    void transientFailuresAreUnavailable() {
        assertEquals(Status.Code.UNAVAILABLE, code(new QueryTimeoutException("Redis command timed out")));
        assertEquals(Status.Code.UNAVAILABLE, code(new RedisConnectionFailureException("down")));
        assertEquals(Status.Code.UNAVAILABLE, code(new CannotGetJdbcConnectionException("pool")));
        // Hikari timeout while JpaTransactionManager opens the transaction
        assertEquals(Status.Code.UNAVAILABLE, code(new CannotCreateTransactionException("Could not open JPA EntityManager")));
    }

    @Test
    void clientMistakesAreNotRetryable() {
        assertEquals(Status.Code.INVALID_ARGUMENT, code(new IllegalArgumentException("bad")));
        assertEquals(Status.Code.INVALID_ARGUMENT, code(new DataIntegrityViolationException("fk")));
        assertEquals(Status.Code.NOT_FOUND, code(new Models.NotFound("x")));
        assertEquals(Status.Code.ALREADY_EXISTS, code(new Models.IdempotencyConflict("x")));
        assertEquals(Status.Code.INTERNAL, code(new IllegalStateException("bug")));
    }
}
