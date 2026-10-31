package com.example.leaderboard.grpc;

import com.example.leaderboard.core.Models;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.grpc.server.exception.GrpcExceptionHandler;
import org.springframework.stereotype.Component;
import org.springframework.transaction.CannotCreateTransactionException;

/**
 * Maps domain and infrastructure exceptions to gRPC status codes.
 * Only UNAVAILABLE is retried by the client, so transient dependency failures map to it
 * and client mistakes never do.
 */
@Component
public class GrpcErrors implements GrpcExceptionHandler {

    @Override
    public StatusException handleException(Throwable e) {
        return status(e).asException();
    }

    static Status status(Throwable e) {
        if (e instanceof StatusRuntimeException sre) return sre.getStatus();
        if (e instanceof StatusException se) return se.getStatus();
        if (e instanceof IllegalArgumentException) return Status.INVALID_ARGUMENT.withDescription(e.getMessage());
        if (e instanceof DataIntegrityViolationException)
            return Status.INVALID_ARGUMENT.withDescription("unknown user or leaderboard");
        if (e instanceof Models.NotFound) return Status.NOT_FOUND.withDescription(e.getMessage());
        if (e instanceof Models.IdempotencyConflict) return Status.ALREADY_EXISTS.withDescription(e.getMessage());
        if (isTransient(e)) return Status.UNAVAILABLE.withDescription("dependency unavailable");
        return Status.INTERNAL.withDescription("internal error");
    }

    /**
     * Pool exhausted, statement timeout, Redis down or slow: safe to retry. A pool timeout while opening the
     * transaction surfaces as CannotCreateTransactionException, which is not a DataAccessException.
     */
    public static boolean isTransient(Throwable e) {
        return e instanceof TransientDataAccessException || e instanceof DataAccessResourceFailureException
                || e instanceof CannotCreateTransactionException;
    }
}
