package com.example.leaderboard.rest;

import com.example.leaderboard.core.Models;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

/** Mirrors the gRPC mapping: 400 = INVALID_ARGUMENT, 404 = NOT_FOUND, 409 = ALREADY_EXISTS, 503 = UNAVAILABLE. */
@RestControllerAdvice
public class RestErrors {
    public record Err(String error) {
    }

    @ExceptionHandler({IllegalArgumentException.class, MethodArgumentTypeMismatchException.class,
            MissingRequestHeaderException.class})
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    Err bad(Exception e) {
        return new Err(e.getMessage());
    }

    @ExceptionHandler(DataIntegrityViolationException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    Err badRef(Exception e) {
        return new Err("unknown user or leaderboard");
    }

    @ExceptionHandler(Models.NotFound.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    Err nf(Exception e) {
        return new Err(e.getMessage());
    }

    @ExceptionHandler(Models.IdempotencyConflict.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    Err conflict(Exception e) {
        return new Err(e.getMessage());
    }

    // same set as GrpcErrors.isTransient: a pool timeout while opening the transaction is CannotCreateTransactionException
    @ExceptionHandler({TransientDataAccessException.class, DataAccessResourceFailureException.class,
            CannotCreateTransactionException.class})
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    Err unavailable(Exception e) {
        return new Err("dependency unavailable");
    }
}
