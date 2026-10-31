package com.example.leaderboard.entity;

import jakarta.persistence.*;

import java.time.Instant;

import org.hibernate.annotations.CreationTimestamp;

@Entity
@Table(name = "idempotency_keys")
public class IdempotencyKey {
    @Id
    @Column(name = "idem_key")
    private String idemKey;

    @Column(name = "request_hash", nullable = false)
    private String requestHash;

    @Column(name = "result_score")
    private Long resultScore;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected IdempotencyKey() {
    }

    public IdempotencyKey(String idemKey, String requestHash) {
        this.idemKey = idemKey;
        this.requestHash = requestHash;
    }

    public String getIdemKey() {
        return idemKey;
    }

    public String getRequestHash() {
        return requestHash;
    }

    public Long getResultScore() {
        return resultScore;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
