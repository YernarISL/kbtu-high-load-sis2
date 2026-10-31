package com.example.leaderboard.entity;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

import org.hibernate.annotations.CreationTimestamp;

/**
 * One result per (board, match, user): the natural dedupe guard.
 */
@Entity
@Table(name = "score_events")
public class ScoreEvent {
    @EmbeddedId
    private ScoreEventId id;

    @Column(nullable = false)
    private long score;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected ScoreEvent() {
    }

    public ScoreEvent(UUID leaderboardId, String matchHash, UUID userId, long score) {
        this.id = new ScoreEventId(leaderboardId, matchHash, userId);
        this.score = score;
    }

    public ScoreEventId getId() {
        return id;
    }

    public long getScore() {
        return score;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
