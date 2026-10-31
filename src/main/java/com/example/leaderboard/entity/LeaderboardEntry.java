package com.example.leaderboard.entity;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

import org.hibernate.annotations.UpdateTimestamp;

@Entity
@Table(name = "leaderboard_entries")
public class LeaderboardEntry {
    @EmbeddedId
    private LeaderboardEntryId id;

    @Column(nullable = false)
    private long score;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected LeaderboardEntry() {
    }

    public LeaderboardEntry(UUID leaderboardId, UUID userId, long score) {
        this.id = new LeaderboardEntryId(leaderboardId, userId);
        this.score = score;
    }

    /**
     * Keeps the best score only.
     */
    public boolean raiseTo(long newScore) {
        if (newScore > score) {
            score = newScore;
            return true;
        }
        return false;
    }

    public LeaderboardEntryId getId() {
        return id;
    }

    public long getScore() {
        return score;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
