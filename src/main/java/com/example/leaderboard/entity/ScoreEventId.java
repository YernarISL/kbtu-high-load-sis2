package com.example.leaderboard.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

import java.io.Serializable;
import java.util.UUID;

@Embeddable
public record ScoreEventId(
        @Column(name = "leaderboard_id") UUID leaderboardId,
        @Column(name = "match_hash") String matchHash,
        @Column(name = "user_id") UUID userId) implements Serializable {
}
