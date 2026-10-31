package com.example.leaderboard.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

import java.io.Serializable;
import java.util.UUID;

/**
 * Composite key. Records as @Embeddable need Hibernate 6.2+ (Spring Boot 3.1+).
 */
@Embeddable
public record LeaderboardEntryId(
        @Column(name = "leaderboard_id") UUID leaderboardId,
        @Column(name = "user_id") UUID userId) implements Serializable {
}
