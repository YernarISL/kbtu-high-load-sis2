package com.example.leaderboard.entity;

import jakarta.persistence.*;

import java.util.UUID;

@Entity
@Table(name = "users")
public class User {
    @Id
    @Column(name = "user_id")
    private UUID userId;

    @Column(nullable = false, unique = true)
    private String username;

    @Column(nullable = false)
    private String region;

    @Column(nullable = false)
    private int wins;

    @Column(nullable = false)
    private int losses;

    @Column(name = "best_rank")
    private Integer bestRank;

    protected User() {
    }                       // required by JPA

    public User(UUID userId, String username, String region) {
        this.userId = userId;
        this.username = username;
        this.region = region;
    }

    public void recordWin() {
        wins++;
    }

    public void recordLoss() {
        losses++;
    }

    public void updateBestRank(int rank) {
        if (bestRank == null || rank < bestRank) bestRank = rank;
    }

    public UUID getUserId() {
        return userId;
    }

    public String getUsername() {
        return username;
    }

    public String getRegion() {
        return region;
    }

    public int getWins() {
        return wins;
    }

    public int getLosses() {
        return losses;
    }

    public Integer getBestRank() {
        return bestRank;
    }
}
