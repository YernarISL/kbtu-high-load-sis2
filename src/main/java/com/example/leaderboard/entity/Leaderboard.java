package com.example.leaderboard.entity;

import jakarta.persistence.*;

import java.util.UUID;

@Entity
@Table(name = "leaderboards")
public class Leaderboard {
    @Id
    private UUID id;

    @Column(nullable = false)
    private int season;

    @Column(name = "is_active", nullable = false)
    private boolean active = true;

    protected Leaderboard() {
    }

    public Leaderboard(UUID id, int season) {
        this.id = id;
        this.season = season;
    }

    public void close() {
        this.active = false;
    }       // used by season-end reward job

    public UUID getId() {
        return id;
    }

    public int getSeason() {
        return season;
    }

    public boolean isActive() {
        return active;
    }
}
