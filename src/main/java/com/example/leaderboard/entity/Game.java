package com.example.leaderboard.entity;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "games")
public class Game {
    @Id
    @Column(name = "game_id")
    private UUID gameId;

    @Column(name = "played_at", nullable = false)
    private Instant playedAt;

    @Column(name = "number_of_players", nullable = false)
    private int numberOfPlayers;

    @Column(name = "match_type", nullable = false)
    private String matchType;

    @Column(name = "top_player")
    private String topPlayer;

    protected Game() {
    }

    public Game(UUID gameId, Instant playedAt, int numberOfPlayers, String matchType, String topPlayer) {
        this.gameId = gameId;
        this.playedAt = playedAt;
        this.numberOfPlayers = numberOfPlayers;
        this.matchType = matchType;
        this.topPlayer = topPlayer;
    }

    public UUID getGameId() {
        return gameId;
    }

    public Instant getPlayedAt() {
        return playedAt;
    }

    public int getNumberOfPlayers() {
        return numberOfPlayers;
    }

    public String getMatchType() {
        return matchType;
    }

    public String getTopPlayer() {
        return topPlayer;
    }
}
