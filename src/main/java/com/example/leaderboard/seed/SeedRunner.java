package com.example.leaderboard.seed;

import com.example.leaderboard.core.RankingStore;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Creates one leaderboard and 100,000 players with deterministic IDs ({@code new UUID(0, i)}),
 * so load scripts can reference stable IDs. Safe to run more than once.
 */
@Component
@Profile("seed")
public class SeedRunner implements CommandLineRunner {
    public static final UUID LB = new UUID(0, 1);
    public static final int PLAYERS = 100_000;
    private final JdbcTemplate jdbc;
    private final StringRedisTemplate redis;

    public SeedRunner(JdbcTemplate jdbc, StringRedisTemplate redis) {
        this.jdbc = jdbc;
        this.redis = redis;
    }

    @Override
    public void run(String... args) {
        String[] regions = {"Kazakhstan", "USA", "Korea", "Russia", "Germany"};
        jdbc.update("INSERT INTO leaderboards(id, season) VALUES (?, 1) ON CONFLICT DO NOTHING", LB);
        var rnd = new Random(42);
        List<Object[]> users = new ArrayList<>(), entries = new ArrayList<>();
        for (int i = 1; i <= PLAYERS; i++) {
            UUID id = new UUID(0, i);
            users.add(new Object[]{id, "player" + i, regions[i % regions.length]});
            entries.add(new Object[]{LB, id, (long) rnd.nextInt(6000)});
        }
        jdbc.batchUpdate("INSERT INTO users(user_id, username, region) VALUES (?,?,?) ON CONFLICT DO NOTHING", users);
        jdbc.batchUpdate("INSERT INTO leaderboard_entries(leaderboard_id, user_id, score) VALUES (?,?,?) ON CONFLICT DO NOTHING", entries);

        // Rebuild the ZSET from Postgres, the source of truth (keeps scores raised by earlier runs).
        // Chunked, because one 100k-member ZADD exceeds the 200 ms Redis command timeout.
        List<ZSetOperations.TypedTuple<String>> z = new ArrayList<>();
        jdbc.query("SELECT user_id, score FROM leaderboard_entries WHERE leaderboard_id = ?",
                rs -> { z.add(ZSetOperations.TypedTuple.of(rs.getObject(1, UUID.class).toString(), (double) rs.getLong(2))); },
                LB);
        redis.delete(RankingStore.key(LB));
        for (int i = 0; i < z.size(); i += 5_000)
            redis.opsForZSet().add(RankingStore.key(LB), new HashSet<>(z.subList(i, Math.min(i + 5_000, z.size()))));
        System.out.printf("seeded leaderboard %s with %d players%n", LB, PLAYERS);
    }
}
