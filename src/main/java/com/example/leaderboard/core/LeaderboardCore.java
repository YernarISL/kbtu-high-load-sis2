package com.example.leaderboard.core;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

@Service
public class LeaderboardCore {
    public record ScoreChanged(UUID leaderboardId) {
    }

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final RankingStore ranking;
    private final ProfileCache profiles;
    private final ApplicationEventPublisher events;

    public LeaderboardCore(JdbcTemplate jdbc, PlatformTransactionManager tm, RankingStore ranking,
                           ProfileCache profiles, ApplicationEventPublisher events) {
        this.jdbc = jdbc;
        this.tx = new TransactionTemplate(tm);
        this.ranking = ranking;
        this.profiles = profiles;
        this.events = events;
    }

    public Models.SubmitResult submitScore(UUID lb, UUID user, long score, String matchHash, String idemKey) {
        if (score < 0 || idemKey == null || idemKey.isBlank() || matchHash == null || matchHash.isBlank())
            throw new IllegalArgumentException("score >= 0, match_hash and idempotency_key are required");
        String reqHash = sha256(lb + "|" + user + "|" + score + "|" + matchHash);

        record Tx(long best, boolean replayed) {
        }
        Tx t = tx.execute(st -> {
            int ins = jdbc.update(
                    "INSERT INTO idempotency_keys(idem_key, request_hash) VALUES (?, ?) ON CONFLICT (idem_key) DO NOTHING",
                    idemKey, reqHash);
            if (ins == 0) {                                   // retry or key reuse
                var prev = jdbc.queryForMap(
                        "SELECT request_hash, result_score FROM idempotency_keys WHERE idem_key = ?", idemKey);
                if (!reqHash.equals(prev.get("request_hash")))
                    throw new Models.IdempotencyConflict("idempotency key reused with a different payload");
                return new Tx(((Number) prev.get("result_score")).longValue(), true);
            }
            int fresh = jdbc.update(
                    "INSERT INTO score_events(leaderboard_id, match_hash, user_id, score) VALUES (?,?,?,?) ON CONFLICT DO NOTHING",
                    lb, matchHash, user, score);
            Long best = fresh == 1
                    ? jdbc.queryForObject(
                    "INSERT INTO leaderboard_entries(leaderboard_id, user_id, score) VALUES (?,?,?) " +
                            "ON CONFLICT (leaderboard_id, user_id) DO UPDATE " +
                            "SET score = GREATEST(leaderboard_entries.score, EXCLUDED.score), updated_at = now() " +
                            "RETURNING score", Long.class, lb, user, score)
                    : jdbc.queryForObject(
                    "SELECT score FROM leaderboard_entries WHERE leaderboard_id = ? AND user_id = ?",
                    Long.class, lb, user);
            jdbc.update("UPDATE idempotency_keys SET result_score = ? WHERE idem_key = ?", best, idemKey);
            return new Tx(best, false);
        });

        boolean raised = ranking.raise(lb, user, t.best());   // after commit, replay-safe
        if (raised) events.publishEvent(new ScoreChanged(lb));
        Long r0 = ranking.rank0(lb, user);
        return new Models.SubmitResult(t.best(), r0 == null ? -1 : r0 + 1, t.replayed());
    }

    public List<Models.Entry> top(UUID lb, int limit, int offset) {
        limit = Math.min(Math.max(limit, 1), 1000);
        offset = Math.max(offset, 0);
        return hydrate(ranking.range(lb, offset, offset + limit - 1L), offset);
    }

    public List<Models.Entry> neighbors(UUID lb, UUID user, int radius) {
        Long r = ranking.rank0(lb, user);
        if (r == null) throw new Models.NotFound("user is not ranked");
        radius = Math.min(Math.max(radius, 1), 50);
        long from = Math.max(0, r - radius);
        return hydrate(ranking.range(lb, from, r + radius), from);
    }

    public Models.Entry userRank(UUID lb, UUID user) {
        Long r = ranking.rank0(lb, user);
        Double s = ranking.score(lb, user);
        if (r == null || s == null) throw new Models.NotFound("user is not ranked");
        var p = profiles.getAll(List.of(user)).get(user);
        return new Models.Entry(user, p == null ? "?" : p.username(), p == null ? "?" : p.region(), (int) (r + 1), s.longValue());
    }

    private List<Models.Entry> hydrate(List<RankingStore.Row> rows, long startRank0) {
        var ps = profiles.getAll(rows.stream().map(RankingStore.Row::userId).toList());
        List<Models.Entry> out = new ArrayList<>(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            var row = rows.get(i);
            var p = ps.get(row.userId());
            out.add(new Models.Entry(row.userId(), p == null ? "?" : p.username(), p == null ? "?" : p.region(),
                    (int) (startRank0 + i + 1), row.score()));
        }
        return out;
    }

    private static String sha256(String s) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
