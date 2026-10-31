package com.example.leaderboard.core;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Component
public class RankingStore {
    public record Row(UUID userId, long score) {
    }

    private static final DefaultRedisScript<Long> RAISE = new DefaultRedisScript<>(
            "local cur = redis.call('ZSCORE', KEYS[1], ARGV[2]) " +
                    "if (not cur) or tonumber(ARGV[1]) > tonumber(cur) then " +
                    "redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2]) return 1 end return 0", Long.class);

    private final StringRedisTemplate redis;

    public RankingStore(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public static String key(UUID lb) {
        return "lb:{" + lb + "}";
    }

    public boolean raise(UUID lb, UUID user, long score) {
        Long r = redis.execute(RAISE, List.of(key(lb)), Long.toString(score), user.toString());
        return r != null && r == 1L;
    }

    public Long rank0(UUID lb, UUID user) {            // 0-based, null if absent
        return redis.opsForZSet().reverseRank(key(lb), user.toString());
    }

    public Double score(UUID lb, UUID user) {
        return redis.opsForZSet().score(key(lb), user.toString());
    }

    public List<Row> range(UUID lb, long from, long to) {   // inclusive, best first
        var set = redis.opsForZSet().reverseRangeWithScores(key(lb), from, to);
        List<Row> out = new ArrayList<>();
        if (set != null) for (var t : set) out.add(new Row(UUID.fromString(t.getValue()), t.getScore().longValue()));
        return out;
    }
}
