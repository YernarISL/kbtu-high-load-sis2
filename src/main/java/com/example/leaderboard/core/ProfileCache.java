package com.example.leaderboard.core;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;

@Component
public class ProfileCache {
    private final Cache<UUID, Models.Profile> cache = Caffeine.newBuilder()
            .maximumSize(200_000).expireAfterWrite(Duration.ofMinutes(10)).build();
    private final JdbcTemplate jdbc;
    public ProfileCache(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    public Map<UUID, Models.Profile> getAll(Collection<UUID> ids) {
        Map<UUID, Models.Profile> out = new HashMap<>();
        List<UUID> missing = new ArrayList<>();
        for (UUID id : ids) {
            var p = cache.getIfPresent(id);
            if (p != null) out.put(id, p); else missing.add(id);
        }
        if (!missing.isEmpty()) {
            jdbc.query(con -> {
                var ps = con.prepareStatement("SELECT user_id, username, region FROM users WHERE user_id = ANY (?)");
                ps.setArray(1, con.createArrayOf("uuid", missing.toArray()));
                return ps;
            }, rs -> {
                var id = rs.getObject(1, UUID.class);
                var p = new Models.Profile(rs.getString(2), rs.getString(3));
                cache.put(id, p); out.put(id, p);
            });
        }
        return out;
    }
}
