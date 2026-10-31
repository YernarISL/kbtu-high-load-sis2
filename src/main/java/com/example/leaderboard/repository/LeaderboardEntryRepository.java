package com.example.leaderboard.repository;

import com.example.leaderboard.entity.*;
import java.util.*;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;

public interface LeaderboardEntryRepository extends JpaRepository<LeaderboardEntry, LeaderboardEntryId> {

  /** Postgres fallback / Redis rebuild source: best first, stable tie-break. */
  @Query("select e from LeaderboardEntry e where e.id.leaderboardId = :lb " +
         "order by e.score desc, e.id.userId")
  List<LeaderboardEntry> findTop(@Param("lb") UUID lb, Pageable page);

  /** Rank fallback when Redis is unavailable: players strictly ahead + 1. */
  @Query("select count(e) from LeaderboardEntry e where e.id.leaderboardId = :lb and e.score > :score")
  long countAhead(@Param("lb") UUID lb, @Param("score") long score);

  /** Atomic keep-best upsert, safe under concurrency and replay. */
  @Modifying
  @Query(nativeQuery = true, value =
      "INSERT INTO leaderboard_entries(leaderboard_id, user_id, score) VALUES (:lb, :user, :score) " +
      "ON CONFLICT (leaderboard_id, user_id) DO UPDATE " +
      "SET score = GREATEST(leaderboard_entries.score, EXCLUDED.score), updated_at = now()")
  int upsertBest(@Param("lb") UUID lb, @Param("user") UUID user, @Param("score") long score);

  /** Season-end rewards: stream everything for one board. */
  @Query("select e from LeaderboardEntry e where e.id.leaderboardId = :lb order by e.score desc, e.id.userId")
  org.springframework.data.domain.Slice<LeaderboardEntry> pageForRewards(@Param("lb") UUID lb, Pageable page);
}
