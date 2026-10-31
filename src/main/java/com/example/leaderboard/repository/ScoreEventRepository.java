package com.example.leaderboard.repository;

import com.example.leaderboard.entity.*;
import java.util.UUID;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;

public interface ScoreEventRepository extends JpaRepository<ScoreEvent, ScoreEventId> {

  /** Returns 1 if recorded, 0 if this (board, match, user) was already counted. */
  @Modifying
  @Query(nativeQuery = true, value =
      "INSERT INTO score_events(leaderboard_id, match_hash, user_id, score) " +
      "VALUES (:lb, :match, :user, :score) ON CONFLICT DO NOTHING")
  int tryInsert(@Param("lb") UUID lb, @Param("match") String match,
                @Param("user") UUID user, @Param("score") long score);
}
