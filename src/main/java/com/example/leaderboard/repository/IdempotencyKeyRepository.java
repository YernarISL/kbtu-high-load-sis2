package com.example.leaderboard.repository;

import com.example.leaderboard.entity.IdempotencyKey;
import java.time.Instant;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;

public interface IdempotencyKeyRepository extends JpaRepository<IdempotencyKey, String> {

  /** 1 = first time we see this key, 0 = retry or reuse. Blocks while a concurrent tx holds the key. */
  @Modifying
  @Query(nativeQuery = true, value =
      "INSERT INTO idempotency_keys(idem_key, request_hash) VALUES (:key, :hash) " +
      "ON CONFLICT (idem_key) DO NOTHING")
  int tryInsert(@Param("key") String key, @Param("hash") String hash);

  @Modifying(flushAutomatically = true, clearAutomatically = true)
  @Query("update IdempotencyKey k set k.resultScore = :score where k.idemKey = :key")
  int storeResult(@Param("key") String key, @Param("score") long score);

  /** Retention job: run daily, e.g. before = now - 24h. */
  @Modifying
  @Query("delete from IdempotencyKey k where k.createdAt < :before")
  int deleteOlderThan(@Param("before") Instant before);
}
