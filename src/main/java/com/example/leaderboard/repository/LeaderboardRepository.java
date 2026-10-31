package com.example.leaderboard.repository;

import com.example.leaderboard.entity.Leaderboard;
import java.util.*;
import org.springframework.data.jpa.repository.JpaRepository;

public interface LeaderboardRepository extends JpaRepository<Leaderboard, UUID> {
  List<Leaderboard> findByActiveTrue();
  Optional<Leaderboard> findBySeasonAndActiveTrue(int season);
}
