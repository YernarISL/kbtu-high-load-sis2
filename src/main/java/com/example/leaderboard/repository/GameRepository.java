package com.example.leaderboard.repository;

import com.example.leaderboard.entity.Game;
import java.util.UUID;
import org.springframework.data.domain.*;
import org.springframework.data.jpa.repository.JpaRepository;

public interface GameRepository extends JpaRepository<Game, UUID> {
  Page<Game> findByMatchTypeOrderByPlayedAtDesc(String matchType, Pageable page);
}
