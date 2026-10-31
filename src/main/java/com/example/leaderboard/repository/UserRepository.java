package com.example.leaderboard.repository;

import com.example.leaderboard.entity.User;
import java.util.*;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;

public interface UserRepository extends JpaRepository<User, UUID> {
  Optional<User> findByUsername(String username);

  /** Batch profile lookup used to hydrate a ranked page. */
  @Query("select u from User u where u.userId in :ids")
  List<User> findAllByIds(@Param("ids") Collection<UUID> ids);
}
