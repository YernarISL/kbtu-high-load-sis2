package com.example.leaderboard.rest;

import com.example.leaderboard.core.LeaderboardCore;
import com.example.leaderboard.core.Models;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

/**
 * REST/JSON baseline. Calls the same {@link LeaderboardCore} as the gRPC service, so measured
 * differences come from the protocol and serialization, not from business logic.
 */
@RestController
@RequestMapping("/v1/leaderboards/{lb}")
public class LeaderboardRestController {
    public record EntryDto(String userId, String username, String region, int rank, long score) {
    }

    public record SubmitBody(String userId, long score, String matchHash) {
    }

    public record SubmitResp(long bestScore, long rank, boolean replayed) {
    }

    private final LeaderboardCore core;

    public LeaderboardRestController(LeaderboardCore core) {
        this.core = core;
    }

    private static EntryDto dto(Models.Entry e) {
        return new EntryDto(e.userId().toString(), e.username(), e.region(), e.rank(), e.score());
    }

    @GetMapping("/top")
    public List<EntryDto> top(@PathVariable UUID lb, @RequestParam(defaultValue = "100") int limit,
                              @RequestParam(defaultValue = "0") int offset) {
        return core.top(lb, limit, offset).stream().map(LeaderboardRestController::dto).toList();
    }

    @GetMapping("/users/{uid}/neighbors")
    public List<EntryDto> neighbors(@PathVariable UUID lb, @PathVariable UUID uid,
                                    @RequestParam(defaultValue = "5") int radius) {
        return core.neighbors(lb, uid, radius).stream().map(LeaderboardRestController::dto).toList();
    }

    @PostMapping("/scores")
    public SubmitResp submit(@PathVariable UUID lb, @RequestHeader("Idempotency-Key") String key,
                             @RequestBody SubmitBody b) {
        if (b.userId() == null) throw new IllegalArgumentException("userId is required");
        var r = core.submitScore(lb, UUID.fromString(b.userId()), b.score(), b.matchHash(), key);
        return new SubmitResp(r.bestScore(), r.rank(), r.replayed());
    }

    @GetMapping("/{uid}")
    public EntryDto user(@PathVariable UUID lb, @PathVariable UUID uid) {
        return dto(core.userRank(lb, uid));
    }
}
