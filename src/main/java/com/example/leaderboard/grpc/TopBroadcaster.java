package com.example.leaderboard.grpc;

import com.example.leaderboard.core.LeaderboardCore;
import com.example.leaderboard.core.Models;
import com.example.leaderboard.grpc.v1.GetTopResponse;
import io.grpc.stub.ServerCallStreamObserver;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Pushes top-N snapshots to WatchTop subscribers. Score changes only mark a board dirty;
 * a 500 ms flush coalesces bursts. A subscriber that is not ready skips a frame and gets the
 * latest snapshot on the next flush (latest-wins backpressure).
 */
@Component
public class TopBroadcaster {
    private record Sub(UUID lb, int limit, ServerCallStreamObserver<GetTopResponse> obs) {
    }

    private final Set<Sub> subs = ConcurrentHashMap.newKeySet();
    private final Set<UUID> dirty = ConcurrentHashMap.newKeySet();
    private final LeaderboardCore core;

    public TopBroadcaster(LeaderboardCore core) {
        this.core = core;
    }

    public void register(UUID lb, int limit, ServerCallStreamObserver<GetTopResponse> obs) {
        var sub = new Sub(lb, Math.min(Math.max(limit, 1), 100), obs);
        obs.setOnCancelHandler(() -> subs.remove(sub));
        subs.add(sub);
        push(sub, core.top(lb, sub.limit(), 0));            // initial snapshot
    }

    @EventListener
    public void on(LeaderboardCore.ScoreChanged e) {
        dirty.add(e.leaderboardId());
    }

    @Scheduled(fixedDelay = 500)
    void flush() {
        for (UUID lb : new ArrayList<>(dirty)) {
            dirty.remove(lb);
            for (Sub s : subs) if (s.lb().equals(lb)) push(s, core.top(lb, s.limit(), 0));
        }
    }

    private void push(Sub s, List<Models.Entry> top) {
        synchronized (s.obs()) {                             // StreamObserver is not thread-safe
            try {
                if (s.obs().isCancelled()) subs.remove(s);
                else if (s.obs().isReady()) s.obs().onNext(Pb.top(top));
            } catch (RuntimeException ex) {
                subs.remove(s);
            }
        }
    }
}
