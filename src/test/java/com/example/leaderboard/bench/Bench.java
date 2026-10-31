package com.example.leaderboard.bench;

import com.example.leaderboard.client.LeaderboardClient;
import com.example.leaderboard.client.RestLeaderboardClient;
import com.example.leaderboard.grpc.v1.*;
import com.example.leaderboard.rest.LeaderboardRestController.EntryDto;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import tools.jackson.databind.json.JsonMapper;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.zip.GZIPOutputStream;

/**
 * Benchmark harness: gRPC vs REST/JSON against a running, seeded service.
 * <pre>
 * modes (first arg): smoke | payload | wire | latency | chaos | all
 * -Dbench.duration=20 -Dbench.warmup=10 -Dbench.runs=3 -Dbench.concurrency=50 -Dbench.channels=10
 * </pre>
 * Both protocols use the production clients (deadlines, retries) and fully deserialize responses.
 */
public class Bench {
    static final String LB = new UUID(0, 1).toString();
    static final String READ_USER = new UUID(0, 40_000).toString();   // never written by the benchmark
    static final String GRPC = "localhost:9090", REST = "http://localhost:8080";
    static final String[] SCENARIOS = {"top100", "neighbors", "rank", "submit"};

    static final int DURATION = Integer.getInteger("bench.duration", 20);
    static final int WARMUP = Integer.getInteger("bench.warmup", 10);
    static final int RUNS = Integer.getInteger("bench.runs", 3);
    static final int CONCURRENCY = Integer.getInteger("bench.concurrency", 50);
    static final int CHANNELS = Integer.getInteger("bench.channels", 10);
    static final Path OUT = Path.of("bench", "out");

    interface Proto {
        String name();

        void call(String scenario, int worker);
    }

    // ---------------------------------------------------------------- protocol drivers

    static final class GrpcProto implements Proto {
        final List<LeaderboardClient> clients = new ArrayList<>();

        GrpcProto(String target, int channels, boolean retries) {
            for (int i = 0; i < channels; i++) clients.add(new LeaderboardClient(target, retries));
        }

        public String name() {
            return "gRPC";
        }

        public void call(String s, int worker) {
            var c = clients.get(worker % clients.size());
            switch (s) {
                case "top100" -> check(c.top(LB, 100).getEntriesCount() == 100);
                case "neighbors" -> check(c.neighbors(LB, READ_USER, 5).getEntriesCount() == 11);
                case "rank" -> check(c.userRank(LB, READ_USER).getRank() > 0);
                case "submit" -> c.submitScore(LB, randomUser(), randomScore(), UUID.randomUUID().toString());
                default -> throw new IllegalArgumentException(s);
            }
        }

        void close() {
            clients.forEach(LeaderboardClient::close);
        }
    }

    static final class RestProto implements Proto {
        final RestLeaderboardClient c;

        RestProto(String base, boolean retries) {
            c = new RestLeaderboardClient(base, retries);
        }

        public String name() {
            return "REST/JSON";
        }

        public void call(String s, int worker) {
            switch (s) {
                case "top100" -> check(c.top(LB, 100).size() == 100);
                case "neighbors" -> check(c.neighbors(LB, READ_USER, 5).size() == 11);
                case "rank" -> check(c.userRank(LB, READ_USER).rank() > 0);
                case "submit" -> c.submitScore(LB, randomUser(), randomScore(), UUID.randomUUID().toString());
                default -> throw new IllegalArgumentException(s);
            }
        }
    }

    static String randomUser() {
        int i;
        do i = ThreadLocalRandom.current().nextInt(1, 100_001); while (i == 40_000);   // keep READ_USER fixed
        return new UUID(0, i).toString();
    }

    static long randomScore() {
        return ThreadLocalRandom.current().nextInt(6000);
    }

    static void check(boolean ok) {
        if (!ok) throw new IllegalStateException("unexpected response shape");
    }

    // ---------------------------------------------------------------- load loop

    record RunResult(long ok, long errors, double seconds, long[] latNanos, double serverCpu, double clientCpu,
                     Map<String, Long> errorKinds) {
        double rps() {
            return ok / seconds;
        }

        double pct(double p) {
            if (latNanos.length == 0) return Double.NaN;
            int i = (int) Math.ceil(p / 100.0 * latNanos.length) - 1;
            return latNanos[Math.max(0, Math.min(i, latNanos.length - 1))] / 1e6;
        }
    }

    /** Runs closed-loop load: `concurrency` workers each issue the next call as soon as the last returns. */
    static RunResult load(Proto p, String scenario, int concurrency, double seconds, Runnable midway)
            throws Exception {
        var stop = new AtomicBoolean();
        var errors = new AtomicLong();
        var kinds = new ConcurrentHashMap<String, Long>();
        List<long[]> lats = Collections.synchronizedList(new ArrayList<>());
        var pool = Executors.newFixedThreadPool(concurrency);
        var cpu = new CpuSampler();
        var start = new CountDownLatch(1);
        List<Future<?>> fs = new ArrayList<>();
        for (int w = 0; w < concurrency; w++) {
            final int worker = w;
            fs.add(pool.submit(() -> {
                long[] buf = new long[1 << 14];
                int n = 0;
                try {
                    start.await();
                } catch (InterruptedException e) {
                    return;
                }
                while (!stop.get()) {
                    long t0 = System.nanoTime();
                    try {
                        p.call(scenario, worker);
                        long d = System.nanoTime() - t0;
                        if (n == buf.length) buf = Arrays.copyOf(buf, n * 2);
                        buf[n++] = d;
                    } catch (RuntimeException e) {
                        errors.incrementAndGet();
                        kinds.merge(errorKind(e), 1L, Long::sum);
                    }
                }
                lats.add(Arrays.copyOf(buf, n));
            }));
        }
        cpu.start();
        long t0 = System.nanoTime();
        start.countDown();
        if (midway != null) midway.run();
        long remaining = (long) (seconds * 1e9) - (System.nanoTime() - t0);
        if (remaining > 0) TimeUnit.NANOSECONDS.sleep(remaining);
        stop.set(true);
        double elapsed = (System.nanoTime() - t0) / 1e9;
        for (var f : fs) f.get();
        pool.shutdown();
        cpu.stop();
        long[] all = lats.stream().flatMapToLong(Arrays::stream).sorted().toArray();
        return new RunResult(all.length, errors.get(), elapsed, all, cpu.server(), cpu.client(), new TreeMap<>(kinds));
    }

    static String errorKind(RuntimeException e) {
        if (e instanceof StatusRuntimeException s) return s.getStatus().getCode().name();
        if (e instanceof RestLeaderboardClient.RestException r) return r.status == 0 ? "IO_ERROR" : "HTTP_" + r.status;
        return e.getClass().getSimpleName();
    }

    /** Samples server CPU via actuator and this JVM's CPU once per second. */
    static final class CpuSampler {
        final List<Double> server = Collections.synchronizedList(new ArrayList<>());
        final List<Double> client = Collections.synchronizedList(new ArrayList<>());
        final HttpClient http = HttpClient.newHttpClient();
        final JsonMapper json = JsonMapper.builder().build();
        ScheduledExecutorService ses;

        void start() {
            ses = Executors.newSingleThreadScheduledExecutor();
            ses.scheduleAtFixedRate(() -> {
                try {
                    var r = http.send(HttpRequest.newBuilder(URI.create(REST + "/actuator/metrics/process.cpu.usage")).build(),
                            HttpResponse.BodyHandlers.ofString());
                    server.add(json.readTree(r.body()).get("measurements").get(0).get("value").asDouble());
                } catch (Exception ignored) {
                }
                var os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
                client.add(os.getProcessCpuLoad());
            }, 1, 1, TimeUnit.SECONDS);
        }

        void stop() {
            ses.shutdownNow();
        }

        double server() {
            return avg(server);
        }

        double client() {
            return avg(client);
        }

        static double avg(List<Double> xs) {
            synchronized (xs) {
                return xs.stream().filter(d -> d >= 0).mapToDouble(d -> d).average().orElse(Double.NaN);
            }
        }
    }

    // ---------------------------------------------------------------- modes

    public static void main(String[] args) {
        try {
            run(args);
            System.exit(0);
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);                                 // don't hang on non-daemon client threads
        }
    }

    static void run(String[] args) throws Exception {
        Locale.setDefault(Locale.ROOT);                    // '.' as decimal separator in the tables
        String mode = args.length > 0 ? args[0] : "all";
        Files.createDirectories(OUT);
        var report = new StringBuilder();
        report.append("# Benchmark results\n\n").append(environment()).append('\n');
        switch (mode) {
            case "smoke" -> smoke();
            case "payload" -> report.append(payload());
            case "wire" -> report.append(wire());
            case "latency" -> report.append(latency());
            case "chaos" -> report.append(chaos());
            case "all" -> {
                smoke();
                report.append(payload()).append(wire()).append(latency()).append(chaos());
            }
            default -> throw new IllegalArgumentException("unknown mode " + mode);
        }
        if (!mode.equals("smoke")) {
            Path f = OUT.resolve(mode.equals("all") ? "results.md" : "results-" + mode + ".md");
            Files.writeString(f, report.toString());
            System.out.println("\n" + report + "\nwritten to " + f.toAbsolutePath());
        }
    }

    static String environment() {
        var os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        return String.format("""
                        | setting | value |
                        |---|---|
                        | date | %s |
                        | OS | %s %s (%s), %d logical CPUs, %.1f GB RAM |
                        | JDK | %s %s |
                        | load | closed loop, %d concurrent callers; gRPC over %d HTTP/2 connections, REST over HTTP/1.1 keep-alive pool |
                        | timing | warm-up %d s, then %d runs x %d s per scenario; table shows the median of the runs |
                        | client | the production clients in `client/` (deadlines + retries on), responses fully deserialized |
                        """, Instant.now(), System.getProperty("os.name"), System.getProperty("os.version"),
                System.getProperty("os.arch"), Runtime.getRuntime().availableProcessors(),
                os.getTotalMemorySize() / 1e9, System.getProperty("java.vm.name"), System.getProperty("java.version"),
                CONCURRENCY, CHANNELS, WARMUP, RUNS, DURATION);
    }

    /** Calls every RPC on both protocols, checks they return the same data, and exercises WatchTop. */
    static void smoke() throws Exception {
        System.out.println("== smoke test");
        try (var g = new LeaderboardClient(GRPC)) {
            var r = new RestLeaderboardClient(REST, true);
            var gTop = g.top(LB, 10).getEntriesList().stream().map(Bench::dto).toList();
            var rTop = r.top(LB, 10);
            System.out.println("GetTop(10)     gRPC == REST: " + gTop.equals(rTop) + "  first=" + rTop.get(0));
            check(gTop.equals(rTop));
            var gN = g.neighbors(LB, READ_USER, 5).getEntriesList().stream().map(Bench::dto).toList();
            System.out.println("GetNeighbors   gRPC == REST: " + gN.equals(r.neighbors(LB, READ_USER, 5)) + "  size=" + gN.size());
            check(gN.equals(r.neighbors(LB, READ_USER, 5)));
            var gR = dto(g.userRank(LB, READ_USER));
            System.out.println("GetUserRank    gRPC == REST: " + gR.equals(r.userRank(LB, READ_USER)) + "  " + gR);
            check(gR.equals(r.userRank(LB, READ_USER)));

            var req = SubmitScoreRequest.newBuilder().setLeaderboardId(LB).setUserId(new UUID(0, 99_999).toString())
                    .setScore(randomScore()).setMatchHash(UUID.randomUUID().toString())
                    .setIdempotencyKey(UUID.randomUUID().toString()).build();
            var first = g.submitScore(req);
            var again = g.submitScore(req);
            System.out.printf("SubmitScore    first replayed=%s, retry replayed=%s, same best=%s%n",
                    first.getReplayed(), again.getReplayed(), first.getBestScore() == again.getBestScore());
            check(!first.getReplayed() && again.getReplayed());

            // WatchTop: subscribe, push a new #1, expect a snapshot containing it within 2 s
            var ch = ManagedChannelBuilder.forTarget(GRPC).usePlaintext().build();
            var frames = new LinkedBlockingQueue<GetTopResponse>();
            LeaderboardServiceGrpc.newStub(ch).watchTop(WatchTopRequest.newBuilder().setLeaderboardId(LB).setLimit(3).build(),
                    new StreamObserver<>() {
                        public void onNext(GetTopResponse v) {
                            frames.add(v);
                        }

                        public void onError(Throwable t) {
                        }

                        public void onCompleted() {
                        }
                    });
            frames.poll(2, TimeUnit.SECONDS);                       // initial snapshot
            long top = g.top(LB, 1).getEntries(0).getScore() + 1;
            String champ = new UUID(0, 77_777).toString();
            g.submitScore(LB, champ, top, UUID.randomUUID().toString());
            boolean pushed = false;
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (!pushed && System.nanoTime() < until) {
                var f = frames.poll(200, TimeUnit.MILLISECONDS);
                pushed = f != null && f.getEntries(0).getUserId().equals(champ);
            }
            System.out.println("WatchTop       pushed new #1 within 2 s: " + pushed);
            check(pushed);
            ch.shutdownNow();
        }
        System.out.println("smoke test OK");
    }

    static EntryDto dto(Entry e) {
        return new EntryDto(e.getUserId(), e.getUsername(), e.getRegion(), e.getRank(), e.getScore());
    }

    /** Message (body) sizes of the real responses from the running service, raw and gzip. */
    static String payload() throws Exception {
        System.out.println("== payload sizes");
        var json = JsonMapper.builder().build();
        var http = HttpClient.newHttpClient();
        Function<String, byte[]> get = path -> {
            try {
                return http.send(HttpRequest.newBuilder(URI.create(REST + path)).build(),
                        HttpResponse.BodyHandlers.ofByteArray()).body();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        var sb = new StringBuilder("""
                ## Payload size (message body only)

                Same data from the running service: Protobuf bytes from the gRPC response vs the REST response body.

                | message | Protobuf B | JSON B | JSON / Protobuf | Protobuf gzip B | JSON gzip B |
                |---|---:|---:|---:|---:|---:|
                """);
        try (var g = new LeaderboardClient(GRPC)) {
            row(sb, "GetTop response, 100 entries", g.top(LB, 100).toByteArray(), get.apply("/v1/leaderboards/" + LB + "/top?limit=100"));
            row(sb, "GetTop response, 10 entries", g.top(LB, 10).toByteArray(), get.apply("/v1/leaderboards/" + LB + "/top?limit=10"));
            row(sb, "GetNeighbors response, 11 entries", g.neighbors(LB, READ_USER, 5).toByteArray(),
                    get.apply("/v1/leaderboards/" + LB + "/users/" + READ_USER + "/neighbors?radius=5"));
            row(sb, "GetUserRank response, 1 entry", g.userRank(LB, READ_USER).toByteArray(),
                    get.apply("/v1/leaderboards/" + LB + "/" + READ_USER));
            String user = new UUID(0, 12_345).toString(), match = UUID.randomUUID().toString(), key = UUID.randomUUID().toString();
            var pbReq = SubmitScoreRequest.newBuilder().setLeaderboardId(LB).setUserId(user).setScore(4321)
                    .setMatchHash(match).setIdempotencyKey(key).build();
            // REST carries leaderboard id in the path and the key in a header, so count those bytes too
            byte[] jsonBody = json.writeValueAsBytes(Map.of("userId", user, "score", 4321, "matchHash", match));
            row(sb, "SubmitScore request (JSON body only)", pbReq.toByteArray(), jsonBody);
            var pbResp = g.submitScore(pbReq);
            row(sb, "SubmitScore response", pbResp.toByteArray(),
                    json.writeValueAsBytes(Map.of("bestScore", pbResp.getBestScore(), "rank", pbResp.getRank(), "replayed", pbResp.getReplayed())));
        }
        sb.append("""

                The SubmitScore JSON body leaves out the leaderboard id (in the URL path) and the idempotency key
                (in the `Idempotency-Key` header), which Protobuf carries in the message. The wire table below includes them.
                """);
        System.out.println(sb);
        return sb.toString();
    }

    static void row(StringBuilder sb, String name, byte[] pb, byte[] js) throws IOException {
        sb.append(String.format("| %s | %d | %d | %.2fx | %d | %d |%n", name, pb.length, js.length,
                (double) js.length / pb.length, gz(pb), gz(js)));
    }

    static int gz(byte[] b) throws IOException {
        var o = new ByteArrayOutputStream();
        try (var g = new GZIPOutputStream(o)) {
            g.write(b);
        }
        return o.size();
    }

    /** Bytes on the wire per call (headers, framing and body, both directions) via a counting TCP proxy. */
    static String wire() throws Exception {
        System.out.println("== wire bytes");
        int n = 5000, conc = 10;      // bytes per call do not depend on concurrency; 10 keeps the proxy unsaturated
        var sb = new StringBuilder(String.format("""

                ## Bytes on the wire per call

                Counted by a TCP proxy between client and server: everything sent in both directions (HTTP headers,
                HTTP/2 frames, bodies), steady state after 500 warm-up calls, averaged over %d calls at concurrency %d.
                Both clients keep their connections open, so connection setup is excluded.

                | call | gRPC up B | gRPC down B | gRPC total B | REST up B | REST down B | REST total B | REST / gRPC |
                |---|---:|---:|---:|---:|---:|---:|---:|
                """, n, conc));
        long failed = 0;
        for (String s : SCENARIOS) {
            long[] gw, rw;
            try (var proxy = new CountingProxy(9090)) {
                var g = new GrpcProto("localhost:" + proxy.port(), CHANNELS, true);
                gw = countBytes(g, s, proxy, n, conc);
                g.close();
            }
            try (var proxy = new CountingProxy(8080)) {
                rw = countBytes(new RestProto("http://localhost:" + proxy.port(), true), s, proxy, n, conc);
            }
            double gt = (gw[0] + gw[1]) / (double) n, rt = (rw[0] + rw[1]) / (double) n;
            sb.append(String.format("| %s | %.0f | %.0f | %.0f | %.0f | %.0f | %.0f | %.2fx |%n", s,
                    gw[0] / (double) n, gw[1] / (double) n, gt, rw[0] / (double) n, rw[1] / (double) n, rt, rt / gt));
            failed += gw[2] + rw[2];
        }
        if (failed > 0) sb.append(String.format("%nNote: %d of the counted calls failed (deadline) and are still in the average.%n", failed));
        System.out.println(sb);
        return sb.toString();
    }

    /** Returns {bytes up, bytes down, failed calls} for n calls after a warm-up. */
    static long[] countBytes(Proto p, String s, CountingProxy proxy, int n, int conc) throws Exception {
        runN(p, s, 500, conc);                              // connection setup, JIT
        proxy.reset();
        long errors = runN(p, s, n, conc);
        return new long[]{proxy.up.get(), proxy.down.get(), errors};
    }

    /** Issues exactly n calls at the configured concurrency and returns the number that failed. */
    static long runN(Proto p, String s, int n, int conc) throws Exception {
        var left = new AtomicLong(n);
        var errors = new AtomicLong();
        var pool = Executors.newFixedThreadPool(conc);
        try {
            List<Future<?>> fs = new ArrayList<>();
            for (int w = 0; w < conc; w++) {
                int worker = w;
                fs.add(pool.submit(() -> {
                    while (left.getAndDecrement() > 0) {
                        try {
                            p.call(s, worker);
                        } catch (RuntimeException e) {
                            errors.incrementAndGet();
                        }
                    }
                }));
            }
            for (var f : fs) f.get();
        } finally {
            pool.shutdownNow();
        }
        return errors.get();
    }

    static String latency() throws Exception {
        System.out.println("== latency");
        var sb = new StringBuilder(String.format("""

                ## Latency and throughput

                %d concurrent callers, closed loop. p50/p95/p99 in ms, measured in the client around the full call
                (serialize, send, server work, receive, deserialize). Median of %d runs of %d s each, after a %d s warm-up.

                | call | protocol | req/s | p50 ms | p95 ms | p99 ms | max ms | errors | server CPU | client CPU |
                |---|---|---:|---:|---:|---:|---:|---:|---:|---:|
                """, CONCURRENCY, RUNS, DURATION, WARMUP));
        var g = new GrpcProto(GRPC, CHANNELS, true);
        var r = new RestProto(REST, true);
        for (String s : SCENARIOS) {
            for (Proto p : List.of(g, r)) {
                System.out.printf("%s %s: warm-up %d s%n", s, p.name(), WARMUP);
                load(p, s, CONCURRENCY, WARMUP, null);
                List<RunResult> runs = new ArrayList<>();
                for (int i = 0; i < RUNS; i++) {
                    var res = load(p, s, CONCURRENCY, DURATION, null);
                    System.out.printf("  run %d: %.0f req/s p50 %.2f p95 %.2f p99 %.2f errors %d %s%n",
                            i + 1, res.rps(), res.pct(50), res.pct(95), res.pct(99), res.errors(),
                            res.errorKinds().isEmpty() ? "" : res.errorKinds());
                    runs.add(res);
                }
                var kinds = new TreeMap<String, Long>();
                runs.forEach(x -> x.errorKinds().forEach((k, v) -> kinds.merge(k, v, Long::sum)));
                long ok = runs.stream().mapToLong(RunResult::ok).sum();
                long errs = runs.stream().mapToLong(RunResult::errors).sum();
                sb.append(String.format("| %s | %s | %.0f | %.2f | %.2f | %.2f | %.1f | %s | %.0f%% | %.0f%% |%n", s, p.name(),
                        med(runs, RunResult::rps), med(runs, x -> x.pct(50)), med(runs, x -> x.pct(95)),
                        med(runs, x -> x.pct(99)), med(runs, x -> x.pct(100)),
                        errs == 0 ? "0" : String.format("%d of %d %s", errs, ok + errs, kinds),
                        100 * med(runs, RunResult::serverCpu), 100 * med(runs, RunResult::clientCpu)));
            }
        }
        g.close();
        sb.append("\nCPU is the average share of the whole machine (all cores) used by the server and the load generator process.\n");
        System.out.println(sb);
        return sb.toString();
    }

    static double med(List<RunResult> runs, java.util.function.ToDoubleFunction<RunResult> f) {
        double[] xs = runs.stream().mapToDouble(f).sorted().toArray();
        return xs[xs.length / 2];
    }

    /**
     * Fault injection: pause Redis for 1 s in the middle of a SubmitScore run, with client retries on and off.
     * Redis commands then time out (200 ms), the service answers UNAVAILABLE / 503, and only the retrying
     * client recovers. Writes that committed in Postgres before Redis failed come back as replays.
     */
    static String chaos() throws Exception {
        System.out.println("== chaos (Redis paused 1 s during submit load)");
        var sb = new StringBuilder("""

                ## Retries under a dependency failure

                SubmitScore load for 8 s at concurrency 50; Redis is paused (`docker pause`) for 1 s starting at t = 3 s.
                The server maps the Redis timeout to `UNAVAILABLE` / `503`; the client retries only those, reusing the idempotency key.

                | protocol | client retries | successful writes | failed writes | error rate | failures by code | p99 ms | max ms |
                |---|---|---:|---:|---:|---|---:|---:|
                """);
        String redis = redisContainer();
        Runnable outage = () -> {
            try {
                Thread.sleep(3000);
                docker("pause", redis);
                Thread.sleep(1000);
                docker("unpause", redis);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        for (boolean retries : new boolean[]{false, true}) {
            var g = new GrpcProto(GRPC, CHANNELS, retries);
            List<Proto> ps = List.of(g, new RestProto(REST, retries));
            for (Proto p : ps) {
                load(p, "submit", CONCURRENCY, 3, null);       // warm-up
                var res = load(p, "submit", CONCURRENCY, 8, outage);
                sb.append(String.format("| %s | %s | %d | %d | %.2f%% | %s | %.0f | %.0f |%n", p.name(), retries ? "on" : "off",
                        res.ok(), res.errors(), 100.0 * res.errors() / (res.ok() + res.errors()),
                        res.errorKinds().isEmpty() ? "-" : res.errorKinds().toString(), res.pct(99), res.pct(100)));
                System.out.println(sb);
                Thread.sleep(1000);
            }
            g.close();
        }
        return sb.toString();
    }

    static String redisContainer() throws Exception {
        var pr = new ProcessBuilder("docker", "ps", "--filter", "publish=6379", "--format", "{{.Names}}").start();
        String name = new String(pr.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim().split("\\R")[0];
        if (name.isBlank()) throw new IllegalStateException("no container publishes port 6379");
        return name;
    }

    static void docker(String... args) throws Exception {
        var cmd = new ArrayList<String>();
        cmd.add("docker");
        cmd.addAll(List.of(args));
        new ProcessBuilder(cmd).inheritIO().start().waitFor();
    }

    // ---------------------------------------------------------------- counting TCP proxy

    static final class CountingProxy implements AutoCloseable {
        final AtomicLong up = new AtomicLong(), down = new AtomicLong();
        final ServerSocket server;
        final ExecutorService pool = Executors.newCachedThreadPool(r -> {
            var t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        final List<Socket> sockets = new CopyOnWriteArrayList<>();

        CountingProxy(int targetPort) throws IOException {
            server = new ServerSocket(0, 200, InetAddress.getLoopbackAddress());
            pool.submit(() -> {
                while (!server.isClosed()) {
                    try {
                        Socket in = server.accept();
                        Socket out = new Socket(InetAddress.getLoopbackAddress(), targetPort);
                        in.setTcpNoDelay(true);
                        out.setTcpNoDelay(true);
                        sockets.add(in);
                        sockets.add(out);
                        pool.submit(() -> pipe(in, out, up));
                        pool.submit(() -> pipe(out, in, down));
                    } catch (IOException e) {
                        return;
                    }
                }
            });
        }

        int port() {
            return server.getLocalPort();
        }

        void reset() {
            up.set(0);
            down.set(0);
        }

        static void pipe(Socket from, Socket to, AtomicLong counter) {
            byte[] buf = new byte[64 * 1024];
            try (var in = from.getInputStream(); var out = to.getOutputStream()) {
                int n;
                while ((n = in.read(buf)) > 0) {
                    counter.addAndGet(n);
                    out.write(buf, 0, n);
                    out.flush();
                }
            } catch (IOException ignored) {
            } finally {
                try {
                    from.close();
                    to.close();
                } catch (IOException ignored) {
                }
            }
        }

        @Override
        public void close() throws IOException {
            server.close();
            for (Socket s : sockets) s.close();
            pool.shutdownNow();
        }
    }
}
