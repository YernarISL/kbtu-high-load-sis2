# Benchmark results

| setting | value |
|---|---|
| date | 2026-10-31T06:51:45.078067500Z |
| OS | Windows 10 10.0 (amd64), 4 logical CPUs, 12.8 GB RAM |
| JDK | Java HotSpot(TM) 64-Bit Server VM 21.0.10 |
| load | closed loop, 50 concurrent callers; gRPC over 10 HTTP/2 connections, REST over HTTP/1.1 keep-alive pool |
| timing | warm-up 10 s, then 5 runs x 20 s per scenario; table shows the median of the runs |
| client | the production clients in `client/` (deadlines + retries on), responses fully deserialized |

## Payload size (message body only)

Same data from the running service: Protobuf bytes from the gRPC response vs the REST response body.

| message | Protobuf B | JSON B | JSON / Protobuf | Protobuf gzip B | JSON gzip B |
|---|---:|---:|---:|---:|---:|
| GetTop response, 100 entries | 6666 | 11659 | 1.75x | 1313 | 1299 |
| GetTop response, 10 entries | 670 | 1162 | 1.73x | 242 | 255 |
| GetNeighbors response, 11 entries | 750 | 1312 | 1.75x | 260 | 271 |
| GetUserRank response, 1 entry | 70 | 122 | 1.74x | 63 | 105 |
| SubmitScore request (JSON body only) | 155 | 113 | 0.73x | 107 | 106 |
| SubmitScore response | 7 | 48 | 6.86x | 27 | 67 |

The SubmitScore JSON body leaves out the leaderboard id (in the URL path) and the idempotency key
(in the `Idempotency-Key` header), which Protobuf carries in the message. The wire table below includes them.

## Bytes on the wire per call

Counted by a TCP proxy between client and server: everything sent in both directions (HTTP headers,
HTTP/2 frames, bodies), steady state after 500 warm-up calls, averaged over 5000 calls at concurrency 10.
Both clients keep their connections open, so connection setup is excluded.

| call | gRPC up B | gRPC down B | gRPC total B | REST up B | REST down B | REST total B | REST / gRPC |
|---|---:|---:|---:|---:|---:|---:|---:|
| top100 | 79 | 6703 | 6783 | 145 | 11793 | 11938 | 1.76x |
| neighbors | 116 | 787 | 903 | 193 | 1420 | 1613 | 1.79x |
| rank | 111 | 107 | 218 | 168 | 229 | 397 | 1.82x |
| submit | 191 | 44 | 234 | 360 | 154 | 514 | 2.19x |

## Latency and throughput

50 concurrent callers, closed loop. p50/p95/p99 in ms, measured in the client around the full call
(serialize, send, server work, receive, deserialize). Median of 5 runs of 20 s each, after a 10 s warm-up.

| call | protocol | req/s | p50 ms | p95 ms | p99 ms | max ms | errors | server CPU | client CPU |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| top100 | gRPC | 3131 | 14.53 | 26.16 | 47.46 | 250.9 | 0 | 39% | 22% |
| top100 | REST/JSON | 2785 | 17.00 | 29.78 | 42.74 | 109.9 | 0 | 35% | 25% |
| neighbors | gRPC | 3731 | 12.75 | 20.52 | 29.61 | 104.5 | 0 | 36% | 19% |
| neighbors | REST/JSON | 3525 | 12.88 | 24.44 | 32.44 | 79.5 | 0 | 34% | 21% |
| rank | gRPC | 3920 | 12.02 | 19.82 | 29.29 | 77.3 | 0 | 35% | 19% |
| rank | REST/JSON | 3845 | 11.77 | 22.38 | 29.88 | 84.1 | 0 | 32% | 20% |
| submit | gRPC | 876 | 54.16 | 83.71 | 105.68 | 156.9 | 0 | 18% | 6% |
| submit | REST/JSON | 871 | 55.70 | 83.43 | 105.36 | 156.9 | 0 | 18% | 7% |

CPU is the average share of the whole machine (all cores) used by the server and the load generator process.

## Retries under a dependency failure

SubmitScore load for 8 s at concurrency 50; Redis is paused (`docker pause`) for 1 s starting at t = 3 s.
The server maps the Redis timeout to `UNAVAILABLE` / `503`; the client retries only those, reusing the idempotency key.

| protocol | client retries | successful writes | failed writes | error rate | failures by code | p99 ms | max ms |
|---|---|---:|---:|---:|---|---:|---:|
| gRPC | off | 5949 | 200 | 3.25% | {UNAVAILABLE=200} | 155 | 223 |
| REST/JSON | off | 5334 | 201 | 3.63% | {HTTP_503=201} | 135 | 175 |
| gRPC | on | 5646 | 0 | 0.00% | - | 161 | 1664 |
| REST/JSON | on | 5689 | 11 | 0.19% | {HTTP_503=11} | 130 | 1386 |
