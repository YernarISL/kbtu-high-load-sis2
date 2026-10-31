#!/usr/bin/env bash
# Runs the gRPC vs REST/JSON benchmark against a running, seeded service.
# Usage: bench/run-bench.sh [smoke|payload|wire|latency|chaos|all]   (default: all)
# Tuning: BENCH_OPTS="-Dbench.duration=60 -Dbench.warmup=30 -Dbench.runs=3" bench/run-bench.sh latency
set -euo pipefail
cd "$(dirname "$0")/.."
mvn -q -DskipTests test-compile
mvn -q dependency:build-classpath -Dmdep.outputFile=target/test-classpath.txt -Dmdep.includeScope=test
SEP=":"; case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) SEP=";";; esac
CP="target/test-classes${SEP}target/classes${SEP}$(cat target/test-classpath.txt)"
java ${BENCH_OPTS:-} -cp "$CP" com.example.leaderboard.bench.Bench "${1:-all}"
