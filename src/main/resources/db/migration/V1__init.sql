CREATE TABLE users
(
    user_id   UUID PRIMARY KEY,
    username  TEXT NOT NULL UNIQUE,
    region    TEXT NOT NULL,
    wins      INT  NOT NULL DEFAULT 0,
    losses    INT  NOT NULL DEFAULT 0,
    best_rank INT
);
CREATE TABLE leaderboards
(
    id        UUID PRIMARY KEY,
    season    INT     NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true
);
CREATE TABLE leaderboard_entries
(
    leaderboard_id UUID        NOT NULL REFERENCES leaderboards (id),
    user_id        UUID        NOT NULL REFERENCES users (user_id),
    score          BIGINT      NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (leaderboard_id, user_id)
);
CREATE INDEX idx_entries_score ON leaderboard_entries (leaderboard_id, score DESC);

CREATE TABLE score_events
(
    leaderboard_id UUID        NOT NULL,
    match_hash     TEXT        NOT NULL,
    user_id        UUID        NOT NULL,
    score          BIGINT      NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (leaderboard_id, match_hash, user_id)
);

CREATE TABLE idempotency_keys
(
    idem_key     TEXT PRIMARY KEY,
    request_hash TEXT        NOT NULL,
    result_score BIGINT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);