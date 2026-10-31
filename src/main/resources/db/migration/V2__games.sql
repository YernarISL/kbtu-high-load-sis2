-- Backs the Game entity from the domain model (match history; not used by the ranking path).
CREATE TABLE games
(
    game_id           UUID PRIMARY KEY,
    played_at         TIMESTAMPTZ NOT NULL,
    number_of_players INT         NOT NULL,
    match_type        TEXT        NOT NULL,
    top_player        TEXT
);
CREATE INDEX idx_games_type_played ON games (match_type, played_at DESC);
