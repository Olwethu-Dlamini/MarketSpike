CREATE TABLE IF NOT EXISTS ticks (
  id                INTEGER PRIMARY KEY,
  symbol            TEXT    NOT NULL,
  venue_ts_ns       INTEGER NOT NULL,
  recv_ts_ns        INTEGER NOT NULL,
  bid               REAL    NOT NULL,
  ask               REAL    NOT NULL,
  bid_qty           REAL,
  ask_qty           REAL,
  excess_transit_us INTEGER,
  engine_us         INTEGER,
  tradeable         INTEGER NOT NULL DEFAULT 1,
  source            TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_ticks_sym_ts ON ticks (symbol, venue_ts_ns);

CREATE TABLE IF NOT EXISTS regime_events (
  id            INTEGER PRIMARY KEY,
  ts_ns         INTEGER NOT NULL,
  symbol        TEXT    NOT NULL,
  from_state    TEXT    NOT NULL,
  to_state      TEXT    NOT NULL,
  score         REAL,
  v_ratio       REAL,
  spread_z      REAL,
  trigger       TEXT,
  event_context TEXT
);

CREATE TABLE IF NOT EXISTS client_latency (
  id            INTEGER PRIMARY KEY,
  ts_ns         INTEGER NOT NULL,
  client_id     TEXT    NOT NULL,
  round_trip_us INTEGER,
  offset_us     INTEGER,
  delivery_us   INTEGER
);

CREATE TABLE IF NOT EXISTS calc_log (
  id             INTEGER PRIMARY KEY,
  ts_ns          INTEGER NOT NULL,
  symbol         TEXT    NOT NULL,
  request_json   TEXT    NOT NULL,
  response_json  TEXT    NOT NULL,
  regime         TEXT,
  model_version  TEXT
);

CREATE TABLE IF NOT EXISTS training_samples (
  id                   INTEGER PRIMARY KEY,
  t_ns                 INTEGER NOT NULL,
  symbol               TEXT    NOT NULL,
  log_v_ratio          REAL,
  spread_z             REAL,
  log_spread_bps       REAL,
  log_latency_ms       REAL,
  quote_rate_hz        REAL,
  book_imbalance       REAL,
  signed_secs_to_event REAL,
  in_event_window      INTEGER,
  abs_return_5s        REAL,
  delta_ms             REAL    NOT NULL,
  direction            INTEGER NOT NULL,
  cost_bps             REAL    NOT NULL,
  regime               TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_train_sym_t ON training_samples (symbol, t_ns);

CREATE TABLE IF NOT EXISTS model_registry (
  version           TEXT PRIMARY KEY,
  symbol            TEXT    NOT NULL,
  trained_at_ns     INTEGER NOT NULL,
  coefficients_json TEXT    NOT NULL,
  metrics_json      TEXT    NOT NULL,
  n_rows            INTEGER,
  is_active         INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS calendar_events (
  id          INTEGER PRIMARY KEY,
  event_ts_ns INTEGER NOT NULL,
  name        TEXT    NOT NULL,
  importance  TEXT    NOT NULL,
  country     TEXT,
  affects     TEXT
);

CREATE TABLE IF NOT EXISTS schema_version (
  version INTEGER NOT NULL
);
