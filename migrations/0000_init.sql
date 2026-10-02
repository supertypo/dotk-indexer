CREATE TABLE vars (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE deeds (
  key            BYTEA PRIMARY KEY,
  kind           SMALLINT NOT NULL,
  name           TEXT,
  owner_type     SMALLINT,
  owner          BYTEA,
  claim          BYTEA,
  outpoint_txid  BYTEA,
  outpoint_index INT,
  value          BIGINT,
  accepted_daa   BIGINT
);
CREATE INDEX deeds_owner   ON deeds (owner_type, owner) WHERE kind = 0;
CREATE INDEX deeds_name    ON deeds (name)              WHERE kind = 0;
CREATE INDEX deeds_pending ON deeds (accepted_daa)      WHERE kind = 1;

CREATE TABLE gaps (
  lo             BYTEA PRIMARY KEY,
  hi             BYTEA NOT NULL,
  outpoint_txid  BYTEA,
  outpoint_index INT
);

CREATE TABLE cards (
  txid           BYTEA    NOT NULL,
  idx            INT      NOT NULL,
  key            BYTEA    NOT NULL,
  records        BYTEA    NOT NULL,
  spender_type   SMALLINT NOT NULL,
  spender        BYTEA    NOT NULL,
  blob           BYTEA    NOT NULL,
  value          BIGINT   NOT NULL,
  swept_at       BIGINT,
  PRIMARY KEY (txid, idx)
);
CREATE INDEX cards_key          ON cards (key);
CREATE INDEX cards_spender_page ON cards (spender_type, spender, txid, idx) WHERE swept_at IS NULL;

CREATE TYPE deed_state AS (
  kind           SMALLINT,
  name           TEXT,
  owner_type     SMALLINT,
  owner          BYTEA,
  claim          BYTEA,
  outpoint_txid  BYTEA,
  outpoint_index INT,
  value          BIGINT,
  accepted_daa   BIGINT
);

CREATE TYPE gap_row AS (
  lo             BYTEA,
  hi             BYTEA,
  outpoint_txid  BYTEA,
  outpoint_index INT
);

CREATE TYPE card_mark AS (
  txid           BYTEA,
  idx            INT,
  existed        BOOLEAN,
  swept_at       BIGINT
);

CREATE TABLE events (
  block_hash BYTEA        NOT NULL,
  seq        INT          NOT NULL,
  blue_score BIGINT       NOT NULL,
  key        BYTEA        NOT NULL,
  prev       deed_state,
  prev_gaps  gap_row[]    NOT NULL,
  prev_cards card_mark[]  NOT NULL,
  PRIMARY KEY (block_hash, seq)
);
CREATE INDEX events_blue ON events (blue_score);

CREATE TABLE history (
  id         BIGSERIAL PRIMARY KEY,
  key        BYTEA    NOT NULL,
  op         SMALLINT NOT NULL,
  card       SMALLINT NOT NULL,
  seq        INT      NOT NULL,
  blue_score BIGINT   NOT NULL,
  daa_score  BIGINT   NOT NULL,
  block_time BIGINT   NOT NULL,
  block_hash BYTEA    NOT NULL,
  txid       BYTEA    NOT NULL,
  state      deed_state
);
CREATE INDEX history_key      ON history (key, blue_score DESC, seq DESC);
CREATE INDEX history_block    ON history (block_hash);
CREATE INDEX history_register ON history (block_time) WHERE op = 0;
