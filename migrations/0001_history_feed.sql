ALTER TABLE history ADD COLUMN IF NOT EXISTS payload BYTEA;
INSERT INTO vars (key, value) VALUES ('history_epoch', md5(random()::text || clock_timestamp()::text)) ON CONFLICT (key) DO NOTHING;
