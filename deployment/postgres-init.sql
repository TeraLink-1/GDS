CREATE TABLE telemetry_points (
  id BIGSERIAL PRIMARY KEY,
  mnemonic TEXT,
  ts TIMESTAMP DEFAULT NOW(),
  value DOUBLE PRECISION,
  sat_id TEXT,
  source TEXT
);
