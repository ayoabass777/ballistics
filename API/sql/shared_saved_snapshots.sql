-- Shared identities survive payload archival. UTC capture month is immutable.
CREATE TABLE IF NOT EXISTS app.saved_snapshots (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fixture_id BIGINT NOT NULL CHECK (fixture_id > 0),
    content_hash TEXT NOT NULL UNIQUE,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    capture_month DATE NOT NULL DEFAULT (date_trunc('month', CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date),
    schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
    provenance TEXT NOT NULL CHECK (provenance IN ('client_card_v1', 'legacy_card_v1')),
    run_ds TEXT,
    archive_key TEXT,
    archive_sha256 TEXT,
    archived_at TIMESTAMPTZ,
    settled_at TIMESTAMPTZ,
    retention_hold BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE (capture_month, id),
    UNIQUE (id, fixture_id),
    CHECK ((archive_key IS NULL) = (archive_sha256 IS NULL))
);
CREATE TABLE IF NOT EXISTS app.saved_snapshot_payloads (
    capture_month DATE NOT NULL,
    snapshot_id BIGINT NOT NULL,
    card JSONB NOT NULL,
    PRIMARY KEY (capture_month, snapshot_id),
    FOREIGN KEY (capture_month, snapshot_id) REFERENCES app.saved_snapshots(capture_month, id)
) PARTITION BY RANGE (capture_month);

CREATE OR REPLACE FUNCTION app.ensure_saved_snapshot_partitions() RETURNS void
LANGUAGE plpgsql AS $$
DECLARE start_month DATE; end_month DATE;
BEGIN
    -- Maintenance/bootstrap only. Requests never execute DDL.
    FOR i IN 0..3 LOOP
        start_month := (date_trunc('month', CURRENT_TIMESTAMP AT TIME ZONE 'UTC') + make_interval(months => i))::date;
        end_month := (start_month + interval '1 month')::date;
        EXECUTE format('CREATE TABLE IF NOT EXISTS app.%I PARTITION OF app.saved_snapshot_payloads FOR VALUES FROM (%L) TO (%L)',
            'saved_snapshot_payloads_' || to_char(start_month, 'YYYYMM'), start_month, end_month);
    END LOOP;
END $$;
SELECT app.ensure_saved_snapshot_partitions();

CREATE TABLE IF NOT EXISTS app.saved_fixture_refs (
    user_id TEXT NOT NULL REFERENCES app.app_users(user_id) ON DELETE CASCADE,
    fixture_id BIGINT NOT NULL,
    saved_snapshot_id BIGINT NOT NULL,
    saved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, fixture_id),
    FOREIGN KEY (saved_snapshot_id, fixture_id) REFERENCES app.saved_snapshots(id, fixture_id)
);
CREATE INDEX IF NOT EXISTS saved_fixture_refs_user_time ON app.saved_fixture_refs(user_id, saved_at DESC, fixture_id DESC);
CREATE INDEX IF NOT EXISTS saved_fixture_refs_snapshot ON app.saved_fixture_refs(saved_snapshot_id);

CREATE OR REPLACE FUNCTION app.saved_snapshot_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_TABLE_NAME = 'saved_snapshots' AND TG_OP = 'UPDATE' THEN
        IF (NEW.id, NEW.fixture_id, NEW.content_hash, NEW.captured_at, NEW.capture_month, NEW.schema_version, NEW.provenance, NEW.run_ds)
            IS NOT DISTINCT FROM
           (OLD.id, OLD.fixture_id, OLD.content_hash, OLD.captured_at, OLD.capture_month, OLD.schema_version, OLD.provenance, OLD.run_ds) THEN
            RETURN NEW;
        END IF;
    END IF;
    RAISE EXCEPTION 'Shared snapshot evidence is immutable';
END $$;
DROP TRIGGER IF EXISTS saved_snapshot_identity_guard ON app.saved_snapshots;
CREATE TRIGGER saved_snapshot_identity_guard BEFORE UPDATE OR DELETE ON app.saved_snapshots
FOR EACH ROW EXECUTE FUNCTION app.saved_snapshot_immutable();
DROP TRIGGER IF EXISTS saved_snapshot_payload_guard ON app.saved_snapshot_payloads;
CREATE TRIGGER saved_snapshot_payload_guard BEFORE UPDATE OR DELETE ON app.saved_snapshot_payloads
FOR EACH ROW EXECUTE FUNCTION app.saved_snapshot_immutable();

CREATE OR REPLACE FUNCTION app.reject_saved_snapshot_truncate() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'Shared snapshots cannot be truncated'; END $$;
DROP TRIGGER IF EXISTS saved_snapshot_no_truncate ON app.saved_snapshots;
CREATE TRIGGER saved_snapshot_no_truncate BEFORE TRUNCATE ON app.saved_snapshots
FOR EACH STATEMENT EXECUTE FUNCTION app.reject_saved_snapshot_truncate();
DROP TRIGGER IF EXISTS saved_payload_no_truncate ON app.saved_snapshot_payloads;
CREATE TRIGGER saved_payload_no_truncate BEFORE TRUNCATE ON app.saved_snapshot_payloads
FOR EACH STATEMENT EXECUTE FUNCTION app.reject_saved_snapshot_truncate();
