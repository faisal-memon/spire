-- +goose Up

CREATE TABLE records (
	  id BIGSERIAL PRIMARY KEY
	, kind TEXT NOT NULL
	, key TEXT NOT NULL
	, data BYTEA NOT NULL
	, created_at TIMESTAMP NOT NULL
	, updated_at TIMESTAMP NOT NULL
	, revision INTEGER NOT NULL
);

CREATE TABLE changes (
	  id BIGSERIAL PRIMARY KEY
	, kind TEXT NOT NULL
	, key TEXT NOT NULL
	, event TEXT NOT NULL
	, timestamp TIMESTAMP NOT NULL
);

CREATE UNIQUE INDEX records_kind_key ON records(kind, key);
CREATE INDEX changes_timestamp ON changes(timestamp);

-- +goose StatementBegin
CREATE FUNCTION process_record_create_or_update() RETURNS TRIGGER AS $record_change$
	BEGIN
		INSERT INTO changes(kind, key, event, timestamp) VALUES (new.kind, new.key, TG_OP, now());
		RETURN NULL;
	END;
$record_change$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION process_record_delete() RETURNS TRIGGER AS $record_change$
	BEGIN
		INSERT INTO changes(kind, key, event, timestamp) VALUES (old.kind, old.key, TG_OP, now());
		RETURN NULL;
	END;
$record_change$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER on_record_create_update
    AFTER INSERT OR UPDATE ON records
        FOR EACH ROW EXECUTE PROCEDURE process_record_create_or_update();

CREATE TRIGGER on_record_delete
    AFTER DELETE ON records
        FOR EACH ROW EXECUTE PROCEDURE process_record_delete();

-- +goose Down
DROP TRIGGER IF EXISTS on_record_delete;
DROP TRIGGER IF EXISTS on_record_create_update;
DROP FUNCTION IF EXISTS process_record_delete;
DROP FUNCTION IF EXISTS process_record_create_or_update;
DROP INDEX IF EXISTS changes_timestamp;
DROP TABLE IF EXISTS changes;
DROP TABLE IF EXISTS records;
