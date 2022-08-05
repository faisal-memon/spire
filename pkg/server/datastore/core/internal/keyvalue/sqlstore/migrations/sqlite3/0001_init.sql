-- +goose Up

CREATE TABLE records (
	  id INTEGER PRIMARY KEY
	, kind TEXT NOT NULL
	, key TEXT NOT NULL
	, data BLOB NOT NULL
	, created_at DATETIME NOT NULL
	, updated_at DATETIME NOT NULL
	, revision INTEGER NOT NULL
);

CREATE TABLE changes (
	  id INTEGER PRIMARY KEY AUTOINCREMENT
	, kind TEXT NOT NULL
	, key TEXT NOT NULL
	, event TEXT NOT NULL
	, timestamp DATETIME NOT NULL
);

CREATE UNIQUE INDEX records_kind_key ON records(kind, key);
CREATE INDEX changes_timestamp ON changes(timestamp);

-- +goose StatementBegin
CREATE TRIGGER on_record_create AFTER INSERT ON changes
BEGIN
	INSERT INTO changes(kind, key, event, timestamp) VALUES (NEW.kind, NEW.key, "CREATED", datetime('now'));
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER on_record_update AFTER UPDATE ON changes
BEGIN
	INSERT INTO changes(kind, key, event, timestamp) VALUES (NEW.kind, NEW.key, "UPDATE", datetime('now'));
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER on_record_delete AFTER DELETE ON changes
BEGIN
	INSERT INTO changes(kind, key, event, timestamp) VALUES (OLD.kind, OLD.key, "DELETE", datetime('now'));
END;
-- +goose StatementEnd

-- +goose Down
DROP TRIGGER IF EXISTS on_record_delete;
DROP TRIGGER IF EXISTS on_record_update;
DROP TRIGGER IF EXISTS on_record_create;
DROP INDEX IF EXISTS changes_timestamp;
DROP INDEX IF EXISTS records_kind_key;
DROP TABLE IF EXISTS changes;
DROP TABLE IF EXISTS records;
