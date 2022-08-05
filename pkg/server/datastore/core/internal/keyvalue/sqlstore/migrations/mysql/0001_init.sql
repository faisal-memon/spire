-- +goose Up

DROP TRIGGER IF EXISTS on_record_delete;
DROP TRIGGER IF EXISTS on_record_update;
DROP TRIGGER IF EXISTS on_record_create;
DROP TABLE IF EXISTS changes;
DROP TABLE IF EXISTS records;

CREATE TABLE records (
	  id BIGINT PRIMARY KEY AUTO_INCREMENT
	, kind VARCHAR(255) CHARACTER SET utf8 NOT NULL
	, `key` VARCHAR(767) CHARACTER SET utf8 NOT NULL
	, tiny_data TINYBLOB
	, small_data BLOB
	, medium_data MEDIUMBLOB
	, large_data LONGBLOB
	, created_at DATETIME NOT NULL
	, updated_at DATETIME NOT NULL
	, revision INTEGER NOT NULL
);

CREATE TABLE changes (
	  id BIGINT PRIMARY KEY AUTO_INCREMENT
	, kind TEXT NOT NULL
	, `key` TEXT NOT NULL
	, event TEXT NOT NULL
	, timestamp TIMESTAMP
);

CREATE UNIQUE INDEX records_kind_key ON records(kind, `key`);
CREATE INDEX changes_timestamp ON changes(timestamp);

-- +goose StatementBegin
CREATE TRIGGER on_record_create AFTER INSERT ON changes
FOR EACH ROW
BEGIN
	INSERT INTO changes(kind, `key`, event) VALUES (NEW.kind, NEW.`key`, "CREATED");
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER on_record_update AFTER UPDATE ON changes
FOR EACH ROW
BEGIN
	INSERT INTO changes(kind, `key`, event) VALUES (NEW.kind, NEW.`key`, "UPDATE");
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER on_record_delete AFTER DELETE ON changes
FOR EACH ROW
BEGIN
	INSERT INTO changes(kind, `key`, event) VALUES (OLD.kind, OLD.`key`, "DELETE");
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
