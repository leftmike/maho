--
-- Test creating and dropping indexes
-- These tests are specific to Maho
--

DROP TABLE IF EXISTS tbl1;

CREATE TABLE tbl1 (c1 int, c2 int, c3 text);

CREATE INDEX idx1 ON tbl1 (c1);

CREATE INDEX IF NOT EXISTS idx1 ON tbl1 (c1);

{{Fail .Test}}
CREATE INDEX idx1 ON tbl1 (c1);

DROP INDEX idx1 ON tbl1;

CREATE INDEX idx1 ON tbl1 (c1);

DROP INDEX IF EXISTS idx123 on tbl1;

DROP INDEX IF EXISTS idx1 on tbl1;

{{Fail .Test}}
DROP INDEX idx1 on tbl1;
