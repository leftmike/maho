--
-- Test query support used by foreign keys
--

DROP TABLE IF EXISTS tbl1;

CREATE TABLE tbl1 (c1 int PRIMARY KEY, c2 int CONSTRAINT idx1 unique, c3 int, c4 int);

CREATE UNIQUE INDEX idx2 on tbl1 (c3, c4);

INSERT INTO tbl1 VALUES
    (1, -1, 10, 11),
    (2, -2, 20, 22),
    (3, -3, 30, 33),
    (4, -4, 40, 44);

SELECT COUNT(*) FROM tbl1 WHERE c1 = 1;

SELECT COUNT(*) FROM tbl1 WHERE c1 = -1;

SELECT COUNT(*) FROM tbl1@idx1 WHERE c2 = -2;

SELECT COUNT(*) FROM tbl1@idx1 WHERE c2 = 2;

SELECT COUNT(*) FROM tbl1@idx2 WHERE c3 = 30 AND c4 = 33;

SELECT COUNT(*) FROM tbl1@idx2 WHERE c3 = 30 AND c4 = 30;
