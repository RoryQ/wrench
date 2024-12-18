-- @wrench.migrationKind=FixedPointIterationDML
-- @wrench.concurrency=1
--
-- This statement inserts one new row if fewer than 5 rows are present in the table.
-- The wrench migrationKind "FixedPointIterationDML" instructs wrench to repeatedly
-- execute this statement until no more rows are affected, i.e. the table has 5 rows.
INSERT INTO Singers (SingerID, FirstName, LastName)
SELECT NextSingerID, CONCAT("Singer", CAST(TotalSingers AS STRING)), ""
FROM (SELECT GENERATE_UUID() AS NextSingerID, COUNT(1) AS TotalSingers FROM Singers HAVING TotalSingers < 5)
