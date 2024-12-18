-- @wrench.migrationKind=FixedPointIterationDML
-- @wrench.concurrency=1

-- Insert one row at a time until a total of 5 singers are inserted
INSERT INTO Singers (SingerID, FirstName, LastName)
SELECT NextSingerID, CAST(NextSingerID AS STRING), CAST(NextSingerID AS STRING)
FROM (SELECT MAX(SingerID) + 1 AS NextSingerID FROM Singers HAVING COUNT(1) <= 5)
