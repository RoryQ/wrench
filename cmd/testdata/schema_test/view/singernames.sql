CREATE VIEW SingerNames SQL SECURITY INVOKER AS SELECT
   Singers.SingerId AS SingerId,
   Singers.FirstName AS Name
FROM Singers