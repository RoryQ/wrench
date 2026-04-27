-- Test semicolon in literal and comments
INSERT INTO AdvancedSyntax (Id, Data) VALUES (1, JSON '{"name": "Semicolon; Test"}');
/* Multi-line comment
   with a semicolon; */
UPDATE AdvancedSyntax SET Data = JSON '{"name": "Updated; name"}' WHERE Id = 1;
