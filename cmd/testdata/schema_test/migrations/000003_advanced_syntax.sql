CREATE TABLE AdvancedSyntax (
    Id INT64 NOT NULL,
    Data JSON,
    GeneratedColumn STRING(MAX) AS (JSON_VALUE(Data, '$.name')) STORED,
    CONSTRAINT ValidId CHECK (Id > 0)
) PRIMARY KEY(Id);
