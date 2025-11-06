CREATE PROTO BUNDLE (
    types.Status,
    types.ComplexType,
    types.NestedComplexType
);
CREATE TABLE TableWithStatus (
    ID STRING(36) NOT NULL,
    Status types.Status,
    NestedField types.NestedComplexType,
) PRIMARY KEY (ID);