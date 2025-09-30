CREATE PROTO BUNDLE (`status.Status`);
CREATE TABLE TableWithStatus (
    ID STRING(36) NOT NULL,
    Status status.Status
) PRIMARY KEY (ID);