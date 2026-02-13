IF OBJECT_ID('dbo.people','U') IS NOT NULL
    DROP TABLE dbo.people;
GO

CREATE TABLE dbo.people (
    person_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    full_name NVARCHAR(200) NOT NULL,
    created_at DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
);
GO