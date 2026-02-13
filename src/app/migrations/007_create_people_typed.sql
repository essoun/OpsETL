IF OBJECT_ID('dbo.people_typed','U') IS NOT NULL
    DROP TABLE dbo.people_typed;
GO

CREATE TABLE dbo.people_typed (
    person_id  BIGINT        NOT NULL,
    full_name  NVARCHAR(200) NOT NULL,
    created_at DATETIME2(0)  NOT NULL,
    CONSTRAINT PK_people_typed PRIMARY KEY (person_id)
);
GO

IF OBJECT_ID('dbo.people_rejects','U') IS NOT NULL
    DROP TABLE dbo.people_rejects;
GO

CREATE TABLE dbo.people_rejects (
    reject_id     BIGINT IDENTITY(1,1) PRIMARY KEY,
    raw_person_id NVARCHAR(4000) NULL,
    raw_full_name NVARCHAR(4000) NULL,
    raw_created_at NVARCHAR(4000) NULL,
    reason        NVARCHAR(4000) NOT NULL,
    rejected_at   DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
