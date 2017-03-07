CREATE TABLE [dbo].[ReportDate] (
    [rptdDate]       DATE          NOT NULL,
    [rptdInsertedOn] DATETIME2 (7) CONSTRAINT [DF_ReportDate_rptdInsertedOn] DEFAULT (sysutcdatetime()) NOT NULL,
    CONSTRAINT [PK_ReportDate] PRIMARY KEY CLUSTERED ([rptdDate] ASC)
);

