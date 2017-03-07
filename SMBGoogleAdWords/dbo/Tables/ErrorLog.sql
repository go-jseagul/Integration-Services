CREATE TABLE [dbo].[ErrorLog] (
    [errlogId]          BIGINT        IDENTITY (1, 1) NOT NULL,
    [errlogInsertedOn]  DATETIME2 (7) CONSTRAINT [DF_ErrorLog_errlogInsertedOn] DEFAULT (sysutcdatetime()) NOT NULL,
    [errlogMachineName] VARCHAR (255) NOT NULL,
    [errlogUserLogin]   VARCHAR (255) NOT NULL,
    [errlogCategory]    VARCHAR (255) NOT NULL,
    [errlogText]        VARCHAR (MAX) NOT NULL,
    CONSTRAINT [PK_ErrorLog] PRIMARY KEY CLUSTERED ([errlogId] ASC)
);

