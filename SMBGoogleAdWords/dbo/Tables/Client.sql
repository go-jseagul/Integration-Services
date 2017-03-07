CREATE TABLE [dbo].[Client] (
    [clientId]          BIGINT        NOT NULL,
    [clientInsertedOn]  DATETIME2 (7) CONSTRAINT [DF_Client_clientInsertedOn] DEFAULT (sysutcdatetime()) NOT NULL,
    [clientLastFoundOn] DATETIME2 (7) CONSTRAINT [DF_Client_clientLastFoundOn] DEFAULT (sysutcdatetime()) NOT NULL,
    [clientStartDate]   DATE          NOT NULL,
    [clientEnabled]     BIT           NOT NULL,
    CONSTRAINT [PK_Client] PRIMARY KEY CLUSTERED ([clientId] ASC)
);


GO
CREATE NONCLUSTERED INDEX [IX_Client_StartDate_Enabled]
    ON [dbo].[Client]([clientStartDate] ASC)
    INCLUDE([clientId]) WHERE ([clientEnabled]=(1));

