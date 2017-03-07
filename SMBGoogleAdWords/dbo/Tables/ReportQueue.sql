CREATE TABLE [dbo].[ReportQueue] (
    [rptqId]                        BIGINT        IDENTITY (1, 1) NOT NULL,
    [rptdDate]                      DATE          NOT NULL,
    [clientId]                      BIGINT        NOT NULL,
    [rptqReportTypes]               INT           NOT NULL,
    [rptqCreatedOn]                 DATETIME2 (7) CONSTRAINT [DF_ReportQueue_rptqCreatedOn] DEFAULT (sysutcdatetime()) NOT NULL,
    [rptqEnabled]                   BIT           NOT NULL,
    [rptqRunning]                   BIT           CONSTRAINT [DF_ReportQueue_rptqRunning] DEFAULT ((0)) NOT NULL,
    [rptqComplete]                  BIT           CONSTRAINT [DF_ReportQueue_rptqComplete] DEFAULT ((0)) NOT NULL,
    [rptqAttemptsRemaining]         INT           NOT NULL,
    [rptqCurrentLog_rptlogId]       BIGINT        NULL,
    [rptqLastSucessfulLog_rptlogId] BIGINT        NULL,
    [rptqLastUpdatedOn]             DATETIME2 (7) CONSTRAINT [DF_ReportQueue_rptqLastUpdatedOn] DEFAULT (sysutcdatetime()) NOT NULL,
    CONSTRAINT [PK_ReportQueue] PRIMARY KEY CLUSTERED ([rptqId] ASC),
    CONSTRAINT [CK_ReportQueue_rptqReportTypes] CHECK ([rptqreporttypes]>=(1) AND [rptqreporttypes]<=(255)),
    CONSTRAINT [FK_ReportQueue_Client] FOREIGN KEY ([clientId]) REFERENCES [dbo].[Client] ([clientId]),
    CONSTRAINT [FK_ReportQueue_ReportDate] FOREIGN KEY ([rptdDate]) REFERENCES [dbo].[ReportDate] ([rptdDate]),
    CONSTRAINT [FK_ReportQueue_ReportLog] FOREIGN KEY ([rptqLastSucessfulLog_rptlogId]) REFERENCES [dbo].[ReportLog] ([rptlogId]),
    CONSTRAINT [FK_ReportQueue_ReportLog1] FOREIGN KEY ([rptqCurrentLog_rptlogId]) REFERENCES [dbo].[ReportLog] ([rptlogId])
);




GO
CREATE NONCLUSTERED INDEX [IX_ReportQueue_queue]
    ON [dbo].[ReportQueue]([rptqCreatedOn] ASC)
    INCLUDE([clientId], [rptdDate], [rptqAttemptsRemaining], [rptqComplete], [rptqEnabled], [rptqId], [rptqReportTypes], [rptqRunning]) WHERE ([rptqEnabled]=(1) AND [rptqRunning]=(0) AND [rptqComplete]=(0) AND [rptqAttemptsRemaining]>(0));


GO
CREATE UNIQUE NONCLUSTERED INDEX [UX_ReportQueue_rptdDate_clientId]
    ON [dbo].[ReportQueue]([rptdDate] ASC, [clientId] ASC);

