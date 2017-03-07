CREATE TABLE [raw].[AccountPerformance] (
    [acctperfId]      BIGINT         IDENTITY (1, 1) NOT NULL,
    [rptlogId]        BIGINT         NOT NULL,
    [rptdDate]        DATE           NOT NULL,
    [clientId]        BIGINT         NOT NULL,
    [impressions]     BIGINT         NOT NULL,
    [avgPosition]     DECIMAL (9, 5) NOT NULL,
    [clicks]          BIGINT         NOT NULL,
    [cost]            BIGINT         NOT NULL,
    [convertedClicks] BIGINT         NOT NULL,
    [account]         VARCHAR (255)  NOT NULL,
    [network]         VARCHAR (50)   NOT NULL
);

