CREATE TABLE [raw].[CampaignPerformance] (
    [campperfId]          BIGINT         IDENTITY (1, 1) NOT NULL,
    [rptlogId]            BIGINT         NOT NULL,
    [rptdDate]            DATE           NOT NULL,
    [clientId]            BIGINT         NOT NULL,
    [campaignID]          BIGINT         NOT NULL,
    [impressions]         BIGINT         NOT NULL,
    [avgPosition]         DECIMAL (9, 5) NOT NULL,
    [clicks]              BIGINT         NOT NULL,
    [cost]                BIGINT         NOT NULL,
    [convertedClicks]     BIGINT         NOT NULL,
    [campaignState]       VARCHAR (255)  NOT NULL,
    [campaign]            VARCHAR (255)  NOT NULL,
    [network]             VARCHAR (50)   NOT NULL,
    [contentLostISBudget] VARCHAR (10)   NOT NULL,
    [contentImprShare]    VARCHAR (10)   NOT NULL,
    [contentLostISRank]   VARCHAR (10)   NOT NULL,
    [searchExactMatchIS]  VARCHAR (10)   NOT NULL,
    [searchImprShare]     VARCHAR (10)   NOT NULL,
    [searchLostISRank]    VARCHAR (10)   NOT NULL
);

