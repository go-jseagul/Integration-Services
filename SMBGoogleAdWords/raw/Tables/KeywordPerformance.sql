CREATE TABLE [raw].[KeywordPerformance] (
    [kwperfId]        BIGINT         IDENTITY (1, 1) NOT NULL,
    [rptlogId]        BIGINT         NOT NULL,
    [rptdDate]        DATE           NOT NULL,
    [clientId]        BIGINT         NOT NULL,
    [campaignID]      BIGINT         NOT NULL,
    [adGroupID]       BIGINT         NOT NULL,
    [keywordID]       BIGINT         NOT NULL,
    [impressions]     BIGINT         NOT NULL,
    [avgPosition]     DECIMAL (9, 5) NOT NULL,
    [clicks]          BIGINT         NOT NULL,
    [cost]            BIGINT         NOT NULL,
    [convertedClicks] BIGINT         NOT NULL,
    [keywordState]    VARCHAR (255)  NOT NULL,
    [matchType]       VARCHAR (255)  NOT NULL,
    [keyword]         VARCHAR (255)  NOT NULL,
    [network]         VARCHAR (50)   NOT NULL
);

