CREATE TABLE [raw].[CampaignChannel] (
    [campid]                BIGINT        IDENTITY (1, 1) NOT NULL,
    [campaignID]            BIGINT        NULL,
    [advertisingChannel]    VARCHAR (100) NULL,
    [advertisingSubChannel] VARCHAR (100) NULL
);



