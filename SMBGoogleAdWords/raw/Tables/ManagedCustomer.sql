CREATE TABLE [raw].[ManagedCustomer] (
    [name]             VARCHAR (500) NULL,
    [companyName]      VARCHAR (500) NULL,
    [customerId]       BIGINT        NOT NULL,
    [canManageClients] BIT           NULL,
    [currencyCode]     VARCHAR (50)  NOT NULL,
    [dateTimeZone]     VARCHAR (50)  NOT NULL,
    [testAccount]      BIT           NULL
);


GO
CREATE NONCLUSTERED INDEX [IX_ManagedCustomer]
    ON [raw].[ManagedCustomer]([canManageClients] ASC, [testAccount] ASC)
    INCLUDE([customerId]);

