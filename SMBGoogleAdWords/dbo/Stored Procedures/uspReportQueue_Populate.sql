-- =============================================
-- Author:		Ryan Fant
-- Create Date: 09/26/2014
-- Description:	Initializes ReportQueue for
--				given date based on known &
--				enabled clientIds
--
-- Mod By:		Ryan Fant
-- Mod Date:	01/28/2015
-- Description:	Can re-queue items for specified
--				report types if desired
-- =============================================  
CREATE PROCEDURE [dbo].[uspReportQueue_Populate]
(
	@rptdDate DATE
	,@reQueue BIT = 0 -- pass 1 to re-queue previously created queue items
	,@reportTypes INT = 255 -- default is download all report types
)
AS
BEGIN

	/*
		EXECUTION EXAMPLES

		STANDARD EXECUTION (DAILY)
			[dbo].[uspReportQueue_Populate] @rptDate = {yesterday's date}

		RE-QUEUE PREVIOUS REPORTS (ON DEMAND)
			[dbo].[uspReportQueue_Populate] @rptDate = {desired date}, @reQueue = 1
	
		RE-QUEUE PREVIOUS REPORTS (ON DEMAND SPECIFIED REPORTS ONLY - Campaign[2] and AdGroup[4])
			[dbo].[uspReportQueue_Populate] @rptDate = {desired date}, @reQueue = 1, @reportTypes = 6
	*/

	SET NOCOUNT ON;

	BEGIN TRY

		BEGIN TRAN;

		IF @rptdDate IS NULL
		BEGIN
			;THROW 50000, '@rptdDate cannot be ''NULL''', 1;
		END;

		-- arbitrary bound, can be moved further back if needed, but before 2009 wouldn't make much sense
		IF @rptdDate < CAST ( '1/1/14' AS DATE )
		BEGIN
			;THROW 50000, '@rptdDate cannot be before Jan 1, 2014', 1;
		END;

		-- since we don't currently plan to handle incomplete dates the date must be in the past
		IF @rptdDate >= CAST ( SYSDATETIME() AS DATE )
		BEGIN
			;THROW 50000, '@rptdDate cannot be today or in the future', 1;
		END;

		IF @reportTypes IS NULL
		BEGIN
			;THROW 50000, '@reportTypes cannot be ''NULL''', 1;
		END;

		IF @reportTypes IS NULL
		BEGIN
			;THROW 50000, '@reportTypes cannot be ''NULL''', 1;
		END;

		/*
			ReportTypes Flags Enumeration

			Account = 1
			Campaign = 2
			AdGroup = 4
			Ad = 8
			Keyword = 16
		*/

		IF @reportTypes NOT BETWEEN 1 AND 255
		BEGIN
			;THROW 50000, '@reportTypes must be between 1 and 255', 1;
		END;

		-- be sure we have the date recorded
		-- keeping track of first time to acquire data for a date separate
		-- from when we first attempt a given account download for a given date
		IF NOT EXISTS ( SELECT 1 FROM [dbo].[ReportDate] WHERE rptdDate = @rptdDate )
		BEGIN
			INSERT [dbo].[ReportDate] ( [rptdDate] ) VALUES ( @rptdDate );
		END;

		DECLARE @defaultAttemptsRemaining INT = 2;

		-- create any non-existing entries for given date and client ids
		-- optionally update existing ones with specified report types to download and requeue
		MERGE [dbo].[ReportQueue] q
		USING
		(
			SELECT
				c.[clientId]
			FROM [dbo].[Client] c
			WHERE
				c.clientStartDate <= @rptdDate -- client was active on or before provided date (see [dbo].[uspProcessManagedClientData] for init logic)
				AND c.clientEnabled = 1 -- actively wanting to acquire stats for this client
		)
		AS src
		(
			[clientId]
		)
		ON
		(
			q.[rptdDate] = @rptdDate
			AND q.[clientId] = src.[clientId]
		)
		WHEN MATCHED AND @reQueue = 1 THEN -- only update existing records if expressly re-queuing
		UPDATE SET
			q.[rptqAttemptsRemaining] = @defaultAttemptsRemaining -- default is 2, means 1 auto-retry
 			,q.[rptqComplete] = 0
			,q.[rptqReportTypes] = @reportTypes
		WHEN NOT MATCHED THEN
		INSERT
		(
			[rptdDate]
			,[clientId]
			,[rptqEnabled]
			,[rptqAttemptsRemaining]
			,[rptqReportTypes]
		)
		VALUES
		(
			@rptdDate
			,[clientId]
			,1
			,@defaultAttemptsRemaining
 			,@reportTypes
		);

		COMMIT TRAN;

	END TRY
	BEGIN CATCH

		IF 0 != XACT_STATE()
		BEGIN
			ROLLBACK TRAN;
		END;

		THROW;

	END CATCH;

END;
