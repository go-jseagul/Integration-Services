using DataWarehouse.DataAcquisition.AdWords.Data;
using DataWarehouse.DataAcquisition.AdWords.Properties;
using Google.Api.Ads.AdWords.Lib;
using Google.Api.Ads.AdWords.Util.Reports;
using Google.Api.Ads.AdWords.v201609;
using System;
using System.IO;

namespace DataWarehouse.DataAcquisition.AdWords.Processors
{
    [Flags]
    public enum ReportTypes
    {
        None = 0,
        Account = 1,
        Campaign = 2,
        AdGroup = 4,
        Ad = 8,
        Keyword = 16,
        Age = 32,
        Gender = 64,
        Video = 128,
    }

    public enum ReportStage
    {
        NotSet,
        ReportRequested,
        Downloaded,
        Decompressed,
        LoadStarted,
        LoadCompleted,
    }

    public class ReportProcessor
    {
        private readonly long _rptlogId;
        private readonly long _clientId;
        private readonly DateTime _rptdDate;
        private readonly AdWordsUser _user;
        private readonly int _retryCount;
        private readonly bool _deleteDownloadedReportOnCompletion;
        private ReportStage _currentReportStage = ReportStage.NotSet;
        private ReportTypes _currentReportType = ReportTypes.None;
        private ReportTypes _reportTypesToDownload;

        public ReportProcessor(
            AdWordsUser user,
            int retryCount,
            long rptlogId,
            long clientId,
            DateTime rptdDate,
            ReportTypes reportTypesToDownload,
            bool deleteDownloadedReportOnCompletion)
        {
            this._user = user;
            this._retryCount = retryCount;
            this._rptlogId = rptlogId;
            this._clientId = clientId;
            this._rptdDate = rptdDate;
            this._reportTypesToDownload = reportTypesToDownload;
            this._deleteDownloadedReportOnCompletion = deleteDownloadedReportOnCompletion;

            this._user.ResetCallHistory();
            ((AdWordsAppConfig)this._user.Config).ClientCustomerId = clientId.ToString();
        }

        public static void Run()
        {
            var deleteDownloadedReportOnCompletion = Settings.Default.DeleteDownloadedReportOnCompletion;

            while (true)
            {
                long? rptlogId;
                long? clientId;
                DateTime? rptdDate;
                ReportTypes? reportTypes;
                var user = new AdWordsUser();
                var retryCount = user.Config.RetryCount;

                try
                {
                    // grab queue item to process
                    if (!Repository.GetNextReportQueueItem(out rptlogId, out clientId, out rptdDate, out reportTypes))
                    {
                        // the queue has no work
                        break;
                    }

                    // go get reports and load them
                    var processor = new ReportProcessor(user,
                                                        retryCount,
                                                        rptlogId.Value,
                                                        clientId.Value,
                                                        rptdDate.Value,
                                                        reportTypes.Value,
                                                        deleteDownloadedReportOnCompletion);
                    processor.Execute();
                }
                catch (Exception ex)
                {
                    Repository.LogError(ex.ToString(), "ReportProcessor");
                }
            }
        }

        public void Execute()
        {
            try
            {
                // account report downloads are different than all other report types
                // we unconditionally download account data to determine if the account has impressions
                // the load for account data is conditionally executed based on the report types to download
                this._currentReportType = ReportTypes.Account;
                this._currentReportStage = ReportStage.NotSet;
                bool hasImpressions = false;
                bool hasVideoCampaign = false;
                LoadAccountPerformanceReport(out hasImpressions);

                if (!hasImpressions) // no impressions means we don't do anything else besides update the log appropriately
                {
                    Repository.SetReportLogZeroImpressions(this._rptlogId);
                }
                else // impressions means we may need to download other reports
                {
                    // only load if the report type is being requested
                    if (this._reportTypesToDownload.Contains(ReportTypes.Campaign))
                    {
                        this._currentReportType = ReportTypes.Campaign;
                        this._currentReportStage = ReportStage.NotSet;
                        LoadCampaignPerformanceReport(out hasVideoCampaign);
                        //Only load if the report type is being requested and if it is a video campaign
                        if (hasVideoCampaign)
                        {
                            if (this._reportTypesToDownload.Contains(ReportTypes.Age))
                            {
                                this._currentReportType = ReportTypes.Age;
                                this._currentReportStage = ReportStage.NotSet;
                                LoadYoutubeAgePerformanceReport();
                            }
                            if (this._reportTypesToDownload.Contains(ReportTypes.Gender))
                            {
                                this._currentReportType = ReportTypes.Gender;
                                this._currentReportStage = ReportStage.NotSet;
                                LoadYoutubeGenderPerformanceReport();
                            }
                            if (this._reportTypesToDownload.Contains(ReportTypes.Video))
                            {
                                this._currentReportType = ReportTypes.Video;
                                this._currentReportStage = ReportStage.NotSet;
                                LoadYoutubeVideoPerformanceReport();
                            }
                        }
                    }

                    // only load if the report type is being requested
                    if (this._reportTypesToDownload.Contains(ReportTypes.AdGroup))
                    {
                        this._currentReportType = ReportTypes.AdGroup;
                        this._currentReportStage = ReportStage.NotSet;
                        LoadAdGroupPerformanceReport();
                    }

                    // only load if the report type is being requested
                    if (this._reportTypesToDownload.Contains(ReportTypes.Ad))
                    {
                        this._currentReportType = ReportTypes.Ad;
                        this._currentReportStage = ReportStage.NotSet;
                        LoadAdPerformanceReport();
                    }

                    // only load if the report type is being requested
                    if (this._reportTypesToDownload.Contains(ReportTypes.Keyword))
                    {
                        this._currentReportType = ReportTypes.Keyword;
                        this._currentReportStage = ReportStage.NotSet;
                        LoadKeywordPerformanceReport();
                    }
                }
            }
            catch (Exception ex)
            {
                // give extra execution info
                var wrappedEx = new System.ApplicationException(string.Format("Failed to process report logid:{0}", this._rptlogId), ex);
                // log exception
                var errlogId = Repository.LogError(wrappedEx.ToString(), string.Format("ReportProcessor - {0}", this._currentReportType));
                // mark log entry failed
                Repository.SetReportLogErrorId(this._rptlogId, errlogId);
            }
        }

        private void LoadAccountPerformanceReport(out bool hasImpressions)
        {

            var definition = new ReportDefinition()
            {
                reportName = "Custom Date ACCOUNT_PERFORMANCE_REPORT",
                reportType = ReportDefinitionReportType.ACCOUNT_PERFORMANCE_REPORT,
                selector = new Selector()
                {
                    fields = new string[]
                        {
                            "AccountDescriptiveName",
                            "Impressions", 
                            "AveragePosition",
                            "Clicks",
                            "Cost",
                            "AdNetworkType1", // create partitioning by search/display networks
                        }
                }
            };

            // download account report
            var reportFilePath = DownloadReport(definition);

            // check report for impressions
            using (var reader = new XmlAccountPerformanceReader())
            {
                // file to datareader
                reader.Initialize(reportFilePath, this._rptlogId, this._rptdDate, this._clientId);
                hasImpressions = reader.HasImpressions;
            }

            // only load if we have impression and the report type is being requested
            if (hasImpressions && this._reportTypesToDownload.Contains(ReportTypes.Account))
            {
                LoadReport<XmlAccountPerformanceReader>(reportFilePath);
            }

            // todo: harden file deletion even during exceptions
            if (this._deleteDownloadedReportOnCompletion)
            {
                File.Delete(reportFilePath);
            }

        }

        private void LoadCampaignPerformanceReport(out bool hasVideoCampaign)
        {
            hasVideoCampaign = false; //had to reinitialize even though its an input parameter

            var definition = new ReportDefinition()
            {
                reportName = "Custom Date CAMPAIGN_PERFORMANCE_REPORT",
                reportType = ReportDefinitionReportType.CAMPAIGN_PERFORMANCE_REPORT,
                selector = new Selector()
                {
                    fields = new string[]
                        {
                            "CampaignId",
                            "CampaignName",
                            "CampaignStatus",
                            "Impressions", 
                            "AveragePosition",
                            "Clicks",
                            "Cost",
                            "AdNetworkType1", // create partitioning by search/display networks
                            "ContentBudgetLostImpressionShare",
                            "ContentImpressionShare",
                            "ContentRankLostImpressionShare",
                            "SearchBudgetLostImpressionShare",
                            "SearchImpressionShare",
                            "SearchRankLostImpressionShare",
                            "AdvertisingChannelSubType",
                            "AdvertisingChannelType",
                        }
                }
            };

            var reportFilePath = DownloadReport(definition);
            // check report for impressions
            using (var reader = new XmlCampaignPerformanceReader())
            {
                // file to datareader
                string AdvertisingChannelType;
                reader.Initialize(reportFilePath, this._rptlogId, this._rptdDate, this._clientId);
                AdvertisingChannelType = reader.AdvertisingChannelType;
                if (AdvertisingChannelType.ToUpper().Equals("VIDEO"))
                {
                    hasVideoCampaign = true;
                }
            }
            LoadCampaignChannel(reportFilePath);
            LoadReport<XmlCampaignChannelReader>(reportFilePath);
            // todo: harden file deletion even during exceptions
            if (this._deleteDownloadedReportOnCompletion)
            {
                File.Delete(reportFilePath);
            }
        }


        private void LoadCampaignChannel(string reportFilePath)
        {
            var definition = new ReportDefinition()
            {
                selector = new Selector()
                {
                    fields = new string[]
                        {
                            "CampaignId",
                            "AdvertisingChannelType",
                        }
                }
            };
            LoadReport<XmlCampaignPerformanceReader>(reportFilePath);
        }

        private void LoadAdGroupPerformanceReport()
        {
            var definition = new ReportDefinition()
            {
                reportName = "Custom Date ADGROUP_PERFORMANCE_REPORT",
                reportType = ReportDefinitionReportType.ADGROUP_PERFORMANCE_REPORT,
                selector = new Selector()
                {
                    fields = new string[]
                        {
                            "CampaignId",
                            "AdGroupId",
                            "AdGroupName",
                            "AdGroupStatus",
                            "Impressions", 
                            "AveragePosition",
                            "Clicks",
                            "Cost",
                            "AdNetworkType1", // create partitioning by search/display networks
                            "Device", // create partitioning by device type (mobile/tablet/desktop)
                        }
                }
            };

            DownloadAndLoadReport<XmlAdGroupPerformanceReader>(definition);
        }

        private void LoadAdPerformanceReport()
        {
            var definition = new ReportDefinition()
            {
                reportName = "Custom Date AD_PERFORMANCE_REPORT",
                reportType = ReportDefinitionReportType.AD_PERFORMANCE_REPORT,
                selector = new Selector()
                {
                    fields = new string[]
                        {
                            "AdType",   //ETA
                            "CampaignId",
                            "AdGroupId",
                            "Id", // ad id
                            "Status", // ad status
                            "Headline",
                            "HeadlinePart1", //ETA
                            "HeadlinePart2", //ETA
                            "Description", //ETA
                            "Description1",
                            "Description2",
                            "DisplayUrl",
                            "Path1", //ETA (DisplayUrlPath1)
                            "Path2", //ETA (DisplayUrlPath2)
                            "CreativeDestinationUrl",
                            "Impressions",
                            "AveragePosition",
                            "Clicks",
                            "Cost",
                            "AdNetworkType1", // create partitioning by search/display networks
                            "CreativeFinalUrls",
                            "CreativeFinalMobileUrls",
                            "CreativeFinalAppUrls",
                        },
                    predicates = new[] { new Predicate() { field = "AdType", @operator = PredicateOperator.EQUALS, values = new[] { "TEXT_AD", "EXPANDED_TEXT_AD" } } }
                }
            };

            DownloadAndLoadReport<XmlAdPerformanceReader>(definition);
        }

        private void LoadKeywordPerformanceReport()
        {
            var definition = new ReportDefinition()
            {
                reportName = "Custom Date KEYWORDS_PERFORMANCE_REPORT",
                reportType = ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT,
                selector = new Selector()
                {
                    fields = new string[]
                        {
                            "CampaignId",
                            "AdGroupId",
                            "Id", // keyword id
                            "Status", // keyword status
                            "Criteria", // keyword text
                            "KeywordMatchType",
                            "Impressions", 
                            "AveragePosition",
                            "Clicks",
                            "Cost",
                            "AdNetworkType1", // create partitioning by search/display networks
                        }
                }
            };

            DownloadAndLoadReport<XmlKeywordPerformanceReader>(definition);
        }


        private void LoadYoutubeAgePerformanceReport()
        {
            var definition = new ReportDefinition()
            {
                reportName = "Custom Date AGE_RANGE_PERFORMANCE_REPORT",
                reportType = ReportDefinitionReportType.AGE_RANGE_PERFORMANCE_REPORT,
                selector = new Selector()
                {
                    fields = new string[]
                        {
                            "AccountDescriptiveName",
                            "ActiveViewCpm", //ActiveViewCPM
                            "ActiveViewCtr",
                            "ActiveViewImpressions",
                            "ActiveViewMeasurability",
                            "ActiveViewMeasurableCost",
                            "ActiveViewMeasurableImpressions",
                            "AdGroupId",
                            "AdGroupName",
                            "AdGroupStatus",
                            "AdNetworkType1",
                            "AdNetworkType2",
                            "AllConversionRate",
                            "AllConversions",
                            "AllConversionValue",
                            "AverageCost",
                            "AverageCpc",
                            "AverageCpe",
                            "AverageCpm",
                            "AverageCpv",
                            "BaseAdGroupId",
                            "BaseCampaignId",
                            "CampaignId",
                            "CampaignName",
                            "CampaignStatus",
                            "Clicks",
                            "Conversions",
                            "ConversionValue",
                            "Cost",
                            "CostPerAllConversion",
                            "CpcBid",
                            "CpcBidSource",
                            "Criteria",  
                            "CriteriaDestinationUrl",
                            "CrossDeviceConversions",
                            "Ctr",
                            "CustomerDescriptiveName",
                            "Date",
                            "Device",
                            "EngagementRate",
                            "Engagements",
                            "ExternalCustomerId",
                            "FinalAppUrls",
                            "FinalMobileUrls",
                            "FinalUrls",
                            "GmailForwards",
                            "GmailSaves",
                            "GmailSecondaryClicks",
                            "Id",
                            "Impressions",
                            "InteractionRate",
                            "Interactions",
                            "InteractionTypes",
                            "IsNegative",
                            "IsRestrict",
                            "PrimaryCompanyName",
                            "Status",
                            "TrackingUrlTemplate",
                            "UrlCustomParameters",
                            "ValuePerAllConversion",
                            "ValuePerConversion",
                            "VideoQuartile100Rate",
                            "VideoQuartile25Rate",
                            "VideoQuartile50Rate",
                            "VideoQuartile75Rate",
                            "VideoViewRate",
                            "VideoViews",
                            "ViewThroughConversions",
                        }
                }
            };

            string reportFilePath=DownloadReport(definition);
            LoadReport<XmlYoutubeAgePerformanceReader>(reportFilePath);
        }


        private void LoadYoutubeGenderPerformanceReport()
        {
            var definition = new ReportDefinition()
            {
                reportName = "Custom Date GENDER_PERFORMANCE_REPORT",
                reportType = ReportDefinitionReportType.GENDER_PERFORMANCE_REPORT,
                selector = new Selector()
                {
                    fields = new string[]
                        {
                            "AccountDescriptiveName",
                            "ActiveViewCpm",
                            "ActiveViewCtr",
                            "ActiveViewImpressions",
                            "ActiveViewMeasurability",
                            "ActiveViewMeasurableCost",
                            "ActiveViewMeasurableImpressions",
                            "AdGroupId",
                            "AdGroupName",
                            "AdGroupStatus",
                            "AdNetworkType1",
                            "AdNetworkType2",
                            "AllConversionRate",
                            "AllConversions",
                            "AllConversionValue",
                            "AverageCost",
                            "AverageCpc",
                            "AverageCpe",
                            "AverageCpm",
                            "AverageCpv",
                            "BaseAdGroupId",
                            "BaseCampaignId",
                            "CampaignId",
                            "CampaignName",
                            "CampaignStatus",
                            "Clicks",
                            "Conversions",
                            "ConversionValue",
                            "Cost",
                            "CostPerAllConversion",
                            "CpcBid",
                            "CpcBidSource",
                            "Criteria",  
                            "CriteriaDestinationUrl",
                            "CrossDeviceConversions",
                            "Ctr",
                            "CustomerDescriptiveName",
                            "Date",
                            "Device",
                            "EngagementRate",
                            "Engagements",
                            "ExternalCustomerId",
                            "FinalAppUrls",
                            "FinalMobileUrls",
                            "FinalUrls",
                            "GmailForwards",
                            "GmailSaves",
                            "GmailSecondaryClicks",
                            "Id",
                            "Impressions",
                            "InteractionRate",
                            "Interactions",
                            "InteractionTypes",
                            "IsNegative",
                            "IsRestrict",
                            "PrimaryCompanyName",
                            "Status",
                            "TrackingUrlTemplate",
                            "UrlCustomParameters",
                            "ValuePerAllConversion",
                            "ValuePerConversion",
                            "VideoQuartile100Rate",
                            "VideoQuartile25Rate",
                            "VideoQuartile50Rate",
                            "VideoQuartile75Rate",
                            "VideoViewRate",
                            "VideoViews",
                            "ViewThroughConversions",
                        }
                }
            };

            DownloadAndLoadReport<XmlYoutubeGenderPerformanceReader>(definition);
        }


        private void LoadYoutubeVideoPerformanceReport()
        {
            var definition = new ReportDefinition()
            {
                reportName = "Custom Date VIDEO_PERFORMANCE_REPORT",
                reportType = ReportDefinitionReportType.VIDEO_PERFORMANCE_REPORT,
                selector = new Selector()
                {
                    fields = new string[]
                        {
                            "AdGroupId",
                            "AdGroupName",
                            "AdGroupStatus",
                            "AdNetworkType1",
                            "AdNetworkType2",
                            "CampaignId",
                            "CampaignName",
                            "CampaignStatus",
                            "Clicks",
                            "Conversions",
                            "ConversionValue",
                            "Cost",
                            "CreativeId",
                            "CreativeStatus",
                            "Date",
                            "Device",
                            "Engagements",
                            "Impressions",
                            "VideoChannelId",
                            "VideoDuration",
                            "VideoId",
                            "VideoQuartile100Rate",
                            "VideoQuartile25Rate",
                            "VideoQuartile50Rate",
                            "VideoQuartile75Rate",
                            "VideoTitle",
                            "VideoViews",
                            "ViewThroughConversions",
                        }
                }
            };

            DownloadAndLoadReport<XmlYoutubeVideoPerformanceReader>(definition);

        }

        private void DownloadAndLoadReport<T>(ReportDefinition definition) where T : XmlReportSqlBulkCopyDataReader, new()
        {
            var reportFilePath = DownloadReport(definition);
            LoadReport<T>(reportFilePath);
            // todo: harden file deletion even during exceptions
            if (this._deleteDownloadedReportOnCompletion)
            {
                File.Delete(reportFilePath);
            }
        }

        private string DownloadReport(ReportDefinition definition)
        {
            try
            {
                // xml is more easily consumed for bulk insert than csv
                // additionally this application is expecting GZipped XML format
                definition.downloadFormat = DownloadFormat.GZIPPED_XML;
                // the specific date is always provided and the range is for only that date
                definition.dateRangeType = ReportDefinitionDateRangeType.CUSTOM_DATE;
                var dateString = this._rptdDate.ToString("yyyyMMdd");
                definition.selector.dateRange = new DateRange()
                {
                    min = dateString,
                    max = dateString
                };

                // temp folder to hold compressed download
                var filePath = BuildTempFileName();

                ResetRetryCount();

                var utilities = new ReportUtilities(this._user, "v201609", definition);

                // todo: handle throttling
                SetReportStage(ReportStage.ReportRequested);
                using (var reportResponse = utilities.GetResponse())
                {
                    reportResponse.Save(filePath); // save file to temp dir
                }
                SetReportStage(ReportStage.Downloaded);

                // decompress it to disk for reading later, delete original file
                var decompressedFilePath = Utilities.ExtractGZipFile(new FileInfo(filePath), "xml", this._deleteDownloadedReportOnCompletion);
                SetReportStage(ReportStage.Decompressed);

                return decompressedFilePath;
            }
            catch (Exception ex)
            {
                throw new System.ApplicationException("Failed to download report.", ex);
            }
        }

        private string BuildTempFileName()
        {
            try
            {
                string extension = "gz";

                var fileName = string.Format(
                    "adwords_{0}_{1}_{2}_{3}.{4}",
                    this._clientId,
                    this._currentReportType,
                    this._rptdDate.ToString("yyyyMMdd"),
                    this._rptlogId,
                    extension);

                var path = Utilities.GetTempDir();

                return path + Path.DirectorySeparatorChar + fileName;
            }
            catch (Exception ex)
            {
                throw new System.ApplicationException("Failed to create report temp filename.", ex);
            }
        }

        private void LoadReport<T>(string reportFilePath) where T : XmlReportSqlBulkCopyDataReader, new()
        {
            try
            {
                using (var reader = new T())
                {
                    // file to datareader
                    reader.Initialize(reportFilePath, this._rptlogId, this._rptdDate, this._clientId);

                    // bulk load xml report datareader
                    SetReportStage(ReportStage.LoadStarted);
                    Repository.BulkInsert(reader);
                    SetReportStage(ReportStage.LoadCompleted);
                }
            }
            catch (Exception ex)
            {
                throw new System.ApplicationException("Failed to load report.", ex);
            }
        }

        private void SetReportStage(ReportStage reportStage)
        {
            this._currentReportStage = reportStage;

            var logMessage = string.Format(
                "ts:{0};logId:{1};clientId:{2};rptdDate:{3};reportType:{4};reportStage:{5}",
                DateTime.Now,
                this._rptlogId,
                this._clientId,
                this._rptdDate.ToString("yyyyMMdd"),
                this._currentReportType,
                this._currentReportStage);

            Console.WriteLine(logMessage);

            bool complete = false;

            // remove the current report type if it completed
            if (reportStage == ReportStage.LoadCompleted)
            {
                this._reportTypesToDownload = this._reportTypesToDownload.Except(this._currentReportType);
                // if there are no more report types to download then we are complete
                complete = this._reportTypesToDownload == ReportTypes.None;
            }

            Repository.UpdateReportLogStatus(this._rptlogId, this._currentReportType, this._currentReportStage, complete);
        }

        private void ResetRetryCount()
        {
            this._user.Config.RetryCount = this._retryCount;
        }
    }
}