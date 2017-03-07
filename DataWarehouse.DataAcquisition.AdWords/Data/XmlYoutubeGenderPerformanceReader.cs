using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataWarehouse.DataAcquisition.AdWords.Data
{
    public class XmlYoutubeGenderPerformanceReader : XmlReportSqlBulkCopyDataReader
    {
        public override string DestinationTable
        {
            get { return "[raw].[ytGenderPerformanceReport]"; }
        }

        public override int FieldCount
        {
            get { return 71; }
        }

        public override int BatchSize
        {
            get { return 0; }
        }

        public override bool EnableStreaming
        {
            get { return false; }
        }

        public override bool Read()
        {
            this._eof = !this._records.MoveNext();

            if (!this._eof)
            {
                var row = this._records.Current;
                this._values = new object[]
                    {
                        this._rptlogId,
                        this._rptdDate,
                        this._clientId,
                        (string)row.Attribute("account"),
                        (string)row.Attribute("activeViewAvgCPM"),
                        (string)row.Attribute("activeViewViewableCTR"),
                        (string)row.Attribute("activeViewViewableImpressions"),
                        (string)row.Attribute("activeViewMeasurableImprImpr"),
                        (string)row.Attribute("activeViewMeasurableCost"),
                        (string)row.Attribute("activeViewMeasurableImpr"),
                        (string)row.Attribute("adGroupID"),
                        (string)row.Attribute("adGroup"),
                        (string)row.Attribute("adGroupState"),
                        (string)row.Attribute("network"),
                        (string)row.Attribute("networkWithSearchPartners"),
                        (string)row.Attribute("allConvRate"),
                        (string)row.Attribute("allConv"),
                        (string)row.Attribute("allConvValue"),
                        (string)row.Attribute("avgCost"),
                        (string)row.Attribute("avgCPC"),
                        (string)row.Attribute("avgCPE"),
                        (string)row.Attribute("avgCPM"),
                        (string)row.Attribute("avgCPV"),
                        (string)row.Attribute("baseAdGroupID"),
                        (string)row.Attribute("baseCampaignID"),
                        (string)row.Attribute("campaignID"),
                        (string)row.Attribute("campaign"),
                        (string)row.Attribute("campaignState"),
                        (string)row.Attribute("clicks"),
                        (string)row.Attribute("conversions"),
                        (string)row.Attribute("totalConvValue"),
                        (string)row.Attribute("cost"),
                        (string)row.Attribute("costAllConv"),
                        (string)row.Attribute("maxCPC"),
                        (string)row.Attribute("maxCPCSource"),
                        (string)row.Attribute("gender"),
                        (string)row.Attribute("destinationURL"),
                        (string)row.Attribute("crossDeviceConv"),
                        (string)row.Attribute("ctr"),
                        (string)row.Attribute("clientName"),
                        (string)row.Attribute("day"),
                        (string)row.Attribute("device"),
                        (string)row.Attribute("engagementRate"),
                        (string)row.Attribute("engagements"),
                        (string)row.Attribute("customerID"),
                        (string)row.Attribute("appFinalURL"),
                        (string)row.Attribute("mobileFinalURL"),
                        (string)row.Attribute("finalURL"),
                        (string)row.Attribute("gmailForwards"),
                        (string)row.Attribute("gmailSaves"),
                        (string)row.Attribute("gmailClicksToWebsite"),
                        (string)row.Attribute("criterionID"),
                        (string)row.Attribute("impressions"),
                        (string)row.Attribute("interactionRate"),
                        (string)row.Attribute("interactions"),
                        (string)row.Attribute("interactionTypes"),
                        (string)row.Attribute("isNegative"),
                        (string)row.Attribute("isRestricting"),
                        (string)row.Attribute("companyName"),
                        (string)row.Attribute("genderState"),
                        (string)row.Attribute("trackingTemplate"),
                        (string)row.Attribute("customParameter"),
                        (string)row.Attribute("valueAllConv"),
                        (string)row.Attribute("valueConv"),
                        (string)row.Attribute("videoPlayedTo100"),
                        (string)row.Attribute("videoPlayedTo25"),
                        (string)row.Attribute("videoPlayedTo50"),
                        (string)row.Attribute("videoPlayedTo75"),
                        (string)row.Attribute("viewRate"),
                        (string)row.Attribute("views"),
                        (string)row.Attribute("viewThroughConv"),
                    };
            }

            return !this._eof;
        }
    }
}
