using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataWarehouse.DataAcquisition.AdWords.Data
{
    public class XmlYoutubeVideoPerformanceReader : XmlReportSqlBulkCopyDataReader
    {
        public override string DestinationTable
        {
            get { return "[raw].[ytVideoPerformanceReport]"; }
        }

        public override int FieldCount
        {
            get { return 31; }
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
                        (string)row.Attribute("adGroupID"),
                        (string)row.Attribute("adGroup"),
                        (string)row.Attribute("adGroupState"),
                        (string)row.Attribute("network"),
                        (string)row.Attribute("networkWithSearchPartners"),
                        (string)row.Attribute("campaignID"),
                        (string)row.Attribute("campaign"),
                        (string)row.Attribute("campaignState"),
                        (string)row.Attribute("clicks"),
                        (string)row.Attribute("conversions"),
                        (string)row.Attribute("totalConvValue"),
                        (string)row.Attribute("cost"),
                        (string)row.Attribute("adID"),
                        (string)row.Attribute("adState"),
                        (string)row.Attribute("day"),
                        (string)row.Attribute("device"),
                        (string)row.Attribute("engagements"),
                        (string)row.Attribute("impressions"),
                        (string)row.Attribute("videoChannelId"),
                        (string)row.Attribute("videoDuration"),
                        (string)row.Attribute("videoId"),
                        (string)row.Attribute("videoPlayedTo100"),
                        (string)row.Attribute("videoPlayedTo25"),
                        (string)row.Attribute("videoPlayedTo50"),
                        (string)row.Attribute("videoPlayedTo75"),
                        (string)row.Attribute("videoTitle"),
                        (string)row.Attribute("views"),
                        (string)row.Attribute("viewThroughConv"),
                    };
            }

            return !this._eof;
        }
    }
}
