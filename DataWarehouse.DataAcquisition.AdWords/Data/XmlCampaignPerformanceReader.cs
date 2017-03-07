using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataWarehouse.DataAcquisition.AdWords.Data
{ 
    public class XmlCampaignPerformanceReader : XmlReportSqlBulkCopyDataReader
    {
        public override string DestinationTable
        {
            get { return "[raw].[CampaignPerformance]"; }
        }

        public override int FieldCount
        {
            get { return 18; }
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
                        (long)row.Attribute("campaignID"),
                        (long)row.Attribute("impressions"),
                        (decimal)row.Attribute("avgPosition"),
                        (long)row.Attribute("clicks"),
                        (long)row.Attribute("cost"),
                        0L,
                        (string)row.Attribute("campaignState"),
                        (string)row.Attribute("campaign"),
                        (string)row.Attribute("network"),
                        (string)row.Attribute("contentLostISBudget"),
                        (string)row.Attribute("contentImprShare"),
                        (string)row.Attribute("contentLostISRank"),
                        (string)row.Attribute("searchLostISBudget"),
                        (string)row.Attribute("searchImprShare"),
                        (string)row.Attribute("searchLostISRank"),
                        (string)row.Attribute("advertisingChannel"),
                        (string)row.Attribute("advertisingSubChannel"),
                    };
            }

            return !this._eof;
        }

        public string _AdvertisingChannelType = "";

        public string AdvertisingChannelType
        {
            get
            {
                if (String.IsNullOrEmpty(_AdvertisingChannelType))
                {
                    _AdvertisingChannelType = null;
                    while (this.Read())
                    {
                        if (this.GetValue(18).ToString().Length > 0) // reading impressions value
                        {
                            _AdvertisingChannelType = this.GetValue(18).ToString();
                            this.Reset(); // reset the enumerator for future use
                            break;
                        }
                    }
                }
                return _AdvertisingChannelType;
            }
        }
    }
}
