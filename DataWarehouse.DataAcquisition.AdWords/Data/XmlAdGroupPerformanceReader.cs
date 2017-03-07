using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataWarehouse.DataAcquisition.AdWords.Data
{
    public class XmlAdGroupPerformanceReader : XmlReportSqlBulkCopyDataReader
    {
        public override string DestinationTable
        {
            get { return "[raw].[AdGroupPerformance]"; }
        }

        public override int FieldCount
        {
            get { return 14; }
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
                        (long)row.Attribute("adGroupID"),
                        (long)row.Attribute("impressions"),
                        (decimal)row.Attribute("avgPosition"),
                        (long)row.Attribute("clicks"),
                        (long)row.Attribute("cost"),
                        0L,
                        (string)row.Attribute("adGroupState"),
                        (string)row.Attribute("adGroup"),
                        (string)row.Attribute("network"),
                        (string)row.Attribute("device")
                    };
            }

            return !this._eof;
        }
    }
}
