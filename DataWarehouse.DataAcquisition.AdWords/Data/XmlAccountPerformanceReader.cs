using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataWarehouse.DataAcquisition.AdWords.Data
{
    public class XmlAccountPerformanceReader : XmlReportSqlBulkCopyDataReader
    {
        public override string DestinationTable
        {
            get { return "[raw].[AccountPerformance]"; }
        }

        public override int FieldCount
        {
            get { return 10; }
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
                        (long)row.Attribute("impressions"),
                        (decimal)row.Attribute("avgPosition"),
                        (long)row.Attribute("clicks"),
                        (long)row.Attribute("cost"),
                        0L,
                        (string)row.Attribute("account"),
                        (string)row.Attribute("network")
                    };
            }

            return !this._eof;
        }

        private bool? _hasImpressions = null;
        public bool HasImpressions
        {
            get
            {
                if (!_hasImpressions.HasValue)
                {
                    _hasImpressions = false;
                    while (this.Read())
                    {
                        if ((long)this.GetValue(3) > 0) // reading impressions value
                        {
                            _hasImpressions = true;
                            this.Reset(); // reset the enumerator for future use
                            break;
                        }
                    }
                }
                return _hasImpressions.Value;
            }
        }
    }
}
