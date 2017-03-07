using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataWarehouse.DataAcquisition.AdWords.Data
{ 
    public class XmlCampaignChannelReader : XmlReportSqlBulkCopyDataReader
    {
        public override string DestinationTable
        {
            get { return "[raw].[CampaignChannel]"; }
        }

        public override int FieldCount
        {
            get { return 3; }
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
                        (long)row.Attribute("campaignID"),
                        (string)row.Attribute("advertisingChannel"),
                        (string)row.Attribute("advertisingSubChannel"),
                    };
            }

            return !this._eof;
        }

        
    }
}
