using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataWarehouse.DataAcquisition.AdWords.Data
{
    public class XmlAdPerformanceReader : XmlReportSqlBulkCopyDataReader
    {
        public override string DestinationTable
        {
            get { return "[raw].[AdPerformance]"; }
        }

        public override int FieldCount
        {
            get { return 21; }
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
                        (long)row.Attribute("adID"),
                        (long)row.Attribute("impressions"),
                        (decimal)row.Attribute("avgPosition"),
                        (long)row.Attribute("clicks"),
                        (long)row.Attribute("cost"),
                        0L,
                        (string)row.Attribute("adState"),
                        NormalizeAd((string)row.Attribute("adType"),(string)row.Attribute("ad"),(string)row.Attribute("headline1"),(string)row.Attribute("headline2")),
                        NormalizeDescription((string)row.Attribute("adType"),(string)row.Attribute("description"),(string)row.Attribute("descriptionLine1")),
                        (string)row.Attribute("descriptionLine2"),
                        NormalizeDisplayUrl((string)row.Attribute("adType"),(string)row.Attribute("displayURL"),(string)row.Attribute("finalURL"),(string)row.Attribute("path1"),(string)row.Attribute("path2")),
                        (string)row.Attribute("destinationURL"),
                        (string)row.Attribute("network"),
                        (string)row.Attribute("finalURL"),
                        (string)row.Attribute("mobileFinalURL"),
                        (string)row.Attribute("appFinalURL")
                    };
            }

            return !this._eof;
        }
        private string NormalizeAd(string adType, string ad, string headline1, string headline2)
        {
            if (IsExpandedTextAd(adType))
                return $"{headline1} - {headline2}";
            return ad;
        }
        private string NormalizeDescription(string adType, string description, string descriptionLine1)
        {
            if (IsExpandedTextAd(adType))
                return description;
            return descriptionLine1;
        }
        private string NormalizeDisplayUrl(string adType, string displayUrl, string finalUrl, string path1, string path2)
        {
            if (IsExpandedTextAd(adType) && !string.IsNullOrEmpty(finalUrl))
            {
                finalUrl = finalUrl.Replace("\"", "")?.Replace("[", "")?.Replace("]", "");
                if (!string.IsNullOrEmpty(finalUrl))
                {
                    Uri uri;
                    if (Uri.TryCreate(finalUrl, UriKind.Absolute, out uri))
                    {
                        var path = $"{path1}{(!string.IsNullOrEmpty(path1) && !string.IsNullOrEmpty(path2) ? "/" : "")}{path2}";
                        var retUrl = $"{uri.Host}{(!string.IsNullOrEmpty(path) ? "/" : "")}{path}";
                        if (retUrl.StartsWith("www."))
                            retUrl = retUrl.Substring(4);
                        return retUrl;
                    }
                }
            }
            return displayUrl;
        }
        private bool IsExpandedTextAd(string adType)
        {
            return (!string.IsNullOrEmpty(adType) && adType.ToUpper() == "EXPANDED TEXT AD");
        }
    }
}
