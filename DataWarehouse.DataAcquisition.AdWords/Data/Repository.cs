using DataWarehouse.DataAcquisition.AdWords.Processors;
using DataWarehouse.DataAcquisition.AdWords.Properties;
using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;

namespace DataWarehouse.DataAcquisition.AdWords.Data
{
    public static class Repository
    {
        private static readonly Database _db;

        static Repository()
        {
            _db = new Database(ConfigurationManager.ConnectionStrings["SMBGoogleAdWords"].ConnectionString);
        }

        public static long LogError(string errlogText, string errlogCategory)
        {
            var cmd = _db.GetStoredProcCommand("[dbo].[uspErrorLog_Insert]");
            _db.AddInParameter(cmd, "errlogText", SqlDbType.VarChar, errlogText);
            _db.AddInParameter(cmd, "errlogCategory", SqlDbType.VarChar, errlogCategory);
            _db.AddOutParameter(cmd, "errlogId", SqlDbType.BigInt);

            _db.ExecuteNonQuery(cmd);

            var errlogId = _db.GetParameterValue<long>(cmd, "errlogId");

            return errlogId.Value;
        }

        public static void UploadMagagedClients(string managedCustomerPageXml)
        {
            var cmd = _db.GetStoredProcCommand("[dbo].[uspManagedClientData_Process]");
            _db.AddInParameter(cmd, "managedCustomerPageXml", SqlDbType.Xml, managedCustomerPageXml);
            _db.ExecuteNonQuery(cmd);
        }

        public static void PopulateReportQueue(DateTime date)
        {
            var cmd = _db.GetStoredProcCommand("[dbo].[uspReportQueue_Populate]");
            _db.AddInParameter(cmd, "rptdDate", SqlDbType.Date, date);
            _db.ExecuteNonQuery(cmd);
        }

        public static bool GetNextReportQueueItem(out long? rptlogId, out long? clientId, out DateTime? rptdDate, out ReportTypes? reportTypes)
        {
            var cmd = _db.GetStoredProcCommand("[dbo].[uspReportQueue_Deque]");
            _db.AddOutParameter(cmd, "rptlogId", SqlDbType.BigInt);
            _db.AddOutParameter(cmd, "clientId", SqlDbType.BigInt);
            _db.AddOutParameter(cmd, "rptdDate", SqlDbType.Date);
            _db.AddOutParameter(cmd, "reportTypes", SqlDbType.Int);

            _db.ExecuteNonQuery(cmd);

            rptlogId = _db.GetParameterValue<long>(cmd, "rptlogId");
            clientId = _db.GetParameterValue<long>(cmd, "clientId");
            rptdDate = _db.GetParameterValue<DateTime>(cmd, "rptdDate");
            reportTypes = _db.GetParameterValue<ReportTypes>(cmd, "reportTypes");

            return rptlogId.HasValue;
        }

        public static void UpdateReportLogStatus(long rptlogId, ReportTypes reportType, ReportStage reportStage, bool complete)
        {
            var cmd = _db.GetStoredProcCommand("[dbo].[uspReportLog_StatusUpdate]");
            _db.AddInParameter(cmd, "rptlogId", SqlDbType.BigInt, rptlogId);
            _db.AddInParameter(cmd, "reportType", SqlDbType.VarChar, reportType.ToString());
            _db.AddInParameter(cmd, "reportStage", SqlDbType.VarChar, reportStage.ToString());
            _db.AddInParameter(cmd, "complete", SqlDbType.Bit, complete);

            _db.ExecuteNonQuery(cmd);
        }

        public static void SetReportLogZeroImpressions(long rptlogId)
        {
            var cmd = _db.GetStoredProcCommand("[dbo].[uspReportLog_SetZeroImpressions]");
            _db.AddInParameter(cmd, "rptlogId", SqlDbType.BigInt, rptlogId);

            _db.ExecuteNonQuery(cmd);
        }

        public static void SetReportLogErrorId(long rptlogId, long errlogId)
        {
            var cmd = _db.GetStoredProcCommand("[dbo].[uspReportLog_LogError]");
            _db.AddInParameter(cmd, "rptlogId", SqlDbType.BigInt, rptlogId);
            _db.AddInParameter(cmd, "errlogId", SqlDbType.BigInt, errlogId);

            _db.ExecuteNonQuery(cmd);
        }

        public static void BulkInsert(XmlReportSqlBulkCopyDataReader reader)
        {
            using (var bulkCopy = new SqlBulkCopy(_db.ConnectionString))
            {
                bulkCopy.DestinationTableName = reader.DestinationTable;
                bulkCopy.BatchSize = reader.BatchSize;
                bulkCopy.EnableStreaming = reader.EnableStreaming;
                bulkCopy.BulkCopyTimeout = Settings.Default.BulkCopyTimeout;
                // assumes the incoming reader matches the destination table schema by ordinal
                // and the destination table's first column is it identity column
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    bulkCopy.ColumnMappings.Add(i, i + 1);
                }
                bulkCopy.WriteToServer(reader);
            }
        }
    }
}