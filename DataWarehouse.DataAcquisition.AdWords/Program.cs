using DataWarehouse.DataAcquisition.AdWords.Data;
using DataWarehouse.DataAcquisition.AdWords.Properties;
using System;
using System.Threading.Tasks;

namespace DataWarehouse.DataAcquisition.AdWords
{
    class Program
    {
        static void Main(string[] args)
        {
            // if the temp directory isn't valid, no point in processing
            if (AppSettingsValid())
            {

                // first acquire latest list of clients
                if (Settings.Default.EnableClientRefresh)
                {
                    RefreshClientList();
                }

                // now populate the report queue for today's date
                if (Settings.Default.EnableReportQueuePopulation)
                {
                    PopulateReportQueue(DateTime.Now.AddDays(-1).Date);
                }

                // now process the report queue
                if (Settings.Default.EnableReportQueueProcessing)
                {
                    ProcessQueue();
                }
            }
        }

        private static bool AppSettingsValid()
        {
            var valid = false;
            try
            {
                var tmpDir = Utilities.GetTempDir();
                valid = true;
            }
            catch (Exception ex)
            {
                var newEx = new ApplicationException("ReportDownloadTempFolder not accessible", ex);
                Repository.LogError(newEx.ToString(), "AppSettings");
            }
            return valid;
        }

        private static void RefreshClientList()
        {
            var clientProcessor = new Processors.ManagedClientProcessor();
            clientProcessor.UpdateManagedClients();
        }

        private static void PopulateReportQueue(DateTime date)
        {
            try
            {
                Repository.PopulateReportQueue(date);
            }
            catch (Exception ex)
            {
                var newEx = new ApplicationException(string.Format("Failed to Populate Report Queue for date:{0}", date.ToString("yyyyMMdd")), ex);
                Repository.LogError(newEx.ToString(), "PopulateReportQueue");
            }
        }

        private static void ProcessQueue()
        {
            var threadCount = Settings.Default.ReportProcessorThreadCount;

            // up the parallel http requests to match number of threads
            System.Net.ServicePointManager.DefaultConnectionLimit = threadCount;

            // setup tasks for parallel report processing
            var processorTasks = new Task[threadCount];
            for (int i = 0; i < threadCount; i++)
            {
                // use LongRunning option to force a dedicated thread per task 
                processorTasks[i] = Task.Factory.StartNew(Processors.ReportProcessor.Run, TaskCreationOptions.LongRunning);
            }

            // wait for all to complete
            Task.WaitAll(processorTasks);
        }
    }
}