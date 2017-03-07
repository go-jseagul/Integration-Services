using DataWarehouse.DataAcquisition.AdWords.Data;
using DataWarehouse.DataAcquisition.AdWords.Properties;
using Google.Api.Ads.AdWords.Lib;
using Google.Api.Ads.AdWords.v201609;
using System;
using System.Collections.Generic;

namespace DataWarehouse.DataAcquisition.AdWords.Processors
{
    public class ManagedClientProcessor
    {
        public void UpdateManagedClients()
        {
            try
            {
                var user = new AdWordsUser();
                // Get the ManagedCustomerService.
                ManagedCustomerService managedCustomerService =
                    (ManagedCustomerService)user.GetService(AdWordsService.v201609.ManagedCustomerService);
                managedCustomerService.RequestHeader.clientCustomerId = Settings.Default.RootMCCClientCustomerId;

                // Create selector.
                var selector = new Selector()
                {
                    fields = new string[]
                    {
                        ManagedCustomer.Fields.CanManageClients,
                        ManagedCustomer.Fields.CurrencyCode,
                        ManagedCustomer.Fields.CustomerId,
                        ManagedCustomer.Fields.DateTimeZone,
                        ManagedCustomer.Fields.Name,
                        ManagedCustomer.Fields.TestAccount,
                    },
                    paging = new Paging
                    {
                        numberResults = 5000,
                        startIndex = 0
                    }
                };

                // list to build up all pages from api
                var managedCustomerPageList = new List<ManagedCustomerPage>();

                // holds the page between each call to the api
                ManagedCustomerPage page = null;

                // get all pages
                do
                {
                    // get current page
                    page = managedCustomerService.get(selector);

                    // add page to list
                    managedCustomerPageList.Add(page);

                    // advance paging to next page
                    selector.paging.IncreaseOffset();

                } while (selector.paging.startIndex < page.totalNumEntries);

                // serialize as xml
                var managedCustomerPageXml = managedCustomerPageList.ToArray().ToXml();

                // send to db for processing
                Repository.UploadMagagedClients(managedCustomerPageXml);
            }
            catch (Exception ex)
            {
                var newEx = new System.ApplicationException("Failed to Refresh Managed Client List", ex);
                Repository.LogError(newEx.ToString(), "ManagedClientProcessor");
            }
        }
    }
}