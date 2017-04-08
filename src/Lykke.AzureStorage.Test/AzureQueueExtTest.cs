using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using AzureStorage.Tables;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using System.IO;
using System.Threading.Tasks;
using AzureStorage.Queue;

namespace Lykke.AzureStorage.Test
{
    [TestClass]
    public class AzureQueueExtTest
    {
        private readonly string _azureStorageConnectionString;
        private AzureQueueExt _testQueue;
        private string _queueName = "LykkeAzureQueueTest";

        //AzureStorage - azure account
        public AzureQueueExtTest()
        {
            var configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: true)
                .Build();

            _azureStorageConnectionString = configuration["AzureStorage"];
        }

        [TestInitialize]
        public void TestInit()
        {
            _testQueue = new AzureQueueExt(_azureStorageConnectionString, _queueName);
        }

        [TestCleanup]
        public async Task TestClean()
        {
            await _testQueue.ClearAsync();
        }

        [TestMethod]
        public async Task AzureQueue_CheckInsert()
        {
            await _testQueue.PutRawMessageAsync("test");

            Assert.AreEqual(1, await _testQueue.Count() ?? 0);
        }

        [TestMethod]
        public async Task AzureQueue_CheckParallelInsert()
        {
            var queue = new AzureQueueExt(_azureStorageConnectionString, _queueName);

            Parallel.For(1, 11, i =>
            {
                queue.PutRawMessageAsync(i.ToString()).Wait();
            });

            Assert.AreEqual(10, await _testQueue.Count() ?? 0);
        }
    }
}
