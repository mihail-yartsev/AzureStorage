using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using AzureStorage.Tables;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using AzureStorage;
using AzureStorage.Blob;
using AzureStorage.Queue;
using Lykke.AzureStorage.Test.Mocks;

namespace Lykke.AzureStorage.Test
{
    [TestClass]
    public class AzureBlobTest
    {
        private readonly string _azureStorageConnectionString;
        private IBlobStorage _testBlob;
        private string _blobContainer = "LykkeAzureBlobTest";

        //AzureStorage - azure account
        public AzureBlobTest()
        {
            var configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: true)
                .Build();

            _azureStorageConnectionString = configuration["AzureStorage"];
        }

        [TestInitialize]
        public void TestInit()
        {
            _testBlob = AzureBlobStorage.Create(new ConnStringReloadingManagerMock(_azureStorageConnectionString));
        }

        [TestCleanup]
        public async Task TestClean()
        {
            var items = await _testBlob.GetListOfBlobKeysAsync(_blobContainer);
            foreach (var item in items)
            {
                await _testBlob.DelBlobAsync(_blobContainer, item);
            }
        }

        [TestMethod]
        public async Task AzureBlob_CheckInsert()
        {
            const string blobName = "Key";

            var data = new byte[] { 0x0, 0xff };

            await _testBlob.SaveBlobAsync(_blobContainer, blobName, new MemoryStream(data));

            using (var result = await _testBlob.GetAsync(_blobContainer, blobName))
            using (var ms = new MemoryStream())
            {
                result.CopyTo(ms);

                CollectionAssert.AreEquivalent(data, ms.ToArray());
            }
        }

        [TestMethod]
        public async Task AzureBlob_CheckParallelInsert()
        {
            var blob = AzureBlobStorage.Create(new ConnStringReloadingManagerMock(_azureStorageConnectionString));

            Parallel.For(1, 11, i =>
            {
                blob.SaveBlobAsync(_blobContainer, Guid.NewGuid().ToString(), new MemoryStream(new[] { (byte)i })).Wait();
            });

            var items = await _testBlob.GetListOfBlobsAsync(_blobContainer);

            Assert.AreEqual(10, items.Count());
        }
    }
}
