using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using AzureStorage.Tables;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using System.IO;
using System.Threading.Tasks;

namespace Lykke.AzureStorage.Test
{
    internal class TestEntity : TableEntity
    {
        public string FakeField { get; set; }
    }
    [TestClass]
    public class AzureTableStorageTest
    {
        private readonly string _azureStorageConnectionString;
        private AzureTableStorage<TestEntity> _testEntityStorage;
        private string _tableName = "LykkeAzureStorageTest";

        //AzureStorage - azure account
        public AzureTableStorageTest()
        {
            var configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: true)
                .Build();

            _azureStorageConnectionString = configuration["AzureStorage"];
        }

        [TestInitialize]
        public void TestInit()
        {
            _testEntityStorage = new AzureTableStorage<TestEntity>(_azureStorageConnectionString, _tableName, null);
        }

        [TestCleanup]
        public async Task TestClean()
        {
            var items = await _testEntityStorage.GetDataAsync();
            await _testEntityStorage.DeleteAsync(items);
        }

        [TestMethod]
        public async Task AzureStorage_CheckInsert()
        {
            TestEntity testEntity = GetTestEntity();

            await _testEntityStorage.InsertAsync(testEntity);
            var createdEntity = await _testEntityStorage.GetDataAsync(testEntity.PartitionKey, testEntity.RowKey);

            Assert.IsNotNull(createdEntity);
        }

        [TestMethod]
        public async Task AzureStorage_CheckParallelInsert()
        {
            var testEntity = GetTestEntity();

            var storage1 = new AzureTableStorage<TestEntity>(_azureStorageConnectionString, _tableName, null);

            Parallel.For(1, 10, i =>
            {
                storage1.CreateIfNotExistsAsync(testEntity).Wait();
            });

            var createdEntity = await _testEntityStorage.GetDataAsync(testEntity.PartitionKey, testEntity.RowKey);

            Assert.IsNotNull(createdEntity);
        }

        private TestEntity GetTestEntity()
        {
            TestEntity testEntity = new TestEntity
            {
                PartitionKey = "TestEntity",
                FakeField = "Test",
                RowKey = Guid.NewGuid().ToString()
            };

            return testEntity;
        }
    }
}
