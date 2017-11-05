using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using AzureStorage.Tables;
using Microsoft.Extensions.Configuration;
using System.Threading.Tasks;
using AzureStorage;
using Lykke.AzureStorage.Tables;
using Lykke.AzureStorage.Test.Mocks;

namespace Lykke.AzureStorage.Test
{
    [TestClass]
    public class AzureTableEntityStorageTest
    {
        public class TestEntity : AzureTableEntity
        {
            [PartitionKey]
            public string Partition { get; set; }
            [RowKey]
            public string Row { get; set; }
            public string FakeField { get; set; }
        }

        private readonly string _azureStorageConnectionString;
        private INoSQLTableStorage<TestEntity> _testEntityStorage;
        private string _tableName = "LykkeAzureStorageTest";

        //AzureStorage - azure account
        public AzureTableEntityStorageTest()
        {
            var configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: true)
                .Build();

            _azureStorageConnectionString = configuration["AzureStorage"];
        }

        [TestInitialize]
        public void TestInit()
        {
            _testEntityStorage = AzureTableStorage<TestEntity>.Create(
                new ConnStringReloadingManagerMock(_azureStorageConnectionString),
                _tableName,
                null);
        }

        [TestCleanup]
        public async Task TestClean()
        {
            var items = await _testEntityStorage.GetDataAsync();
            await _testEntityStorage.DeleteAsync(items);
        }

        [TestMethod]
        public async Task AzureStorage_CheckInsertAndRead()
        {
            TestEntity testEntity = GetTestEntity();

            await _testEntityStorage.InsertAsync(testEntity);
            var createdEntity = await _testEntityStorage.GetDataAsync(testEntity.Partition, testEntity.Row);

            Assert.AreEqual(createdEntity.Partition, testEntity.Partition);
            Assert.AreEqual(createdEntity.Row, testEntity.Row);
            Assert.AreEqual(createdEntity.FakeField, testEntity.FakeField);
        }

        private TestEntity GetTestEntity()
        {
            TestEntity testEntity = new TestEntity
            {
                Partition = "TestEntity",
                FakeField = "Test",
                Row = Guid.NewGuid().ToString()
            };

            return testEntity;
        }
    }
}
