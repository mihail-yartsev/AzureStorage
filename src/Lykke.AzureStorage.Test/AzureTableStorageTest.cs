using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using AzureStorage.Tables;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using System.IO;

namespace Lykke.AzureStorage.Test
{
    internal class TestEntity : TableEntity
    {
        public string FakeField { get; set; }
    }
    [TestClass]
    public class AzureTableStorageTest
    {
        private static IConfigurationRoot _configuration { get; set; }
        private string _azureStorageConnectionString = @"";
        private AzureTableStorage<TestEntity> _testEntityStorage;
        private string _tableName = "LykkeAzureStorageTest";

        //AzureStorage - azure account
        public AzureTableStorageTest()
        {
           _configuration = new ConfigurationBuilder()
               .AddJsonFile("appsettings.json", optional: true)
               .Build();

           _azureStorageConnectionString  = _configuration["AzureStorage"];
        }

        [TestInitialize]
        public void TestInit()
        {
            _testEntityStorage =
                new AzureTableStorage<TestEntity>(_azureStorageConnectionString, _tableName, null);
        }

        [TestMethod]
        public void AzureStorage_CheckInsert()
        {
            TestEntity testEntity = new TestEntity();
            testEntity.PartitionKey = "TestEntity";
            testEntity.FakeField = "Test";
            testEntity.RowKey = Guid.NewGuid().ToString();
            _testEntityStorage.InsertAsync(testEntity).Wait();
            TestEntity createdEntity = _testEntityStorage.GetDataAsync(testEntity.PartitionKey, testEntity.RowKey).Result;

            Assert.IsTrue(createdEntity != null);
        }

        [TestMethod]
        public void AzureStorage_CheckNoError_TableCreation()
        {
            //We call table creation in constructor
            new AzureTableStorage<TestEntity>(_azureStorageConnectionString, _tableName, null);
            new AzureTableStorage<TestEntity>(_azureStorageConnectionString, _tableName, null);
        }
    }
}
