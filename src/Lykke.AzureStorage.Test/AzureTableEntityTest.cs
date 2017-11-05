using System;
using System.Collections.Generic;
using System.Globalization;
using Lykke.AzureStorage.Tables;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage.Table;

namespace Lykke.AzureStorage.Test
{
    [TestClass]
    public class AzureTableEntityTest
    {
        [TestMethod]
        public void Read_test()
        {
            var dict = new Dictionary<string, EntityProperty>(StringComparer.OrdinalIgnoreCase)
            {
                {"strProp", new EntityProperty("qweqwe")},
                {"intProp", new EntityProperty(123)},
                {"extraProp", new EntityProperty(123.123)},
                {"ignoredProp", new EntityProperty(123.123)},
                {"decimalProp", new EntityProperty(1.123123123)},
            };
            var entity = new SimpleTestEntity();
            ((ITableEntity) entity).RowKey = "row";
            ((ITableEntity) entity).PartitionKey = "part";
            ((ITableEntity)entity).ReadEntity(dict, null);
            Assert.AreEqual(123, entity.IntProp);
            Assert.AreEqual("qweqwe", entity.StrProp);
            Assert.AreEqual(1.123123123m, entity.DecimalProp);
            Assert.AreEqual("row", entity.Row);
            Assert.AreEqual("part", entity.Partition);
            Assert.AreEqual(0.0, entity.IgnoredProp);
        }

        [TestMethod]
        public void Read_fields_missed_in_table_test()
        {
            var dict = new Dictionary<string, EntityProperty>();
            var entity = new SimpleTestEntity();
            ((ITableEntity)entity).ReadEntity(dict, null);
            Assert.AreEqual(0, entity.IntProp);
            Assert.AreEqual(null, entity.StrProp);
            Assert.AreEqual(0m, entity.DecimalProp);
            Assert.AreEqual(null, entity.Row);
            Assert.AreEqual(null, entity.Partition);
        }

        [TestMethod]
        public void Write_test()
        {
            var entity = new SimpleTestEntity
            {
                StrProp = "qweqwe",
                IntProp = 123,
                DecimalProp = 1.123123123m,
                Partition = "part",
                Row = "row",
                IgnoredProp = 123.222,
            };
            var dict = ((ITableEntity)entity).WriteEntity(null);
            Assert.AreEqual(3, dict.Count);
            Assert.AreEqual(new EntityProperty("qweqwe"), dict["StrProp"]);
            Assert.AreEqual(new EntityProperty(123), dict["IntProp"]);
            Assert.AreEqual(new EntityProperty(1.123123123), dict["DecimalProp"]);

            Assert.AreEqual("row", ((ITableEntity) entity).RowKey);
            Assert.AreEqual("part", ((ITableEntity)entity).PartitionKey);
        }

        [TestMethod]
        public void Read_const_PartitionKey_test()
        {
            var dict = new Dictionary<string, EntityProperty>();
            var entity = new TestEntityConstPartitionKey();
            ((ITableEntity) entity).RowKey = "row";
            ((ITableEntity) entity).PartitionKey = "smth";
            ((ITableEntity)entity).ReadEntity(dict, null);
            Assert.AreEqual("row", entity.Row);
        }

        [TestMethod]
        public void Write_const_PartitionKey_test()
        {
            var entity = new TestEntityConstPartitionKey()
            {
                Row = "row",
            };
            var dict = ((ITableEntity)entity).WriteEntity(null);
            Assert.AreEqual(0, dict.Count);

            Assert.AreEqual("row", ((ITableEntity)entity).RowKey);
            Assert.AreEqual("part", ((ITableEntity)entity).PartitionKey);
        }

        [TestMethod]
        public void Write_DateTimeKeysTestEntity_test()
        {
            var entity = new DateTimeKeysTestEntity()
            {
                Row = DateTime.Now.AddDays(1),
                Partition = DateTime.Now.AddDays(2),
            };
            var dict = ((ITableEntity)entity).WriteEntity(null);
            Assert.AreEqual(0, dict.Count);
            Assert.AreEqual(entity.Row.ToString(CultureInfo.InvariantCulture), ((ITableEntity)entity).RowKey);
            Assert.AreEqual(entity.Partition.ToString(CultureInfo.InvariantCulture), ((ITableEntity)entity).PartitionKey);
        }
        
        [TestMethod]
        public void Read_DateTimeKeysTestEntity_test()
        {
            var entity = new DateTimeKeysTestEntity();
            var dict = new Dictionary<string, EntityProperty>();
            ((ITableEntity)entity).RowKey = DateTime.Now.AddDays(1).ToString(CultureInfo.InvariantCulture);
            ((ITableEntity)entity).PartitionKey = DateTime.Now.AddDays(2).ToString(CultureInfo.InvariantCulture);
            ((ITableEntity)entity).ReadEntity(dict, null);
            Assert.AreEqual(entity.Row.ToString(CultureInfo.InvariantCulture), ((ITableEntity)entity).RowKey);
            Assert.AreEqual(entity.Partition.ToString(CultureInfo.InvariantCulture), ((ITableEntity)entity).PartitionKey);
        }

        private class SimpleTestEntity : AzureTableEntity
        {
            [RowKey]
            public string Row { get; set; }

            [PartitionKey]
            public string Partition { get; set; }

            public string StrProp { get; set; }
            public int IntProp { get; set; }
            public decimal DecimalProp { get; set; }

            [IgnoreProperty]
            public double IgnoredProp { get; set; }
        }

        private class DateTimeKeysTestEntity : AzureTableEntity
        {
            [RowKey]
            public DateTime Row { get; set; }

            [PartitionKey]
            public DateTime Partition { get; set; }
        }

        [ConstPartitionKey("part")]
        private class TestEntityConstPartitionKey : AzureTableEntity
        {
            [RowKey]
            public string Row { get; set; }
        }
    }
}
