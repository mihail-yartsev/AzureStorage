using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Common;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureStorage.Tables
{

    public class Row
    {
        public Row()
        {
            Data = new Dictionary<string, object>();
        }

        public Dictionary<string, object> Data { get; }

        private static IEnumerable<PropertyInfo> GetProps(Type type)
        {
            return type.GetProperties().Where(prop => prop.CanWrite && prop.CanRead);
        }

        public void Merge(object instance)
        {
            foreach (var prop in GetProps(instance.GetType()))
            {
                var value = prop.GetValue(instance, null);
                if (Data.ContainsKey(prop.Name))
                {
                    if (value == null)
                        Data.Remove(prop.Name);
                    else
                        Data[prop.Name] = value;
                }
                else
                {
                    if (value != null)
                        Data.Add(prop.Name, value);
                }
            }
        }


        public void Replace(object instance)
        {
            Data.Clear();
            foreach (var prop in GetProps(instance.GetType()))
            {
                var value = prop.GetValue(instance, null);
                if (value != null) Data.Add(prop.Name, value);
            }
        }

        public static Row Serialize(object instance)
        {
            var result = new Row();
            result.Replace(instance);
            return result;
        }

        public T Deserialize<T>() where T : new()
        {
            var result = new T();
            foreach (var prop in GetProps(result.GetType()).Where(prop => Data.ContainsKey(prop.Name)))
                prop.SetValue(result, Data[prop.Name], null);

            return result;
        }
    }

    public class Partition
    {
        public Partition()
        {
            Rows = new ConcurrentDictionary<string, Row>();
        }

        public ConcurrentDictionary<string, Row> Rows { get; private set; }
    }

    public class NoSqlTableInMemory<T> : INoSQLTableStorage<T> where T : class, ITableEntity, new()
    {
        private readonly SemaphoreSlim _lockSlim = new SemaphoreSlim(1);


        public readonly ConcurrentDictionary<string, Partition> Partitions = new ConcurrentDictionary<string, Partition>();

        private IEnumerable<T> GetAllData(Func<T, bool> filter = null)
        {
            var result = from partition in Partitions.Values from row in partition.Rows.Values select row.Deserialize<T>();
            if (filter != null)
                result = result.Where(filter);

            return result;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }


        public IEnumerator<T> GetEnumerator()
        {
            return GetAllData().GetEnumerator();
        }

        private Partition GetPartition(string partitionKey, bool createNewIfNotExists)
        {
            if (!createNewIfNotExists)
            {
                if (Partitions.TryGetValue(partitionKey, out var data))
                    return data;
                return null;
            }

            return Partitions.GetOrAdd(partitionKey, new Partition());
        }

        private Row GetRow(string partitionKey, string rowKey)
        {
            var partition = GetPartition(partitionKey, false);

            if (partition == null)
                return null;

            return partition.Rows.TryGetValue(rowKey, out var data) ? data : null;
        }

        private bool HasElement(T item)
        {
            return GetRow(item.PartitionKey, item.RowKey) != null;
        }

        private void PrivateInsert(T item)
        {
            var partition = GetPartition(item.PartitionKey, true);
            partition.Rows.TryAdd(item.RowKey, Row.Serialize(item));
        }

        public async Task InsertAsync(T item, params int[] notLogCodes)
        {
            await _lockSlim.WaitAsync();
            try
            {
                if (HasElement(item))
                {

                    var message =
                        string.Format("Can not insert. Item is already in the table. Data:{0}", AzureStorageUtils.PrintItem(item));

                    var exception = StorageException.TranslateException(new Exception(message),

                        new RequestResult
                        {
                            HttpStatusCode = AzureStorageUtils.Conflict
                        }

                    );

                    throw exception;
                }
                PrivateInsert(item);
            }
            finally
            {
                _lockSlim.Release();
            }
        }

        public async Task InsertAsync(IEnumerable<T> items)
        {
            foreach (var entity in items)
                await InsertAsync(entity);
        }

        public async Task InsertOrMergeAsync(T item)
        {
            await _lockSlim.WaitAsync();
            try
            {
                var row = GetRow(item.PartitionKey, item.RowKey);

                if (row == null)
                    PrivateInsert(item);
                else
                    row.Merge(item);
            }
            finally
            {
                _lockSlim.Release();
            }
        }

        public async Task InsertOrMergeBatchAsync(IEnumerable<T> items)
        {
            foreach (var entity in items)
                await InsertOrMergeAsync(entity);
        }

        public async Task<T> ReplaceAsync(string partitionKey, string rowKey, Func<T, T> item)
        {
            await _lockSlim.WaitAsync();
            try
            {
                var row = GetRow(partitionKey, rowKey);
                if (row == null)
                    return null;

                var entity = row.Deserialize<T>();
                var result = item(entity);

                if (result != null)
                    row.Replace(result);

                return result;

            }
            finally
            {
                _lockSlim.Release();
            }
        }

        public async Task<T> MergeAsync(string partitionKey, string rowKey, Func<T, T> item)
        {
            await _lockSlim.WaitAsync();
            try
            {
                var row = GetRow(partitionKey, rowKey);
                if (row == null)
                    throw new Exception(
                        string.Format("Could not replace item with Partition:{0}, Row:{1}. Item is not in list.",
                            partitionKey, rowKey));

                var entity = row.Deserialize<T>();
                var result = item(entity);

                if (result != null)
                    row.Merge(result);

                return result;

            }
            finally
            {
                _lockSlim.Release();
            }
        }

        public async Task InsertOrReplaceBatchAsync(IEnumerable<T> entites)
        {
            foreach (var entity in entites)
                await InsertOrReplaceAsync(entity);
        }

        public async Task InsertOrReplaceAsync(T item)
        {
            await _lockSlim.WaitAsync();
            try
            {
                var row = GetRow(item.PartitionKey, item.RowKey);

                if (row == null)
                    PrivateInsert(item);
                else
                    row.Replace(item);
            }
            finally
            {
                _lockSlim.Release();
            }
        }

        public async Task InsertOrReplaceAsync(IEnumerable<T> items)
        {
            foreach (var entity in items)
                await InsertOrReplaceAsync(entity);
        }

        public Task DeleteAsync(T item)
        {
            return DeleteAsync(item.PartitionKey, item.RowKey);
        }


        public async Task<T> DeleteAsync(string partitionKey, string rowKey)
        {
            await _lockSlim.WaitAsync();
            try
            {
                var row = GetRow(partitionKey, rowKey);
                if (row == null)
                    return null;

                var partition = GetPartition(partitionKey, false);

                if (!partition.Rows.ContainsKey(rowKey))
                    return null;

                var itemToDelete = partition.Rows[rowKey];
                partition.Rows.TryRemove(rowKey, out var _);
                return itemToDelete.Deserialize<T>();
            }
            finally
            {
                _lockSlim.Release();
            }
        }

        public async Task<bool> DeleteIfExistAsync(string partitionKey, string rowKey)
        {
            await DeleteAsync(partitionKey, rowKey);
            return true;
        }

        public async Task DeleteAsync(IEnumerable<T> items)
        {
            foreach (var entity in items)
                await DeleteAsync(entity);
        }

        public async Task<bool> CreateIfNotExistsAsync(T item)
        {
            await _lockSlim.WaitAsync();
            try
            {
                var row = GetRow(item.PartitionKey, item.RowKey);
                if (row == null)
                {
                    PrivateInsert(item);
                    return true;
                }

                return false;
            }
            finally
            {
                _lockSlim.Release();
            }
        }

        public bool RecordExists(T item)
        {
            return this[item.PartitionKey, item.RowKey] != null;
        }

        public void DoBatch(TableBatchOperation batch)
        {
            throw new NotImplementedException();
        }

        public Task DoBatchAsync(TableBatchOperation batch)
        {
            throw new NotImplementedException();
        }

        public T this[string partitionKey, string rowKey]
        {
            get
            {
                var row = GetRow(partitionKey, rowKey);

                return row?.Deserialize<T>();
            }
        }

        private readonly T[] _empty = new T[0];

        public Task<IList<T>> GetDataAsync(Func<T, bool> filter = null)
        {
            return Task.Run(() => (IList<T>)GetAllData(filter).ToList());
        }

        public Task<IEnumerable<T>> GetDataAsync(string partitionKey, IEnumerable<string> rowKeys, int pieces = 15, Func<T, bool> filter = null)
        {
            return Task.Run(() =>
            {
                var partition = GetPartition(partitionKey, false);

                if (partition == null)
                    return _empty;

                return filter == null

                    ? rowKeys.Where(rowKey => partition.Rows.ContainsKey(rowKey))
                        .Select(rowKey => partition.Rows[rowKey].Deserialize<T>())

                    : rowKeys.Where(rowKey => partition.Rows.ContainsKey(rowKey))
                        .Select(rowKey => partition.Rows[rowKey].Deserialize<T>()).Where(filter);

            });

        }

        public Task<IEnumerable<T>> GetDataAsync(IEnumerable<string> partitionKeys, int pieceSize = 100, Func<T, bool> filter = null)
        {
            return Task.Run(() =>
            {
                var result = new List<T>();

                foreach (var partitionKey in partitionKeys)
                    result.AddRange(this[partitionKey]);


                return (IEnumerable<T>)result;
            });
        }

        public Task<IEnumerable<T>> GetDataAsync(IEnumerable<Tuple<string, string>> keys, int pieceSize = 100, Func<T, bool> filter = null)
        {
            var result = keys.Select(tuple => this[tuple.Item1, tuple.Item2]).Where(entity => entity != null).ToArray();
            return Task.FromResult((IEnumerable<T>)result);
        }

        public Task<T> GetTopRecordAsync(string partitionKey)
        {
            var partition = GetPartition(partitionKey, false);
            return Task.FromResult(partition?.Rows?.FirstOrDefault().Value?.Deserialize<T>());
        }

        public Task<IEnumerable<T>> GetTopRecordsAsync(string partitionKey, int n)
        {
            var partition = GetPartition(partitionKey, false);
            return Task.FromResult(partition.Rows.Take(n).Select(x => x.Value.Deserialize<T>()));
        }

        public Task GetDataByChunksAsync(Func<IEnumerable<T>, Task> chunks)
        {
            var data = GetData();
            return chunks(data);
        }

        public Task GetDataByChunksAsync(Action<IEnumerable<T>> chunks)
        {
            var data = GetData();
            chunks(data);
            return Task.FromResult(0);
        }

        public Task GetDataByChunksAsync(string partitionKey, Action<IEnumerable<T>> chunks)
        {
            return Task.Run(() => { chunks(this[partitionKey]); });
        }

        public Task<T> GetDataAsync(string partition, string row)
        {
            return Task.FromResult(this[partition, row]);
        }

        public Task ScanDataAsync(string partitionKey, Func<IEnumerable<T>, Task> chunk)
        {
            return Task.Run(() => { chunk(this[partitionKey]); });
        }

        public Task ScanDataAsync(TableQuery<T> rangeQuery, Func<IEnumerable<T>, Task> chunk)
        {
            throw new NotImplementedException();
        }

        public Task<T> FirstOrNullViaScanAsync(string partitionKey, Func<IEnumerable<T>, T> dataToSearch)
        {
            return Task.Run(() => dataToSearch(this[partitionKey]));
        }

        public IEnumerable<T> this[string partitionKey]
        {
            get
            {
                var partition = GetPartition(partitionKey, false);
                return partition?.Rows.Values.Select(row => row.Deserialize<T>()).ToArray() ?? _empty;
            }
        }
        
        public IEnumerable<T> GetData(Func<T, bool> filter = null)
        {
            return GetAllData(filter);
        }

        public IEnumerable<T> GetData(string partitionKey, Func<T, bool> filter = null)
        {
            var result = this[partitionKey];

            return filter == null ? result : result.Where(filter);
        }

        public Task<IEnumerable<T>> GetDataAsync(string partition, Func<T, bool> filter)
        {
            return Task.Run(() => AzureStorageUtils.ApplyFilter(this[partition], filter));
        }

        public Task<IEnumerable<T>> GetDataRowKeysOnlyAsync(IEnumerable<string> rowKeys)
        {
            return Task.Run(() =>
            {
                var result = new List<T>();

                foreach (var rowKey in rowKeys)
                    result.AddRange(this.GetDataRowKeyOnlyAsync(rowKey).Result);

                return (IEnumerable<T>)result;
            });
        }

        public IEnumerable<T> Where(TableQuery<T> rangeQuery, Func<T, bool> filter = null)
        {
            var whereInMemory = new WhereInMemory(rangeQuery.FilterString);

            var data = whereInMemory.PartitionKey == null ? this.ToArray() : this[whereInMemory.PartitionKey];

            var result = data.Where(whereInMemory.PassRowKey);

            if (filter != null)
                result = result.Where(filter);

            return result.ToArray();
        }

        public async Task<IEnumerable<T>> WhereAsyncc(TableQuery<T> rangeQuery, Func<T, Task<bool>> filter = null)
        {
            var data = Where(rangeQuery);
            var result = new List<T>();
            foreach (var itm in data)
            {
                if (filter == null || await filter(itm))
                    result.Add(itm);
            }

            return result;
        }

        public Task<IEnumerable<T>> WhereAsync(TableQuery<T> rangeQuery, Func<T, bool> filter = null)
        {
            return Task.Run(() => Where(rangeQuery, filter));
        }

        public Task ExecuteAsync(TableQuery<T> rangeQuery, Action<IEnumerable<T>> yieldResult, Func<bool> stopCondition = null)
        {
            return Task.Run(() =>
            {
                var items = Where(rangeQuery);
                yieldResult(items);
            });
        }
    }

    public class WhereInMemory
    {
        public class RowKeyCondition
        {
            public RowKeyCondition(string op, string value)
            {
                Op = op;
                Value = value;
            }

            public string Value { get; set; }
            public string Op { get; set; }

            public bool IsCondition(ITableEntity tableEntity)
            {
                switch (Op)
                {
                    case QueryComparisons.Equal:
                        return Value == tableEntity.RowKey;

                    case QueryComparisons.GreaterThanOrEqual:
                        return Value == tableEntity.RowKey || String.Compare(tableEntity.RowKey, Value, StringComparison.Ordinal) > 0;

                    case QueryComparisons.GreaterThan:
                        return String.Compare(tableEntity.RowKey, Value, StringComparison.Ordinal) > 0;

                    case QueryComparisons.LessThan:
                        return String.Compare(tableEntity.RowKey, Value, StringComparison.Ordinal) < 0;

                    case QueryComparisons.LessThanOrEqual:
                        return Value == tableEntity.RowKey || String.Compare(tableEntity.RowKey, Value, StringComparison.Ordinal) < 0;

                    case QueryComparisons.NotEqual:
                        return Value != tableEntity.RowKey;

                }
                return false;
            }
        }

        private readonly List<RowKeyCondition> _rowKeyConditions = new List<RowKeyCondition>();

        private static IEnumerable<string> Split(string data, string separator)
        {
            data = data.Replace(separator, "&");
            return data.Split('&');
        }

        private static Tuple<string, string, string> ParseParam(string data)
        {
            var lines = data.Trim().Split(' ');

            var sb = new StringBuilder();
            for (var i = 2; i < lines.Length; i++)
                if (!string.IsNullOrEmpty(lines[i]))
                    sb.Append(lines[i] + " ");

            var value = sb.ToString();
            return new Tuple<string, string, string>(lines[0], lines[1], value.Substring(1, value.Length - 3));
        }

        public WhereInMemory(string data)
        {
            var lines = Split(data, "and");
            foreach (var line in lines)
            {
                var p = ParseParam(line);
                Parse(p.Item1, p.Item2, p.Item3);
            }

        }

        private void Parse(string field, string op, string value)
        {
            if (field == "PartitionKey")
                PartitionKey = value;

            if (field == "RowKey")
                _rowKeyConditions.Add(new RowKeyCondition(op, value));
        }

        public string PartitionKey { get; private set; }

        public bool PassRowKey(ITableEntity tableEntity)
        {
            return _rowKeyConditions.All(rowKeyCondition => rowKeyCondition.IsCondition(tableEntity));
        }
    }
}
