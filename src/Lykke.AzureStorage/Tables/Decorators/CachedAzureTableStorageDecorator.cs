using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common.Log;
using Lykke.AzureStorage.Tables.Paging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureStorage.Tables.Decorators
{
    /// <summary>
    ///  Cached <see cref="INoSQLTableStorage{T}"/> decorator
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class CachedAzureTableStorageDecorator<T> : INoSQLTableStorage<T> where T : class, ITableEntity, new()
    {
        public string Name => _table.Name;
        
        private readonly INoSQLTableStorage<T> _table;
        private readonly NoSqlTableInMemory<T> _cache;

        public CachedAzureTableStorageDecorator(INoSQLTableStorage<T> table)
        {
            _table = table;
            _cache = new NoSqlTableInMemory<T>();
            Init();
        }

        public async Task InsertAsync(T item, params int[] notLogCodes)
        {
            await _table.InsertAsync(item, notLogCodes);
            await _cache.InsertAsync(item, notLogCodes);
        }

        public async Task InsertAsync(IEnumerable<T> items)
        {
            await _table.InsertAsync(items);
            await _cache.InsertAsync(items);
        }

        public async Task InsertOrMergeAsync(T item)
        {
            await _table.InsertOrMergeAsync(item);
            await _cache.InsertOrMergeAsync(item);
        }

        public async Task InsertOrMergeBatchAsync(IEnumerable<T> items)
        {
            foreach (var entity in items)
                await InsertOrMergeAsync(entity);
        }

        public async Task<T> ReplaceAsync(string partitionKey, string rowKey, Func<T, T> item)
        {
            var result = await _table.ReplaceAsync(partitionKey, rowKey, item);
            await _cache.ReplaceAsync(partitionKey, rowKey, item);

            return result;
        }

        public async Task<T> MergeAsync(string partitionKey, string rowKey, Func<T, T> item)
        {
            var result = await _table.MergeAsync(partitionKey, rowKey, item);
            await _cache.MergeAsync(partitionKey, rowKey, item);
            return result;
        }

        public async Task InsertOrReplaceBatchAsync(IEnumerable<T> entites)
        {
            var myArray = entites as T[] ?? entites.ToArray();
            await _table.InsertOrReplaceBatchAsync(myArray);
            await _cache.InsertOrReplaceBatchAsync(myArray);
        }

        public async Task InsertOrReplaceAsync(T item)
        {
            await _table.InsertOrReplaceAsync(item);
            await _cache.InsertOrReplaceAsync(item);
        }

        public async Task InsertOrReplaceAsync(IEnumerable<T> items)
        {
            foreach (var entity in items)
                await InsertOrReplaceAsync(entity);
        }

        public async Task DeleteAsync(T item)
        {
            await _table.DeleteAsync(item);
            await _cache.DeleteAsync(item);
        }

        public async Task<T> DeleteAsync(string partitionKey, string rowKey)
        {
            var result = await _table.DeleteAsync(partitionKey, rowKey);
            await _cache.DeleteAsync(partitionKey, rowKey);
            return result;
        }

        public async Task<bool> DeleteIfExistAsync(string partitionKey, string rowKey)
        {
            try
            {
                await DeleteAsync(partitionKey, rowKey);
                await _cache.DeleteAsync(partitionKey, rowKey);
            }
            catch (StorageException ex)
            {
                if (ex.RequestInformation.HttpStatusCode == 404)
                    return false;

                throw;
            }

            return true;
        }

        public async Task DeleteAsync(IEnumerable<T> items)
        {
            await _table.DeleteAsync(items);
            await _cache.DeleteAsync(items);
        }

        public async Task<bool> CreateIfNotExistsAsync(T item)
        {
            var res = await _table.CreateIfNotExistsAsync(item);
            await _cache.CreateIfNotExistsAsync(item);

            return res;
        }

        public bool RecordExists(T item)
        {
            return _table.RecordExists(item);
        }

        public Task<bool> RecordExistsAsync(T item)
        {
            return _table.RecordExistsAsync(item);
        }

        public async Task DoBatchAsync(TableBatchOperation batch)
        {
            await _table.DoBatchAsync(batch);
            await _cache.DoBatchAsync(batch);
        }

        public Task<PagedItems<T>> ExecuteQueryWithPaginationAsync(TableQuery<T> query,
            AzurePagingInfo azurePagingInfo) => _table.ExecuteQueryWithPaginationAsync(query, azurePagingInfo);

        T INoSQLTableStorage<T>.this[string partition, string row] => _cache[partition, row];

        IEnumerable<T> INoSQLTableStorage<T>.this[string partition] => _cache[partition];

        public Task<T> GetDataAsync(string partition, string row) => _cache.GetDataAsync(partition, row);

        public Task<IList<T>> GetDataAsync(Func<T, bool> filter = null) => _cache.GetDataAsync(filter);

        public Task<IEnumerable<T>> GetDataAsync(string partitionKey, IEnumerable<string> rowKeys, int pieceSize = 100,
            Func<T, bool> filter = null)
            => _cache.GetDataAsync(partitionKey, rowKeys, pieceSize, filter);

        public Task<IEnumerable<T>> GetDataAsync(IEnumerable<string> partitionKeys, int pieceSize = 100,
            Func<T, bool> filter = null)
            => _cache.GetDataAsync(partitionKeys, pieceSize, filter);


        public Task<IEnumerable<T>> GetDataAsync(IEnumerable<Tuple<string, string>> keys, int pieceSize = 100,
            Func<T, bool> filter = null)
            => _cache.GetDataAsync(keys, pieceSize, filter);

        public async Task<T> GetTopRecordAsync(string partition) => await _cache.GetTopRecordAsync(partition);

        public async Task<IEnumerable<T>> GetTopRecordsAsync(string partition, int n)
            => await _cache.GetTopRecordsAsync(partition, n);

        public Task GetDataByChunksAsync(Func<IEnumerable<T>, Task> chunks)
            => _cache.GetDataByChunksAsync(chunks);

        public Task GetDataByChunksAsync(TableQuery<T> rangeQuery, Func<IEnumerable<T>, Task> chunks) =>
            _table.GetDataByChunksAsync(rangeQuery, chunks);

        public Task GetDataByChunksAsync(Action<IEnumerable<T>> chunks)
            => _cache.GetDataByChunksAsync(chunks);

        public Task GetDataByChunksAsync(TableQuery<T> rangeQuery, Action<IEnumerable<T>> chunks) =>
            _table.GetDataByChunksAsync(rangeQuery, chunks);

        public Task GetDataByChunksAsync(string partitionKey, Action<IEnumerable<T>> chunks)
            => _cache.GetDataByChunksAsync(partitionKey, chunks);

        public Task ScanDataAsync(string partitionKey, Func<IEnumerable<T>, Task> chunk)
            => _cache.ScanDataAsync(partitionKey, chunk);

        public Task ScanDataAsync(TableQuery<T> rangeQuery, Func<IEnumerable<T>, Task> chunk)
        {
            throw new NotImplementedException();
        }

        public Task<T> FirstOrNullViaScanAsync(string partitionKey, Func<IEnumerable<T>, T> dataToSearch)
            => _cache.FirstOrNullViaScanAsync(partitionKey, dataToSearch);

        public Task<IEnumerable<T>> GetDataAsync(string partition, Func<T, bool> filter = null)
            => _cache.GetDataAsync(partition, filter);

        public Task<IEnumerable<T>> GetDataRowKeysOnlyAsync(IEnumerable<string> rowKeys)
            => _cache.GetDataRowKeysOnlyAsync(rowKeys);

        public Task<IEnumerable<T>> WhereAsyncc(TableQuery<T> rangeQuery, Func<T, Task<bool>> filter = null)
            => _table.WhereAsyncc(rangeQuery, filter);

        public Task<IEnumerable<T>> WhereAsync(TableQuery<T> rangeQuery, Func<T, bool> filter = null)
            => _table.WhereAsync(rangeQuery, filter);

        public Task ExecuteAsync(TableQuery<T> rangeQuery, Action<IEnumerable<T>> yieldResult, Func<bool> stopCondition)
            => _table.ExecuteAsync(rangeQuery, yieldResult, stopCondition);


        public IEnumerator<T> GetEnumerator()
            => _cache.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator()
            => _cache.GetEnumerator();

        private void Init()
        {
            // Вычитаем вообще все элементы в кэш
            Task.WhenAll(_cache.InsertAsync(_table));
        }

        public IEnumerable<T> GetData(Func<T, bool> filter = null) => _cache.GetData(filter);

        public IEnumerable<T> GetData(string partitionKey, Func<T, bool> filter = null)
            => _cache.GetData(partitionKey, filter);
    }
}