using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using Lykke.AzureStorage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureStorage.Tables.Decorators
{
    /// <summary>
    /// Decorator, which adds retries functionality to <see cref="INoSQLTableStorage{T}"/> implementation
    /// </summary>
    public class RetryOnFailureAzureTableStorageDecorator<TEntity> : INoSQLTableStorage<TEntity> 
        where TEntity : ITableEntity, new()
    {
        private readonly INoSQLTableStorage<TEntity> _impl;
        private readonly int _onModificationsRetryCount;
        private readonly int _onGettingRetryCount;

        public RetryOnFailureAzureTableStorageDecorator(INoSQLTableStorage<TEntity> impl, int onModificationsRetryCount = 10, int onGettingRetryCount = 1)
        {
            _impl = impl ?? throw new ArgumentNullException(nameof(impl));

            if (onModificationsRetryCount < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(onModificationsRetryCount), onModificationsRetryCount, "Value should be greater than 0");
            }

            if (onGettingRetryCount < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(onGettingRetryCount), onGettingRetryCount, "Value should be greater than 0");
            }

            _onModificationsRetryCount = onModificationsRetryCount;
            _onGettingRetryCount = onGettingRetryCount;
        }

        public IEnumerator<TEntity> GetEnumerator() => RetryUtils.Retry(_impl.GetEnumerator, _onGettingRetryCount);

        IEnumerator IEnumerable.GetEnumerator() => RetryUtils.Retry(((IEnumerable)_impl).GetEnumerator, _onGettingRetryCount);

        TEntity INoSQLTableStorage<TEntity>.this[string partition, string row] => RetryUtils.Retry(() => _impl[partition, row], _onGettingRetryCount);

        IEnumerable<TEntity> INoSQLTableStorage<TEntity>.this[string partition] => RetryUtils.Retry(() => _impl[partition], _onGettingRetryCount);

        public async Task InsertAsync(TEntity item, params int[] notLogCodes)
        {
            await RetryUtils.RetryAsync(async () => await _impl.InsertAsync(item, notLogCodes), _onModificationsRetryCount);
        }

        public async Task InsertAsync(IEnumerable<TEntity> items)
        {
            await RetryUtils.RetryAsync(async () => await _impl.InsertAsync(items), _onModificationsRetryCount);
        }

        public async Task InsertOrMergeAsync(TEntity item)
        {
            await RetryUtils.RetryAsync(async () => await _impl.InsertOrMergeAsync(item), _onModificationsRetryCount);
        }

        public async Task InsertOrMergeBatchAsync(IEnumerable<TEntity> items)
        {
            await RetryUtils.RetryAsync(async () => await _impl.InsertOrMergeBatchAsync(items), _onModificationsRetryCount);
        }

        public async Task<TEntity> ReplaceAsync(string partitionKey, string rowKey, Func<TEntity, TEntity> item)
        {
            return await RetryUtils.RetryAsync(async () => await _impl.ReplaceAsync(partitionKey, rowKey, item), _onModificationsRetryCount);
        }

        public async Task<TEntity> MergeAsync(string partitionKey, string rowKey, Func<TEntity, TEntity> item)
        {
            return await RetryUtils.RetryAsync(async () => await _impl.MergeAsync(partitionKey, rowKey, item), _onModificationsRetryCount);
        }

        public async Task InsertOrReplaceBatchAsync(IEnumerable<TEntity> entites)
        {
            await RetryUtils.RetryAsync(async () => await _impl.InsertOrReplaceBatchAsync(entites), _onModificationsRetryCount);
        }

        public async Task InsertOrReplaceAsync(TEntity item)
        {
            await RetryUtils.RetryAsync(async () => await _impl.InsertOrReplaceAsync(item), _onModificationsRetryCount);
        }

        public async Task InsertOrReplaceAsync(IEnumerable<TEntity> items)
        {
            await RetryUtils.RetryAsync(async () => await _impl.InsertOrReplaceAsync(items), _onModificationsRetryCount);
        }

        public async Task DeleteAsync(TEntity item)
        {
            await RetryUtils.RetryAsync(async () => await _impl.DeleteAsync(item), _onModificationsRetryCount);
        }

        public async Task<TEntity> DeleteAsync(string partitionKey, string rowKey)
        {
            return await RetryUtils.RetryAsync(async () => await _impl.DeleteAsync(partitionKey, rowKey), _onModificationsRetryCount);
        }

        public async Task<bool> DeleteIfExistAsync(string partitionKey, string rowKey)
        {
            return await RetryUtils.RetryAsync(async () => await _impl.DeleteIfExistAsync(partitionKey, rowKey), _onModificationsRetryCount);
        }

        public async Task DeleteAsync(IEnumerable<TEntity> items)
        {
            await RetryUtils.RetryAsync(async () => await _impl.DeleteAsync(items), _onModificationsRetryCount);
        }

        public async Task<bool> CreateIfNotExistsAsync(TEntity item)
        {
            return await RetryUtils.RetryAsync(async () => await _impl.CreateIfNotExistsAsync(item), _onModificationsRetryCount);
        }

        public bool RecordExists(TEntity item)
        {
            return RetryUtils.Retry(() => _impl.RecordExists(item), _onGettingRetryCount);
        }

        public async Task<TEntity> GetDataAsync(string partition, string row)
        {
            return await RetryUtils.RetryAsync(async () => await _impl.GetDataAsync(partition, row), _onGettingRetryCount);
        }

        public async Task<IList<TEntity>> GetDataAsync(Func<TEntity, bool> filter = null)
        {
            return await RetryUtils.RetryAsync(async () => await _impl.GetDataAsync(filter), _onGettingRetryCount);
        }

        public async Task<IEnumerable<TEntity>> GetDataAsync(string partitionKey, IEnumerable<string> rowKeys, int pieceSize = 100, Func<TEntity, bool> filter = null)
        {
            return await RetryUtils.RetryAsync(async () => await _impl.GetDataAsync(partitionKey, rowKeys, pieceSize, filter), _onGettingRetryCount);
        }

        public async Task<IEnumerable<TEntity>> GetDataAsync(IEnumerable<string> partitionKeys, int pieceSize = 100, Func<TEntity, bool> filter = null)
        {
            return await RetryUtils.RetryAsync(async () => await _impl.GetDataAsync(partitionKeys, pieceSize, filter), _onGettingRetryCount);
        }

        public async Task<IEnumerable<TEntity>> GetDataAsync(IEnumerable<Tuple<string, string>> keys, int pieceSize = 100, Func<TEntity, bool> filter = null)
        {
            return await RetryUtils.RetryAsync(async () => await _impl.GetDataAsync(keys, pieceSize, filter), _onGettingRetryCount);
        }

        public async Task GetDataByChunksAsync(Func<IEnumerable<TEntity>, Task> chunks)
        {
            await RetryUtils.RetryAsync(async () => await _impl.GetDataByChunksAsync(chunks), _onGettingRetryCount);
        }

        public async Task GetDataByChunksAsync(Action<IEnumerable<TEntity>> chunks)
        {
            await RetryUtils.RetryAsync(async () => await _impl.GetDataByChunksAsync(chunks), _onGettingRetryCount);
        }

        public async Task GetDataByChunksAsync(string partitionKey, Action<IEnumerable<TEntity>> chunks)
        {
            await RetryUtils.RetryAsync(async () => await _impl.GetDataByChunksAsync(partitionKey, chunks), _onGettingRetryCount);
        }

        public async Task ScanDataAsync(string partitionKey, Func<IEnumerable<TEntity>, Task> chunk)
        {
            await RetryUtils.RetryAsync(async () => await _impl.ScanDataAsync(partitionKey, chunk), _onGettingRetryCount);
        }

        public async Task ScanDataAsync(TableQuery<TEntity> rangeQuery, Func<IEnumerable<TEntity>, Task> chunk)
        {
            await RetryUtils.RetryAsync(async () => await _impl.ScanDataAsync(rangeQuery, chunk), _onGettingRetryCount);
        }

        public async Task<TEntity> FirstOrNullViaScanAsync(string partitionKey, Func<IEnumerable<TEntity>, TEntity> dataToSearch)
        {
            return await RetryUtils.RetryAsync(async () => await _impl.FirstOrNullViaScanAsync(partitionKey, dataToSearch), _onGettingRetryCount);
        }

        public async Task<IEnumerable<TEntity>> GetDataAsync(string partition, Func<TEntity, bool> filter = null)
        {
            return await RetryUtils.RetryAsync(async () => await _impl.GetDataAsync(partition, filter), _onGettingRetryCount);
        }

        public async Task<TEntity> GetTopRecordAsync(string partition)
        {
            return await RetryUtils.RetryAsync(async () => await _impl.GetTopRecordAsync(partition), _onGettingRetryCount);
        }

        public async Task<IEnumerable<TEntity>> GetTopRecordsAsync(string partition, int n)
        {
            return await RetryUtils.RetryAsync(async () => await _impl.GetTopRecordsAsync(partition, n), _onGettingRetryCount);
        }

        public async Task<IEnumerable<TEntity>> GetDataRowKeysOnlyAsync(IEnumerable<string> rowKeys)
        {
            return await RetryUtils.RetryAsync(async () => await _impl.GetDataRowKeysOnlyAsync(rowKeys), _onGettingRetryCount);
        }

        public async Task<IEnumerable<TEntity>> WhereAsyncc(TableQuery<TEntity> rangeQuery, Func<TEntity, Task<bool>> filter = null)
        {
            return await RetryUtils.RetryAsync(async () => await _impl.WhereAsyncc(rangeQuery, filter), _onGettingRetryCount);
        }

        public async Task<IEnumerable<TEntity>> WhereAsync(TableQuery<TEntity> rangeQuery, Func<TEntity, bool> filter = null)
        {
            return await RetryUtils.RetryAsync(async () => await _impl.WhereAsync(rangeQuery, filter), _onGettingRetryCount);
        }

        public async Task ExecuteAsync(TableQuery<TEntity> rangeQuery, Action<IEnumerable<TEntity>> yieldResult, Func<bool> stopCondition = null)
        {
            await RetryUtils.RetryAsync(async () => await _impl.ExecuteAsync(rangeQuery, yieldResult, stopCondition), _onGettingRetryCount);
        }

        public async Task DoBatchAsync(TableBatchOperation batch)
        {
            await RetryUtils.RetryAsync(async () => await _impl.DoBatchAsync(batch), _onModificationsRetryCount);
        }
    }   
}