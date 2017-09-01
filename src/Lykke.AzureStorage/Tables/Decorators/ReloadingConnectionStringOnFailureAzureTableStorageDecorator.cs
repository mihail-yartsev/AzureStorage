using System;
using System.Collections;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureStorage.Tables.Decorators
{
    /// <summary>
    /// Decorator, which adds reloading ConnectionString on authenticate failure to operations of <see cref="INoSQLTableStorage{T}"/> implementation
    /// </summary>
    public class ReloadingConnectionStringOnFailureAzureTableStorageDecorator<TEntity> : INoSQLTableStorage<TEntity> 
        where TEntity : ITableEntity, new()
    {
        private readonly Func<Task<INoSQLTableStorage<TEntity>>> _makeStorage;

        public ReloadingConnectionStringOnFailureAzureTableStorageDecorator(Func<Task<INoSQLTableStorage<TEntity>>> makeStorage)
        {
            _makeStorage = makeStorage;
        }

        private bool CheckException(Exception ex)
        {
            if (ex is StorageException storageException)
            {
                var statusCode = (HttpStatusCode)storageException.RequestInformation.HttpStatusCode;
                return statusCode == HttpStatusCode.Forbidden;
            }

            return false;
        }

        private readonly ReaderWriterLockSlim _sync = new ReaderWriterLockSlim();
        private Task<INoSQLTableStorage<TEntity>> _currentTask;

        private Task<INoSQLTableStorage<TEntity>> GetStorageAsync(bool reload = false)
        {
            bool CheckCurrentTask() => _currentTask != null && !(_currentTask.IsCompleted && reload);

            try
            {
                _sync.EnterReadLock();

                if (CheckCurrentTask())
                {
                    return _currentTask;
                }
            }
            finally
            {
                _sync.ExitReadLock();
            }

            try
            {
                _sync.EnterWriteLock();

                // double check
                if (CheckCurrentTask())
                {
                    return _currentTask;
                }

                return _currentTask = _makeStorage();
            }
            finally
            {
                _sync.ExitWriteLock();
            }
        }

        private T Wrap<T>(Func<INoSQLTableStorage<TEntity>, T> func)
        {
            try
            {
                return func(GetStorageAsync().Result);
            }
            catch (Exception ex)
            {
                if (!CheckException(ex))
                {
                    throw;
                }
            }

            return func(GetStorageAsync(reload: true).Result);
        }

        private async Task WrapAsync(Func<INoSQLTableStorage<TEntity>, Task> func)
        {
            try
            {
                await func(await GetStorageAsync());
                return;
            }
            catch (Exception ex)
            {
                if (!CheckException(ex))
                {
                    throw;
                }
            }

            await func(await GetStorageAsync(reload: true));
        }

        private async Task<T> WrapAsync<T>(Func<INoSQLTableStorage<TEntity>, Task<T>> func)
        {
            try
            {
                return await func(await GetStorageAsync());
            }
            catch (Exception ex)
            {
                if (!CheckException(ex))
                {
                    throw;
                }
            }

            return await func(await GetStorageAsync(reload: true));
        }

        public IEnumerator<TEntity> GetEnumerator() => Wrap(x => x.GetEnumerator());

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        TEntity INoSQLTableStorage<TEntity>.this[string partition, string row] => Wrap(x => x[partition, row]);

        IEnumerable<TEntity> INoSQLTableStorage<TEntity>.this[string partition] => Wrap(x => x[partition]);

        public Task InsertAsync(TEntity item, params int[] notLogCodes)
        {
            return WrapAsync(x => x.InsertAsync(item, notLogCodes));
        }

        public Task InsertAsync(IEnumerable<TEntity> items)
        {
            return WrapAsync(x => x.InsertAsync(items));
        }

        public Task InsertOrMergeAsync(TEntity item)
        {
            return WrapAsync(x => x.InsertOrMergeAsync(item));
        }

        public Task InsertOrMergeBatchAsync(IEnumerable<TEntity> items)
        {
            return WrapAsync(x => x.InsertOrMergeBatchAsync(items));
        }

        public Task<TEntity> ReplaceAsync(string partitionKey, string rowKey, Func<TEntity, TEntity> item)
        {
            return WrapAsync(x => x.ReplaceAsync(partitionKey, rowKey, item));
        }

        public Task<TEntity> MergeAsync(string partitionKey, string rowKey, Func<TEntity, TEntity> item)
        {
            return WrapAsync(x => x.MergeAsync(partitionKey, rowKey, item));
        }

        public Task InsertOrReplaceBatchAsync(IEnumerable<TEntity> entities)
        {
            return WrapAsync(x => x.InsertOrReplaceBatchAsync(entities));
        }

        public Task InsertOrReplaceAsync(TEntity item)
        {
            return WrapAsync(x => x.InsertOrReplaceAsync(item));
        }

        public Task InsertOrReplaceAsync(IEnumerable<TEntity> items)
        {
            return WrapAsync(x => x.InsertOrReplaceAsync(items));
        }

        public Task DeleteAsync(TEntity item)
        {
            return WrapAsync(x => x.DeleteAsync(item));
        }

        public Task<TEntity> DeleteAsync(string partitionKey, string rowKey)
        {
            return WrapAsync(x => x.DeleteAsync(partitionKey, rowKey));
        }

        public Task<bool> DeleteIfExistAsync(string partitionKey, string rowKey)
        {
            return WrapAsync(x => x.DeleteIfExistAsync(partitionKey, rowKey));
        }

        public Task DeleteAsync(IEnumerable<TEntity> items)
        {
            return WrapAsync(x => x.DeleteAsync(items));
        }

        public Task<bool> CreateIfNotExistsAsync(TEntity item)
        {
            return WrapAsync(x => x.CreateIfNotExistsAsync(item));
        }

        public bool RecordExists(TEntity item)
        {
            return Wrap(x => x.RecordExists(item));
        }

        public Task<TEntity> GetDataAsync(string partition, string row)
        {
            return WrapAsync(x => x.GetDataAsync(partition, row));
        }

        public Task<IList<TEntity>> GetDataAsync(Func<TEntity, bool> filter = null)
        {
            return WrapAsync(x => x.GetDataAsync(filter));
        }

        public Task<IEnumerable<TEntity>> GetDataAsync(string partitionKey, IEnumerable<string> rowKeys, int pieceSize = 100, Func<TEntity, bool> filter = null)
        {
            return WrapAsync(x => x.GetDataAsync(partitionKey, rowKeys, pieceSize, filter));
        }

        public Task<IEnumerable<TEntity>> GetDataAsync(IEnumerable<string> partitionKeys, int pieceSize = 100, Func<TEntity, bool> filter = null)
        {
            return WrapAsync(x => x.GetDataAsync(partitionKeys, pieceSize, filter));
        }

        public Task<IEnumerable<TEntity>> GetDataAsync(IEnumerable<Tuple<string, string>> keys, int pieceSize = 100, Func<TEntity, bool> filter = null)
        {
            return WrapAsync(x => x.GetDataAsync(keys, pieceSize, filter));
        }

        public Task GetDataByChunksAsync(Func<IEnumerable<TEntity>, Task> chunks)
        {
            return WrapAsync(x => x.GetDataByChunksAsync(chunks));
        }

        public Task GetDataByChunksAsync(Action<IEnumerable<TEntity>> chunks)
        {
            return WrapAsync(x => x.GetDataByChunksAsync(chunks));
        }

        public Task GetDataByChunksAsync(string partitionKey, Action<IEnumerable<TEntity>> chunks)
        {
            return WrapAsync(x => x.GetDataByChunksAsync(partitionKey, chunks));
        }

        public Task ScanDataAsync(string partitionKey, Func<IEnumerable<TEntity>, Task> chunk)
        {
            return WrapAsync(x => x.ScanDataAsync(partitionKey, chunk));
        }

        public Task ScanDataAsync(TableQuery<TEntity> rangeQuery, Func<IEnumerable<TEntity>, Task> chunk)
        {
            return WrapAsync(x => x.ScanDataAsync(rangeQuery, chunk));
        }

        public Task<TEntity> FirstOrNullViaScanAsync(string partitionKey, Func<IEnumerable<TEntity>, TEntity> dataToSearch)
        {
            return WrapAsync(x => x.FirstOrNullViaScanAsync(partitionKey, dataToSearch));
        }

        public Task<IEnumerable<TEntity>> GetDataAsync(string partition, Func<TEntity, bool> filter = null)
        {
            return WrapAsync(x => x.GetDataAsync(partition, filter));
        }

        public Task<TEntity> GetTopRecordAsync(string partition)
        {
            return WrapAsync(x => x.GetTopRecordAsync(partition));
        }

        public Task<IEnumerable<TEntity>> GetTopRecordsAsync(string partition, int n)
        {
            return WrapAsync(x => x.GetTopRecordsAsync(partition, n));
        }

        public Task<IEnumerable<TEntity>> GetDataRowKeysOnlyAsync(IEnumerable<string> rowKeys)
        {
            return WrapAsync(x => x.GetDataRowKeysOnlyAsync(rowKeys));
        }

        public Task<IEnumerable<TEntity>> WhereAsyncc(TableQuery<TEntity> rangeQuery, Func<TEntity, Task<bool>> filter = null)
        {
            return WrapAsync(x => x.WhereAsyncc(rangeQuery, filter));
        }

        public Task<IEnumerable<TEntity>> WhereAsync(TableQuery<TEntity> rangeQuery, Func<TEntity, bool> filter = null)
        {
            return WrapAsync(x => x.WhereAsync(rangeQuery, filter));
        }

        public Task ExecuteAsync(TableQuery<TEntity> rangeQuery, Action<IEnumerable<TEntity>> yieldResult, Func<bool> stopCondition = null)
        {
            return WrapAsync(x => x.ExecuteAsync(rangeQuery, yieldResult, stopCondition));
        }

        public Task DoBatchAsync(TableBatchOperation batch)
        {
            return WrapAsync(x => x.DoBatchAsync(batch));
        }
    }   
}