using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using Common.Extensions;
using Common.Log;
using Lykke.AzureStorage.Tables.Paging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace AzureStorage.Tables.Decorators
{
    /// <summary>
    /// Decorator which logs Azure Table Storage exceptions
    /// </summary>
    internal class LogExceptionsAzureTableStorageDecorator<TEntity> : INoSQLTableStorage<TEntity> 
        where TEntity : ITableEntity, new()
    {
        public string Name => _impl.Name;

        private readonly INoSQLTableStorage<TEntity> _impl;
        private readonly ILog _log;
        private readonly JsonSerializerSettings _dumpSettings;

        public LogExceptionsAzureTableStorageDecorator(INoSQLTableStorage<TEntity> impl, ILog log)
        {
            _impl = impl;
            _log = log;

            _dumpSettings = new JsonSerializerSettings
            {
                DateTimeZoneHandling = DateTimeZoneHandling.Utc,
                Formatting = Formatting.Indented,
                Culture = CultureInfo.InvariantCulture
            };
        }

        #region INoSqlTableStorage{TEntity} decoration

        public IEnumerator<TEntity> GetEnumerator() 
            => Wrap(() => _impl.GetEnumerator(), nameof(GetEnumerator));

        IEnumerator IEnumerable.GetEnumerator() 
            => Wrap(() => ((IEnumerable) _impl).GetEnumerator(), nameof(GetEnumerator));

        TEntity INoSQLTableStorage<TEntity>.this[string partition, string row] 
            => Wrap(() => _impl[partition, row], "[partition, row]", new {partition, row});

        IEnumerable<TEntity> INoSQLTableStorage<TEntity>.this[string partition] 
            => Wrap(() => _impl[partition], "[partition]", new{partition});

        public Task InsertAsync(TEntity item, params int[] notLogCodes) 
            => WrapAsync(() => _impl.InsertAsync(item, notLogCodes), nameof(InsertAsync), item, notLogCodes);

        public Task InsertAsync(IEnumerable<TEntity> items) 
            => WrapAsync(() => _impl.InsertAsync(items), nameof(InsertAsync), items);

        public Task InsertOrMergeAsync(TEntity item) 
            => WrapAsync(() => _impl.InsertOrMergeAsync(item), nameof(InsertOrMergeAsync), item);

        public Task InsertOrMergeBatchAsync(IEnumerable<TEntity> items) 
            => WrapAsync(() => _impl.InsertOrMergeBatchAsync(items), nameof(InsertOrMergeBatchAsync), items);

        public Task<TEntity> ReplaceAsync(string partitionKey, string rowKey, Func<TEntity, TEntity> item)
        {
            object result = "Not read";

            return WrapAsync(() => _impl.ReplaceAsync(partitionKey, rowKey, entity =>
                {
                    var replacedItem = item(entity);
                    result = replacedItem;
                    return replacedItem;
                }),
                nameof(ReplaceAsync),
                result);
        }

        public Task<TEntity> MergeAsync(string partitionKey, string rowKey, Func<TEntity, TEntity> item)
        {
            object result = "Not read";

            return WrapAsync(() => _impl.MergeAsync(partitionKey, rowKey, entity =>
                {
                    var replacedItem = item(entity);
                    result = replacedItem;
                    return replacedItem;
                }),
                nameof(MergeAsync),
                result);
        }

        public Task InsertOrReplaceBatchAsync(IEnumerable<TEntity> entities) 
            => WrapAsync(() => _impl.InsertOrReplaceBatchAsync(entities), nameof(InsertOrReplaceBatchAsync), entities);

        public Task InsertOrReplaceAsync(TEntity item) 
            => WrapAsync(() => _impl.InsertOrReplaceAsync(item), nameof(InsertOrReplaceAsync), item);

        public Task InsertOrReplaceAsync(IEnumerable<TEntity> items) 
            => WrapAsync(() => _impl.InsertOrReplaceAsync(items), nameof(InsertOrReplaceAsync), items);

        public Task DeleteAsync(TEntity item) 
            => WrapAsync(() => _impl.DeleteAsync(item), nameof(DeleteAsync), item);

        public Task<TEntity> DeleteAsync(string partitionKey, string rowKey) 
            => WrapAsync(() => _impl.DeleteAsync(partitionKey, rowKey), nameof(DeleteAsync), new {partitionKey, rowKey});

        public Task<bool> DeleteIfExistAsync(string partitionKey, string rowKey) 
            => WrapAsync(() => _impl.DeleteIfExistAsync(partitionKey, rowKey), nameof(DeleteIfExistAsync), new {partitionKey, rowKey});

        public Task DeleteAsync(IEnumerable<TEntity> items) 
            => WrapAsync(() => _impl.DeleteAsync(items), nameof(DeleteAsync), items);

        public Task<bool> CreateIfNotExistsAsync(TEntity item) 
            => WrapAsync(() => _impl.CreateIfNotExistsAsync(item), nameof(CreateIfNotExistsAsync), item);

        public bool RecordExists(TEntity item) 
            => Wrap(() => _impl.RecordExists(item), nameof(RecordExists), item);

        public Task<bool> RecordExistsAsync(TEntity item) 
            => WrapAsync(() => _impl.RecordExistsAsync(item), nameof(RecordExistsAsync), item);

        public Task<TEntity> GetDataAsync(string partition, string row) 
            => WrapAsync(() => _impl.GetDataAsync(partition, row), nameof(GetDataAsync), new {partition, row});

        public Task<IList<TEntity>> GetDataAsync(Func<TEntity, bool> filter = null) 
            => WrapAsync(() => _impl.GetDataAsync(filter), nameof(GetDataAsync));

        public Task<IEnumerable<TEntity>> GetDataAsync(string partitionKey, IEnumerable<string> rowKeys, int pieceSize = 100, Func<TEntity, bool> filter = null) 
            => WrapAsync(() => _impl.GetDataAsync(partitionKey, rowKeys, pieceSize, filter), nameof(GetDataAsync), new {partitionKey, rowKeys, pieceSize});

        public Task<IEnumerable<TEntity>> GetDataAsync(IEnumerable<string> partitionKeys, int pieceSize = 100, Func<TEntity, bool> filter = null) 
            => WrapAsync(() => _impl.GetDataAsync(partitionKeys, pieceSize, filter), nameof(GetDataAsync), new {partitionKeys, pieceSize});

        public Task<IEnumerable<TEntity>> GetDataAsync(IEnumerable<Tuple<string, string>> keys, int pieceSize = 100, Func<TEntity, bool> filter = null) 
            => WrapAsync(() => _impl.GetDataAsync(keys, pieceSize, filter), nameof(GetDataAsync), new {keys, pieceSize});

        public Task GetDataByChunksAsync(Func<IEnumerable<TEntity>, Task> chunks) 
            => WrapAsync(() => _impl.GetDataByChunksAsync(chunks), nameof(GetDataByChunksAsync));

        public Task GetDataByChunksAsync(TableQuery<TEntity> rangeQuery, Func<IEnumerable<TEntity>, Task> chunks) 
            => WrapAsync(() =>_impl.GetDataByChunksAsync(rangeQuery, chunks), nameof(GetDataByChunksAsync));

        public Task GetDataByChunksAsync(Action<IEnumerable<TEntity>> chunks) 
            => WrapAsync(() => _impl.GetDataByChunksAsync(chunks), nameof(GetDataByChunksAsync));

        public Task GetDataByChunksAsync(TableQuery<TEntity> rangeQuery, Action<IEnumerable<TEntity>> chunks)
            => WrapAsync(() => _impl.GetDataByChunksAsync(rangeQuery, chunks), nameof(GetDataByChunksAsync));

        public Task GetDataByChunksAsync(string partitionKey, Action<IEnumerable<TEntity>> chunks) 
            => WrapAsync(() => _impl.GetDataByChunksAsync(partitionKey, chunks), nameof(GetDataByChunksAsync), new {partitionKey});

        public Task ScanDataAsync(string partitionKey, Func<IEnumerable<TEntity>, Task> chunk) 
            => WrapAsync(() => _impl.ScanDataAsync(partitionKey, chunk), nameof(ScanDataAsync), new {partitionKey});

        public Task ScanDataAsync(TableQuery<TEntity> rangeQuery, Func<IEnumerable<TEntity>, Task> chunk) 
            => WrapAsync(() => _impl.ScanDataAsync(rangeQuery, chunk), nameof(ScanDataAsync), rangeQuery.FilterString);

        public Task<TEntity> FirstOrNullViaScanAsync(string partitionKey, Func<IEnumerable<TEntity>, TEntity> dataToSearch) 
            => WrapAsync(() => _impl.FirstOrNullViaScanAsync(partitionKey, dataToSearch), nameof(FirstOrNullViaScanAsync), new {partitionKey});

        public Task<IEnumerable<TEntity>> GetDataAsync(string partition, Func<TEntity, bool> filter = null) 
            => WrapAsync(() => _impl.GetDataAsync(partition, filter), nameof(GetDataAsync), new {partition});

        public Task<TEntity> GetTopRecordAsync(string partition) 
            => WrapAsync(() => _impl.GetTopRecordAsync(partition), nameof(GetTopRecordAsync), new {partition});

        public Task<IEnumerable<TEntity>> GetTopRecordsAsync(string partition, int n) 
            => WrapAsync(() => _impl.GetTopRecordsAsync(partition, n), nameof(GetTopRecordsAsync), new {partition, n});

        public Task<IEnumerable<TEntity>> GetDataRowKeysOnlyAsync(IEnumerable<string> rowKeys) 
            => WrapAsync(() => _impl.GetDataRowKeysOnlyAsync(rowKeys), nameof(GetDataRowKeysOnlyAsync), rowKeys);

        public Task<IEnumerable<TEntity>> WhereAsyncc(TableQuery<TEntity> rangeQuery, Func<TEntity, Task<bool>> filter = null) 
            => WrapAsync(() => _impl.WhereAsyncc(rangeQuery, filter), nameof(WhereAsyncc), rangeQuery.FilterString);

        public Task<IEnumerable<TEntity>> WhereAsync(TableQuery<TEntity> rangeQuery, Func<TEntity, bool> filter = null) 
            => WrapAsync(() => _impl.WhereAsync(rangeQuery, filter), nameof(WhereAsync), rangeQuery.FilterString);

        public Task ExecuteAsync(TableQuery<TEntity> rangeQuery, Action<IEnumerable<TEntity>> yieldResult, Func<bool> stopCondition = null) 
            => WrapAsync(() => _impl.ExecuteAsync(rangeQuery, yieldResult, stopCondition), nameof(ExecuteAsync), rangeQuery.FilterString);

        public Task DoBatchAsync(TableBatchOperation batch) 
            => WrapAsync(() => _impl.DoBatchAsync(batch), nameof(DoBatchAsync));

        public Task<PagedItems<TEntity>> ExecuteQueryWithPaginationAsync(TableQuery<TEntity> query,
            AzurePagingInfo azurePagingInfo)
            => WrapAsync(() => _impl.ExecuteQueryWithPaginationAsync(query, azurePagingInfo),
                nameof(ExecuteQueryWithPaginationAsync));

        #endregion


        #region Private members

        private async Task<TResult> WrapAsync<TResult>(Func<Task<TResult>> func, string process, object context = null, IEnumerable<int> notLogAzureCodes = null)
        {
            try
            {
                return await func();
            }
            catch (Exception ex)
            {
                await HandleExceptionAsync(ex, process, context, notLogAzureCodes);
                throw;
            }
        }

        private async Task WrapAsync(Func<Task> func, string process, object context = null, IEnumerable<int> notLogAzureCodes = null)
        {
            try
            {
                await func();
            }
            catch (Exception ex)
            {
                await HandleExceptionAsync(ex, process, context, notLogAzureCodes);
                throw;
            }
        }

        private TResult Wrap<TResult>(Func<TResult> func, string process, object context = null, IEnumerable<int> notLogAzureCodes = null)
        {
            try
            {
                return func();
            }
            catch (Exception ex)
            {
                HandleExceptionAsync(ex, process, context, notLogAzureCodes).RunSync();
                throw;
            }
        }

        private async Task HandleExceptionAsync(Exception exception, string process, object context, IEnumerable<int> notLogAzureCodes)
        {
            if (exception is TaskCanceledException)
            {
                await _log.WriteWarningAsync(
                    $"Table storage: {Name}",
                    process,
                    Dump(context),
                    exception.GetBaseException().Message);
            }
            else if (exception is StorageException storageException)
            {
                if (notLogAzureCodes == null || !storageException.HandleStorageException(notLogAzureCodes))
                {
                    await _log.WriteErrorAsync(
                        $"Table storage: {Name}",
                        process,
                        Dump(context),
                        exception);
                }
            }
            else
            {
                await _log.WriteErrorAsync(
                    $"Table storage: {Name}",
                    process,
                    Dump(context),
                    exception);
            }
        }

        private string Dump(object context)
        {
            return JsonConvert.SerializeObject(context, _dumpSettings);
        }
        
        #endregion
    }
}