using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using Lykke.AzureStorage.Tables.Paging;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureStorage.Tables.Decorators
{
    /// <summary>
    /// Decorator, which adds reloading ConnectionString on authenticate failure to operations of <see cref="INoSQLTableStorage{T}"/> implementation
    /// </summary>
    internal class ReloadingConnectionStringOnFailureAzureTableStorageDecorator<TEntity> : ReloadingOnFailureDecoratorBase<INoSQLTableStorage<TEntity>>, INoSQLTableStorage<TEntity> 
        where TEntity : ITableEntity, new()
    {
        public string Name => Wrap(x => x.Name);

        protected override Func<Task<INoSQLTableStorage<TEntity>>> MakeStorage { get; }

        public ReloadingConnectionStringOnFailureAzureTableStorageDecorator(Func<Task<INoSQLTableStorage<TEntity>>> makeStorage)
        {
            MakeStorage = makeStorage;
        }

        public IEnumerator<TEntity> GetEnumerator()
            => Wrap(x => x.GetEnumerator());

        IEnumerator IEnumerable.GetEnumerator()
            => GetEnumerator();

        TEntity INoSQLTableStorage<TEntity>.this[string partition, string row]
            => Wrap(x => x[partition, row]);

        IEnumerable<TEntity> INoSQLTableStorage<TEntity>.this[string partition]
            => Wrap(x => x[partition]);

        public Task InsertAsync(TEntity item, params int[] notLogCodes)
            => WrapAsync(x => x.InsertAsync(item, notLogCodes));

        public Task InsertAsync(IEnumerable<TEntity> items)
            => WrapAsync(x => x.InsertAsync(items));

        public Task InsertOrMergeAsync(TEntity item)
            => WrapAsync(x => x.InsertOrMergeAsync(item));

        public Task InsertOrMergeBatchAsync(IEnumerable<TEntity> items)
            => WrapAsync(x => x.InsertOrMergeBatchAsync(items));

        public Task<TEntity> ReplaceAsync(string partitionKey, string rowKey, Func<TEntity, TEntity> item)
            => WrapAsync(x => x.ReplaceAsync(partitionKey, rowKey, item));

        public Task<TEntity> MergeAsync(string partitionKey, string rowKey, Func<TEntity, TEntity> item)
            => WrapAsync(x => x.MergeAsync(partitionKey, rowKey, item));

        public Task InsertOrReplaceBatchAsync(IEnumerable<TEntity> entities)
            => WrapAsync(x => x.InsertOrReplaceBatchAsync(entities));

        public Task InsertOrReplaceAsync(TEntity item)
            => WrapAsync(x => x.InsertOrReplaceAsync(item));

        public Task InsertOrReplaceAsync(IEnumerable<TEntity> items)
            => WrapAsync(x => x.InsertOrReplaceAsync(items));

        public Task DeleteAsync(TEntity item)
            => WrapAsync(x => x.DeleteAsync(item));

        public Task<TEntity> DeleteAsync(string partitionKey, string rowKey)
            => WrapAsync(x => x.DeleteAsync(partitionKey, rowKey));

        public Task<bool> DeleteIfExistAsync(string partitionKey, string rowKey)
            => WrapAsync(x => x.DeleteIfExistAsync(partitionKey, rowKey));

        public Task DeleteAsync(IEnumerable<TEntity> items)
            => WrapAsync(x => x.DeleteAsync(items));

        public Task<bool> CreateIfNotExistsAsync(TEntity item)
            => WrapAsync(x => x.CreateIfNotExistsAsync(item));

        public bool RecordExists(TEntity item)
            => Wrap(x => x.RecordExists(item));

        public Task<bool> RecordExistsAsync(TEntity item)
            => WrapAsync(x => x.RecordExistsAsync(item));

        public Task<TEntity> GetDataAsync(string partition, string row)
            => WrapAsync(x => x.GetDataAsync(partition, row));

        public Task<IList<TEntity>> GetDataAsync(Func<TEntity, bool> filter = null)
            => WrapAsync(x => x.GetDataAsync(filter));

        public Task<IEnumerable<TEntity>> GetDataAsync(string partitionKey, IEnumerable<string> rowKeys, int pieceSize = 100, Func<TEntity, bool> filter = null)
            => WrapAsync(x => x.GetDataAsync(partitionKey, rowKeys, pieceSize, filter));

        public Task<IEnumerable<TEntity>> GetDataAsync(IEnumerable<string> partitionKeys, int pieceSize = 100, Func<TEntity, bool> filter = null)
            => WrapAsync(x => x.GetDataAsync(partitionKeys, pieceSize, filter));

        public Task<IEnumerable<TEntity>> GetDataAsync(IEnumerable<Tuple<string, string>> keys, int pieceSize = 100, Func<TEntity, bool> filter = null)
            => WrapAsync(x => x.GetDataAsync(keys, pieceSize, filter));

        public Task GetDataByChunksAsync(Func<IEnumerable<TEntity>, Task> chunks)
            => WrapAsync(x => x.GetDataByChunksAsync(chunks));

        public Task GetDataByChunksAsync(TableQuery<TEntity> rangeQuery, Func<IEnumerable<TEntity>, Task> chunks)
            => WrapAsync(x => x.GetDataByChunksAsync(rangeQuery, chunks));

        public Task GetDataByChunksAsync(Action<IEnumerable<TEntity>> chunks)
            => WrapAsync(x => x.GetDataByChunksAsync(chunks));

        public Task GetDataByChunksAsync(TableQuery<TEntity> rangeQuery, Action<IEnumerable<TEntity>> chunks)
            => WrapAsync(x => x.GetDataByChunksAsync(rangeQuery, chunks));

        public Task GetDataByChunksAsync(string partitionKey, Action<IEnumerable<TEntity>> chunks)
            => WrapAsync(x => x.GetDataByChunksAsync(partitionKey, chunks));

        public Task ScanDataAsync(string partitionKey, Func<IEnumerable<TEntity>, Task> chunk)
            => WrapAsync(x => x.ScanDataAsync(partitionKey, chunk));

        public Task ScanDataAsync(TableQuery<TEntity> rangeQuery, Func<IEnumerable<TEntity>, Task> chunk)
            => WrapAsync(x => x.ScanDataAsync(rangeQuery, chunk));

        public Task<TEntity> FirstOrNullViaScanAsync(string partitionKey, Func<IEnumerable<TEntity>, TEntity> dataToSearch)
            => WrapAsync(x => x.FirstOrNullViaScanAsync(partitionKey, dataToSearch));

        public Task<IEnumerable<TEntity>> GetDataAsync(string partition, Func<TEntity, bool> filter = null)
            => WrapAsync(x => x.GetDataAsync(partition, filter));

        public Task<TEntity> GetTopRecordAsync(string partition)
            => WrapAsync(x => x.GetTopRecordAsync(partition));

        public Task<IEnumerable<TEntity>> GetTopRecordsAsync(string partition, int n)
            => WrapAsync(x => x.GetTopRecordsAsync(partition, n));

        public Task<IEnumerable<TEntity>> GetDataRowKeysOnlyAsync(IEnumerable<string> rowKeys)
            => WrapAsync(x => x.GetDataRowKeysOnlyAsync(rowKeys));

        public Task<IEnumerable<TEntity>> WhereAsyncc(TableQuery<TEntity> rangeQuery, Func<TEntity, Task<bool>> filter = null)
            => WrapAsync(x => x.WhereAsyncc(rangeQuery, filter));

        public Task<IEnumerable<TEntity>> WhereAsync(TableQuery<TEntity> rangeQuery, Func<TEntity, bool> filter = null)
            => WrapAsync(x => x.WhereAsync(rangeQuery, filter));

        public Task ExecuteAsync(TableQuery<TEntity> rangeQuery, Action<IEnumerable<TEntity>> yieldResult, Func<bool> stopCondition = null)
            => WrapAsync(x => x.ExecuteAsync(rangeQuery, yieldResult, stopCondition));

        public Task DoBatchAsync(TableBatchOperation batch)
            => WrapAsync(x => x.DoBatchAsync(batch));

        public Task<IPagedResult<TEntity>> ExecuteQueryWithPaginationAsync(TableQuery<TEntity> query,
            PagingInfo pagingInfo)
            => WrapAsync(x => x.ExecuteQueryWithPaginationAsync(query, pagingInfo));
    }
}