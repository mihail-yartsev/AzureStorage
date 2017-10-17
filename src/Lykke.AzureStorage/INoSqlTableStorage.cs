using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using AzureStorage.Tables;
using Lykke.AzureStorage.Tables.Paging;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureStorage
{
    public interface INoSQLTableStorage<T> : IEnumerable<T> where T : ITableEntity, new()
    {
        /// <summary>
        /// Storage name
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Queries a row.
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        /// <param name="partition">Partition</param>
        /// <param name="row">Row</param>
        /// <returns>null or row item</returns>
        T this[string partition, string row] { get; }

        /// <summary>
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        IEnumerable<T> this[string partition] { get; }

        /// <summary>
        /// Add new row to the table.
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        /// <param name="item">Row item to insert</param>
        /// <param name="notLogCodes">Azure table storage exceptions codes, which are should not be logged</param>
        Task InsertAsync(T item, params int[] notLogCodes);

        /// <summary>
        /// Add new row to the table.
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        Task InsertAsync(IEnumerable<T> items);

        /// <summary>
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        Task InsertOrMergeAsync(T item);

        /// <summary>
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        Task InsertOrMergeBatchAsync(IEnumerable<T> items);

        /// <summary>
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        Task<T> ReplaceAsync(string partitionKey, string rowKey, Func<T, T> item);

        /// <summary>
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        Task<T> MergeAsync(string partitionKey, string rowKey, Func<T, T> item);

        /// <summary>
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        Task InsertOrReplaceBatchAsync(IEnumerable<T> entities);

        /// <summary>
        /// Adds or entirely replaces row.
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        Task InsertOrReplaceAsync(T item);

        /// <summary>
        /// Adds or entirely replaces row.
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        Task InsertOrReplaceAsync(IEnumerable<T> items);

        /// <summary>
        /// Deletes row from the table.
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        Task DeleteAsync(T item);

        /// <summary>
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        Task<T> DeleteAsync(string partitionKey, string rowKey);

        /// <summary>
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        Task<bool> DeleteIfExistAsync(string partitionKey, string rowKey);

        /// <summary>
        /// Deletes the table.
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        Task<bool> DeleteAsync();

        /// <summary>
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        Task DeleteAsync(IEnumerable<T> items);

        /// <summary>
        /// Creates record if not existed before.
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        /// <returns>true if created, false if existed before</returns>
        Task<bool> CreateIfNotExistsAsync(T item);

        /// <summary>
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        bool RecordExists(T item);

        /// <summary>
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        Task<bool> RecordExistsAsync(T item);

        /// <summary>
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        Task<T> GetDataAsync(string partition, string row);

        /// <summary>
        /// Queries data with client-side filtering.
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        Task<IList<T>> GetDataAsync(Func<T, bool> filter = null);

        /// <summary>
        /// Queries multiple rows of single partition.
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        /// <param name="partitionKey">Partition key</param>
        /// <param name="rowKeys">Row keys</param>
        /// <param name="pieceSize">Chank size</param>
        /// <param name="filter">Rows filter</param>
        Task<IEnumerable<T>> GetDataAsync(string partitionKey, IEnumerable<string> rowKeys, int pieceSize = 100,
            Func<T, bool> filter = null);

        /// <summary>
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        Task<IEnumerable<T>> GetDataAsync(IEnumerable<string> partitionKeys, int pieceSize = 100,
            Func<T, bool> filter = null);

        /// <summary>
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        Task<IEnumerable<T>> GetDataAsync(IEnumerable<Tuple<string, string>> keys, int pieceSize = 100,
            Func<T, bool> filter = null);
        
        /// <summary>
        /// Not auto-retried, if <see cref="AzureTableStorage{T}"/> implementation is used, since this is not atomic operation
        /// </summary>
        Task GetDataByChunksAsync(Func<IEnumerable<T>, Task> chunks);

        /// <summary>
        /// Not auto-retried, if <see cref="AzureTableStorage{T}"/> implementation is used, since this is not atomic operation
        /// </summary>
        Task GetDataByChunksAsync(TableQuery<T> rangeQuery, Func<IEnumerable<T>, Task> chunks);

        /// <summary>
        /// Not auto-retried, if <see cref="AzureTableStorage{T}"/> implementation is used, since this is not atomic operation
        /// </summary>
        Task GetDataByChunksAsync(Action<IEnumerable<T>> chunks);

        /// <summary>
        /// Not auto-retried, if <see cref="AzureTableStorage{T}"/> implementation is used, since this is not atomic operation
        /// </summary>
        Task GetDataByChunksAsync(TableQuery<T> rangeQuery, Action<IEnumerable<T>> chunks);

        /// <summary>
        /// Not auto-retried, if <see cref="AzureTableStorage{T}"/> implementation is used, since this is not atomic operation
        /// </summary>
        Task GetDataByChunksAsync(string partitionKey, Action<IEnumerable<T>> chunks);

        /// <summary>
        /// Not auto-retried, if <see cref="AzureTableStorage{T}"/> implementation is used, since this is not atomic operation
        /// </summary>
        Task ScanDataAsync(string partitionKey, Func<IEnumerable<T>, Task> chunk);
        
        /// <summary>
        /// Not auto-retried, if <see cref="AzureTableStorage{T}"/> implementation is used, since this is not atomic operation
        /// </summary>
        Task ScanDataAsync(TableQuery<T> rangeQuery, Func<IEnumerable<T>, Task> chunk);

        /// <summary>
        /// Scan table by chinks and find an instane.
        /// Not auto-retried, if <see cref="AzureTableStorage{T}"/> implementation is used, since this is not atomic operation
        /// </summary>
        /// <param name="partitionKey">Partition we are going to scan</param>
        /// <param name="dataToSearch">CallBack, which we going to call when we have chunk of data to scan. </param>
        /// <returns>Null or instance</returns>
        Task<T> FirstOrNullViaScanAsync(string partitionKey, Func<IEnumerable<T>, T> dataToSearch);

        /// <summary>
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        Task<IEnumerable<T>> GetDataAsync(string partition, Func<T, bool> filter = null);

        /// <summary>
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        Task<T> GetTopRecordAsync(string partition);

        /// <summary>
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        Task<IEnumerable<T>> GetTopRecordsAsync(string partition, int n);

        /// <summary>
        /// Not auto-retried, if <see cref="AzureTableStorage{T}"/> implementation is used, since this is not atomic operation
        /// </summary>
        Task<IEnumerable<T>> GetDataRowKeysOnlyAsync(IEnumerable<string> rowKeys);

        /// <summary>
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        Task<IEnumerable<T>> WhereAsyncc(TableQuery<T> rangeQuery, Func<T, Task<bool>> filter = null);

        /// <summary>
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        Task<IEnumerable<T>> WhereAsync(TableQuery<T> rangeQuery, Func<T, bool> filter = null);

        /// <summary>
        /// Recieves data asynchronously. Could be used for memory saving
        /// Not auto-retried, if <see cref="AzureTableStorage{T}"/> implementation is used, since this is not atomic operation
        /// </summary>
        /// <param name="rangeQuery">Query</param>
        /// <param name="yieldResult">Data chank processing delegate</param>
        /// <param name="stopCondition">Stop condition func</param>
        Task ExecuteAsync(TableQuery<T> rangeQuery, Action<IEnumerable<T>> yieldResult, Func<bool> stopCondition = null);

        /// <summary>
        /// Executes batch of operations.
        /// Auto retries, if <see cref="AzureTableStorage{T}"/> implementation is used
        /// </summary>
        Task DoBatchAsync(TableBatchOperation batch);

        /// <summary>
        /// Executes provided query with pagination. Not auto-retried.
        /// </summary>
        /// <param name="query">Query</param>
        /// <param name="pagingInfo">Paging information</param>
        /// <returns></returns>
        Task<IPagedResult<T>> ExecuteQueryWithPaginationAsync(TableQuery<T> query, PagingInfo pagingInfo);
    }
}