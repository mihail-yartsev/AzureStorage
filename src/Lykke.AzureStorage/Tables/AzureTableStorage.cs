using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using AzureStorage.Tables.Decorators;

using Common;
using Common.Extensions;
using Common.Log;

using Lykke.AzureStorage.Tables;
using Lykke.AzureStorage.Tables.Paging;
using Lykke.SettingsReader;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureStorage.Tables
{
    public class AzureTableStorage<T> : INoSQLTableStorage<T> where T : class, ITableEntity, new()
    {
        private class AzurePagingInfo : PagingInfo
        {
            public TableContinuationToken NextToken
            {
                get => DeserializeToken(NextPage);
                set => NextPage = SerializeToken(value);
            }

            public IReadOnlyList<TableContinuationToken> PreviousTokens
            {
                get => PreviousPages.Select(DeserializeToken).ToList();
                set => PreviousPages = value.Select(SerializeToken).ToList();
            }

            public static AzurePagingInfo Create(PagingInfo pagingInfo)
            {
                return new AzurePagingInfo
                {
                    CurrentPage = pagingInfo.CurrentPage,
                    ElementCount = pagingInfo.ElementCount,
                    NavigateToPageIndex = pagingInfo.NavigateToPageIndex,
                    NextPage = pagingInfo.NextPage,
                    PreviousPages = pagingInfo.PreviousPages
                };
            }

            private static string SerializeToken(TableContinuationToken token)
            {
                if (token == null)
                    return null;

                return $"{token.NextPartitionKey}|{token.NextRowKey}|{token.NextTableName}|{token.TargetLocation}";
            }

            private static TableContinuationToken DeserializeToken(string token)
            {
                if (token == null)
                    return null;

                var splited = token.Split('|');

                StorageLocation? location = null;
                if (splited[3] == "Primary")
                {
                    location = StorageLocation.Primary;
                }
                if (splited[3] == "Secondary")
                {
                    location = StorageLocation.Secondary;
                }

                return new TableContinuationToken()
                {
                    NextPartitionKey = splited[0],
                    NextRowKey = splited[1],
                    NextTableName = splited[2],
                    TargetLocation = location
                };
            }
        }

        public const int Conflict = 409;

        public string Name => _tableName;

        protected ILog Log { get; }

        private readonly TimeSpan _maxExecutionTime;
        private readonly string _tableName;

        private readonly CloudStorageAccount _cloudStorageAccount;
        private bool _tableCreated;

        private AzureTableStorage(
            string connectionString,
            string tableName,
            ILog log,
            TimeSpan? maxExecutionTimeout = null) 
        {
            _tableName = tableName;
            _cloudStorageAccount = CloudStorageAccount.Parse(connectionString);

            Log = log;

            _maxExecutionTime = maxExecutionTimeout.GetValueOrDefault(TimeSpan.FromSeconds(5));
        }

        /// <summary>
        /// Creates <see cref="AzureTableStorage{T}"/> with auto retries (only atomic operations) and connection string reloading.
        /// </summary>
        /// <remarks>
        /// Not atomic methods without retries:
        /// - GetDataByChunksAsync
        /// - ScanDataAsync
        /// - FirstOrNullViaScanAsync
        /// - GetDataRowKeysOnlyAsync
        /// - ExecuteAsync
        /// </remarks>
        /// <param name="connectionStringManager">Connection string reloading manager</param>
        /// <param name="tableName">Table's name</param>
        /// <param name="log">Log</param>
        /// <param name="maxExecutionTimeout">Maximum execution time of single request (within retries). Default is 5 seconds</param>
        /// <param name="onModificationRetryCount">Retries count when performs modification operation. Default value is 10</param>
        /// <param name="onGettingRetryCount">Retries count when performs reading operation. Default value is 10</param>
        /// <param name="retryDelay">Delay between retries. Default is 200 milliseconds</param>
        /// <returns></returns>
        public static INoSQLTableStorage<T> Create(
            IReloadingManager<string> connectionStringManager,
            string tableName,
            ILog log,
            TimeSpan? maxExecutionTimeout = null,
            int onModificationRetryCount = 10,
            int onGettingRetryCount = 10,
            TimeSpan? retryDelay = null)
        {
            async Task<INoSQLTableStorage<T>> MakeStorage() 
                => new AzureTableStorage<T>(await connectionStringManager.Reload(), tableName, log, maxExecutionTimeout);

            return
                new LogExceptionsAzureTableStorageDecorator<T>(
                    new RetryOnFailureAzureTableStorageDecorator<T>(
                        new ReloadingConnectionStringOnFailureAzureTableStorageDecorator<T>(MakeStorage),
                        onModificationRetryCount,
                        onGettingRetryCount,
                        retryDelay),
                    log);
        }

        /// <summary>
        /// Creates <see cref="AzureTableStorage{T}"/> with cache, auto retries (only atomic operations) and connection string reloading.
        /// </summary>
        /// <remarks>
        /// Not atomic methods without retries:
        /// - GetDataByChunksAsync
        /// - ScanDataAsync
        /// - FirstOrNullViaScanAsync
        /// - GetDataRowKeysOnlyAsync
        /// - ExecuteAsync
        /// </remarks>
        /// <param name="connectionStringManager">Connection string reloading manager</param>
        /// <param name="tableName">Table's name</param>
        /// <param name="log">Log</param>
        /// <param name="maxExecutionTimeout">Maximum execution time of single request (within retries). Default is 5 seconds</param>
        /// <param name="onModificationRetryCount">Retries count when performs modification operation. Default value is 10</param>
        /// <param name="onGettingRetryCount">Retries count when performs reading operation. Default value is 10</param>
        /// <param name="retryDelay">Delay between retries. Default is 200 milliseconds</param>
        /// <returns></returns>
        public static INoSQLTableStorage<T> CreateWithCache(
            IReloadingManager<string> connectionStringManager,
            string tableName,
            ILog log,
            TimeSpan? maxExecutionTimeout = null,
            int onModificationRetryCount = 10,
            int onGettingRetryCount = 10,
            TimeSpan? retryDelay = null)
        {
            async Task<INoSQLTableStorage<T>> MakeStorage() 
                => new AzureTableStorage<T>(await connectionStringManager.Reload(), tableName, log, maxExecutionTimeout);

            return
                new LogExceptionsAzureTableStorageDecorator<T>(
                    new CachedAzureTableStorageDecorator<T>(
                        new RetryOnFailureAzureTableStorageDecorator<T>(
                            new ReloadingConnectionStringOnFailureAzureTableStorageDecorator<T>(MakeStorage),
                            onModificationRetryCount,
                            onGettingRetryCount,
                            retryDelay)
                    ),
                    log);
        }

        private TableRequestOptions GetRequestOptions()
        {
            return new TableRequestOptions
            {
                MaximumExecutionTime = _maxExecutionTime
            };
        }

        public async Task DoBatchAsync(TableBatchOperation batch)
        {
            var table = await GetTableAsync();
            await table.ExecuteLimitSafeBatchAsync(batch, GetRequestOptions(), null);
        }

        public async Task<IPagedResult<T>> ExecuteQueryWithPaginationAsync(TableQuery<T> query, PagingInfo pagingInfo)
        {
            query = query ?? new TableQuery<T>();
            var azurePagingInfo = AzurePagingInfo.Create(pagingInfo ?? new PagingInfo());
            var table = await GetTableAsync();
            query.TakeCount = azurePagingInfo.ElementCount;

            TableContinuationToken continuationToken = null;
            if (azurePagingInfo.NavigateToPageIndex != 0)
            {
                if (azurePagingInfo.NavigateToPageIndex > azurePagingInfo.CurrentPage)
                {
                    continuationToken = azurePagingInfo.NextToken;
                }
                else
                {
                    continuationToken =
                        azurePagingInfo.PreviousTokens[azurePagingInfo.NavigateToPageIndex - 1];
                }
            }
            var queryResponse = await table.ExecuteQuerySegmentedAsync(query, continuationToken);

            if (queryResponse.Results == null || !queryResponse.Results.Any())
                return new PagedResult<T>(pagingInfo: new PagingInfo
                {
                    ElementCount = azurePagingInfo.ElementCount
                });

            var newPagingInfo = new AzurePagingInfo
            {
                ElementCount = azurePagingInfo.ElementCount,
                NavigateToPageIndex = azurePagingInfo.NavigateToPageIndex + 1,
            };


            if (azurePagingInfo.NavigateToPageIndex > azurePagingInfo.CurrentPage)
            {
                newPagingInfo.CurrentPage = azurePagingInfo.CurrentPage + 1;
                var previousTokens = azurePagingInfo.PreviousTokens.ToList();
                previousTokens.Add(azurePagingInfo.NextToken);
                newPagingInfo.PreviousTokens = previousTokens;
            }
            else
            {
                newPagingInfo.CurrentPage = azurePagingInfo.NavigateToPageIndex;
                newPagingInfo.PreviousTokens = azurePagingInfo.PreviousTokens.Take(azurePagingInfo.NavigateToPageIndex).ToList();
            }
            newPagingInfo.NextToken = queryResponse.ContinuationToken;

            return new PagedResult<T>(queryResponse.Results, newPagingInfo);
        }

        public virtual IEnumerator<T> GetEnumerator()
        {
            return GetDataAsync().RunSync().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public virtual async Task InsertAsync(T item, params int[] notLogCodes)
        {
            var table = await GetTableAsync();
            await table.ExecuteAsync(TableOperation.Insert(item), GetRequestOptions(), null);
        }

        public async Task InsertAsync(IEnumerable<T> items)
        {
            items = items.ToArray();
            
            if (items.Any())
            {
                var insertBatchOperation = new TableBatchOperation();
                foreach (var item in items)
                    insertBatchOperation.Insert(item);
                var table = await GetTableAsync();
                await table.ExecuteLimitSafeBatchAsync(insertBatchOperation, GetRequestOptions(), null);
            }
        }

        public async Task InsertOrMergeAsync(T item)
        {
            var table = await GetTableAsync();

            await table.ExecuteAsync(TableOperation.InsertOrMerge(item), GetRequestOptions(), null);
        }

        public async Task InsertOrMergeBatchAsync(IEnumerable<T> items)
        {
            items = items.ToArray();
            
            if (items.Any())
            {
                var insertBatchOperation = new TableBatchOperation();
                foreach (var item in items)
                {
                    insertBatchOperation.InsertOrMerge(item);
                }
                var table = await GetTableAsync();
                await table.ExecuteLimitSafeBatchAsync(insertBatchOperation, GetRequestOptions(), null);
            }
        }

        public async Task<T> ReplaceAsync(string partitionKey, string rowKey, Func<T, T> replaceAction)
        {
            while (true)
            {
                try
                {
                    var entity = await GetDataAsync(partitionKey, rowKey);
                    if (entity != null)
                    {
                        var result = replaceAction(entity);
                        if (result != null)
                        {
                            var table = await GetTableAsync();
                            await table.ExecuteAsync(TableOperation.Replace(result), GetRequestOptions(), null);
                        }

                        return result;
                    }

                    return null;
                }
                catch (StorageException e)
                {
                    // Если поймали precondition fall = 412, значит в другом потоке данную сущность успели поменять
                    // - нужно повторить операцию, пока не исполнится без ошибок
                    if (e.RequestInformation.HttpStatusCode != 412)
                        throw;
                }
            }
        }

        public async Task<T> MergeAsync(string partitionKey, string rowKey, Func<T, T> mergeAction)
        {
            while (true)
            {
                try
                {
                    var entity = await GetDataAsync(partitionKey, rowKey);
                    if (entity != null)
                    {
                        var result = mergeAction(entity);
                        if (result != null)
                        {
                            var table = await GetTableAsync();
                            await table.ExecuteAsync(TableOperation.Merge(result), GetRequestOptions(), null);
                        }

                        return result;
                    }
                    return null;
                }
                catch (StorageException e)
                {
                    // Если поймали precondition fall = 412, значит в другом потоке данную сущность успели поменять
                    // - нужно повторить операцию, пока не исполнится без ошибок
                    if (e.RequestInformation.HttpStatusCode != 412)
                        throw;
                }
            }
        }

        public async Task InsertOrReplaceBatchAsync(IEnumerable<T> entites)
        {
            var operationsBatch = new TableBatchOperation();

            foreach (var entity in entites)
                operationsBatch.Add(TableOperation.InsertOrReplace(entity));
            var table = await GetTableAsync();

            await table.ExecuteLimitSafeBatchAsync(operationsBatch, GetRequestOptions(), null);
        }

        public virtual async Task InsertOrReplaceAsync(T item)
        {
            var table = await GetTableAsync();
            await table.ExecuteAsync(TableOperation.InsertOrReplace(item), GetRequestOptions(), null);
        }

        public async Task InsertOrReplaceAsync(IEnumerable<T> items)
        {
            items = items.ToArray();

            if (items.Any())
            {
                var insertBatchOperation = new TableBatchOperation();
                foreach (var item in items)
                {
                    insertBatchOperation.InsertOrReplace(item);
                }
                var table = await GetTableAsync();
                await table.ExecuteLimitSafeBatchAsync(insertBatchOperation, GetRequestOptions(), null);
            }
        }

        public virtual async Task DeleteAsync(T item)
        {
            var table = await GetTableAsync();
            await table.ExecuteAsync(TableOperation.Delete(item), GetRequestOptions(), null);
        }

        public async Task<T> DeleteAsync(string partitionKey, string rowKey)
        {
            var itm = await GetDataAsync(partitionKey, rowKey);
            if (itm != null)
                await DeleteAsync(itm);
            return itm;
        }

        public async Task<bool> DeleteIfExistAsync(string partitionKey, string rowKey)
        {
            try
            {
                await DeleteAsync(partitionKey, rowKey);
            }
            catch (StorageException ex)
            {
                if (ex.RequestInformation.HttpStatusCode == 404)
                    return false;

                throw;
            }

            return true;
        }

        public async Task<bool> DeleteAsync()
        {
            bool deleted;

            try
            {
                var table = await GetTableAsync();

                deleted = await table.DeleteIfExistsAsync();

                if (deleted)
                {
                    _tableCreated = false;
                }
            }
            catch (StorageException ex)
            {
                if (ex.RequestInformation.HttpStatusCode == 404)
                    return false;

                throw;
            }

            return deleted;
        }

        public async Task DeleteAsync(IEnumerable<T> items)
        {
            items = items.ToArray();

            if (items.Any())
            {
                var deleteBatchOperation = new TableBatchOperation();
                foreach (var item in items)
                    deleteBatchOperation.Delete(item);
                var table = await GetTableAsync();
                await table.ExecuteLimitSafeBatchAsync(deleteBatchOperation, GetRequestOptions(), null);
            }
        }

        public virtual async Task<bool> CreateIfNotExistsAsync(T item)
        {
            try
            {
                await InsertAsync(item, Conflict);
                return true;
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode != Conflict)
                    throw;
            }

            return false;
        }

        public virtual bool RecordExists(T item)
        {
            return this[item.PartitionKey, item.RowKey] != null;
        }

        public virtual async Task<bool> RecordExistsAsync(T item)
        {
            return await GetDataAsync(item.PartitionKey, item.RowKey) != null;
        }

        public virtual T this[string partition, string row] => GetDataAsync(partition, row).RunSync();

        public async Task<IEnumerable<T>> GetDataAsync(string partitionKey, IEnumerable<string> rowKeys,
            int pieceSize = 15, Func<T, bool> filter = null)
        {
            var result = new List<T>();

            await Task.WhenAll(
                rowKeys.ToPieces(pieceSize).Select(piece =>
                        ExecuteQueryAsync(
                            AzureStorageUtils.QueryGenerator<T>.MultipleRowKeys(partitionKey, piece.ToArray()), filter,
                            items =>
                            {
                                lock (result)
                                {
                                    result.AddRange(items);
                                }
                                return true;
                            })
                )
            );

            return result;
        }

        public async Task<IEnumerable<T>> GetDataAsync(IEnumerable<string> partitionKeys, int pieceSize = 100,
            Func<T, bool> filter = null)
        {
            var result = new List<T>();

            await Task.WhenAll(
                partitionKeys.ToPieces(pieceSize).Select(piece =>
                        ExecuteQueryAsync(
                            AzureStorageUtils.QueryGenerator<T>.MultiplePartitionKeys(piece.ToArray()), filter,
                            items =>
                            {
                                lock (result)
                                {
                                    result.AddRange(items);
                                }
                                return true;
                            })
                )
            );

            return result;
        }

        public async Task<IEnumerable<T>> GetDataAsync(IEnumerable<Tuple<string, string>> keys, int pieceSize = 100,
            Func<T, bool> filter = null)
        {
            var result = new List<T>();

            await Task.WhenAll(
                keys.ToPieces(pieceSize).Select(piece =>
                        ExecuteQueryAsync(
                            AzureStorageUtils.QueryGenerator<T>.MultipleKeys(piece), filter,
                            items =>
                            {
                                lock (result)
                                {
                                    result.AddRange(items);
                                }
                                return true;
                            })
                )
            );

            return result;
        }

        public Task GetDataByChunksAsync(Func<IEnumerable<T>, Task> chunks)
        {
            return GetDataByChunksAsync(new TableQuery<T>(), chunks);
        }

        public Task GetDataByChunksAsync(TableQuery<T> rangeQuery, Func<IEnumerable<T>, Task> chunks)
        {
            return ExecuteQueryAsync(rangeQuery, null, async itms => { await chunks(itms); });
        }

        public Task GetDataByChunksAsync(Action<IEnumerable<T>> chunks)
        {
            return GetDataByChunksAsync(new TableQuery<T>(), chunks);
        }

        public Task GetDataByChunksAsync(TableQuery<T> rangeQuery, Action<IEnumerable<T>> chunks)
        {
            return ExecuteQueryAsync(rangeQuery, null, itms =>
            {
                chunks(itms);
                return true;
            });
        }

        public Task GetDataByChunksAsync(string partitionKey, Action<IEnumerable<T>> chunks)
        {
            var query = CompileTableQuery(partitionKey);
            return ExecuteAsync(query, chunks);
        }

        public Task ScanDataAsync(string partitionKey, Func<IEnumerable<T>, Task> chunk)
        {
            var rangeQuery = CompileTableQuery(partitionKey);

            return ExecuteQueryAsync(rangeQuery, null, chunk);
        }

        public Task ScanDataAsync(TableQuery<T> rangeQuery, Func<IEnumerable<T>, Task> chunk)
        {
            return ExecuteQueryAsync(rangeQuery, null, chunk);
        }

        public virtual async Task<T> GetDataAsync(string partition, string row)
        {
            var retrieveOperation = TableOperation.Retrieve<T>(partition, row);
            var table = await GetTableAsync();
            var retrievedResult = await table.ExecuteAsync(retrieveOperation, GetRequestOptions(), null);
            return (T) retrievedResult.Result;
        }


        public async Task<T> FirstOrNullViaScanAsync(string partitionKey, Func<IEnumerable<T>, T> dataToSearch)
        {
            var query = CompileTableQuery(partitionKey);

            T result = null;

            await ExecuteQueryAsync(query, itm => true,
                itms =>
                {
                    result = dataToSearch(itms);
                    return result == null;
                });

            return result;
        }

        public virtual IEnumerable<T> this[string partition] => GetDataAsync(partition).RunSync();


        public async Task<IEnumerable<T>> GetDataRowKeysOnlyAsync(IEnumerable<string> rowKeys)
        {
            var query = AzureStorageUtils.QueryGenerator<T>.RowKeyOnly.GetTableQuery(rowKeys);
            var result = new List<T>();

            await ExecuteQueryAsync(query, null, chunk =>
            {
                result.AddRange(chunk);
                return Task.FromResult(0);
            });

            return result;
        }

        public async Task<IEnumerable<T>> WhereAsyncc(TableQuery<T> rangeQuery, Func<T, Task<bool>> filter = null)
        {
            var result = new List<T>();
            await ExecuteQueryAsync2(rangeQuery, filter, itm =>
            {
                result.Add(itm);
                return true;
            });

            return result;
        }

        public async Task<IList<T>> GetDataAsync(Func<T, bool> filter = null)
        {
            var rangeQuery = new TableQuery<T>();
            var result = new List<T>();
            await ExecuteQueryAsync(rangeQuery, filter, itms =>
            {
                result.AddRange(itms);
                return true;
            });
            return result;
        }

        public virtual async Task<IEnumerable<T>> GetDataAsync(string partition, Func<T, bool> filter = null)
        {
            var rangeQuery = CompileTableQuery(partition);
            var result = new List<T>();

            await ExecuteQueryAsync(rangeQuery, filter, itms =>
            {
                result.AddRange(itms);
                return true;
            });

            return result;
        }

        public virtual async Task<T> GetTopRecordAsync(string partition)
        {
            var rangeQuery = CompileTableQuery(partition);
            var result = new List<T>();

            await ExecuteQueryAsync(rangeQuery, null, itms =>
            {
                result.AddRange(itms);
                return false;
            });

            return result.FirstOrDefault();
        }

        public virtual async Task<IEnumerable<T>> GetTopRecordsAsync(string partition, int n)
        {
            var rangeQuery = CompileTableQuery(partition);
            var result = new List<T>();

            await ExecuteQueryAsync(rangeQuery, null, itms =>
            {
                result.AddRange(itms);

                if (n > result.Count)
                    return true;

                return false;
            });

            return result.Take(n);
        }

        public async Task<IEnumerable<T>> WhereAsync(TableQuery<T> rangeQuery, Func<T, bool> filter = null)
        {
            var result = new List<T>();
            await ExecuteQueryAsync(rangeQuery, filter, itms =>
            {
                result.AddRange(itms);
                return true;
            });
            return result;
        }

        public Task ExecuteAsync(TableQuery<T> rangeQuery, Action<IEnumerable<T>> result, Func<bool> stopCondition = null)
        {
            return ExecuteQueryAsync(rangeQuery, null, itms =>
            {
                result(itms);

                if (stopCondition != null)
                {
                    return stopCondition();
                }

                return true;
            });
        }

        private CloudTable GetTableReference()
        {
            var cloudTableClient = _cloudStorageAccount.CreateCloudTableClient();
            return cloudTableClient.GetTableReference(_tableName);
        }

        private async Task<CloudTable> CreateTableIfNotExists()
        {
            var table = GetTableReference();

            await table.CreateIfNotExistsAsync();

            _tableCreated = true;

            return table;
        }

        protected async Task<CloudTable> GetTableAsync()
        {
            return _tableCreated ? GetTableReference() : await CreateTableIfNotExists();
        }

        private async Task ExecuteQueryAsync(TableQuery<T> rangeQuery, Func<T, bool> filter,
            Func<IEnumerable<T>, Task> yieldData)
        {
            TableContinuationToken tableContinuationToken = null;
            var table = await GetTableAsync();
            do
            {
                var queryResponse = await table.ExecuteQuerySegmentedAsync(rangeQuery, tableContinuationToken, GetRequestOptions(), null);
                tableContinuationToken = queryResponse.ContinuationToken;
                await yieldData(AzureStorageUtils.ApplyFilter(queryResponse.Results, filter));
            } while (tableContinuationToken != null);
        }


        /// <summary>
        ///     Выполнить запрос асинхроно
        /// </summary>
        /// <param name="rangeQuery">Параметры запроса</param>
        /// <param name="filter">Фильтрация запроса</param>
        /// <param name="yieldData">Данные которые мы выдаем наружу. Если возвращается false - данные можно больше не запрашивать</param>
        /// <returns></returns>
        private async Task ExecuteQueryAsync(TableQuery<T> rangeQuery, Func<T, bool> filter, Func<IEnumerable<T>, bool> yieldData)
        {
            TableContinuationToken tableContinuationToken = null;
            var table = await GetTableAsync();
            do
            {
                var queryResponse = await table.ExecuteQuerySegmentedAsync(rangeQuery, tableContinuationToken, GetRequestOptions(), null);
                tableContinuationToken = queryResponse.ContinuationToken;
                var shouldWeContinue = yieldData(AzureStorageUtils.ApplyFilter(queryResponse.Results, filter));
                if (!shouldWeContinue)
                    break;
            } while (tableContinuationToken != null);
        }

        private async Task ExecuteQueryAsync2(TableQuery<T> rangeQuery, Func<T, Task<bool>> filter, Func<T, bool> yieldData)
        {
            TableContinuationToken tableContinuationToken = null;
            var table = await GetTableAsync();
            do
            {
                var queryResponse = await table.ExecuteQuerySegmentedAsync(rangeQuery, tableContinuationToken, GetRequestOptions(), null);
                tableContinuationToken = queryResponse.ContinuationToken;

                foreach (var itm in queryResponse.Results)
                    if ((filter == null) || await filter(itm))
                    {
                        var shouldWeContinue = yieldData(itm);
                        if (!shouldWeContinue)
                            return;
                    }
            } while (tableContinuationToken != null);
        }

        private TableQuery<T> CompileTableQuery(string partition)
        {
            var filter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partition);
            return new TableQuery<T>().Where(filter);
        }
    }
}
