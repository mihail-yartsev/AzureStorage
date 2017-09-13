using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using AzureStorage.Tables.Decorators;

using Common;
using Common.Extensions;
using Common.Log;

using Lykke.AzureStorage;
using Lykke.AzureStorage.Tables;
using Lykke.SettingsReader;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureStorage.Tables
{
    public class AzureTableStorage<T> : INoSQLTableStorage<T> where T : class, ITableEntity, new()
    {
        public const int Conflict = 409;

        private readonly TimeSpan _maxExecutionTime;

        private readonly ILog _log;
        private readonly string _tableName;

        private readonly CloudStorageAccount _cloudStorageAccount;
        private bool _tableCreated;

        [Obsolete("Have to use the AzureTableStorage.Create method to reloading ConnectionString on access failure.", false)]
        public AzureTableStorage(string connectionString, string tableName, ILog log, TimeSpan? maxExecutionTimeout = null) 
        {
            _tableName = tableName;
            _log = log ?? EmptyLog.Instance;
            _cloudStorageAccount = CloudStorageAccount.Parse(connectionString);

            _maxExecutionTime = maxExecutionTimeout.GetValueOrDefault(TimeSpan.FromSeconds(30));
        }

        public static INoSQLTableStorage<T> Create(IReloadingManager<string> connectionStringManager, string tableName, ILog log, TimeSpan? maxExecutionTimeout = null)
        {
            return new ReloadingConnectionStringOnFailureAzureTableStorageDecorator<T>(
#pragma warning disable 618
                async () => new AzureTableStorage<T>(await connectionStringManager.Reload(), tableName, log, maxExecutionTimeout)
#pragma warning restore 618
            );
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
            var table = await GetTable();
            await table.ExecuteLimitSafeBatchAsync(batch, GetRequestOptions(), null);
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
            try
            {
                var table = await GetTable();
                await table.ExecuteAsync(TableOperation.Insert(item), GetRequestOptions(), null);
            }
            catch (Exception ex)
            {
                await HandleException(item, ex, notLogCodes);
                throw;
            }
        }

        public async Task InsertAsync(IEnumerable<T> items)
        {
            items = items.ToArray();
            try
            {
                if (items.Any())
                {
                    var insertBatchOperation = new TableBatchOperation();
                    foreach (var item in items)
                        insertBatchOperation.Insert(item);
                    var table = await GetTable();
                    await table.ExecuteLimitSafeBatchAsync(insertBatchOperation, GetRequestOptions(), null);
                }
            }
            catch (Exception ex)
            {
                if (ex is TaskCanceledException)
                    await _log.WriteWarningAsync(
                        "Table storage: " + _tableName,
                        nameof(InsertAsync),
                        AzureStorageUtils.PrintItems(items),
                        ex.GetBaseException().Message);
                else
                    await _log.WriteFatalErrorAsync("Table storage: " + _tableName, "InsertAsync batch", AzureStorageUtils.PrintItems(items), ex);
                throw;
            }
        }

        public async Task InsertOrMergeAsync(T item)
        {
            try
            {
                var table = await GetTable();
                await table.ExecuteAsync(TableOperation.InsertOrMerge(item), GetRequestOptions(), null);
            }
            catch (Exception ex)
            {
                if (ex is TaskCanceledException)
                    await _log.WriteWarningAsync(
                        "Table storage: " + _tableName,
                        nameof(InsertOrMergeAsync),
                        AzureStorageUtils.PrintItem(item),
                        ex.GetBaseException().Message);
                else
                    await _log.WriteFatalErrorAsync("Table storage: " + _tableName, "InsertOrMerge item", AzureStorageUtils.PrintItem(item), ex);
                throw;
            }
        }

        public async Task InsertOrMergeBatchAsync(IEnumerable<T> items)
        {
            items = items.ToArray();
            try
            {
                if (items.Any())
                {
                    var insertBatchOperation = new TableBatchOperation();
                    foreach (var item in items)
                    {
                        insertBatchOperation.InsertOrMerge(item);
                    }
                    var table = await GetTable();
                    await table.ExecuteLimitSafeBatchAsync(insertBatchOperation, GetRequestOptions(), null);
                }
            }
            catch (Exception ex)
            {
                if (ex is TaskCanceledException)
                    await _log.WriteWarningAsync(
                        "Table storage: " + _tableName,
                        nameof(InsertOrMergeBatchAsync),
                        AzureStorageUtils.PrintItems(items),
                        ex.GetBaseException().Message);
                else
                    await _log.WriteFatalErrorAsync(
                        "Table storage: " + _tableName, nameof(InsertOrMergeBatchAsync), AzureStorageUtils.PrintItems(items), ex);
                throw;
            }
        }

        public async Task<T> ReplaceAsync(string partitionKey, string rowKey, Func<T, T> replaceAction)
        {
            object itm = "Not read";
            try
            {
                while (true)
                    try
                    {
                        var entity = await GetDataAsync(partitionKey, rowKey);
                        if (entity != null)
                        {
                            var result = replaceAction(entity);
                            itm = result;
                            if (result != null)
                            {
                                var table = await GetTable();
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
            catch (Exception ex)
            {
                if (ex is TaskCanceledException)
                    await _log.WriteWarningAsync(
                        "Table storage: " + _tableName,
                        nameof(ReplaceAsync),
                        AzureStorageUtils.PrintItem(itm),
                        ex.GetBaseException().Message);
                else
                    await _log.WriteFatalErrorAsync("Table storage: " + _tableName, "Replace item", AzureStorageUtils.PrintItem(itm), ex);
                throw;
            }
        }

        public async Task<T> MergeAsync(string partitionKey, string rowKey, Func<T, T> mergeAction)
        {
            object itm = "Not read";

            try
            {
                while (true)
                    try
                    {
                        var entity = await GetDataAsync(partitionKey, rowKey);
                        if (entity != null)
                        {
                            var result = mergeAction(entity);
                            itm = result;
                            if (result != null)
                            {
                                var table = await GetTable();
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
            catch (Exception ex)
            {
                if (ex is TaskCanceledException)
                    await _log.WriteWarningAsync(
                        "Table storage: " + _tableName,
                        nameof(MergeAsync),
                        AzureStorageUtils.PrintItem(itm),
                        ex.GetBaseException().Message);
                else
                    await _log.WriteFatalErrorAsync("Table storage: " + _tableName, nameof(MergeAsync), AzureStorageUtils.PrintItem(itm), ex);
                throw;
            }
        }

        public async Task InsertOrReplaceBatchAsync(IEnumerable<T> entites)
        {
            var operationsBatch = new TableBatchOperation();

            foreach (var entity in entites)
                operationsBatch.Add(TableOperation.InsertOrReplace(entity));
            var table = await GetTable();

            await table.ExecuteLimitSafeBatchAsync(operationsBatch, GetRequestOptions(), null);
        }

        public virtual async Task InsertOrReplaceAsync(T item)
        {
            try
            {
                var table = await GetTable();
                await table.ExecuteAsync(TableOperation.InsertOrReplace(item), GetRequestOptions(), null);
            }
            catch (Exception ex)
            {
                if (ex is TaskCanceledException)
                    await _log.WriteWarningAsync(
                        "Table storage: " + _tableName,
                        nameof(InsertOrReplaceAsync),
                        AzureStorageUtils.PrintItem(item),
                        ex.GetBaseException().Message);
                else
                    await _log.WriteFatalErrorAsync("Table storage: " + _tableName, "InsertOrReplace item", AzureStorageUtils.PrintItem(item), ex);
                throw;
            }
        }

        public async Task InsertOrReplaceAsync(IEnumerable<T> items)
        {
            items = items.ToArray();
            try
            {
                if (items.Any())
                {
                    var insertBatchOperation = new TableBatchOperation();
                    foreach (var item in items)
                    {
                        insertBatchOperation.InsertOrReplace(item);
                    }
                    var table = await GetTable();
                    await table.ExecuteLimitSafeBatchAsync(insertBatchOperation, GetRequestOptions(), null);
                }
            }
            catch (Exception ex)
            {
                if (ex is TaskCanceledException)
                    await _log.WriteWarningAsync(
                        "Table storage: " + _tableName,
                        nameof(InsertOrReplaceAsync),
                        AzureStorageUtils.PrintItems(items),
                        ex.GetBaseException().Message);
                else
                    await _log.WriteFatalErrorAsync(
                        "Table storage: " + _tableName, "InsertOrReplaceAsync batch", AzureStorageUtils.PrintItems(items), ex);
                throw;
            }
        }

        public virtual async Task DeleteAsync(T item)
        {
            try
            {
                var table = await GetTable();
                await table.ExecuteAsync(TableOperation.Delete(item), GetRequestOptions(), null);
            }
            catch (Exception ex)
            {
                if (ex is TaskCanceledException)
                    await _log.WriteWarningAsync(
                        "Table storage: " + _tableName,
                        nameof(DeleteAsync),
                        AzureStorageUtils.PrintItem(item),
                        ex.GetBaseException().Message);
                else
                    await _log.WriteFatalErrorAsync("Table storage: " + _tableName, "Delete item", AzureStorageUtils.PrintItem(item), ex);
                throw;
            }
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

        public async Task DeleteAsync(IEnumerable<T> items)
        {
            items = items.ToArray();
            try
            {
                if (items.Any())
                {
                    var deleteBatchOperation = new TableBatchOperation();
                    foreach (var item in items)
                        deleteBatchOperation.Delete(item);
                    var table = await GetTable();
                    await table.ExecuteLimitSafeBatchAsync(deleteBatchOperation, GetRequestOptions(), null);
                }
            }
            catch (Exception ex)
            {
                if (ex is TaskCanceledException)
                    await _log.WriteWarningAsync(
                        "Table storage: " + _tableName,
                        nameof(DeleteAsync),
                        AzureStorageUtils.PrintItems(items),
                        ex.GetBaseException().Message);
                else
                    await _log.WriteFatalErrorAsync("Table storage: " + _tableName, "DeleteAsync batch", AzureStorageUtils.PrintItems(items), ex);
                throw;
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

        public virtual T this[string partition, string row] => GetDataAsync(partition, row).RunSync();

        public async Task<IEnumerable<T>> GetDataAsync(string partitionKey, IEnumerable<string> rowKeys,
            int pieceSize = 15, Func<T, bool> filter = null)
        {
            var result = new List<T>();

            await Task.WhenAll(
                rowKeys.ToPieces(pieceSize).Select(piece =>
                        ExecuteQueryAsync("GetDataWithMutipleRows",
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
                        ExecuteQueryAsync("GetDataWithMutiplePartitionKeys",
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
                        ExecuteQueryAsync("GetDataWithMoltipleKeysAsync",
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
            var rangeQuery = new TableQuery<T>();
            return ExecuteQueryAsync("GetDataByChunksAsync", rangeQuery, null, async itms => { await chunks(itms); });
        }

        public Task GetDataByChunksAsync(Action<IEnumerable<T>> chunks)
        {
            var rangeQuery = new TableQuery<T>();
            return ExecuteQueryAsync("GetDataByChunksAsync", rangeQuery, null, itms =>
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

            return ExecuteQueryAsync("ScanDataAsync", rangeQuery, null, chunk);
        }

        public Task ScanDataAsync(TableQuery<T> rangeQuery, Func<IEnumerable<T>, Task> chunk)
        {
            return ExecuteQueryAsync("ScanDataAsync", rangeQuery, null, chunk);
        }

        public virtual async Task<T> GetDataAsync(string partition, string row)
        {
            try
            {
                var retrieveOperation = TableOperation.Retrieve<T>(partition, row);
                var table = await GetTable();
                var retrievedResult = await table.ExecuteAsync(retrieveOperation, GetRequestOptions(), null);
                return (T)retrievedResult.Result;
            }
            catch (Exception ex)
            {
                if (ex is TaskCanceledException)
                    await _log.WriteWarningAsync(
                        "Table storage: " + _tableName,
                        nameof(GetDataAsync),
                        "partitionId=" + partition + "; rowId=" + row,
                        ex.GetBaseException().Message);
                else
                    await _log.WriteFatalErrorAsync(
                        "Table storage: " + _tableName, "Get item async by partId and rowId", "partitionId=" + partition + "; rowId=" + row, ex);
                throw;
            }
        }


        public async Task<T> FirstOrNullViaScanAsync(string partitionKey, Func<IEnumerable<T>, T> dataToSearch)
        {
            var query = CompileTableQuery(partitionKey);

            T result = null;

            await ExecuteQueryAsync("ScanDataAsync", query, itm => true,
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

            await ExecuteQueryAsync("GetDataRowKeysOnlyAsync", query, null, chunk =>
            {
                result.AddRange(chunk);
                return Task.FromResult(0);
            });

            return result;
        }

        public async Task<IEnumerable<T>> WhereAsyncc(TableQuery<T> rangeQuery, Func<T, Task<bool>> filter = null)
        {
            var result = new List<T>();
            await ExecuteQueryAsync2("WhereAsyncc", rangeQuery, filter, itm =>
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
            await ExecuteQueryAsync("GetDataAsync", rangeQuery, filter, itms =>
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

            await ExecuteQueryAsync("GetDataAsync", rangeQuery, filter, itms =>
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

            await ExecuteQueryAsync("GetTopRecordAsync", rangeQuery, null, itms =>
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

            await ExecuteQueryAsync("GetTopRecordsAsync", rangeQuery, null, itms =>
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
            await ExecuteQueryAsync("WhereAsync", rangeQuery, filter, itms =>
            {
                result.AddRange(itms);
                return true;
            });
            return result;
        }

        public Task ExecuteAsync(TableQuery<T> rangeQuery, Action<IEnumerable<T>> result, Func<bool> stopCondition = null)
        {
            return ExecuteQueryAsync("ExecuteAsync", rangeQuery, null, itms =>
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

            try
            {
                await table.CreateIfNotExistsAsync();
            }
            catch (Exception ex)
            {
                if (ex is TaskCanceledException)
                    await _log.WriteWarningAsync(
                        "Table storage: " + _tableName,
                        nameof(CreateTableIfNotExists),
                        "unknown case",
                        ex.GetBaseException().Message);
                else
                    await _log.WriteFatalErrorAsync("Table storage: " + _tableName, "CreateTable error", "unknown case", ex);
                throw;
            }

            _tableCreated = true;

            return table;
        }

        private async Task<CloudTable> GetTable()
        {
            return _tableCreated ? GetTableReference() : await CreateTableIfNotExists();
        }

        private async Task ExecuteQueryAsync(string processName, TableQuery<T> rangeQuery, Func<T, bool> filter,
            Func<IEnumerable<T>, Task> yieldData)
        {
            try
            {
                TableContinuationToken tableContinuationToken = null;
                var table = await GetTable();
                do
                {
                    var queryResponse = await table.ExecuteQuerySegmentedAsync(rangeQuery, tableContinuationToken, GetRequestOptions(), null);
                    tableContinuationToken = queryResponse.ContinuationToken;
                    await yieldData(AzureStorageUtils.ApplyFilter(queryResponse.Results, filter));
                } while (tableContinuationToken != null);
            }
            catch (Exception ex)
            {
                if (ex is TaskCanceledException)
                    await _log.WriteWarningAsync(
                        "Table storage: " + _tableName,
                        processName,
                        rangeQuery.FilterString ?? "[null]",
                        ex.GetBaseException().Message);
                else
                    await _log.WriteFatalErrorAsync("Table storage: " + _tableName, processName, rangeQuery.FilterString ?? "[null]", ex);
                throw;
            }
        }


        /// <summary>
        ///     Выполнить запрос асинхроно
        /// </summary>
        /// <param name="processName">Имя процесса (для лога)</param>
        /// <param name="rangeQuery">Параметры запроса</param>
        /// <param name="filter">Фильтрация запроса</param>
        /// <param name="yieldData">Данные которые мы выдаем наружу. Если возвращается false - данные можно больше не запрашивать</param>
        /// <returns></returns>
        private async Task ExecuteQueryAsync(string processName, TableQuery<T> rangeQuery, Func<T, bool> filter,
            Func<IEnumerable<T>, bool> yieldData)
        {
            try
            {
                TableContinuationToken tableContinuationToken = null;
                var table = await GetTable();
                do
                {
                    var queryResponse = await table.ExecuteQuerySegmentedAsync(rangeQuery, tableContinuationToken, GetRequestOptions(), null);
                    tableContinuationToken = queryResponse.ContinuationToken;
                    var shouldWeContinue = yieldData(AzureStorageUtils.ApplyFilter(queryResponse.Results, filter));
                    if (!shouldWeContinue)
                        break;
                } while (tableContinuationToken != null);
            }
            catch (Exception ex)
            {
                if (ex is TaskCanceledException)
                    await _log.WriteWarningAsync(
                        "Table storage: " + _tableName,
                        processName,
                        rangeQuery.FilterString ?? "[null]",
                        ex.GetBaseException().Message);
                else
                    await _log.WriteFatalErrorAsync("Table storage: " + _tableName, processName, rangeQuery.FilterString ?? "[null]", ex);
                throw;
            }
        }

        private async Task ExecuteQueryAsync2(string processName, TableQuery<T> rangeQuery, Func<T, Task<bool>> filter,
            Func<T, bool> yieldData)
        {
            try
            {
                TableContinuationToken tableContinuationToken = null;
                var table = await GetTable();
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
            catch (Exception ex)
            {
                if (ex is TaskCanceledException)
                    await _log.WriteWarningAsync(
                        "Table storage: " + _tableName,
                        processName,
                        rangeQuery.FilterString ?? "[null]",
                        ex.GetBaseException().Message);
                else
                    await _log.WriteFatalErrorAsync("Table storage: " + _tableName, processName, rangeQuery.FilterString ?? "[null]", ex);
                throw;
            }
        }


        private async Task HandleException(T item, Exception ex, IEnumerable<int> notLogCodes)
        {
            var storageException = ex as StorageException;
            if (storageException != null)
            {
                if (!storageException.HandleStorageException(notLogCodes))
                    await _log.WriteFatalErrorAsync("Table storage: " + _tableName, "Insert item", AzureStorageUtils.PrintItem(item), ex);
            }
            else
            {
                await _log.WriteFatalErrorAsync("Table storage: " + _tableName, "Insert item", AzureStorageUtils.PrintItem(item), ex);
            }
        }

        private TableQuery<T> CompileTableQuery(string partition)
        {
            var filter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partition);
            return new TableQuery<T>().Where(filter);
        }
    }
}