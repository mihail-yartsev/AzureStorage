﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Lykke.AzureStorage.Tables
{
    public static class CloudTableExtensions
    {
        public static async Task<IList<TableResult>> ExecuteLimitSafeBatchAsync(this CloudTable table, TableBatchOperation batchOperation)
        {
            return await CloudStorageBatchManager.ExecuteAsync(batchOperation, table.ExecuteBatchAsync);
        }

        public static async Task<IList<TableResult>> ExecuteLimitSafeBatchAsync(this CloudTable table,
            TableBatchOperation batchOperation, TableRequestOptions requestOptions, OperationContext operationContext)
        {
            return await CloudStorageBatchManager.ExecuteAsync(batchOperation,
                op => table.ExecuteBatchAsync(op, requestOptions, operationContext));
        }

        public static async Task<IList<TableResult>> ExecuteLimitSafeBatchAsync(this CloudTable table,
            TableBatchOperation batchOperation, TableRequestOptions requestOptions, OperationContext operationContext,
            CancellationToken cancellationToken)
        {
            return await CloudStorageBatchManager.ExecuteAsync(batchOperation, op =>
                table.ExecuteBatchAsync(op, requestOptions, operationContext, cancellationToken));
        }
    }

    public class CloudStorageBatchManager
    {
        public static async Task<IList<TableResult>> ExecuteAsync(TableBatchOperation batchOperation,
            Func<TableBatchOperation, Task<IList<TableResult>>> batchExecutionFunc)
        {
            var result = new List<TableResult>();

            var groups = batchOperation.GroupBy(i => i.Entity.PartitionKey);
            foreach (var group in groups)
            {
                result.AddRange(await ExecuteSinglePartitionAsync(group, batchExecutionFunc));
            }

            return result;
        }

        private const int BatchLimitPerPartition = 100;

        private static async Task<IList<TableResult>> ExecuteSinglePartitionAsync(IEnumerable<TableOperation> operations, 
            Func<TableBatchOperation, Task<IList<TableResult>>> batchExecutionFunc)
        {
            var result = new List<TableResult>();

            using (var enumerator = operations.GetEnumerator())
            {
                while (true)
                {
                    var batchOperations = GetNextBatchOperations(enumerator);

                    if (!batchOperations.Any())
                    {
                        return result;
                    }

                    result.AddRange(await batchExecutionFunc(batchOperations));
                }
            }
        }

        private static TableBatchOperation GetNextBatchOperations(IEnumerator<TableOperation> enumerator)
        {
            var batchOperations = new TableBatchOperation();

            while (enumerator.MoveNext() && batchOperations.Count < BatchLimitPerPartition)
            {
                batchOperations.Add(enumerator.Current);
            }

            return batchOperations;
        }
    }
}
