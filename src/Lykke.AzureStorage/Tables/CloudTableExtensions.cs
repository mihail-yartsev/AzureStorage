using System;
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

    /// <summary>
    /// Executes batch operation for the collection of TableOperation items with the same Partition key,
    /// Doesn't depend on Azure limit
    /// </summary>
    public class CloudStorageBatchExecutor
    {
        public static int BatchLimitPerPartition = 100;

        private readonly IList<TableOperation> _operations;
        private int _batchCounter;

        public CloudStorageBatchExecutor(IList<TableOperation> operations)
        {
            if (operations.GroupBy(op => op.Entity.PartitionKey).Count() > 1)
                throw new ArgumentOutOfRangeException("There are more than 1 unique partition key in a batch");

            _operations = operations;
            _batchCounter = 0;
        }

        public async Task<IList<TableResult>> ExecuteAsync(Func<TableBatchOperation, Task<IList<TableResult>>> batchExecutionFunc)
        {
            var result = new List<TableResult>();

            var nextBatch = GetNextBatch().ToList();
            while (nextBatch.Any())
            {
                var tblOperation = new TableBatchOperation();
                nextBatch.ForEach(op => tblOperation.Add(op));

                result.AddRange(await batchExecutionFunc(tblOperation));

                _batchCounter++;

                nextBatch = GetNextBatch().ToList();
            }

            return result;
        }

        private IEnumerable<TableOperation> GetNextBatch()
        {
            return _operations.Skip(_batchCounter * BatchLimitPerPartition)
                .Take(BatchLimitPerPartition);
        }

    }

    public class CloudStorageBatchManager
    {
        public static async Task<IList<TableResult>> ExecuteAsync(TableBatchOperation batchOperation,
            Func<TableBatchOperation, Task<IList<TableResult>>> batchExecutionFunc)
        {
            var result = new List<TableResult>();

            var groups = batchOperation.GroupBy(i => i.Entity.PartitionKey).ToList();
            foreach (var group in groups)
            {
                var batchExecutor = new CloudStorageBatchExecutor(group.ToList());
                result.AddRange(await batchExecutor.ExecuteAsync(batchExecutionFunc));
            }

            return result;
        }
    }
}
