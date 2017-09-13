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

    public class CloudStorageBatchManager
    {
        private class CloudStorageBatchExecutor
        {
            private int BatchLimitPerPartition = 100;
            private readonly IList<TableOperation> _operations;

            public CloudStorageBatchExecutor(IList<TableOperation> operations)
            {
                _operations = operations;
            }

            public async Task<IList<TableResult>> ExecuteAsync(Func<TableBatchOperation, Task<IList<TableResult>>> batchExecutionFunc)
            {
                var result = new List<TableResult>();

                IEnumerator<TableOperation> enumerator;
                using (enumerator = _operations.GetEnumerator())
                {
                    var nextBatch = GetNextBatch(enumerator).ToList();
                    while (nextBatch.Any())
                    {
                        var tblOperation = new TableBatchOperation();
                        nextBatch.ForEach(op => tblOperation.Add(op));

                        result.AddRange(await batchExecutionFunc(tblOperation));

                        nextBatch = GetNextBatch(enumerator).ToList();
                    }
                }

                return result;
            }

            private IEnumerable<TableOperation> GetNextBatch(IEnumerator<TableOperation> enumerator)
            {
                var result = new List<TableOperation>();
                var counter = 0;

                while (counter < BatchLimitPerPartition && enumerator.MoveNext())
                {
                    result.Add(enumerator.Current);
                    counter++;
                }

                return result;
            }
        }

        public static async Task<IList<TableResult>> ExecuteAsync(TableBatchOperation batchOperation,
            Func<TableBatchOperation, Task<IList<TableResult>>> batchExecutionFunc)
        {
            var result = new List<TableResult>();

            var groups = batchOperation.GroupBy(i => i.Entity.PartitionKey);
            foreach (var group in groups)
            {
                var batchExecutor = new CloudStorageBatchExecutor(group.ToList());
                result.AddRange(await batchExecutor.ExecuteAsync(batchExecutionFunc));
            }

            return result;
        }
    }
}
