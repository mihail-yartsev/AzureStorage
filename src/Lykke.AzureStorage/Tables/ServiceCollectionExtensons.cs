using System;

using AzureStorage;
using AzureStorage.Tables;

using Common.Log;

using Lykke.SettingsReader;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.WindowsAzure.Storage.Table;

using AzureStorage.Tables.Decorators;

namespace Lykke.AzureStorage.Tables
{
    public static class ServiceCollectionExtensons
    {
        public static INoSQLTableStorage<TTableEntity> CreateTableStorage<TTableEntity>(
            this IServiceProvider serviceProvider,
            IReloadingManager<string> connectionStringManager,
            string tableName,
            ILog log = null,
            TimeSpan? maxExecutionTimeout = null)
            where TTableEntity : class, ITableEntity, new()
        {
            log = log ?? serviceProvider.GetService<ILog>();

            return new ReloadingConnectionStringOnFailureAzureTableStorageDecorator<TTableEntity>(
                async () => new AzureTableStorage<TTableEntity>(await connectionStringManager.Reload(), tableName, log, maxExecutionTimeout)
            );
        }

        public static IServiceCollection AddTableStorage<TTableEntity>(
            this IServiceCollection services,
            IReloadingManager<string> connectionStringManager,
            string tableName,
            ILog log = null,
            TimeSpan? maxExecutionTimeout = null)

            where TTableEntity : class, ITableEntity, new()
        {
            services.AddSingleton<INoSQLTableStorage<TTableEntity>>(
                x => x.CreateTableStorage<TTableEntity>(connectionStringManager, tableName, log, maxExecutionTimeout)
            );

            return services;
        }
    }
}
