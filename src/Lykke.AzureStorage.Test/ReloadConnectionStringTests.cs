using System;
using System.Linq.Expressions;
using System.Net;
using System.Threading.Tasks;

using AzureStorage;
using AzureStorage.Tables.Decorators;

using Common.Log;

using Lykke.AzureStorage.Tables;
using Lykke.SettingsReader;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

using Moq;

namespace Lykke.AzureStorage.Test
{
    [TestClass]
    public class ReloadConnectionStringTests
    {
        public void AddTableStorage_Test()
        {
            //Arrange
            var servicesMock = new Mock<IServiceCollection>();
            ServiceDescriptor addedDescriptor = null;
            servicesMock
                .Setup(x => x.Add(It.IsAny<ServiceDescriptor>()))
                .Callback<ServiceDescriptor>(item => addedDescriptor = item);

            var servicesProviderMock = new Mock<IServiceProvider>();
            Type resolvedService = null;
            servicesProviderMock
                .Setup(x => x.GetService(It.IsAny<Type>()))
                .Callback<Type>(item => resolvedService = item);

            var reloadingMock = new Mock<IReloadingManager<string>>();
            reloadingMock
                .Setup(x => x.Reload())
                .Returns(() => Task.FromResult("connectionString"));

            //Act / Assert
            servicesMock.Object.AddTableStorage<TableEntity>(reloadingMock.Object, "tabble");
            servicesMock.Verify(x => x.Add(It.IsAny<ServiceDescriptor>()), Times.Once());

            var tableSorage = addedDescriptor?.ImplementationFactory?.Invoke(servicesProviderMock.Object);
            Assert.AreEqual(typeof(ReloadingConnectionStringOnFailureAzureTableStorageDecorator<TableEntity>), tableSorage?.GetType());

            servicesProviderMock.Verify(x => x.GetService(It.IsAny<Type>()), Times.Once());
            Assert.AreEqual(typeof(ILog), resolvedService);
        }

        #region Good ConnectionString

        [TestMethod]
        public void Test_that_executes_action_once_when_is_used_good_connection_string()
        {
            // Arrange
            var mi = MockInfo<TestEntity, bool>.CreateForResult(x => x.RecordExists(It.IsAny<TestEntity>()));
            var m = mi.CreateMockForSequence(MockConfig.GoodConnectionString);

            var tableStorage = new ReloadingConnectionStringOnFailureAzureTableStorageDecorator<TestEntity>(() => Task.FromResult(m.Object));

            // Act
            tableStorage.RecordExists(It.IsAny<TestEntity>());

            // Assert
            m.Verify(foo => foo.RecordExists(It.IsAny<TestEntity>()), Times.Once);
        }

        [TestMethod]
        public async Task Test_that_async_executes_action_once_when_is_used_good_connection_string()
        {
            // Arrange
            var mi = MockInfo<TestEntity, Task>.CreateForTask(x => x.DeleteAsync(It.IsAny<TestEntity>()));
            var m = mi.CreateMockForSequence(MockConfig.GoodConnectionString);

            var tableStorage = new ReloadingConnectionStringOnFailureAzureTableStorageDecorator<TestEntity>(() => Task.FromResult(m.Object));

            // Act
            await tableStorage.DeleteAsync(It.IsAny<TestEntity>());

            // Assert
            m.Verify(foo => foo.DeleteAsync(It.IsAny<TestEntity>()), Times.Once);
        }

        [TestMethod]
        public async Task Test_that_async_with_result_executes_action_once_when_is_used_good_connection_string()
        {
            // Arrange
            var mi = MockInfo<TestEntity, Task<TestEntity>>.CreateForTaskResult(x => x.GetTopRecordAsync(It.IsAny<string>()));
            var m = mi.CreateMockForSequence(MockConfig.GoodConnectionString);

            var tableStorage = new ReloadingConnectionStringOnFailureAzureTableStorageDecorator<TestEntity>(() => Task.FromResult(m.Object));

            // Act
            await tableStorage.GetTopRecordAsync(It.IsAny<string>());

            // Assert
            m.Verify(foo => foo.GetTopRecordAsync(It.IsAny<string>()), Times.Once);
        }

        #endregion

        #region Wrong ConnectionString

        [TestMethod]
        public void Test_that_executes_action_twice_when_is_used_wrong_connection_string()
        {
            // Arrange
            var mi = MockInfo<TestEntity, bool>.CreateForResult(x => x.RecordExists(It.IsAny<TestEntity>()));
            var m = mi.CreateMockForSequence(MockConfig.WrongConnectionString);

            var tableStorage = new ReloadingConnectionStringOnFailureAzureTableStorageDecorator<TestEntity>(() => Task.FromResult(m.Object));

            // Act - Assert
            Assert.ThrowsException<StorageException>(() => tableStorage.RecordExists(It.IsAny<TestEntity>()));
            m.Verify(foo => foo.RecordExists(It.IsAny<TestEntity>()), Times.Exactly(2));
        }

        [TestMethod]
        public async Task Test_that_async_executes_action_twice_when_is_used_wrong_connection_string()
        {
            // Arrange
            var mi = MockInfo<TestEntity, Task>.CreateForTask(x => x.DeleteAsync(It.IsAny<TestEntity>()));
            var m = mi.CreateMockForSequence(MockConfig.WrongConnectionString);

            var tableStorage = new ReloadingConnectionStringOnFailureAzureTableStorageDecorator<TestEntity>(() => Task.FromResult(m.Object));

            // Act - Assert
            await Assert.ThrowsExceptionAsync<StorageException>(() => tableStorage.DeleteAsync(It.IsAny<TestEntity>()));
            m.Verify(foo => foo.DeleteAsync(It.IsAny<TestEntity>()), Times.Exactly(2));
        }

        [TestMethod]
        public async Task Test_that_async_with_result_executes_action_twice_when_is_used_wrong_connection_string()
        {
            // Arrange
            var mi = MockInfo<TestEntity, Task<TestEntity>>.CreateForTaskResult(x => x.GetTopRecordAsync(It.IsAny<string>()));
            var m = mi.CreateMockForSequence(MockConfig.WrongConnectionString);

            var tableStorage = new ReloadingConnectionStringOnFailureAzureTableStorageDecorator<TestEntity>(() => Task.FromResult(m.Object));

            // Act - Assert
            await Assert.ThrowsExceptionAsync<StorageException>(() => tableStorage.GetTopRecordAsync(It.IsAny<string>()));
            m.Verify(foo => foo.GetTopRecordAsync(It.IsAny<string>()), Times.Exactly(2));
        }

        #endregion

        #region Wrong => Good ConnectionString

        [TestMethod]
        public void Test_that_executes_action_twice_when_is_reloaded_good_connection_string()
        {
            // Arrange
            var mi = MockInfo<TestEntity, bool>.CreateForResult(x => x.RecordExists(It.IsAny<TestEntity>()));
            var m = mi.CreateMockForSequence(MockConfig.WrongConnectionString, MockConfig.GoodConnectionString);

            var tableStorage = new ReloadingConnectionStringOnFailureAzureTableStorageDecorator<TestEntity>(() => Task.FromResult(m.Object));

            // Act
            tableStorage.RecordExists(It.IsAny<TestEntity>());

            // Assert
            m.Verify(foo => foo.RecordExists(It.IsAny<TestEntity>()), Times.Exactly(2));
        }

        [TestMethod]
        public async Task Test_that_async_executes_action_twice_when_is_reloaded_good_connection_string()
        {
            // Arrange
            var mi = MockInfo<TestEntity, Task>.CreateForTask(x => x.DeleteAsync(It.IsAny<TestEntity>()));
            var m = mi.CreateMockForSequence(MockConfig.WrongConnectionString, MockConfig.GoodConnectionString);

            var tableStorage = new ReloadingConnectionStringOnFailureAzureTableStorageDecorator<TestEntity>(() => Task.FromResult(m.Object));

            // Act
            await tableStorage.DeleteAsync(It.IsAny<TestEntity>());

            // Assert
            m.Verify(foo => foo.DeleteAsync(It.IsAny<TestEntity>()), Times.Exactly(2));
        }

        [TestMethod]
        public async Task Test_that_async_with_result_executes_action_twice_when_is_reloaded_good_connection_string()
        {
            // Arrange
            var mi = MockInfo<TestEntity, Task<TestEntity>>.CreateForTaskResult(x => x.GetTopRecordAsync(It.IsAny<string>()));
            var m = mi.CreateMockForSequence(MockConfig.WrongConnectionString, MockConfig.GoodConnectionString);

            var tableStorage = new ReloadingConnectionStringOnFailureAzureTableStorageDecorator<TestEntity>(() => Task.FromResult(m.Object));

            // Act
            await tableStorage.GetTopRecordAsync(It.IsAny<string>());

            // Assert
            m.Verify(foo => foo.GetTopRecordAsync(It.IsAny<string>()), Times.Exactly(2));
        }

        #endregion

        #region Helpers

        public static class MockConfig
        {
            public const string GoodConnectionString = "good";

            public const string WrongConnectionString = "wrong";

            public static readonly Exception ExceptionIfWrongConnectionString = new StorageException(new RequestResult
            {
                HttpStatusCode = (int)HttpStatusCode.Forbidden
            }, "", null);
        }

        public class MockInfo<T, TResult> where T : ITableEntity, new()
        {
            private MockInfo(Expression<Func<INoSQLTableStorage<T>, TResult>> expr, Func<TResult> doIfGood, Func<TResult> doIfWrong)
            {
                Expr = expr;
                DoIfGood = doIfGood;
                DoIfWrong = doIfWrong;
            }

            public static MockInfo<T, Task<TT>> CreateForTaskResult<TT>(Expression<Func<INoSQLTableStorage<T>, Task<TT>>> expr)
            {
                return new MockInfo<T, Task<TT>>(
                    expr,
                    () => Task.FromResult(default(TT)),
                    () => Task.FromException<TT>(MockConfig.ExceptionIfWrongConnectionString)
                );
            }

            public static MockInfo<T, Task> CreateForTask(Expression<Func<INoSQLTableStorage<T>, Task>> expr)
            {
                return new MockInfo<T, Task>(
                    expr,
                    () => Task.CompletedTask,
                    () => Task.FromException(MockConfig.ExceptionIfWrongConnectionString)
                );
            }

            public static MockInfo<T, TT> CreateForResult<TT>(Expression<Func<INoSQLTableStorage<T>, TT>> expr)
            {
                return new MockInfo<T, TT>(
                    expr,
                    () => default(TT),
                    () => throw MockConfig.ExceptionIfWrongConnectionString
                );
            }

            public Expression<Func<INoSQLTableStorage<T>, TResult>> Expr { get; }

            public Func<TResult> DoIfGood { get; }

            public Func<TResult> DoIfWrong { get; }

            public Mock<INoSQLTableStorage<T>> CreateMockForSequence(string firstConnectionString, string secondConnectionString = null)
            {
                var ts = new Mock<INoSQLTableStorage<T>>();
                ts
                    .Setup(Expr)
                    .Returns(() =>
                    {
                        var currentConnectionString = firstConnectionString;
                        firstConnectionString = secondConnectionString ?? firstConnectionString;

                        switch (currentConnectionString)
                        {
                            case MockConfig.GoodConnectionString:
                                return DoIfGood();
                            case MockConfig.WrongConnectionString:
                                return DoIfWrong();
                            default:
                                throw new NotSupportedException();
                        }
                    });

                return ts;
            }
        }

        #endregion
    }
}