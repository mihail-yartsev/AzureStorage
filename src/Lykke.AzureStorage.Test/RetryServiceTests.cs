using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;

namespace Lykke.AzureStorage.Test
{
    [TestClass]
    public class RetryServiceTests
    {
        #region Success

        [TestMethod]
        public void Test_that_Retry_executes_action_once_when_it_success()
        {
            // Arrange
            var executionsCount = 0;
            var retryService = new RetryService(e => RetryService.ExceptionFilterResult.ThrowAfterRetries);

            // Act
            retryService.Retry(() => ++executionsCount, 10);


            // Assert
            Assert.AreEqual(1, executionsCount);
        }

        [TestMethod]
        public async Task Test_that_RetryAsync_executes_action_once_when_it_success()
        {
            // Arrange
            var executionsCount = 0;
            var retryService = new RetryService(e => RetryService.ExceptionFilterResult.ThrowAfterRetries);

            // Act
            await retryService.RetryAsync(async () =>
            {
                ++executionsCount;
                await Task.FromResult(0);
            }, 10);
            
            // Assert
            Assert.AreEqual(1, executionsCount);
        }

        [TestMethod]
        public async Task Test_that_RetryAsyncWithResult_executes_action_once_when_it_success()
        {
            // Arrange
            var executionsCount = 0;
            var retryService = new RetryService(e => RetryService.ExceptionFilterResult.ThrowAfterRetries);

            // Act
            await retryService.RetryAsync(async () =>
            {
                ++executionsCount;
                return await Task.FromResult(0);
            }, 10);

            // Assert
            Assert.AreEqual(1, executionsCount);
        }

        #endregion

        
        #region Failure

        [TestMethod]
        public void Test_that_Retry_executes_action_retryCount_and_throws_when_it_fails()
        {
            // Arrange
            var executionsCount = 0;
            var retryService = new RetryService(e => RetryService.ExceptionFilterResult.ThrowAfterRetries);

            // Act/Assert
            Assert.ThrowsException<InvalidOperationException>(() => retryService.Retry<int>(() =>
            {
                ++executionsCount;

                throw new InvalidOperationException();
            }, 10));
            Assert.AreEqual(10, executionsCount);
        }

        [TestMethod]
        public async Task Test_that_RetryAsync_executes_action_retryCount_and_throws_when_it_fails()
        {
            // Arrange
            var executionsCount = 0;
            var retryService = new RetryService(e => RetryService.ExceptionFilterResult.ThrowAfterRetries);

            // Act
            var retryTask = retryService.RetryAsync(async () =>
            {
                ++executionsCount;

                await Task.FromResult(0);

                throw new InvalidOperationException();
            }, 10);

            // Assert
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => retryTask);
            Assert.AreEqual(10, executionsCount);
        }

        [TestMethod]
        public async Task Test_that_RetryAsyncWithResult_executes_action_retryCount_and_throws_when_it_fails()
        {
            // Arrange
            var executionsCount = 0;
            var retryService = new RetryService(e => RetryService.ExceptionFilterResult.ThrowAfterRetries);

            // Act
            var retryTask = retryService.RetryAsync(async () =>
            {
                ++executionsCount;

                return await Task.FromException<int>(new InvalidOperationException());
            }, 10);

            // Assert
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => retryTask);
            Assert.AreEqual(10, executionsCount);
        }

        #endregion


        #region Restoring after failure

        [TestMethod]
        public void Test_that_Retry_executes_action_until_it_restores_after_failure()
        {
            // Arrange
            var executionsCount = 0;
            var retryService = new RetryService(e => RetryService.ExceptionFilterResult.ThrowAfterRetries);

            // Act/Assert
            var result = retryService.Retry(() =>
            {
                ++executionsCount;

                if (executionsCount < 5)
                {
                    throw new InvalidOperationException();
                }

                return 4;
            }, 10);

            Assert.AreEqual(5, executionsCount);
            Assert.AreEqual(4, result);
        }

        [TestMethod]
        public async Task Test_that_RetryAsync_executes_action_until_it_restores_after_failure()
        {
            // Arrange
            var executionsCount = 0;
            var retryService = new RetryService(e => RetryService.ExceptionFilterResult.ThrowAfterRetries);

            // Act
            await retryService.RetryAsync(async () =>
            {
                ++executionsCount;

                await Task.FromResult(0);

                if (executionsCount < 5)
                {
                    throw new InvalidOperationException();
                }

            }, 10);

            // Assert
            Assert.AreEqual(5, executionsCount);
        }

        [TestMethod]
        public async Task Test_that_RetryAsyncWithResult_executes_action_until_it_restores_after_failure()
        {
            // Arrange
            var executionsCount = 0;
            var retryService = new RetryService(e => RetryService.ExceptionFilterResult.ThrowAfterRetries);

            // Act
            var result = await retryService.RetryAsync(async () =>
            {
                ++executionsCount;

                if (executionsCount < 5)
                {
                    return await Task.FromException<int>(new InvalidOperationException());
                }

                return await Task.FromResult(4);
            }, 10);

            // Assert
            Assert.AreEqual(5, executionsCount);
            Assert.AreEqual(4, result);
        }


        #endregion


        #region Throw immediately afteer failure

        [TestMethod]
        public void Test_that_Retry_executes_action_once_and_throws_when_it_fails_and_filterreturns_ThrowImmediately()
        {
            // Arrange
            var executionsCount = 0;
            var retryService = new RetryService(e => RetryService.ExceptionFilterResult.ThrowImmediately);

            // Act/Assert
            Assert.ThrowsException<InvalidOperationException>(() => retryService.Retry<int>(() =>
            {
                ++executionsCount;

                throw new InvalidOperationException();
            }, 10));
            Assert.AreEqual(1, executionsCount);
        }

        [TestMethod]
        public async Task Test_that_RetryAsync_executes_action_once_and_throws_when_it_fails_and_filterreturns_ThrowImmediately()
        {
            // Arrange
            var executionsCount = 0;
            var retryService = new RetryService(e => RetryService.ExceptionFilterResult.ThrowImmediately);

            // Act
            var retryTask = retryService.RetryAsync(async () =>
            {
                ++executionsCount;

                await Task.FromResult(0);

                throw new InvalidOperationException();
            }, 1);

            // Assert
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => retryTask);
            Assert.AreEqual(1, executionsCount);
        }

        [TestMethod]
        public async Task Test_that_RetryAsyncWithResult_executes_action_once_and_throws_when_it_fails_and_filterreturns_ThrowImmediately()
        {
            // Arrange
            var executionsCount = 0;
            var retryService = new RetryService(e => RetryService.ExceptionFilterResult.ThrowImmediately);

            // Act
            var retryTask = retryService.RetryAsync(async () =>
            {
                ++executionsCount;

                return await Task.FromException<int>(new InvalidOperationException());
            }, 1);

            // Assert
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => retryTask);
            Assert.AreEqual(1, executionsCount);
        }

        #endregion
    }
}