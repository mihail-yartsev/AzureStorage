using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;

namespace Lykke.AzureStorage.Test
{
    [TestClass]
    public class RetryUtilsTests
    {
        #region Success

        [TestMethod]
        public void Test_that_Retry_executes_action_once_when_it_success()
        {
            // Arrange
            var executionsCount = 0;
            
            // Act
            RetryUtils.Retry(() => ++executionsCount, 10);


            // Assert
            Assert.AreEqual(1, executionsCount);
        }

        [TestMethod]
        public async Task Test_that_RetryAsync_executes_action_once_when_it_success()
        {
            // Arrange
            var executionsCount = 0;

            // Act
            await RetryUtils.RetryAsync(async () =>
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

            // Act
            await RetryUtils.RetryAsync(async () =>
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

            // Act/Assert
            Assert.ThrowsException<InvalidOperationException>(() => RetryUtils.Retry<int>(() =>
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

            // Act
            var retryTask = RetryUtils.RetryAsync(async () =>
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

            // Act
            var retryTask = RetryUtils.RetryAsync(async () =>
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

            // Act/Assert
            var result = RetryUtils.Retry<int>(() =>
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

            // Act
            await RetryUtils.RetryAsync(async () =>
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

            // Act
            var result = await RetryUtils.RetryAsync(async () =>
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
    }
}