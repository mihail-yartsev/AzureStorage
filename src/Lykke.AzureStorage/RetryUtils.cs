using System;
using System.Threading.Tasks;

namespace Lykke.AzureStorage
{
    internal static class RetryUtils
    {
        public static TResult Retry<TResult>(Func<TResult> func, int retryCount)
        {
            if (retryCount < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(retryCount), retryCount, "Value should be greater than 0");
            }

            var i = 0;

            while (true)
            {
                try
                {
                    return func();
                }
                catch
                {
                    if (++i >= retryCount)
                    {
                        throw;
                    }
                }
            }
        }

        public static async Task RetryAsync(Func<Task> func, int retryCount)
        {
            if (retryCount < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(retryCount), retryCount, "Value should be greater than 0");
            }

            var i = 0;

            while (true)
            {
                try
                {
                    await func();

                    return;
                }
                catch
                {
                    if (++i >= retryCount)
                    {
                        throw;
                    }
                }
            }
        }

        public static async Task<TResult> RetryAsync<TResult>(Func<Task<TResult>> func, int retryCount)
        {
            if (retryCount < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(retryCount), retryCount, "Value should be greater than 0");
            }

            var i = 0;

            while (true)
            {
                try
                {
                    return await func();
                }
                catch
                {
                    if (++i >= retryCount)
                    {
                        throw;
                    }
                }
            }
        }
    }
}