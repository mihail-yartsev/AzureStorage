using System;
using System.Threading.Tasks;

namespace Lykke.AzureStorage
{
    internal class RetrySrvice
    {
        public enum ExceptionFilterResult
        {
            ThrowImmediately,
            ThrowAfterRetries
        }

        private readonly Func<Exception, ExceptionFilterResult> _exceptionFilter;

        public RetrySrvice(Func<Exception, ExceptionFilterResult> exceptionFilter)
        {
            _exceptionFilter = exceptionFilter;
        }

        public TResult Retry<TResult>(Func<TResult> func, int retryCount)
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
                catch(Exception ex)
                {
                    switch (_exceptionFilter(ex))
                    {
                        case ExceptionFilterResult.ThrowImmediately:
                            throw;

                        case ExceptionFilterResult.ThrowAfterRetries:
                            if (++i >= retryCount)
                            {
                                throw;
                            }
                            break;

                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }
        }

        public async Task RetryAsync(Func<Task> func, int retryCount)
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
                catch (Exception ex)
                {
                    switch (_exceptionFilter(ex))
                    {
                        case ExceptionFilterResult.ThrowImmediately:
                            throw;

                        case ExceptionFilterResult.ThrowAfterRetries:
                            if (++i >= retryCount)
                            {
                                throw;
                            }
                            break;

                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }
        }

        public async Task<TResult> RetryAsync<TResult>(Func<Task<TResult>> func, int retryCount)
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
                catch (Exception ex)
                {
                    switch (_exceptionFilter(ex))
                    {
                        case ExceptionFilterResult.ThrowImmediately:
                            throw;

                        case ExceptionFilterResult.ThrowAfterRetries:
                            if (++i >= retryCount)
                            {
                                throw;
                            }
                            break;

                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }
        }
    }
}