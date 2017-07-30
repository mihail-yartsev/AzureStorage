using System;
using System.Threading.Tasks;

namespace Lykke.AzureStorage
{
    internal class RetryService
    {
        public enum ExceptionFilterResult
        {
            ThrowImmediately,
            ThrowAfterRetries
        }

        private readonly TimeSpan _retryDelay;
        private readonly Func<Exception, ExceptionFilterResult> _exceptionFilter;

        public RetryService(TimeSpan retryDelay) :
            this(retryDelay, e => ExceptionFilterResult.ThrowAfterRetries)

        {
        }

        public RetryService(Func<Exception, ExceptionFilterResult> exceptionFilter) :
            this(TimeSpan.Zero, exceptionFilter)
        {
        }

        public RetryService(TimeSpan retryDelay, Func<Exception, ExceptionFilterResult> exceptionFilter)
        {
            _retryDelay = retryDelay;
            _exceptionFilter = exceptionFilter;
        }

        public TResult Retry<TResult>(Func<TResult> func, int retryCount)
        {
            if (retryCount < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(retryCount), retryCount, "Value should be greater than 0");
            }

            var remainingRetries = retryCount;

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
                            if (--remainingRetries == 0)
                            {
                                throw;
                            }
                            break;

                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    Task.Delay(_retryDelay).Wait();
                }
            }
        }

        public async Task RetryAsync(Func<Task> func, int retryCount)
        {
            if (retryCount < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(retryCount), retryCount, "Value should be greater than 0");
            }

            var remainingRetries = retryCount;

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
                            if (--remainingRetries == 0)
                            {
                                throw;
                            }
                            break;

                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    await Task.Delay(_retryDelay);
                }
            }
        }

        public async Task<TResult> RetryAsync<TResult>(Func<Task<TResult>> func, int retryCount)
        {
            if (retryCount < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(retryCount), retryCount, "Value should be greater than 0");
            }

            var remainingRetries = retryCount;

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
                            if (--remainingRetries == 0)
                            {
                                throw;
                            }
                            break;

                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    await Task.Delay(_retryDelay);
                }
            }
        }
    }
}