using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Lykke.AzureStorage;
using Microsoft.WindowsAzure.Storage;

namespace AzureStorage.Blob.Decorators
{
    /// <summary>
    /// Decorator, which adds retries functionality to the operations of the <see cref="IBlobStorage"/> implementation
    /// </summary>
    internal class RetryOnFailureAzureBlobDecorator : IBlobStorage
    {
        public Stream this[string container, string key] 
            => _retryService.Retry(() => _impl[container, key], _onGettingRetryCount);

        private readonly IBlobStorage _impl;
        private readonly int _onModificationsRetryCount;
        private readonly int _onGettingRetryCount;
        private readonly RetryService _retryService;
            
        /// <summary>
        /// Creates decorator, which adds retries functionality to the operations of the <see cref="IBlobStorage"/> implementation
        /// </summary>
        /// <param name="impl"><see cref="IBlobStorage"/> instance to which actual work will be delegated</param>
        /// <param name="onModificationsRetryCount">Retries count for write operations</param>
        /// <param name="onGettingRetryCount">Retries count for read operations</param>
        /// <param name="retryDelay">Delay before next retry. Default value is 200 milliseconds</param>

        public RetryOnFailureAzureBlobDecorator(
            IBlobStorage impl,
            int onModificationsRetryCount = 10,
            int onGettingRetryCount = 10,
            TimeSpan? retryDelay = null)
        {
            _impl = impl ?? throw new ArgumentNullException(nameof(impl));

            if (onModificationsRetryCount < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(onModificationsRetryCount), onModificationsRetryCount, "Value should be greater than 0");
            }

            if (onGettingRetryCount < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(onGettingRetryCount), onGettingRetryCount, "Value should be greater than 0");
            }

            _onModificationsRetryCount = onModificationsRetryCount;
            _onGettingRetryCount = onGettingRetryCount;
            _retryService = new RetryService(
                retryDelay: retryDelay ?? TimeSpan.FromMilliseconds(200),
                exceptionFilter: e =>
                {
                    var storageException = e as StorageException;
                    var noRetryStatusCodes = new[]
                    {
                        HttpStatusCode.Conflict,
                        HttpStatusCode.BadRequest
                    };

                    return storageException != null && noRetryStatusCodes.Contains((HttpStatusCode)storageException.RequestInformation.HttpStatusCode)
                        ? RetryService.ExceptionFilterResult.ThrowImmediately
                        : RetryService.ExceptionFilterResult.ThrowAfterRetries;
                });
        }

        public async Task<string> SaveBlobAsync(string container, string key, Stream bloblStream, bool anonymousAccess = false) 
            => await _retryService.RetryAsync(async () => await _impl.SaveBlobAsync(container, key, bloblStream, anonymousAccess), _onModificationsRetryCount);

        public async Task SaveBlobAsync(string container, string key, byte[] blob) 
            => await _retryService.RetryAsync(async () => await _impl.SaveBlobAsync(container, key, blob), _onModificationsRetryCount);

        public async Task<bool> HasBlobAsync(string container, string key) 
            => await _retryService.RetryAsync(async () => await _impl.HasBlobAsync(container, key), _onGettingRetryCount);

        public async Task<bool> CreateContainerIfNotExistsAsync(string container) 
            => await _retryService.RetryAsync(async () => await _impl.CreateContainerIfNotExistsAsync(container), _onModificationsRetryCount);

        public async Task<DateTime> GetBlobsLastModifiedAsync(string container) 
            => await _retryService.RetryAsync(async () => await _impl.GetBlobsLastModifiedAsync(container), _onGettingRetryCount);

        public async Task<Stream> GetAsync(string blobContainer, string key) 
            => await _retryService.RetryAsync(async () => await _impl.GetAsync(blobContainer, key), _onGettingRetryCount);

        public async Task<string> GetAsTextAsync(string blobContainer, string key) 
            => await _retryService.RetryAsync(async () => await _impl.GetAsTextAsync(blobContainer, key), _onGettingRetryCount);

        public string GetBlobUrl(string container, string key) 
            => _retryService.Retry(() => _impl.GetBlobUrl(container, key), _onGettingRetryCount);

        public async Task<IEnumerable<string>> FindNamesByPrefixAsync(string container, string prefix) 
            => await _retryService.RetryAsync(async () => await _impl.FindNamesByPrefixAsync(container, prefix), _onGettingRetryCount);

        public async Task<IEnumerable<string>> GetListOfBlobsAsync(string container) 
            => await _retryService.RetryAsync(async () => await _impl.GetListOfBlobsAsync(container), _onGettingRetryCount);

        public async Task<IEnumerable<string>> GetListOfBlobKeysAsync(string container) 
            => await _retryService.RetryAsync(async () => await _impl.GetListOfBlobKeysAsync(container), _onGettingRetryCount);

        public async Task DelBlobAsync(string blobContainer, string key) 
            => await _retryService.RetryAsync(async () => await _impl.DelBlobAsync(blobContainer, key), _onModificationsRetryCount);

        public async Task<string> GetMetadataAsync(string container, string key, string metaDataKey) 
            => await _retryService.RetryAsync(async () => await _impl.GetMetadataAsync(container, key, metaDataKey), _onGettingRetryCount);

        public async Task<IDictionary<string, string>> GetMetadataAsync(string container, string key) 
            => await _retryService.RetryAsync(async () => await _impl.GetMetadataAsync(container, key), _onGettingRetryCount);
    }
}
