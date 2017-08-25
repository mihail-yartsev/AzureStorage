using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common.Extensions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace AzureStorage.Blob
{
    public class AzureBlobStorage : IBlobStorage
    {
        private readonly CloudStorageAccount _storageAccount;
        private readonly TimeSpan _maxExecutionTime;

        public AzureBlobStorage(string connectionString, TimeSpan? maxExecutionTimeout = null)
        {
            _storageAccount = CloudStorageAccount.Parse(connectionString);
            _maxExecutionTime = maxExecutionTimeout.GetValueOrDefault(TimeSpan.FromSeconds(30));
        }

        private CloudBlobContainer GetContainerReference(string container)
        {
            var blobClient = _storageAccount.CreateCloudBlobClient();
            return blobClient.GetContainerReference(container.ToLower());
        }

        private BlobRequestOptions GetRequestOptions()
        {
            return new BlobRequestOptions
            {
                MaximumExecutionTime = _maxExecutionTime
            };
        }

        public async Task<string> SaveBlobAsync(string container, string key, Stream bloblStream, bool anonymousAccess = false)
        {
            var blockBlob = await GetBlockBlobReference(container, key, anonymousAccess);

            bloblStream.Position = 0;
            await blockBlob.UploadFromStreamAsync(bloblStream, null, GetRequestOptions(), null);

            return blockBlob.Uri.AbsoluteUri;
        }

        private async Task<CloudBlockBlob> GetBlockBlobReference(string container, string key, bool anonymousAccess)
        {
            var containerRef = GetContainerReference(container);

            if (!await containerRef.ExistsAsync(GetRequestOptions(), null))
            {
                await containerRef.CreateIfNotExistsAsync(GetRequestOptions(), null);
                if (anonymousAccess)
                {
                    BlobContainerPermissions permissions = await containerRef.GetPermissionsAsync(null, GetRequestOptions(), null);
                    permissions.PublicAccess = BlobContainerPublicAccessType.Container;
                    await containerRef.SetPermissionsAsync(permissions, null, GetRequestOptions(), null);
                }
            }

            return containerRef.GetBlockBlobReference(key);
        }

        public async Task SaveBlobAsync(string container, string key, byte[] blob)
        {
            var containerRef = GetContainerReference(container);
            await containerRef.CreateIfNotExistsAsync(GetRequestOptions(), null);

            var blockBlob = containerRef.GetBlockBlobReference(key);
            await blockBlob.UploadFromByteArrayAsync(blob, 0, blob.Length, null, GetRequestOptions(), null);
        }

        public async Task<bool> CreateContainerIfNotExistsAsync(string container)
        {
            var containerRef = GetContainerReference(container);
            return await containerRef.CreateIfNotExistsAsync(GetRequestOptions(), null);
        }

        public Task<bool> HasBlobAsync(string container, string key)
        {
            var blobRef = GetContainerReference(container).GetBlobReference(key);
            return blobRef.ExistsAsync(GetRequestOptions(), null);
        }

        public async Task<DateTime> GetBlobsLastModifiedAsync(string container)
        {
            BlobContinuationToken continuationToken = null;
            var results = new List<IListBlobItem>();
            var containerRef = GetContainerReference(container);

            do
            {
                var response = await containerRef.ListBlobsSegmentedAsync(null, false, BlobListingDetails.None, null, continuationToken, GetRequestOptions(), null);
                continuationToken = response.ContinuationToken;
                foreach (var listBlobItem in response.Results)
                {
                    if (listBlobItem is CloudBlob)
                        results.Add(listBlobItem);
                }
            } while (continuationToken != null);

            var dateTimeOffset = results.Where(x => x is CloudBlob).Max(x => ((CloudBlob)x).Properties.LastModified);

            return dateTimeOffset.GetValueOrDefault().UtcDateTime;
        }

        public async Task<Stream> GetAsync(string blobContainer, string key)
        {
            var containerRef = GetContainerReference(blobContainer);
            var blockBlob = containerRef.GetBlockBlobReference(key);

            var ms = new MemoryStream();
            await blockBlob.DownloadToStreamAsync(ms, null, GetRequestOptions(), null);
            ms.Position = 0;
            return ms;
        }

        public async Task<string> GetAsTextAsync(string blobContainer, string key)
        {
            var containerRef = GetContainerReference(blobContainer);

            var blockBlob = containerRef.GetBlockBlobReference(key);
            return await blockBlob.DownloadTextAsync(null, GetRequestOptions(), null);
        }

        public string GetBlobUrl(string container, string key)
        {
            var containerRef = GetContainerReference(container);
            var blockBlob = containerRef.GetBlockBlobReference(key);

            return blockBlob.Uri.AbsoluteUri;
        }

        public async Task<IEnumerable<string>> FindNamesByPrefixAsync(string container, string prefix)
        {
            BlobContinuationToken continuationToken = null;
            var results = new List<string>();
            var containerRef = GetContainerReference(container);

            do
            {
                var response = await containerRef.ListBlobsSegmentedAsync(null, false, BlobListingDetails.None, null, continuationToken, GetRequestOptions(), null);
                continuationToken = response.ContinuationToken;
                foreach (var listBlobItem in response.Results)
                {
                    if (listBlobItem.Uri.ToString().StartsWith(prefix))
                        results.Add(listBlobItem.Uri.ToString());
                }
            } while (continuationToken != null);

            return results;
        }

        public async Task<IEnumerable<string>> GetListOfBlobsAsync(string container)
        {
            var containerRef = GetContainerReference(container);

            BlobContinuationToken token = null;
            var results = new List<string>();
            do
            {
                var result = await containerRef.ListBlobsSegmentedAsync(null, false, BlobListingDetails.None, null, token, GetRequestOptions(), null);
                token = result.ContinuationToken;
                foreach (var listBlobItem in result.Results)
                {
                    results.Add(listBlobItem.Uri.ToString());
                }

                //Now do something with the blobs
            } while (token != null);

            return results;
        }

        public async Task<IEnumerable<string>> GetListOfBlobKeysAsync(string container)
        {
            var containerRef = GetContainerReference(container);

            BlobContinuationToken token = null;
            var results = new List<string>();
            do
            {
                var result = await containerRef.ListBlobsSegmentedAsync(null, false, BlobListingDetails.None, null, token, GetRequestOptions(), null);
                token = result.ContinuationToken;
                foreach (var listBlobItem in result.Results.OfType<CloudBlockBlob>())
                {
                    results.Add(listBlobItem.Name);
                }

                //Now do something with the blobs
            } while (token != null);

            return results;
        }

        public Task DelBlobAsync(string blobContainer, string key)
        {
            var containerRef = GetContainerReference(blobContainer);

            var blockBlob = containerRef.GetBlockBlobReference(key);
            return blockBlob.DeleteAsync(DeleteSnapshotsOption.None, null, GetRequestOptions(), null);
        }

        public Stream this[string container, string key]
        {
            get
            {
                var containerRef = GetContainerReference(container);

                var blockBlob = containerRef.GetBlockBlobReference(key);
                var ms = new MemoryStream();
                blockBlob.DownloadToStreamAsync(ms, null, GetRequestOptions(), null).RunSync();
                ms.Position = 0;
                return ms;
            }
        }

        public async Task<string> GetMetadataAsync(string container, string key, string metaDataKey)
        {
            var metadata = await GetMetadataAsync(container, key);
            if ((metadata?.Count ?? 0) == 0 || !metadata.ContainsKey(metaDataKey))
                return null;

            return metadata[metaDataKey];
        }

        public async Task<IDictionary<string, string>> GetMetadataAsync(string container, string key)
        {
            if (string.IsNullOrWhiteSpace(container) || string.IsNullOrWhiteSpace(key))
                return new Dictionary<string, string>();

            if (!await HasBlobAsync(container, key))
                return new Dictionary<string, string>();

            var containerRef = GetContainerReference(container);
            var blockBlob = containerRef.GetBlockBlobReference(key);
            await blockBlob.FetchAttributesAsync();

            return blockBlob.Metadata;
        }
    }
}