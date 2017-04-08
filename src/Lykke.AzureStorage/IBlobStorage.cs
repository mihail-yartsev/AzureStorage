using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace AzureStorage
{
    public interface IBlobStorage
    {
        /// <summary>
        ///     Сохранить двоичный поток в контейнер
        /// </summary>
        /// <param name="container">Имя контейнера</param>
        /// <param name="key">Ключ</param>
        /// <param name="bloblStream">Поток</param>
        Task<string> SaveBlobAsync(string container, string key, Stream bloblStream, bool anonymousAccess = false);

        Task SaveBlobAsync(string container, string key, byte[] blob);

        Task<bool> HasBlobAsync(string container, string key);

        /// <summary>
        ///     Returns datetime of latest modification among all blobs
        /// </summary>
        Task<DateTime> GetBlobsLastModifiedAsync(string container);

        Task<Stream> GetAsync(string blobContainer, string key);
        Task<string> GetAsTextAsync(string blobContainer, string key);

        string GetBlobUrl(string container, string key);

        Task<IEnumerable<string>> FindNamesByPrefixAsync(string container, string prefix);


        Task<IEnumerable<string>> GetListOfBlobsAsync(string container);
        Task<IEnumerable<string>> GetListOfBlobKeysAsync(string container);

        Task DelBlobAsync(string blobContainer, string key);

        Stream this[string container, string key] { get; }
    }
}