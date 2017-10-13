using System.Collections.Generic;

namespace Lykke.AzureStorage.Tables.Paging
{
    public interface IPagedResult<out T> : IEnumerable<T>
    {
        PagingInfo PagingInfo { get; }
    }
}