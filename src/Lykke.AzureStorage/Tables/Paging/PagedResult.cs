using System.Collections;
using System.Collections.Generic;

namespace Lykke.AzureStorage.Tables.Paging
{
    public class PagedResult<T>: IEnumerable<T>
    {
        public IEnumerable<T> Result { get; }
        public PagingInfo PagingInfo { get; }

        public PagedResult(IEnumerable<T> items = null, PagingInfo pagingInfo = null)
        {
            Result = items;
            PagingInfo = pagingInfo;
        }

        public IEnumerator<T> GetEnumerator()
        {
            return Result.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
