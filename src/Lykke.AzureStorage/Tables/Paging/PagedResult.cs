using System.Collections;
using System.Collections.Generic;

namespace Lykke.AzureStorage.Tables.Paging
{
    internal class PagedResult<T>: IPagedResult<T>
    {
        private readonly IEnumerable<T> _result;

        public PagingInfo PagingInfo { get; }

        public PagedResult(IEnumerable<T> items = null, PagingInfo pagingInfo = null)
        {
            _result = items ?? new List<T>();
            PagingInfo = pagingInfo;
        }

        public IEnumerator<T> GetEnumerator()
        {
            return _result.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
