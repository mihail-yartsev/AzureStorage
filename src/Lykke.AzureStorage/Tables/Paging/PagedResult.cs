using System.Collections;
using System.Collections.Generic;

namespace Lykke.AzureStorage.Tables.Paging
{
    public interface IPagedResult<out T> : IEnumerable<T>
    {
        PagingInfo PagingInfo { get; }
    }

    public class PagedResult<T>: IPagedResult<T>
    {
        private readonly IEnumerable<T> _result;

        public PagingInfo PagingInfo { get; }

        public PagedResult(IEnumerable<T> items = null, PagingInfo pagingInfo = null)
        {
            _result = items;
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
