using System;
using System.Collections.Generic;
using System.Linq;

namespace Lykke.AzureStorage.Tables.Paging
{
    public class PagedItems<T>
    {
        public PagedItems()
        {
            ResultList = new List<T>();
        }
        public List<T> ResultList { get; set; }
        public PagingInfo PagingInfo { get; set; }

        public PagedItems<T2> Cast<T2>()
        {
            return new PagedItems<T2>()
            {
                PagingInfo = PagingInfo,
                ResultList = ResultList?.Cast<T2>().ToList()
            };
        }

        public PagedItems<T2> Select<T2>(Func<T, T2> select)
        {
            return new PagedItems<T2>()
            {
                PagingInfo = PagingInfo,
                ResultList = ResultList?.Select(select).ToList()
            };
        }
    }
}
