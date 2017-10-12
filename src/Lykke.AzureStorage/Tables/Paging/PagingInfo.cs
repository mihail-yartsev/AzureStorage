using System.Collections.Generic;

namespace Lykke.AzureStorage.Tables.Paging
{
    public class PagingInfo
    {
        public PagingInfo()
        {
            PreviousPages = new List<string>();
        }
        /// <summary>
        /// specify null to fetch all records
        /// </summary>
        public int? ElementCount { get; set; }
        public virtual string NextPage { get; set; }
        public List<string> PreviousPages { get; set; }
        public int CurrentPage { get; set; }
        public int NavigateToPageIndex { get; set; }
    }
}
