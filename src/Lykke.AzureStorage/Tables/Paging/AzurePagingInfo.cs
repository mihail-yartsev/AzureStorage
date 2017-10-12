using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Lykke.AzureStorage.Tables.Paging
{
    public class AzurePagingInfo : PagingInfo
    {
        public TableContinuationToken NextToken
        {
            get => _deserializeToken(NextPage);
            set => NextPage = _serializeToken(value);
        }

        public IReadOnlyList<TableContinuationToken> PreviousTokens
        {
            get => PreviousPages.Select(_deserializeToken).ToList();
            set => PreviousPages = value.Select(_serializeToken).ToList();
        }


        private string _serializeToken(TableContinuationToken token)
        {
            if (token == null)
                return null;

            return $"{token.NextPartitionKey}|{token.NextRowKey}|{token.NextTableName}|{token.TargetLocation}";
        }

        private TableContinuationToken _deserializeToken(string token)
        {
            if (token == null)
                return null;

            var splited = token.Split('|');

            StorageLocation? location = null;
            if (splited[3] == "Primary")
            {
                location = StorageLocation.Primary;
            }
            if (splited[3] == "Secondary")
            {
                location = StorageLocation.Secondary;
            }

            return new TableContinuationToken()
            {
                NextPartitionKey = splited[0],
                NextRowKey = splited[1],
                NextTableName = splited[2],
                TargetLocation = location
            };
        }


    }
}
