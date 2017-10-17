using System.Threading.Tasks;
using Lykke.SettingsReader;

namespace Lykke.AzureStorage.Test.Mocks
{
    internal class ConnStringReloadingManagerMock : IReloadingManager<string>
    {
        public bool HasLoaded => true;
        public string CurrentValue => _connectionString;

        private readonly string _connectionString;

        public ConnStringReloadingManagerMock(string connectionString)
        {
            _connectionString = connectionString;
        }

        public Task<string> Reload()
        {
            return Task.FromResult(_connectionString);
        }
    }
}