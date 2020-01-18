using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;
using Nebula.Config;
using Xunit;
using Xunit.Abstractions;

namespace Nebula.Tests
{
    public abstract class VersionedStoreTests : IAsyncLifetime
    {
        private readonly ITestOutputHelper _testOutputHelper;

        private readonly string _databaseId;
        private readonly List<DocumentDbAccess> _dbAccesses;

        protected VersionedStoreTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
            _databaseId = "Nebula-Tests-" + Guid.NewGuid();
            _dbAccesses = new List<DocumentDbAccess>();
        }

        Task IAsyncLifetime.InitializeAsync()
        {
            return Task.CompletedTask;
        }

        async Task IAsyncLifetime.DisposeAsync()
        {
            // Drop the database.
            if (_dbAccesses.Count > 0)
            {
                var firstDbAccess = _dbAccesses[0];

                if (!firstDbAccess.IsStarted)
                {
                    return;
                }

                var documentClient = firstDbAccess.GetClient();

                await documentClient.DeleteDatabaseAsync(UriFactory.CreateDatabaseUri(_databaseId));
            }
        }

        protected ITestOutputHelper TestOutputHelper
        {
            get { return _testOutputHelper; }
        }

        protected async Task<DocumentDbAccess> CreateDbAccess(ServiceDbConfigManager configManager, int collectionRuLimit = 1000)
        {
            var dbAccess = new DocumentDbAccess(CreateDbConfig(_databaseId, collectionRuLimit), configManager);
            _dbAccesses.Add(dbAccess);

            await dbAccess.Open();

            return dbAccess;
        }

        private DocumentDbConfig CreateDbConfig(string databaseId, int collectionRuLimit)
        {
            return new DocumentDbConfigBuilder(
                    "https://localhost:8081",
                    "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")
                .SetDatabaseId(databaseId)
                .SetDefaultRus(collectionRuLimit)
                .Finish();
        }
    }
}