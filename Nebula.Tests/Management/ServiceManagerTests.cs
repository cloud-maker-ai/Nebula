using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Nebula.Config;
using Nebula.Management;
using Nebula.Service;
using NSubstitute;
using Xunit;

namespace Nebula.Tests.Management
{
    public class ServiceManagerTests
    {
        [Fact]
        public async void PurgeDocumentsMultipleDocuments()
        {
            var client = Substitute.For<IDocumentClient>();

            var docs = new[]
            {
                new ServiceManager.DocumentMetadata
                {
                    Id = "1",
                    PartitionKey = "100"
                },
                new ServiceManager.DocumentMetadata
                {
                    Id = "2",
                    PartitionKey = "100"
                },
                new ServiceManager.DocumentMetadata
                {
                    Id = "3",
                    PartitionKey = "101"
                }
            };

            client.CreateDocumentQuery<ServiceManager.DocumentMetadata>(Arg.Any<Uri>(), Arg.Any<string>(), Arg.Any<FeedOptions>())
                .Returns(CreateQueryResult(docs));

            await client.DeleteDocumentAsync(Arg.Any<Uri>(), Arg.Any<RequestOptions>());

            var dbAccess = await CreateDbAccess(client);
            var manager = new ServiceManager(dbAccess);

            await manager.PurgeDocumentsAsync();
            
            await client.Received(1).DeleteDocumentAsync(
                Arg.Is<Uri>(x => x.ToString().EndsWith("/1")),
                Arg.Is<RequestOptions>(x => x.PartitionKey.ToString() == "[\"100\"]"));

            await client.Received(1).DeleteDocumentAsync(
                Arg.Is<Uri>(x => x.ToString().EndsWith("/2")),
                Arg.Is<RequestOptions>(x => x.PartitionKey.ToString() == "[\"100\"]"));

            await client.Received(1).DeleteDocumentAsync(
                Arg.Is<Uri>(x => x.ToString().EndsWith("/3")),
                Arg.Is<RequestOptions>(x => x.PartitionKey.ToString() == "[\"101\"]"));
        }

        [Fact]
        public async void PurgeDocumentsNoData()
        {
            var client = Substitute.For<IDocumentClient>();

            client.CreateDocumentQuery<ServiceManager.DocumentMetadata>(Arg.Any<Uri>(), Arg.Any<string>(), Arg.Any<FeedOptions>())
                .Returns(CreateQueryResult(new ServiceManager.DocumentMetadata[0]));

            await client.DeleteDocumentAsync(Arg.Any<Uri>(), Arg.Any<RequestOptions>());

            var dbAccess = await CreateDbAccess(client);
            var manager = new ServiceManager(dbAccess);

            await manager.PurgeDocumentsAsync();

            await client.DidNotReceive().DeleteDocumentAsync(Arg.Any<Uri>(), Arg.Any<RequestOptions>());
        }

        private async Task<DocumentDbAccess> CreateDbAccess(IDocumentClient documentClient)
        {
            var signatureGenerator = Substitute.For<IServiceConfigSignatureGenerator>();

            var configManager = new ServiceDbConfigManager("Test", signatureGenerator);

            var queryPolicy = Substitute.For<IDocumentQueryPolicy>();
            queryPolicy.GetIdSearchLimit(Arg.Any<ICollection<string>>()).Returns(1000);
            queryPolicy.IsQueryValid(Arg.Any<string>()).Returns(true);

            var dbService = Substitute.For<IDocumentDbService>();

            var documentDbAccess = new DocumentDbAccess(CreateDbConfig(), configManager, documentClient, dbService, queryPolicy);

            await documentDbAccess.Open(new IDocumentStoreConfigSource[0]);

            return documentDbAccess;
        }

        private DocumentDbConfig CreateDbConfig()
        {
            return new DocumentDbConfig("https://test", "key", "Db", "Col");
        }

        private IQueryable<ServiceManager.DocumentMetadata> CreateQueryResult(IEnumerable<ServiceManager.DocumentMetadata> docs)
        {
            return new EnumerableQuery<ServiceManager.DocumentMetadata>(docs);
        }
    }
}