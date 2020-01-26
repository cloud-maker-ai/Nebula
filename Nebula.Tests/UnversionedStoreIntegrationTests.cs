using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Nebula.Config;
using Nebula.Unversioned;
using Xunit;
using Xunit.Abstractions;

namespace Nebula.Tests
{
    public class UnversionedStoreIntegrationTests : VersionedStoreTests
    {
        private readonly ServiceDbConfigManager _configManager;

        public UnversionedStoreIntegrationTests(ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        {
            _configManager = new ServiceDbConfigManager("TestService");
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void TestDocumentOperations()
        {
            var store = await StartNebula(dbAccess => new DocumentStore(dbAccess));

            var docA = new Document1 { Id = Guid.NewGuid() };
            var docB = new Document1 { Id = Guid.NewGuid() };

            await store.CreateDocument1(docA);
            var docAResult1 = await store.GetDocument1(docA.Id);
            Assert.NotNull(docAResult1);
            Assert.Equal(docA.Id, docAResult1.Document.Id);

            var docBResult1 = await store.GetDocument1(docB.Id);
            Assert.Null(docBResult1);

            await Task.Delay(1000);
            await store.UpdateDocument1(docA);
            var docAResult2 = await store.GetDocument1(docA.Id);
            Assert.NotNull(docAResult2);
            Assert.True(docAResult2.Metadata.CreatedTime == docAResult1.Metadata.CreatedTime);
            Assert.True(docAResult2.Metadata.ModifiedTime > docAResult1.Metadata.ModifiedTime);

            await Assert.ThrowsAsync<NebulaStoreException>(() => store.UpdateDocument1(docB));

            var docBResult2 = await store.GetDocument1(docB.Id);
            Assert.Null(docBResult2);

            await store.UpsertDocument1(docB);

            var docBResult3 = await store.GetDocument1(docB.Id);
            Assert.NotNull(docBResult3);
            Assert.Equal(docB.Id, docBResult3.Document.Id);

            await Task.Delay(1000);
            await store.UpsertDocument1(docA);
            var docAResult3 = await store.GetDocument1(docA.Id);
            Assert.NotNull(docAResult3);
            Assert.True(docAResult3.Metadata.CreatedTime == docAResult2.Metadata.CreatedTime);
            Assert.True(docAResult3.Metadata.ModifiedTime > docAResult2.Metadata.ModifiedTime);

            var allDocs = await store.GetAllDocuments1();

            Assert.Equal(2, allDocs.Count);
            Assert.NotNull(allDocs.SingleOrDefault(x => x.Document.Id == docA.Id));
            Assert.NotNull(allDocs.SingleOrDefault(x => x.Document.Id == docB.Id));

            var batchResult = await store.GetDocument1(new[] { docA.Id });
            Assert.Equal(1, batchResult.Loaded.Count);
            Assert.NotNull(batchResult.Loaded.SingleOrDefault(x => x.Document.Id == docA.Id));

            var queryResult1 = await store.GetDocument1("true");
            Assert.Equal(2, queryResult1.Loaded.Count);

            var queryResult2 = await store.GetDocument1($"[x].Id = '{docB.Id}'");
            Assert.Equal(1, queryResult2.Loaded.Count);
            Assert.NotNull(queryResult2.Loaded.SingleOrDefault(x => x.Document.Id == docB.Id));

            // Set ttl on DocB.
            await store.UpsertDocument1(docB, new OperationOptions { Ttl = 1 });
            await Task.Delay(2000);
            var docBResult4 = await store.GetDocument1(docB.Id);
            Assert.Null(docBResult4);

            await store.DeleteDocument1(docA.Id);

            var docAResult4 = await store.GetDocument1(docA.Id);
            Assert.Null(docAResult4);
        }

        private async Task<TStore> StartNebula<TStore>(
            Func<IDocumentDbAccessProvider, TStore> createStoreFunc,
            int collectionRuLimit = 1000) where TStore : IDocumentStoreConfigSource
        {
            var dbAccess = CreateDbAccess(_configManager, collectionRuLimit);
            var dbAccessProvider = new TestDocumentDbAccessProvider(dbAccess);

            var store = createStoreFunc(dbAccessProvider);

            await dbAccess.Open(new IDocumentStoreConfigSource[] { store });

            return store;
        }

        private class DocumentStore : UnversionedDocumentStore
        {
            private readonly DocumentTypeMapping<Document1> _documentMapping;
            private readonly DocumentTypeMapping<Document2> _document2Mapping;

            private readonly DocumentStoreConfig _config;
            private readonly IUnversionedDocumentStoreClient _client;

            public DocumentStore(IDocumentDbAccessProvider dbAccessProvider) : base(dbAccessProvider)
            {
                var config = new DocumentStoreConfigBuilder("Documents");

                var documentType = config.AddDocument("Document1");
                var document2Type = config.AddDocument("Document2");

                _documentMapping = config.AddDocumentMapping<Document1>(documentType.Name)
                    .SetIdMapper(x => x.Id.ToString())
                    .SetPartitionMapper(x => x.Id.ToString())
                    .Finish();

                _document2Mapping = config.AddDocumentMapping<Document2>(document2Type.Name)
                    .SetIdMapper(x => x.Id.ToString())
                    .SetPartitionMapper(x => x.Id.ToString())
                    .Finish();

                _config = config.Finish();
                _client = CreateStoreLogic(DbAccess, _config);
            }

            protected override DocumentStoreConfig StoreConfig
            {
                get { return _config; }
            }

            protected override IUnversionedDocumentStoreClient StoreClient
            {
                get { return _client; }
            }

            public async Task CreateDocument1(Document1 document)
            {
                await StoreClient.CreateDocumentAsync(document, _documentMapping, null);
            }

            public async Task UpdateDocument1(Document1 document)
            {
                await StoreClient.UpdateDocumentAsync(document, _documentMapping, null);
            }

            public async Task UpsertDocument1(Document1 document, OperationOptions options = null)
            {
                await StoreClient.UpsertDocumentAsync(document, _documentMapping, options);
            }

            public async Task DeleteDocument1(Guid id)
            {
                await StoreClient.DeleteDocumentAsync(id.ToString(), _documentMapping);
            }

            public async Task<DocumentReadResult<Document1>> GetDocument1(Guid id)
            {
                return await StoreClient.GetDocumentAsync(id.ToString(), _documentMapping);
            }

            public async Task<IList<DocumentWithMetadata<Document1>>> GetAllDocuments1()
            {
                return await StoreClient.GetDocumentsAsync(_documentMapping);
            }

            public async Task<DocumentBatchReadResult<Document1>> GetDocument1(IEnumerable<Guid> ids)
            {
                return await StoreClient.GetDocumentsAsync(ids.Select(x => x.ToString()).ToList(), _documentMapping);
            }

            public async Task<DocumentQueryResult<Document1>> GetDocument1(string query)
            {
                return await StoreClient.GetDocumentsAsync(query, _documentMapping);
            }
        }

        private class Document1
        {
            public Guid Id { get; set; }

            public string Type { get; set; } = "Doc1";
        }

        private class Document2
        {
            public Guid Id { get; set; }

            public string Type { get; set; } = "Doc2";
        }
    }
}
