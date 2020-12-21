using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Nebula.Config;
using Xunit;
using Xunit.Abstractions;

namespace Nebula.Tests.Versioned
{
    public class VersionedStoreQueryIntegrationTests : VersionedStoreTests, IAsyncLifetime
    {
        private TestDocumentStore _store;

        public VersionedStoreQueryIntegrationTests(ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        {
        }

        async Task IAsyncLifetime.InitializeAsync()
        {
            await SetupStore();
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void GetByIdWithSingleVersion()
        {
            var docs = await StoreDocument(1);
            var doc1 = docs[0];

            await SetupIrrelevantDocs();

            var result = await _store.GetById(doc1.Id);
            Assert.NotNull(result);
            Assert.Equal(doc1.Id, result.Document.Id);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void GetByIdWithMultipleVersions()
        {
            var docs = await StoreDocument(5);
            var doc1 = docs[0];

            await SetupIrrelevantDocs();

            var result = await _store.GetById(doc1.Id);
            Assert.NotNull(result);
            Assert.Equal("v5", result.Document.Name);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void GetByIdWithMultipleVersionsAndLatestDeleted()
        {
            var docs = await StoreDocument(2);
            var doc1 = docs[0];
            var doc2 = docs[1];

            await DeleteDocument(doc2);

            await SetupIrrelevantDocs();

            var result = await _store.GetByIdIncludingDeleted(doc1.Id);
            Assert.NotNull(result);
            Assert.Equal("v2", result.Document.Name);
            Assert.True(result.Metadata.IsDeleted);

            result = await _store.GetById(doc1.Id);
            Assert.Null(result);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void GetByIdAndVersionWithSingleVersion()
        {
            var docs = await StoreDocument(1);
            var doc1 = docs[0];

            await SetupIrrelevantDocs();

            var result = await _store.GetById(doc1.Id, 1);
            Assert.NotNull(result);
            Assert.Equal(doc1.Id, result.Document.Id);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void GetByIdAndVersionWithMultipleVersions()
        {
            var docs = await StoreDocument(5);
            var doc1 = docs[0];

            await SetupIrrelevantDocs();

            var result = await _store.GetById(doc1.Id, 1);
            Assert.NotNull(result);
            Assert.Equal("v1", result.Document.Name);

            result = await _store.GetById(doc1.Id, 2);
            Assert.NotNull(result);
            Assert.Equal("v2", result.Document.Name);

            result = await _store.GetById(doc1.Id, 3);
            Assert.NotNull(result);
            Assert.Equal("v3", result.Document.Name);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void GetByIdAndVersionWithMultipleVersionsAndLatestDeleted()
        {
            var docs = await StoreDocument(2);
            var doc1 = docs[0];
            var doc2 = docs[1];

            await DeleteDocument(doc2);

            await SetupIrrelevantDocs();

            var result = await _store.GetById(doc1.Id, 1);
            Assert.NotNull(result);
            Assert.Equal("v1", result.Document.Name);

            result = await _store.GetById(doc1.Id, 2);
            Assert.NotNull(result);
            Assert.Equal("v2", result.Document.Name);

            result = await _store.GetById(doc1.Id, 3);
            Assert.NotNull(result);
            Assert.Equal("v2", result.Document.Name);
            Assert.True(result.Metadata.IsDeleted);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void QueryLatestWithSingleVersion()
        {
            var docs = await StoreDocument(1);

            var result = await _store.QueryLatest("[x].Name = 'v1'");
            Assert.NotEmpty(result.Loaded);
            Assert.Equal(docs[0].Id, result.Loaded.First().Document.Id);

            result = await _store.QueryLatest("[x].Name != 'v1'");
            Assert.Empty(result.Loaded);
        }

        [Theory]
        [InlineData("[x].Name = 'v1'")]
        [InlineData("[x].Name = 'v2'")]
        [Trait("Category", "Integration")]
        public async void QueryLatestDoesNotMatchOldVersions(string query)
        {
            await StoreDocument(3);

            var result = await _store.QueryLatest(query);
            Assert.Empty(result.Loaded);
        }

        [Theory]
        [InlineData("true AND [x].Name = 'v1'")]
        [InlineData("(true) AND ([x].Name = 'v1')")]
        [InlineData("[x].Name = 'v1' AND true")]
        [Trait("Category", "Integration")]
        public async void QueryLatestWithIntersectDoesNotMatchOldVersions(string query)
        {
            await StoreDocument(3);

            var result = await _store.QueryLatest(query);
            Assert.Empty(result.Loaded);
        }

        [Theory]
        [InlineData("[x].Name = 'v2'")]
        [InlineData("[x].Name = 'v1' OR false")]
        [InlineData("[x].Name = 'v2' OR false")]
        [InlineData("[x].Name = 'v1' OR [x].Name = 'v2' OR false")]
        [InlineData("[x].Name = 'v2' OR [x].Name = 'v3'")]
        [InlineData("[x].Name = 'v1' OR [x].Name = 'v1'")]
        [InlineData("false OR [x].Name = 'v1'")]
        [InlineData("(false) OR ([x].Name = 'v1')")]
        [Trait("Category", "Integration")]
        public async void QueryLatestWithUnionDoesNotMatchOldVersions(string query)
        {
            await StoreDocument(4);

            var result = await _store.QueryLatest(query);
            Assert.Empty(result.Loaded);
        }

        [Theory]
        [InlineData("true")]
        [InlineData("false OR true OR false")]
        [InlineData("[x].Name = 'v3' OR [x].Name = 'v4' OR [x].Name = 'v5'")]
        [InlineData("[x].Name != 'v99'")]
        [Trait("Category", "Integration")]
        public async void QueryLatestWithLatestDeleted(string query)
        {
            var docs1 = await StoreDocument(3);
            var docs2 = await StoreDocument(4);
            var docs3 = await StoreDocument(5);

            await DeleteDocument(docs1[0]);
            await DeleteDocument(docs3[0]);

            var result = await _store.QueryLatestIncludingDeleted(query);
            Assert.NotNull(result);
            Assert.Equal(3, result.Loaded.Count);
            Assert.Empty(result.Failed);

            var doc1 = result.Loaded.SingleOrDefault(d => d.Document.Id == docs1[0].Id);
            Assert.NotNull(doc1);
            Assert.Equal("v3", doc1.Document.Name);
            Assert.True(doc1.Metadata.IsDeleted);

            var doc2 = result.Loaded.SingleOrDefault(d => d.Document.Id == docs2[0].Id);
            Assert.NotNull(doc2);
            Assert.Equal("v4", doc2.Document.Name);
            Assert.False(doc2.Metadata.IsDeleted);

            var doc3 = result.Loaded.SingleOrDefault(d => d.Document.Id == docs3[0].Id);
            Assert.NotNull(doc3);
            Assert.Equal("v5", doc3.Document.Name);
            Assert.True(doc3.Metadata.IsDeleted);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void QueryAllLatestWithSingleVersion()
        {
            await StoreDocument(1);

            var result = await _store.QueryAll();
            Assert.NotNull(result);
            Assert.Equal(1, result.Count);
            Assert.Equal("v1", result[0].Document.Name);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void QueryAllLatestWithMultipleVersionsOfSingleDocument()
        {
            var docs = await StoreDocument(3);

            var result = await _store.QueryAll();
            Assert.NotNull(result);
            Assert.Equal(1, result.Count);

            Assert.Equal(docs[0].Id, result[0].Document.Id);
            Assert.Equal("v3", result[0].Document.Name);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void QueryAllLatestWithMultipleVersionsOfMultipleDocuments()
        {
            var docs1 = await StoreDocument(3);
            var docs2 = await StoreDocument(4);

            var result = await _store.QueryAll();
            Assert.NotNull(result);
            Assert.Equal(2, result.Count);

            var doc1 = result.SingleOrDefault(d => d.Document.Id == docs1[0].Id);
            Assert.NotNull(doc1);
            Assert.Equal("v3", doc1.Document.Name);

            var doc2 = result.SingleOrDefault(d => d.Document.Id == docs2[0].Id);
            Assert.NotNull(doc2);
            Assert.Equal("v4", doc2.Document.Name);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void QueryAllLatestWithLatestDeleted()
        {
            var docs1 = await StoreDocument(3);
            var docs2 = await StoreDocument(4);
            var docs3 = await StoreDocument(5);

            await DeleteDocument(docs1[0]);
            await DeleteDocument(docs3[0]);

            var result = await _store.QueryAllIncludingDeleted();
            Assert.NotNull(result);
            Assert.Equal(3, result.Count);

            var doc1 = result.SingleOrDefault(d => d.Document.Id == docs1[0].Id);
            Assert.NotNull(doc1);
            Assert.Equal("v3", doc1.Document.Name);
            Assert.True(doc1.Metadata.IsDeleted);

            var doc2 = result.SingleOrDefault(d => d.Document.Id == docs2[0].Id);
            Assert.NotNull(doc2);
            Assert.Equal("v4", doc2.Document.Name);
            Assert.False(doc2.Metadata.IsDeleted);

            var doc3 = result.SingleOrDefault(d => d.Document.Id == docs3[0].Id);
            Assert.NotNull(doc3);
            Assert.Equal("v5", doc3.Document.Name);
            Assert.True(doc3.Metadata.IsDeleted);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void GetByIdsWithSingleVersion()
        {
            var docs = await StoreDocument(1);

            var result = await _store.GetByIds(docs.Select(d => d.Id));
            Assert.NotNull(result);
            Assert.Equal(1, result.Loaded.Count);
            Assert.Empty(result.Missing);
            Assert.Empty(result.Failed);
            Assert.Equal("v1", result.Loaded[0].Document.Name);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void GetByIdsWithMultipleVersionsOfSingleDocument()
        {
            var docs = await StoreDocument(3);

            var result = await _store.GetByIds(docs.Select(d => d.Id));
            Assert.NotNull(result);
            Assert.Equal(1, result.Loaded.Count);
            Assert.Empty(result.Missing);
            Assert.Empty(result.Failed);

            Assert.Equal(docs[0].Id, result.Loaded[0].Document.Id);
            Assert.Equal("v3", result.Loaded[0].Document.Name);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void GetByIdsWithMultipleVersionsOfMultipleDocuments()
        {
            var docs1 = await StoreDocument(3);
            var docs2 = await StoreDocument(4);

            var result = await _store.GetByIds(docs1.Concat(docs2).Select(d => d.Id));
            Assert.NotNull(result);
            Assert.Equal(2, result.Loaded.Count);
            Assert.Empty(result.Missing);
            Assert.Empty(result.Failed);

            var doc1 = result.Loaded.SingleOrDefault(d => d.Document.Id == docs1[0].Id);
            Assert.NotNull(doc1);
            Assert.Equal("v3", doc1.Document.Name);

            var doc2 = result.Loaded.SingleOrDefault(d => d.Document.Id == docs2[0].Id);
            Assert.NotNull(doc2);
            Assert.Equal("v4", doc2.Document.Name);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void GetByIdsWitMissingIds()
        {
            var docs1 = await StoreDocument(3);
            var docs2 = await StoreDocument(4);

            var missing1 = new TestDocument();
            var missing2 = new TestDocument();
            var missingDocs = new[] { missing1, missing2 };

            var result = await _store.GetByIds(docs1.Concat(docs2).Concat(missingDocs).Select(d => d.Id));
            Assert.NotNull(result);
            Assert.Equal(2, result.Loaded.Count);
            Assert.Equal(2, result.Missing.Count);
            Assert.Empty(result.Failed);

            Assert.NotNull(result.Missing.SingleOrDefault(id => id == missing1.Id.ToString()));
            Assert.NotNull(result.Missing.SingleOrDefault(id => id == missing2.Id.ToString()));
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void GetByIdsWithLatestDeleted()
        {
            var docs1 = await StoreDocument(3);
            var docs2 = await StoreDocument(4);
            var docs3 = await StoreDocument(5);

            await DeleteDocument(docs1[0]);
            await DeleteDocument(docs3[0]);

            var result = await _store.QueryAllIncludingDeleted();
            Assert.NotNull(result);
            Assert.Equal(3, result.Count);

            var doc1 = result.SingleOrDefault(d => d.Document.Id == docs1[0].Id);
            Assert.NotNull(doc1);
            Assert.Equal("v3", doc1.Document.Name);
            Assert.True(doc1.Metadata.IsDeleted);

            var doc2 = result.SingleOrDefault(d => d.Document.Id == docs2[0].Id);
            Assert.NotNull(doc2);
            Assert.Equal("v4", doc2.Document.Name);
            Assert.False(doc2.Metadata.IsDeleted);

            var doc3 = result.SingleOrDefault(d => d.Document.Id == docs3[0].Id);
            Assert.NotNull(doc3);
            Assert.Equal("v5", doc3.Document.Name);
            Assert.True(doc3.Metadata.IsDeleted);
        }

        private async Task SetupStore()
        {
            var configManager = new ServiceDbConfigManager("TestService");
            var dbAccess = CreateDbAccess(configManager);
            var dbAccessProvider = new TestDocumentDbAccessProvider(dbAccess);

            var store = new TestDocumentStore(dbAccessProvider);

            await dbAccess.Open(new[] { store });

            _store = store;
        }

        private async Task SetupIrrelevantDocs()
        {
            var docs = new List<TestDocument>();

            for (var i = 0; i < 5; i++)
            {
                Guid docId = Guid.NewGuid();

                docs.Add(new TestDocument(docId, "v1"));

                if (i % 2 == 1)
                {
                    docs.Add(new TestDocument(docId, "v2"));
                    docs.Add(new TestDocument(docId, "v3"));
                }
            }

            await StoreDocuments(docs.ToArray());

            await DeleteDocument(docs[3]);
            await DeleteDocument(docs[4]);
        }

        private async Task<IList<TestDocument>> StoreDocument(int versions)
        {
            var docs = new List<TestDocument>();

            var docId = Guid.NewGuid();

            for (var i = 0; i < versions; i++)
            {
                var doc = new TestDocument(docId, $"v{i + 1}");
                docs.Add(doc);
            }

            await StoreDocuments(docs.ToArray());

            return docs.ToImmutableList();
        }

        private async Task StoreDocuments(params TestDocument[] documents)
        {
            foreach (var doc in documents)
            {
                await _store.Upsert(doc);
            }
        }

        private async Task DeleteDocument(TestDocument document)
        {
            await _store.Delete(document.Id);
        }
    }
}