using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Nebula.Config;
using Nebula.Versioned;
using Newtonsoft.Json;
using Xunit;
using Xunit.Abstractions;

namespace Nebula.Tests
{
    public class VersionedStorePerformanceTests : VersionedStoreTests
    {
        private readonly ServiceDbConfigManager _configManager;

        public VersionedStorePerformanceTests(ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        {
            _configManager = new ServiceDbConfigManager("TestService");
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void TestSingleLargeDocumentPerformance()
        {
            // Test the performance of writing and reading a single large, complex document.
            //
            // CosmosDb emulator setup
            // - 1000 RU/s collection.
            // - Rate limiting.
            //
            // Current performance
            // - Write: 0.5sec
            // - Read: 0.9sec

            var store = await StartNebula(dbAccess => new LargeDocumentStore(dbAccess));

            var document = JsonConvert.DeserializeObject<LargeDocument>(File.ReadAllText("TestData/LargeDocument.json"));
            document.Id = Guid.NewGuid();

            Stopwatch sw = new Stopwatch();
            sw.Start();

            await store.UpsertDocument(document);
            TestOutputHelper.WriteLine("Write={0}", sw.Elapsed);

            sw.Restart();

            var result = await store.GetLargeDocument(document.Id);
            TestOutputHelper.WriteLine("Read={0}", sw.Elapsed);

            Assert.NotNull(result);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void TestSingleLargeDocumentWithMultipleVersionsPerformance()
        {
            // Test the performance of writing and reading a single document with multiple versions.
            //
            // CosmosDb emulator setup
            // - 1000 RU/s collection.
            // - Rate limiting.
            //
            // Current performance
            // - Write: 60sec
            // - Read: 2.6sec

            const int numberOfVersions = 20;

            var store = await StartNebula(dbAccess => new LargeDocumentStore(dbAccess));

            var document = JsonConvert.DeserializeObject<LargeDocument>(File.ReadAllText("TestData/LargeDocument.json"));
            document.Id = Guid.NewGuid();

            Stopwatch sw = new Stopwatch();
            sw.Start();

            // The same document is stored multiple times to simulate multiple versions.
            for (var i = 0; i < numberOfVersions; i++)
            {
                await store.UpsertDocument(document);
            }

            TestOutputHelper.WriteLine("Write={0}", sw.Elapsed);

            sw.Restart();

            var result = await store.GetLargeDocument(document.Id);
            TestOutputHelper.WriteLine("Read={0}", sw.Elapsed);

            Assert.NotNull(result);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void TestLargeDocumentAsAttachmentWithMultipleVersionsPerformance()
        {
            // Test the performance of writing and reading a single document with multiple versions and attachment data.
            //
            // CosmosDb emulator setup
            // - 1000 RU/s collection.
            // - Rate limiting.
            //
            // Current performance
            // - Write: 1.9sec
            // - Read: 0.1sec

            const int numberOfVersions = 20;

            var store = await StartNebula(dbAccess => new LargeDocumentStore(dbAccess));

            var attachment = JsonConvert.DeserializeObject<LargeDocument>(File.ReadAllText("TestData/LargeDocument.json"));

            var document = new SmallDocumentWithLargeAttachment();
            document.Id = Guid.NewGuid();

            Stopwatch sw = new Stopwatch();
            sw.Start();

            // The same document is stored multiple times to simulate multiple versions.
            for (var i = 0; i < numberOfVersions; i++)
            {
                await store.UpsertDocument(document, attachment);
            }

            TestOutputHelper.WriteLine("Write={0}", sw.Elapsed);

            sw.Restart();

            var result = await store.GetSmallDocumentAttachment(document.Id);
            TestOutputHelper.WriteLine("Read={0}", sw.Elapsed);

            Assert.NotNull(result);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void TestLargeNumberOfVersionsPerformance()
        {
            // Test the performance of writing and reading a single document with a large number of versions.
            //
            // CosmosDb emulator setup
            // - 2000 RU/s collection
            // - Rate limiting.
            //
            // Current performance
            // - 2nd write: 0.04 sec
            // - 1000th write: 0.04 sec
            // - Read: 0.04 sec
            //
            // The current implementation does not work when thrashed with lower RU/s due to the RU cost of
            // the read and write operations. There is a timeout waiting for retries. An improved
            // implementation should be considered to lower the cost of versioned read and write against
            // latest documents. E.g., a @latest flag would solve this issue.

            const int collectionRuLimit = 2000;
            const int numberOfVersions = 1000;

            var store = await StartNebula(dbAccess => new LargeDocumentStore(dbAccess), collectionRuLimit);

            var attachment = JsonConvert.DeserializeObject<LargeDocument>(File.ReadAllText("TestData/LargeDocument.json"));

            var document = new SmallDocumentWithLargeAttachment();
            document.Id = Guid.NewGuid();

            Stopwatch sw = new Stopwatch();
            sw.Start();

            for (var i = 0; i < numberOfVersions; i++)
            {
                sw.Restart();

                await store.UpsertDocument(document, attachment);

                TestOutputHelper.WriteLine("Write={0}", sw.Elapsed);
            }

            var result = await store.GetSmallDocument(document.Id);
            TestOutputHelper.WriteLine("Read={0}", sw.Elapsed);

            Assert.NotNull(result);
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

        private class LargeDocumentStore : VersionedDocumentStore
        {
            private readonly DocumentTypeMapping<LargeDocument> _largeMapping;
            private readonly DocumentTypeMapping<SmallDocumentWithLargeAttachment> _smallMapping;
            private readonly AttachmentTypeMapping<SmallDocumentWithLargeAttachment, LargeDocument> _attachmentMapping;

            private readonly DocumentStoreConfig _config;
            private readonly IVersionedDocumentStoreClient _client;

            public LargeDocumentStore(IDocumentDbAccessProvider dbAccessProvider) : base(dbAccessProvider, false)
            {
                var config = new DocumentStoreConfigBuilder("LargeStore");

                var largeDocumentType = config.AddDocument("LargeDocument");

                var attachmentDocumentType = config.AddDocument("AttachmentDocument");
                var attachmentType = attachmentDocumentType.AddAttachment("Attachment");

                _largeMapping = config.AddDocumentMapping<LargeDocument>(largeDocumentType.Name)
                    .SetIdMapper(x => x.Id.ToString())
                    .SetPartitionMapper(x => x.Id.ToString())
                    .Finish();

                _smallMapping = config.AddDocumentMapping<SmallDocumentWithLargeAttachment>(attachmentDocumentType.Name)
                    .SetIdMapper(x => x.Id.ToString())
                    .SetPartitionMapper(x => x.Id.ToString())
                    .Finish();

                _attachmentMapping = _smallMapping.AddAttachmentMapping<LargeDocument>(attachmentType.Name)
                    .Finish();

                _config = config.Finish();
                _client = CreateStoreLogic(DbAccess, _config);

                DbAccess.ConfigRegistry.RegisterStoreConfigSource(this);
            }

            protected override DocumentStoreConfig StoreConfig
            {
                get { return _config; }
            }

            protected override IVersionedDocumentStoreClient StoreClient
            {
                get { return _client; }
            }

            public async Task<VersionedDocumentReadResult<LargeDocument>> GetLargeDocument(Guid id)
            {
                var result = await StoreClient.GetDocumentAsync(id.ToString(), _largeMapping, null);

                return result;
            }

            public async Task<VersionedDocumentReadResult<SmallDocumentWithLargeAttachment>> GetSmallDocument(Guid id)
            {
                var result = await StoreClient.GetDocumentAsync(id.ToString(), _smallMapping, null);

                return result;
            }

            public async Task<LargeDocument> GetSmallDocumentAttachment(Guid id)
            {
                var result = await StoreClient.GetDocumentAsync(id.ToString(), _smallMapping, null);

                return await StoreClient.GetAttachmentAsync(result.Document, result.Metadata.Version, _attachmentMapping);
            }

            public async Task UpsertDocument(LargeDocument document)
            {
                await StoreClient.UpsertDocumentAsync(document, _largeMapping, new OperationOptions());
            }

            public async Task UpsertDocument(SmallDocumentWithLargeAttachment doc, LargeDocument attachment)
            {
                var upsertResult = await StoreClient.UpsertDocumentAsync(doc, _smallMapping, new OperationOptions());

                await StoreClient.CreateAttachmentAsync(doc, upsertResult.DocumentVersion, _attachmentMapping, attachment);
            }
        }

        private class LargeDocument
        {
            public Guid Id { get; set; }

            public IDictionary<string, object> Design { get; set; }
        }

        private class SmallDocumentWithLargeAttachment
        {
            public Guid Id { get; set; }
        }
    }
}
