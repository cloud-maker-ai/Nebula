using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Nebula.Config;
using Nebula.Management;
using Nebula.Versioned;
using Newtonsoft.Json;
using Xunit;
using Xunit.Abstractions;

namespace Nebula.Tests
{
    public class VersionedStoreIntegrationTests : VersionedStoreTests
    {
        public VersionedStoreIntegrationTests(ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        {
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void TestMultipleServices()
        {
            var configManager1 = new ServiceDbConfigManager("TestService1");
            var dbAccess1 = await CreateDbAccess(configManager1);
            var dbAccessProvider1 = new TestDocumentDbAccessProvider(dbAccess1);

            var configManager2 = new ServiceDbConfigManager("TestService2");
            var dbAccess2 = await CreateDbAccess(configManager2);
            var dbAccessProvider2 = new TestDocumentDbAccessProvider(dbAccess2);

            var fruitStore1 = new FruitStore(dbAccessProvider1);
            var fruitStore2 = new FruitStore(dbAccessProvider2);

            var apples = new List<Apple>();

            for (int i = 0; i < 20; i++)
            {
                var gala = new Apple
                {
                    Id = Guid.NewGuid(),
                    Type = "Gala"
                };

                var fuji = new Apple
                {
                    Id = Guid.NewGuid(),
                    Type = "Fuji"
                };

                await fruitStore1.UpsertApple(gala);
                await fruitStore1.UpsertApple(fuji);

                await fruitStore2.UpsertApple(gala);
                await fruitStore2.UpsertApple(fuji);

                apples.AddRange(new[] { gala, fuji });
            }

            var r1 = await fruitStore1.GetAppleByQuery("[x].Type = 'Gala'");
            Assert.Equal(20, r1.Length);
            Assert.True(r1.All(x => x.Type == "Gala"));

            var r2 = await fruitStore1.GetAllApples();
            Assert.Equal(40, r2.Length);

            var lastApple = apples.Last();
            var r3 = await fruitStore1.GetAppleById(lastApple.Id);
            Assert.Equal(lastApple.Id, r3.Id);
            Assert.Equal(lastApple.Type, r3.Type);

            var r4 = await fruitStore1.GetAppleByIds(apples.Select(x => x.Id.ToString()));
            Assert.Equal(40, r4.Length);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void TestMultipleServiceStores()
        {
            var configManager = new ServiceDbConfigManager("TestService");
            var dbAccess = await CreateDbAccess(configManager);
            var dbAccessProvider = new TestDocumentDbAccessProvider(dbAccess);

            var fruitStore = new FruitStore(dbAccessProvider);
            var flowerStore = new FlowerStore(dbAccessProvider);

            var apples = new List<Apple>();
            var daisies = new List<Daisy>();

            for (int i = 0; i < 10; i++)
            {
                var gala = new Apple
                {
                    Id = Guid.NewGuid(),
                    Type = "Gala"
                };

                var fuji = new Apple
                {
                    Id = Guid.NewGuid(),
                    Type = "Fuji"
                };

                await fruitStore.UpsertApple(gala);
                await fruitStore.UpsertApple(fuji);

                apples.AddRange(new[] { gala, fuji });

                var daisy = new Daisy
                {
                    Id = Guid.NewGuid(),
                    Colour = "Red"
                };

                await flowerStore.Upsert(daisy);

                daisies.Add(daisy);
            }

            var r1 = await fruitStore.GetAppleByQuery("[x].Type = 'Gala'");
            Assert.Equal(10, r1.Length);
            Assert.True(r1.All(x => x.Type == "Gala"));

            var r2 = await fruitStore.GetAllApples();
            Assert.Equal(20, r2.Length);

            var lastApple = apples.Last();
            var r3 = await fruitStore.GetAppleById(lastApple.Id);
            Assert.Equal(lastApple.Id, r3.Id);
            Assert.Equal(lastApple.Type, r3.Type);

            var r4 = await fruitStore.GetAppleByIds(apples.Select(x => x.Id.ToString()));
            Assert.Equal(20, r4.Length);

            var lastDaisy = daisies.Last();
            var r5 = await flowerStore.GetById(lastDaisy.Id);
            Assert.Equal(lastDaisy.Id, r5.Id);
            Assert.Equal(lastDaisy.Colour, r5.Colour);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void TestMultipleStoreDocumentTypes()
        {
            var configManager = new ServiceDbConfigManager("TestService");
            var dbAccess = await CreateDbAccess(configManager);
            var dbAccessProvider = new TestDocumentDbAccessProvider(dbAccess);

            var fruitStore = new FruitStore(dbAccessProvider);

            var apples = new List<Apple>();
            var pears = new List<Pear>();

            for (int i = 0; i < 50; i++)
            {
                var gala = new Apple
                {
                    Id = Guid.NewGuid(),
                    Type = "Gala"
                };

                var fuji = new Apple
                {
                    Id = Guid.NewGuid(),
                    Type = "Fuji"
                };

                await fruitStore.UpsertApple(gala);
                await fruitStore.UpsertApple(fuji);

                apples.AddRange(new[] { gala, fuji });

                var bartlett = new Pear
                {
                    Id = Guid.NewGuid(),
                    Colour = "Red"
                };

                var comice = new Pear
                {
                    Id = Guid.NewGuid(),
                    Colour = "Green"
                };

                await fruitStore.UpsertPear(bartlett);
                await fruitStore.UpsertPear(comice);

                pears.AddRange(new[] { bartlett, comice });
            }

            var r1 = await fruitStore.GetAppleByQuery("[x].Type = 'Gala'");
            Assert.Equal(50, r1.Length);
            Assert.True(r1.All(x => x.Type == "Gala"));

            var r2 = await fruitStore.GetAllApples();
            Assert.Equal(100, r2.Length);

            var lastApple = apples.Last();
            var r3 = await fruitStore.GetAppleById(lastApple.Id);
            Assert.Equal(lastApple.Id, r3.Id);
            Assert.Equal(lastApple.Type, r3.Type);

            var r4 = await fruitStore.GetAppleByIds(apples.Select(x => x.Id.ToString()));
            Assert.Equal(100, r4.Length);

            var r5 = await fruitStore.GetPearByQuery("[x].Colour = @colour", new[] { new DbParameter("colour", "Red") });
            Assert.Equal(50, r5.Length);
            Assert.True(r5.All(x => x.Colour == "Red"));

            var r6 = await fruitStore.GetAllPears();
            Assert.Equal(100, r6.Length);

            var lastPear = pears.Last();
            var r7 = await fruitStore.GetPearById(lastPear.Id);
            Assert.Equal(lastPear.Id, r7.Id);
            Assert.Equal(lastPear.Colour, r7.Colour);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void TestMultipleServicesDocumentPurge()
        {
            var configManager1 = new ServiceDbConfigManager("TestService1");
            var dbAccess1 = await CreateDbAccess(configManager1);
            var dbAccessProvider1 = new TestDocumentDbAccessProvider(dbAccess1);

            var configManager2 = new ServiceDbConfigManager("TestService2");
            var dbAccess2 = await CreateDbAccess(configManager2);
            var dbAccessProvider2 = new TestDocumentDbAccessProvider(dbAccess2);

            var fruitStore1 = new FruitStore(dbAccessProvider1);
            var fruitStore2 = new FruitStore(dbAccessProvider2);

            var apples = new List<Apple>();

            for (int i = 0; i < 20; i++)
            {
                var gala = new Apple
                {
                    Id = Guid.NewGuid(),
                    Type = "Gala"
                };

                var fuji = new Apple
                {
                    Id = Guid.NewGuid(),
                    Type = "Fuji"
                };

                await fruitStore1.UpsertApple(gala);
                await fruitStore1.UpsertApple(fuji);

                await fruitStore2.UpsertApple(gala);
                await fruitStore2.UpsertApple(fuji);

                apples.AddRange(new[] { gala, fuji });
            }

            var r1 = await fruitStore1.GetAppleByQuery("[x].Type = 'Gala'");
            Assert.Equal(20, r1.Length);
            Assert.True(r1.All(x => x.Type == "Gala"));

            var r2 = await fruitStore1.GetAllApples();
            Assert.Equal(40, r2.Length);

            var lastApple = apples.Last();
            var r3 = await fruitStore1.GetAppleById(lastApple.Id);
            Assert.Equal(lastApple.Id, r3.Id);
            Assert.Equal(lastApple.Type, r3.Type);

            var r4 = await fruitStore1.GetAppleByIds(apples.Select(x => x.Id.ToString()));
            Assert.Equal(40, r4.Length);

            ServiceManager serviceManager1 = new ServiceManager(dbAccess1);
            ServiceManager serviceManager2 = new ServiceManager(dbAccess2);

            // Purge a service's docs.
            await serviceManager1.PurgeDocumentsAsync();
            Assert.Empty(await fruitStore1.GetAllApples());
            Assert.NotEmpty(await fruitStore2.GetAllApples());

            // Purge other service's docs.
            await serviceManager2.PurgeDocumentsAsync();
            Assert.Empty(await fruitStore1.GetAllApples());
            Assert.Empty(await fruitStore2.GetAllApples());
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void TestLargeNumberOfDocuments()
        {
            var configManager = new ServiceDbConfigManager("TestService");
            var dbAccess = await CreateDbAccess(configManager);
            var dbAccessProvider = new TestDocumentDbAccessProvider(dbAccess);

            var fruitStore = new FruitStore(dbAccessProvider);

            var apples = new List<Apple>();

            for (int i = 0; i < 800; i++)
            {
                var gala = new Apple
                {
                    Id = Guid.NewGuid(),
                    Type = "Gala"
                };

                var fuji = new Apple
                {
                    Id = Guid.NewGuid(),
                    Type = "Fuji"
                };

                await fruitStore.UpsertApple(gala);
                await fruitStore.UpsertApple(fuji);

                apples.AddRange(new[] { gala, fuji });
            }

            var r1 = await fruitStore.GetAppleByQuery("[x].Type = 'Gala'");
            Assert.Equal(800, r1.Length);
            Assert.True(r1.All(x => x.Type == "Gala"));

            var r2 = await fruitStore.GetAllApples();
            Assert.Equal(1600, r2.Length);

            var lastApple = apples.Last();
            var r3 = await fruitStore.GetAppleById(lastApple.Id);
            Assert.Equal(lastApple.Id, r3.Id);
            Assert.Equal(lastApple.Type, r3.Type);

            var r4 = await fruitStore.GetAppleByIds(apples.Select(x => x.Id.ToString()));
            Assert.Equal(1600, r4.Length);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void TestServiceRegistrationAndUpdateConcurrency()
        {
            // This test performs a concurrency check to ensure that multiple threads can successfully interact with
            // the same config manager. The config manager is thread safe so multiple stores registering and performing
            // actions on different threads should produce a consistent result.

            var configManager = new ServiceDbConfigManager("TestService");
            var dbAccess = await CreateDbAccess(configManager);
            var dbAccessProvider = new TestDocumentDbAccessProvider(dbAccess);

            List<Task> tasks = new List<Task>
            {
                Task.Run(async () =>
                {
                    var fruitStore = new FruitStore(dbAccessProvider);

                    var gala = new Apple { Id = Guid.NewGuid(), Type = "Gala" };
                    await fruitStore.UpsertApple(gala);
                }),
                Task.Run(async () =>
                {
                    var flowerStore = new FlowerStore(dbAccessProvider);

                    var daisy = new Daisy { Id = Guid.NewGuid(), Colour = "Red" };
                    await flowerStore.Upsert(daisy);
                })
            };

            Task.WaitAll(tasks.ToArray());
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void TestDbConcurrency()
        {
            var configManager = new ServiceDbConfigManager("TestService");
            var dbAccess = await CreateDbAccess(configManager);
            var dbAccessProvider = new TestDocumentDbAccessProvider(dbAccess);

            var fruitStore = new FruitStore(dbAccessProvider);

            var gala = new Apple
            {
                Id = Guid.NewGuid(),
                Type = "Gala"
            };

            await fruitStore.UpsertApple(gala);

            var t1 = Task.Run(async () => await SeedCounterAsync(gala.Id, fruitStore));
            var t2 = Task.Run(async () => await SeedCounterAsync(gala.Id, fruitStore, shouldSleep:true));

            var exception = Assert.Throws<AggregateException>(() => Task.WaitAll(t1, t2));
            Assert.Equal(typeof(NebulaStoreConcurrencyException), exception.InnerException.GetType());
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void TestDocumentAttachments()
        {
            var configManager = new ServiceDbConfigManager("TestService");
            var dbAccess = await CreateDbAccess(configManager);
            var dbAccessProvider = new TestDocumentDbAccessProvider(dbAccess);

            var store = new EmailStore(dbAccessProvider);

            var email = new Email
            {
                Id = Guid.NewGuid(),
                Subject = "Re: Holiday"
            };

            var attachment = new EmailAttachment
            {
                Data = "test"
            };

            // Store without attachment.
            await store.Upsert(email, null);

            var actualAttachment = await store.GetEmailAttachment(email.Id);
            Assert.Null(actualAttachment);

            // Store with attachment.
            await store.Upsert(email, attachment);

            actualAttachment = await store.GetEmailAttachment(email.Id);
            Assert.Equal(attachment.Data, actualAttachment.Data);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void TestDbParameterQueries()
        {
            var configManager = new ServiceDbConfigManager("TestService");
            var dbAccess = await CreateDbAccess(configManager);
            var dbAccessProvider = new TestDocumentDbAccessProvider(dbAccess);

            var fruitStore = new FruitStore(dbAccessProvider);

            var pears = new List<Pear>();

            for (int i = 0; i < 50; i++)
            {
                var bartlett = new Pear
                {
                    Id = Guid.NewGuid(),
                    Colour = "Red"
                };

                var comice = new Pear
                {
                    Id = Guid.NewGuid(),
                    Colour = "Green"
                };

                await fruitStore.UpsertPear(bartlett);
                await fruitStore.UpsertPear(comice);

                pears.AddRange(new[] { bartlett, comice });
            }

            var r1 = await fruitStore.GetPearByQuery("[x].Colour = @colour", new[] { new DbParameter("colour", "Red") });
            Assert.Equal(50, r1.Length);
            Assert.True(r1.All(x => x.Colour == "Red"));

            var r2 = await fruitStore.GetPearByQuery("[x].Colour = @colour", new[] { new DbParameter("@colour", "Red") });
            Assert.Equal(50, r2.Length);
            Assert.True(r2.All(x => x.Colour == "Red"));

            var r3 = await fruitStore.GetPearByQuery("[x].Colour = @colour", new[] { new DbParameter("colour", "Green") });
            Assert.Equal(50, r3.Length);
            Assert.True(r3.All(x => x.Colour == "Green"));

            var r4 = await fruitStore.GetPearByQuery("[x].Colour = @colour", new[] { new DbParameter("colour", "Blue") });
            Assert.Empty(r4);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void TestMetadataQueries()
        {
            var configManager = new ServiceDbConfigManager("TestService");
            var dbAccess = await CreateDbAccess(configManager);
            var dbAccessProvider = new TestDocumentDbAccessProvider(dbAccess);

            var fruitStore = new FruitStore(dbAccessProvider);

            var bartlett = new Pear
            {
                Id = Guid.NewGuid(),
                Colour = "Red"
            };

            var comice = new Pear
            {
                Id = Guid.NewGuid(),
                Colour = "Green"
            };

            for (int i = 0; i < 50; i++)
            {
                // Mutate the record.
                bartlett.Colour = i % 2 == 0 ? "Red" : "DarkRed";

                await fruitStore.UpsertPear(bartlett);
                await fruitStore.UpsertPear(comice);
            }

            await fruitStore.DeletePearById(bartlett.Id);

            var r1 = await fruitStore.GetPearByQuery("[x].Colour = @colour", new[] { new DbParameter("colour", "Red") });
            Assert.True(r1.All(x => x.Colour == "Red"));

            var r2 = await fruitStore.GetPearVersions(bartlett.Id.ToString());
            Assert.NotNull(r2);
            Assert.Equal(51, r2.Metadata.Count);
            Assert.Equal(1, r2.Metadata[0].Version);
            Assert.False(r2.Metadata[0].IsDeleted);

            var lastVersion = r2.Metadata.Last();

            Assert.Equal(51, lastVersion.Version);
            Assert.True(lastVersion.IsDeleted);
            Assert.True(lastVersion.CreatedTime < lastVersion.ModifiedTime);
            Assert.Equal(r2.Metadata[0].CreatedTime, lastVersion.CreatedTime);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void TestVersionedQueries()
        {
            var configManager = new ServiceDbConfigManager("TestService");
            var dbAccess = await CreateDbAccess(configManager);
            var dbAccessProvider = new TestDocumentDbAccessProvider(dbAccess);

            var fruitStore = new FruitStore(dbAccessProvider);

            var bartlett = new Pear
            {
                Id = Guid.NewGuid(),
                Colour = "Red"
            };

            var comice = new Pear
            {
                Id = Guid.NewGuid(),
                Colour = "Green"
            };

            for (int i = 0; i < 50; i++)
            {
                // Mutate the record.
                bartlett.Colour = i % 2 == 0 ? "Red" : "DarkRed";

                await fruitStore.UpsertPear(bartlett);
                await fruitStore.UpsertPear(comice);
            }

            await fruitStore.DeletePearById(bartlett.Id);

            const int lastVersion = 51;

            var r1 = await fruitStore.GetPearById(bartlett.Id, lastVersion);
            Assert.Equal(lastVersion, r1.Metadata.Version);
            Assert.True(r1.Metadata.IsDeleted);
            Assert.Equal("DarkRed", r1.Document.Colour);

            var r2 = await fruitStore.GetPearById(bartlett.Id, lastVersion - 1);
            Assert.Equal(lastVersion - 1, r2.Metadata.Version);
            Assert.False(r2.Metadata.IsDeleted);
            Assert.Equal("DarkRed", r2.Document.Colour);

            var r3 = await fruitStore.GetPearById(bartlett.Id, lastVersion - 2);
            Assert.Equal(lastVersion - 2, r3.Metadata.Version);
            Assert.False(r3.Metadata.IsDeleted);
            Assert.Equal("Red", r3.Document.Colour);

            var r4 = await fruitStore.GetPearById(bartlett.Id, 1);
            Assert.Equal(1, r4.Metadata.Version);
            Assert.False(r4.Metadata.IsDeleted);
            Assert.Equal("Red", r4.Document.Colour);

            var r5 = await fruitStore.GetPearById(comice.Id, 1);
            Assert.Equal(1, r5.Metadata.Version);
            Assert.False(r5.Metadata.IsDeleted);
            Assert.Equal("Green", r5.Document.Colour);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public async void TestCustomDocumentMetadata()
        {
            var configManager = new ServiceDbConfigManager("TestService");
            var dbAccess = await CreateDbAccess(configManager);
            var dbAccessProvider = new TestDocumentDbAccessProvider(dbAccess);
            var metadataSource = new TestDocumentMetadataSource("User1");

            var fruitStore = new FruitStore(dbAccessProvider, metadataSource);

            var bartlett = new Pear
            {
                Id = Guid.NewGuid(),
                Colour = "Red"
            };

            await fruitStore.UpsertPear(bartlett);

            metadataSource.ActorId = "User2";
            await fruitStore.UpsertPear(bartlett);

            metadataSource.ActorId = "User3";
            await fruitStore.DeletePearById(bartlett.Id);

            var r1 = await fruitStore.GetPearById(bartlett.Id, 1);
            Assert.Equal("User1", r1.Metadata.ActorId);

            var r2 = await fruitStore.GetPearById(bartlett.Id, 2);
            Assert.Equal("User2", r2.Metadata.ActorId);

            var r3 = await fruitStore.GetPearById(bartlett.Id, 3);
            Assert.True(r3.Metadata.IsDeleted);
            Assert.Equal("User3", r3.Metadata.ActorId);
        }

        private async Task SeedCounterAsync(Guid id, FruitStore fruitStore, bool shouldSleep = false)
        {
            var versionedApple = await fruitStore.GetVersionedAppleById(id);

            var apple = versionedApple.Document;
            apple.SeedCount += 1;

            if (shouldSleep)
            {
                // Sleep here to give the first thread enough time to store the document.
                // A sleep time less than 10 milli-seconds doesnt give cosmos db enough time
                // to store the first update.
                Thread.Sleep(10);
            }

            await fruitStore.UpsertApple(apple, (int)versionedApple.Metadata.Version);
        }

        private class FlowerStore : VersionedDocumentStore
        {
            private readonly DocumentTypeMapping<Daisy> _mapping;

            private readonly DocumentStoreConfig _config;
            private readonly IVersionedDocumentStoreClient _client;

            public FlowerStore(IDocumentDbAccessProvider dbAccessProvider) : base(dbAccessProvider, false)
            {
                var config = new DocumentStoreConfigBuilder("Flowers");

                var documentType = config.AddDocument("Daisy").Finish();

                _mapping = config.AddDocumentMapping<Daisy>(documentType.DocumentName)
                    .SetIdMapper(x => x.Id.ToString())
                    .SetPartitionMapper(x => x.Id.ToString())
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

            public async Task<Daisy> GetById(Guid id)
            {
                var result = await StoreClient.GetDocumentAsync(id.ToString(), _mapping, null);

                return result.Document;
            }

            public async Task Upsert(Daisy doc)
            {
                await StoreClient.UpsertDocumentAsync(doc, _mapping, new OperationOptions());
            }
        }

        private class FruitStore : VersionedDocumentStore
        {
            private readonly DocumentTypeMapping<Apple> _appleMapping;
            private readonly DocumentTypeMapping<Pear> _pearMapping;

            private readonly DocumentStoreConfig _config;
            private readonly IVersionedDocumentStoreClient _client;

            public FruitStore(IDocumentDbAccessProvider dbAccessProvider)
                : this(dbAccessProvider, null)
            {
            }

            public FruitStore(IDocumentDbAccessProvider dbAccessProvider, IDocumentMetadataSource metadataSource)
                : base(dbAccessProvider, false)
            {
                var config = new DocumentStoreConfigBuilder("Fruit");

                var appleDocumentType = config.AddDocument("Apple").Finish();
                var pearDocumentType = config.AddDocument("Pear").Finish();

                _appleMapping = config.AddDocumentMapping<Apple>(appleDocumentType.DocumentName)
                    .SetIdMapper(x => x.Id.ToString())
                    .SetPartitionMapper(x => x.Id.ToString())
                    .Finish();

                _pearMapping = config.AddDocumentMapping<Pear>(pearDocumentType.DocumentName)
                    .SetIdMapper(x => x.Id.ToString())
                    .SetPartitionMapper(x => x.Id.ToString())
                    .Finish();

                _config = config.Finish();
                _client = CreateStoreLogic(DbAccess, _config, metadataSource);

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

            public async Task<Apple> GetAppleById(Guid id)
            {
                var result = await StoreClient.GetDocumentAsync(id.ToString(), _appleMapping, null);

                return result.Document;
            }

            public async Task<VersionedDocumentReadResult<Apple>> GetVersionedAppleById(Guid id)
            {
                var result = await StoreClient.GetDocumentAsync(id.ToString(), _appleMapping, null);

                return result;
            }

            public async Task DeleteAppleById(Guid id)
            {
                await StoreClient.DeleteDocumentAsync(id.ToString(), _appleMapping, new OperationOptions());
            }

            public async Task UpsertApple(Apple doc, int? checkVersion = null)
            {
                await StoreClient.UpsertDocumentAsync(doc, _appleMapping, checkVersion != null ? new OperationOptions{CheckVersion = checkVersion.Value} : null);
            }

            public async Task<Apple[]> GetAppleByQuery(string query)
            {
                var result = await StoreClient.GetDocumentsAsync(query, _appleMapping, null);

                return result.Loaded.Where(x => !x.Metadata.IsDeleted).Select(x => x.Document).ToArray();
            }

            public async Task<Apple[]> GetAppleByIds(IEnumerable<string> ids)
            {
                var result = await StoreClient.GetDocumentsAsync(ids, _appleMapping, null);

                return result.Loaded.Where(x => !x.Metadata.IsDeleted).Select(x => x.Document).ToArray();
            }

            public async Task<Apple[]> GetAllApples()
            {
                var result = await StoreClient.GetDocumentsAsync(_appleMapping, null);

                return result.Where(x => !x.Metadata.IsDeleted).Select(x => x.Document).ToArray();
            }

            public async Task<Pear> GetPearById(Guid id)
            {
                var result = await StoreClient.GetDocumentAsync(id.ToString(), _pearMapping, null);

                return result.Document;
            }

            public async Task<VersionedDocumentReadResult<Pear>> GetPearById(Guid id, int version)
            {
                return await StoreClient.GetDocumentAsync(id.ToString(), version, _pearMapping);
            }

            public async Task UpsertPear(Pear doc)
            {
                await StoreClient.UpsertDocumentAsync(doc, _pearMapping, new OperationOptions());
            }

            public async Task<Pear[]> GetAllPears()
            {
                var result = await StoreClient.GetDocumentsAsync(_pearMapping, null);

                return result.Where(x => !x.Metadata.IsDeleted).Select(x => x.Document).ToArray();
            }

            public async Task<Pear[]> GetPearByQuery(string query, IEnumerable<DbParameter> parameters)
            {
                var result = await StoreClient.GetDocumentsAsync(query, parameters, _pearMapping);

                return result.Loaded.Where(x => !x.Metadata.IsDeleted).Select(x => x.Document).ToArray();
            }

            public async Task<VersionedDocumentMetadataReadResult> GetPearVersions(string id)
            {
                return await StoreClient.GetDocumentMetadataAsync(id, _pearMapping);
            }

            public async Task DeletePearById(Guid id)
            {
                await StoreClient.DeleteDocumentAsync(id.ToString(), _pearMapping, new OperationOptions());
            }
        }

        private class EmailStore : VersionedDocumentStore
        {
            private readonly DocumentTypeMapping<Email> _mapping;

            private readonly DocumentStoreConfig _config;
            private readonly IVersionedDocumentStoreClient _client;
            private readonly AttachmentTypeMapping<Email, EmailAttachment> _attachmentMapping;

            public EmailStore(IDocumentDbAccessProvider dbAccessProvider) : base(dbAccessProvider, false)
            {
                var config = new DocumentStoreConfigBuilder("EmailClient");

                var documentType = config.AddDocument("Email");
                var attachmentType = documentType.AddAttachment("EmailAttachment");

                _mapping = config.AddDocumentMapping<Email>(documentType.Name)
                    .SetIdMapper(x => x.Id.ToString())
                    .SetPartitionMapper(x => x.Id.ToString())
                    .Finish();

                _attachmentMapping = _mapping.AddAttachmentMapping<EmailAttachment>(attachmentType.Name)
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

            public async Task<EmailAttachment> GetEmailAttachment(Guid emailId)
            {
                var result = await StoreClient.GetDocumentAsync(emailId.ToString(), _mapping, null);

                return await StoreClient.GetAttachmentAsync(result.Document, result.Metadata.Version, _attachmentMapping);
            }

            public async Task Upsert(Email doc, EmailAttachment attachment)
            {
                var upsertResult = await StoreClient.UpsertDocumentAsync(doc, _mapping, null);

                if (attachment != null)
                {
                    await StoreClient.CreateAttachmentAsync(doc, upsertResult.DocumentVersion, _attachmentMapping, attachment);
                }
            }
        }

        private class Daisy
        {
            public Guid Id { get; set; }

            [JsonProperty(Required = Required.Always)]
            public string Colour { get; set; }
        }

        private class Apple
        {
            public Guid Id { get; set; }

            [JsonProperty(Required = Required.Always)]
            public string Type { get; set; }
            public int SeedCount { get; set; }
        }

        private class Pear
        {
            public Guid Id { get; set; }

            [JsonProperty(Required = Required.Always)]
            public string Colour { get; set; }
        }

        private class Email
        {
            public Guid Id { get; set; }

            public string Subject { get; set; }
        }

        private class EmailAttachment
        {
            public string Data { get; set; }
        }

        private class TestDocumentMetadataSource : IDocumentMetadataSource
        {
            private string _actorId;

            public TestDocumentMetadataSource(string actorId)
            {
                _actorId = actorId;
            }

            public string ActorId
            {
                set { _actorId = value; }
            }

            public string GetActorId()
            {
                return _actorId;
            }
        }
    }
}
