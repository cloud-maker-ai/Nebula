using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Nebula.Config;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace Nebula.Tests.Config
{
    public class ServiceDbConfigManagerTests
    {
        private readonly IServiceConfigSignatureGenerator _signatureGenerator;
        private readonly IDocumentClient _documentClient;

        private readonly IDocumentStoreConfigSource _configSourceA;

        public ServiceDbConfigManagerTests()
        {
            _signatureGenerator = Substitute.For<IServiceConfigSignatureGenerator>();
            _documentClient = Substitute.For<IDocumentClient>();

            _configSourceA = CreateConfigSource("StoreA");
        }

        [Fact]
        public async void EnsureConfigCurrentWithChangedSignature()
        {
            var dbConfig = CreateDbConfig();

            string oldSig = "old_sig";
            string newSig = "new_sig";
            var configDoc = CreateConfigDoc("Test", oldSig);

            DocumentStoreConfigBuilder storeABuilder = new DocumentStoreConfigBuilder("StoreA");
            storeABuilder.AddDocument("DocA");

            DocumentStoreConfigBuilder storeBBuilder = new DocumentStoreConfigBuilder("StoreB");
            storeBBuilder.AddDocument("DocA");
            storeBBuilder.AddDocument("DocB");

            var configSourceA = CreateConfigSource(storeABuilder);
            var configSourceB = CreateConfigSource(storeBBuilder);

            _signatureGenerator.CreateSignature(Arg.Any<IList<DocumentStoreConfig>>()).Returns(newSig);
            _documentClient.ReadDocumentAsync(Arg.Any<Uri>(), Arg.Is<RequestOptions>(x => x.PartitionKey != null)).Returns(WrapResource(configDoc));

            // Existing index that should remain.
            var includeIdx1 = new IncludedPath();
            includeIdx1.Path = "/content_Test_StoreA_DocA/*";
            includeIdx1.Indexes.Add(new HashIndex(DataType.String, -1));

            // Existing index that should be removed. It is no longer present.
            var includeIdx2 = new IncludedPath();
            includeIdx2.Path = "/content_Test_StoreB_DocA/PropA/*";
            includeIdx2.Indexes.Add(new RangeIndex(DataType.String));

            var col1 = new DocumentCollection();
            col1.IndexingPolicy.IncludedPaths.Add(includeIdx1);
            col1.IndexingPolicy.IncludedPaths.Add(includeIdx2);

            _documentClient.ReadDocumentCollectionAsync(Arg.Any<Uri>()).Returns(WrapResource(col1));

            var manager = new ServiceDbConfigManager("Test", _signatureGenerator);

            var foo = WrapResource(col1);
            _documentClient.ReplaceDocumentCollectionAsync(Arg.Any<DocumentCollection>(), Arg.Any<RequestOptions>()).Returns(foo);

            _documentClient.UpsertDocumentAsync(
                    Arg.Any<Uri>(),
                    Arg.Is<object>(r => ((ServiceDbConfigManager.ServiceConfigRecord)r).Signature == newSig),
                    Arg.Any<RequestOptions>())
                .Returns(WrapResource(CreateConfigDoc(configDoc.Id, newSig)));

            manager.RegisterStoreConfigSource(configSourceA);
            manager.RegisterStoreConfigSource(configSourceB);

            manager.EnsureConfigCurrent(_documentClient, dbConfig);

            await _documentClient.Received(1).UpsertDocumentAsync(Arg.Any<Uri>(), Arg.Any<object>(), Arg.Any<RequestOptions>());

            await _documentClient.Received().ReplaceDocumentCollectionAsync(
                Arg.Is<DocumentCollection>(c =>
                    IncludedPathCheck(
                        c.IndexingPolicy.IncludedPaths, "/content_Test_StoreA_DocA/*", "/content_Test_StoreB_DocA/*", "/content_Test_StoreB_DocB/*")),
                Arg.Any<RequestOptions>());
        }

        [Fact]
        public async void EnsureConfigCurrentWithUnchangedSignature()
        {
            var dbConfig = CreateDbConfig();

            string signature = "sig";
            var configDoc = CreateConfigDoc("Test", signature);

            _signatureGenerator.CreateSignature(Arg.Any<IList<DocumentStoreConfig>>()).Returns(signature);
            _documentClient.ReadDocumentAsync(Arg.Any<Uri>(), Arg.Is<RequestOptions>(x => x.PartitionKey != null)).Returns(WrapResource(configDoc));

            // Existing index that should remain.
            var includeIdx1 = new IncludedPath();
            includeIdx1.Path = "/content_Test_StoreA_DocA/*";
            includeIdx1.Indexes.Add(new HashIndex(DataType.String, -1));

            // Existing index that should be removed. It is no longer present.
            var includeIdx2 = new IncludedPath();
            includeIdx2.Path = "/content_Test_StoreB_DocA/PropA/*";
            includeIdx2.Indexes.Add(new RangeIndex(DataType.String));

            var manager = new ServiceDbConfigManager("Test", _signatureGenerator);

            _documentClient.UpsertDocumentAsync(Arg.Any<Uri>(), Arg.Is<Document>(x => x.GetPropertyValue<string>("Signature") == signature))
                .Returns(WrapResource(CreateConfigDoc(configDoc.Id, signature)));

            manager.RegisterStoreConfigSource(CreateConfigSource("A"));
            manager.RegisterStoreConfigSource(CreateConfigSource("B"));

            manager.EnsureConfigCurrent(_documentClient, dbConfig);

            await AssertNoUpdateTriggered();
        }

        [Fact]
        public async void EnsureConfigCurrentWithDuplicateStoreConfigRegistrationsAndNoSignatureChange()
        {
            var dbConfig = CreateDbConfig();
            var configDoc = CreateConfigDoc("Test", "sig");

            _signatureGenerator.CreateSignature(Arg.Any<IList<DocumentStoreConfig>>()).Returns("sig");
            _documentClient.ReadDocumentAsync(Arg.Any<Uri>(), Arg.Is<RequestOptions>(x => x.PartitionKey != null)).Returns(WrapResource(configDoc));

            var manager = new ServiceDbConfigManager("Test", _signatureGenerator);

            manager.RegisterStoreConfigSource(CreateConfigSource("A"));
            manager.RegisterStoreConfigSource(CreateConfigSource("A"));

            manager.EnsureConfigCurrent(_documentClient, dbConfig);

            await AssertNoUpdateTriggered();
        }

        [Fact]
        public void EnsureConfigCurrentThrowsForSignatureFailure()
        {
            var dbConfig = CreateDbConfig();
            var configDoc = CreateConfigDoc("Test", "sig");

            _signatureGenerator.CreateSignature(Arg.Any<IList<DocumentStoreConfig>>()).Throws(new Exception());
            _documentClient.ReadDocumentAsync(Arg.Any<Uri>()).Returns(WrapResource(configDoc));

            var manager = new ServiceDbConfigManager("Test", _signatureGenerator);
            manager.RegisterStoreConfigSource(_configSourceA);

            var ex = Assert.Throws<NebulaConfigException>(() => manager.EnsureConfigCurrent(_documentClient, dbConfig));
            
            Assert.Contains("signature generation failed", ex.Message);
        }

        [Fact]
        public void IsStoreConfigRegisteredTrueWhenRegistered()
        {
            var configDoc = CreateConfigDoc("Test", "sig");

            _signatureGenerator.CreateSignature(Arg.Any<IList<DocumentStoreConfig>>()).Throws(new Exception());
            _documentClient.ReadDocumentAsync(Arg.Any<Uri>()).Returns(WrapResource(configDoc));

            var manager = new ServiceDbConfigManager("Test", _signatureGenerator);

            manager.RegisterStoreConfigSource(_configSourceA);

            Assert.True(manager.IsStoreConfigRegistered(_configSourceA));
        }

        [Fact]
        public void IsStoreConfigRegisteredFalseWhenNotRegistered()
        {
            var configDoc = CreateConfigDoc("Test", "sig");

            _signatureGenerator.CreateSignature(Arg.Any<IList<DocumentStoreConfig>>()).Throws(new Exception());
            _documentClient.ReadDocumentAsync(Arg.Any<Uri>()).Returns(WrapResource(configDoc));

            var manager = new ServiceDbConfigManager("Test", _signatureGenerator);

            Assert.False(manager.IsStoreConfigRegistered(_configSourceA));
        }

        private DocumentDbConfig CreateDbConfig()
        {
            return new DocumentDbConfig("https://test", "key", "Db", "Col");
        }

        private Document CreateConfigDoc(string id, string sig)
        {
            var configDoc = new Document();
            configDoc.Id = id;
            configDoc.SetPropertyValue("Signature", sig);

            return configDoc;
        }

        private IDocumentStoreConfigSource CreateConfigSource(string storeName)
        {
            DocumentStoreConfigBuilder storeABuilder = new DocumentStoreConfigBuilder(storeName);
            storeABuilder.AddDocument("DocA");

            var configSource = Substitute.For<IDocumentStoreConfigSource>();
            configSource.GetConfig().Returns(storeABuilder.Finish());

            return configSource;
        }

        private IDocumentStoreConfigSource CreateConfigSource(DocumentStoreConfigBuilder storeBuilder)
        {
            var configSource = Substitute.For<IDocumentStoreConfigSource>();
            configSource.GetConfig().Returns(storeBuilder.Finish());

            return configSource;
        }

        private Task<ResourceResponse<TResource>> WrapResource<TResource>(TResource resource) where TResource : Resource, new()
        {
            return Task.FromResult(new ResourceResponse<TResource>(resource));
        }

        private bool IncludedPathCheck(ICollection<IncludedPath> actualPaths, params string[] expectedPaths)
        {
            if (expectedPaths.Length != actualPaths.Count)
                return false;

            foreach (var path in expectedPaths)
            {
                if (actualPaths.All(x => x.Path != path))
                {
                    return false;
                }
            }

            return true;
        }

        private async Task AssertNoUpdateTriggered()
        {
            await _documentClient.Received(0).ReplaceDocumentCollectionAsync(Arg.Any<DocumentCollection>(), Arg.Any<RequestOptions>());
            await _documentClient.Received(1).ReadDocumentAsync(Arg.Any<Uri>(), Arg.Any<RequestOptions>());
        }
    }
}
