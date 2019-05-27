using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Nebula.Config;
using Nebula.Service;
using Nebula.Versioned;
using Newtonsoft.Json;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace Nebula.Tests.Versioned
{
    public class VersionedDocumentStoreClientTests
    {
        [Fact]
        public async Task DeleteDocumentAsyncForNonDeletedId()
        {
            var client = Substitute.For<IDocumentClient>();

            var dbAccess = await CreateDbAccess(client);
            CreateTestStore(out var storeConfig, out var storeMapping);

            // Create an existing document so that this a delete of an existing document.
            var doc = CreateTestDoc1();
            var storeDoc1 = CreateDoc();
            SetDocContent(storeDoc1, doc, storeConfig, dbAccess, storeMapping);

            DoDeleteTest(
                client,
                dbAccess,
                new[] { storeDoc1 },
                doc, 2, storeDoc1.Version, true);
        }

        [Fact]
        public async Task DeleteDocumentAsyncForMissingId()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);

            DoDeleteNoChangeTest(client, dbAccess, null, "123");
        }

        [Fact]
        public async Task DeleteDocumentAsyncForAlreadyDeletedId()
        {
            var client = Substitute.For<IDocumentClient>();

            var dbAccess = await CreateDbAccess(client);
            CreateTestStore(out var storeConfig, out var storeMapping);

            // Create an existing document so that this a delete of an existing document.
            var doc = CreateTestDoc1();
            var storeDoc1 = CreateDoc();
            SetDocContent(storeDoc1, doc, storeConfig, dbAccess, storeMapping);
            storeDoc1.Deleted = true;
            storeDoc1.Version = 2;

            DoDeleteNoChangeTest(client, dbAccess, new[] { storeDoc1 }, doc.Id);
        }

        [Fact]
        public async Task DeleteDocumentAsyncWithCreateQueryFailure()
        {
            var client = Substitute.For<IDocumentClient>();

            var queryPolicy = Substitute.For<IDocumentQueryPolicy>();
            queryPolicy.IsQueryValid(Arg.Any<string>()).Returns(false);

            var dbAccess = await CreateDbAccess(client, queryPolicy);

            DoDeleteFailureTest<NebulaStoreException>(client, dbAccess, null, "123", 0, "Failed to create document query");
        }

        [Fact]
        public async Task DeleteDocumentAsyncExecuteQueryFailure()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);
            CreateTestStore(out var storeConfig, out var storeMapping);

            var doc1 = CreateTestDoc1();
            var storeDoc1 = CreateDoc();
            SetDocContent(storeDoc1, doc1, storeConfig, dbAccess, storeMapping);

            client.CreateDocumentQuery<VersionedDocumentStoreClient.VersionedDbDocument>(Arg.Any<Uri>(), Arg.Any<string>(), Arg.Any<FeedOptions>())
                .Returns(CreateQueryResult(CreateFailingQueryEnumerable()));

            DoDeleteFailureTest<NebulaStoreException>(client, dbAccess, null, doc1.Id, storeDoc1.Version, "Failed to execute query");
        }

        [Fact]
        public async Task DeleteDocumentAsyncWithWriteDocumentFailure()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);
            CreateTestStore(out var storeConfig, out var storeMapping);

            var doc1 = CreateTestDoc1();
            var storeDoc1 = CreateDoc();
            SetDocContent(storeDoc1, doc1, storeConfig, dbAccess, storeMapping);

            client.CreateDocumentAsync(Arg.Any<Uri>(), Arg.Any<object>()).Throws<Exception>();

            DoDeleteFailureTest<NebulaStoreException>(client, dbAccess, new[] { storeDoc1 }, doc1.Id, storeDoc1.Version, "Failed to write document");
        }

        [Fact]
        public async Task DeleteDocumentAsyncWithVersionChangedFailure()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);
            CreateTestStore(out var storeConfig, out var storeMapping);

            var doc1 = CreateTestDoc1();
            var storeDoc1 = CreateDoc();
            SetDocContent(storeDoc1, doc1, storeConfig, dbAccess, storeMapping);

            DoDeleteFailureTest<NebulaStoreConcurrencyException>(client, dbAccess, new[] { storeDoc1 }, doc1.Id, 0, "Existing document version does not match the specified check version");
        }

        [Fact]
        public async Task DeleteDocumentTwiceAsyncWithVersionChangedFailure()
        {
            var client = Substitute.For<IDocumentClient>();

            var dbAccess = await CreateDbAccess(client);
            CreateTestStore(out var storeConfig, out var storeMapping);

            // Create an existing document so that this a delete of an existing document.
            var doc = CreateTestDoc1();
            var storeDoc1 = CreateDoc();
            storeDoc1.Deleted = true;
            storeDoc1.Version = 2;
            SetDocContent(storeDoc1, doc, storeConfig, dbAccess, storeMapping);

            DoDeleteFailureTest<NebulaStoreConcurrencyException>(client, dbAccess, new[] { storeDoc1 }, doc.Id, storeDoc1.Version - 1, "Existing document version does not match the specified check version");
        }

        [Fact]
        public async Task DeleteDocumentAsyncWithNoVersion()
        {
            var client = Substitute.For<IDocumentClient>();

            var dbAccess = await CreateDbAccess(client);
            CreateTestStore(out var storeConfig, out var storeMapping);

            // Create an existing document so that this a delete of an existing document.
            var doc = CreateTestDoc1();
            var storeDoc1 = CreateDoc();
            SetDocContent(storeDoc1, doc, storeConfig, dbAccess, storeMapping);

            DoDeleteTest(
                client,
                dbAccess,
                new[] { storeDoc1 },
                doc, 2, null, true);
        }

        [Fact]
        public async Task UpsertDocumentAsyncAsCreate()
        {
            var client = Substitute.For<IDocumentClient>();
            DoUpsertTest(client, await CreateDbAccess(client), null, CreateTestDoc1(), 1, 0, false);
        }

        [Fact]
        public async Task UpsertDocumentAsyncAsUpdate()
        {
            var client = Substitute.For<IDocumentClient>();

            var doc = CreateTestDoc1();

            var dbAccess = await CreateDbAccess(client);
            CreateTestStore(out var storeConfig, out var storeMapping);

            // Create an existing document so that this is an update.
            var storeDoc1 = CreateDoc();
            SetDocContent(storeDoc1, doc, storeConfig, dbAccess, storeMapping);

            DoUpsertTest(
                client,
                dbAccess,
                new[] { storeDoc1 },
                doc, 2, storeDoc1.Version, false);
        }

        [Fact]
        public async Task UpsertDocumentAsyncWithInvalidQuery()
        {
            var client = Substitute.For<IDocumentClient>();

            var queryPolicy = Substitute.For<IDocumentQueryPolicy>();
            queryPolicy.IsQueryValid(Arg.Any<string>()).Returns(false);

            var dbAccess = await CreateDbAccess(client, queryPolicy);

            DoUpsertFailureTest<NebulaStoreException>(client, dbAccess, null, CreateTestDoc1(), 0, "Failed to create document query");
        }

        [Fact]
        public async Task UpsertDocumentAsyncWithCreateQueryFailure()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);

            client.CreateDocumentQuery<VersionedDocumentStoreClient.VersionedDbDocument>(Arg.Any<Uri>(), Arg.Any<string>(), Arg.Any<FeedOptions>())
                .Throws<Exception>();

            DoUpsertFailureTest<NebulaStoreException>(client, dbAccess, null, CreateTestDoc1(), 0, "Failed to create document query");
        }

        [Fact]
        public async Task UpsertDocumentAsyncWithExecuteQueryFailure()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);
            CreateTestStore(out var storeConfig, out var storeMapping);

            var storeDoc1 = CreateDoc();
            SetDocContent(storeDoc1, CreateTestDoc1(), storeConfig, dbAccess, storeMapping);

            client.CreateDocumentQuery<VersionedDocumentStoreClient.VersionedDbDocument>(Arg.Any<Uri>(), Arg.Any<string>(), Arg.Any<FeedOptions>())
                .Returns(CreateQueryResult(CreateFailingQueryEnumerable()));

            DoUpsertFailureTest<NebulaStoreException>(client, dbAccess, null, CreateTestDoc1(), 0, "Failed to execute query");
        }

        [Fact]
        public async Task UpsertDocumentAsyncWithContentSerialisationFailure()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);

            var failDoc = new TestFailureDoc();

            DoUpsertFailureTest<NebulaStoreException>(client, dbAccess, null, failDoc, 0, "Failed to serialise document content");
        }

        [Fact]
        public async Task UpsertDocumentAsyncWithWriteDocumentFailure()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);

            client.CreateDocumentAsync(Arg.Any<Uri>(), Arg.Any<object>()).Throws<Exception>();

            DoUpsertFailureTest<NebulaStoreException>(client, dbAccess, null, CreateTestDoc1(), 0, "Failed to write document");
        }

        [Fact]
        public async Task UpsertDocumentAsyncWithVersionChangedFailure()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);
            var doc = CreateTestDoc1();

            CreateTestStore(out var storeConfig, out var storeMapping);

            // Create an existing document so that this is an update.
            var storeDoc1 = CreateDoc();
            SetDocContent(storeDoc1, doc, storeConfig, dbAccess, storeMapping);

            DoUpsertFailureTest<NebulaStoreConcurrencyException>(client, dbAccess, new[] { storeDoc1 }, doc, 3, "Existing document version does not match the specified check version");
        }

        [Fact]
        public async Task UpsertExistingDocumentAsyncAsUpdateWithNoVersion()
        {
            var client = Substitute.For<IDocumentClient>();

            var doc = CreateTestDoc1();

            var dbAccess = await CreateDbAccess(client);
            CreateTestStore(out var storeConfig, out var storeMapping);

            // Create an existing document so that this is an update.
            var storeDoc1 = CreateDoc();
            SetDocContent(storeDoc1, doc, storeConfig, dbAccess, storeMapping);

            DoUpsertTest(
                client,
                dbAccess,
                new[] { storeDoc1 },
                doc, 2, null, false);
        }

        [Fact]
        public async void GetDocumentAsyncForExistingId()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);
            CreateTestStore(out var storeConfig, out var storeMapping);

            var createdTime = GetUtcNowSeconds();

            var doc = CreateTestDoc1();
            var storeDoc1 = CreateDoc();
            SetDocContent(storeDoc1, doc, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc1, createdTime);

            client.CreateDocumentQuery<VersionedDocumentStoreClient.VersionedDbDocument>(Arg.Any<Uri>(), Arg.Any<SqlQuerySpec>(), Arg.Any<FeedOptions>())
                .Returns(CreateQueryResult(new[] { storeDoc1 }));

            var logic = new VersionedDocumentStoreClient(dbAccess, storeConfig);

            var result = await logic.GetDocumentAsync("10", storeMapping, null);

            Assert.Equal(DocumentReadResultType.Loaded, result.ResultType);
            Assert.Equal(doc.Data, result.Document.Data);
            Assert.Equal(doc.Id, result.Document.Id);
            Assert.Null(result.FailureDetails);

            Assert.Equal(1, result.Metadata.Version);
            Assert.False(result.Metadata.IsDeleted);
            Assert.Equal(createdTime, result.Metadata.CreatedTime);
            Assert.Equal(createdTime, result.Metadata.ModifiedTime);
        }

        [Fact]
        public async void GetDocumentAsyncForDeletedId()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);
            CreateTestStore(out var storeConfig, out var storeMapping);

            var createdTime = GetUtcNowSeconds();

            var doc = CreateTestDoc1();
            var storeDoc1 = CreateDoc();
            SetDocContent(storeDoc1, doc, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc1, createdTime);
            storeDoc1.Version = 2;
            storeDoc1.Deleted = true;

            client.CreateDocumentQuery<VersionedDocumentStoreClient.VersionedDbDocument>(Arg.Any<Uri>(), Arg.Any<SqlQuerySpec>(), Arg.Any<FeedOptions>())
                .Returns(CreateQueryResult(new[] { storeDoc1 }));

            var logic = new VersionedDocumentStoreClient(dbAccess, storeConfig);

            var options = new VersionedDocumentReadOptions { IncludeDeleted = true };
            var result = await logic.GetDocumentAsync("10", storeMapping, options);

            Assert.Equal(DocumentReadResultType.Loaded, result.ResultType);
            Assert.Equal(doc.Data, result.Document.Data);
            Assert.Equal(doc.Id, result.Document.Id);
            Assert.Null(result.FailureDetails);

            Assert.Equal(2, result.Metadata.Version);
            Assert.True(result.Metadata.IsDeleted);
            Assert.Equal(createdTime, result.Metadata.CreatedTime);
            Assert.Equal(createdTime, result.Metadata.ModifiedTime);
        }

        [Fact]
        public async void GetDocumentAsyncForDeletedIdExcludingDeletedByDefault()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);
            CreateTestStore(out var storeConfig, out var storeMapping);

            var createdTime = GetUtcNowSeconds();

            var doc = CreateTestDoc1();
            var storeDoc1 = CreateDoc();
            SetDocContent(storeDoc1, doc, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc1, createdTime);
            storeDoc1.Version = 2;
            storeDoc1.Deleted = true;

            client.CreateDocumentQuery<VersionedDocumentStoreClient.VersionedDbDocument>(Arg.Any<Uri>(), Arg.Any<SqlQuerySpec>(), Arg.Any<FeedOptions>())
                .Returns(CreateQueryResult(new[] { storeDoc1 }));

            var logic = new VersionedDocumentStoreClient(dbAccess, storeConfig);

            var result = await logic.GetDocumentAsync("10", storeMapping, null);

            Assert.Null(result);
        }

        [Fact]
        public async void GetDocumentAsyncForMissingId()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);
            CreateTestStore(out var storeConfig, out var storeMapping);

            var doc = CreateTestDoc1();
            var storeDoc1 = CreateDoc();
            SetDocContent(storeDoc1, doc, storeConfig, dbAccess, storeMapping);

            var logic = new VersionedDocumentStoreClient(dbAccess, storeConfig);

            var result = await logic.GetDocumentAsync("10", storeMapping, null);

            Assert.Null(result);
        }

        [Fact]
        public async void GetDocumentAsyncWithReadFailure()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);
            CreateTestFailureStore(out var storeConfig, out var storeMapping);
            CreateTestStore(out _, out var okMaping);

            var createdTime = GetUtcNowSeconds();

            var storeDoc1 = CreateDoc();
            SetDocContent(storeDoc1, CreateTestDoc1(), storeConfig, dbAccess, okMaping);
            SetDocTimestamp(storeDoc1, createdTime);

            client.CreateDocumentQuery<VersionedDocumentStoreClient.VersionedDbDocument>(Arg.Any<Uri>(), Arg.Any<SqlQuerySpec>(), Arg.Any<FeedOptions>())
                .Returns(CreateQueryResult(new[] { storeDoc1 }));

            var logic = new VersionedDocumentStoreClient(dbAccess, storeConfig);

            var result = await logic.GetDocumentAsync("10", storeMapping, null);

            Assert.Equal(DocumentReadResultType.Failed, result.ResultType);
            Assert.Null(result.Document);
            Assert.Equal(1, result.Metadata.Version);
            Assert.False(result.Metadata.IsDeleted);
            Assert.NotNull(result.FailureDetails);
            Assert.Equal("Failed to deserialise document", result.FailureDetails.Message);
            Assert.Equal("Failed to deserialise document", result.FailureDetails.Message);
            Assert.Contains("Required property 'Error' not found in JSON", result.FailureDetails.Detail);
        }

        [Fact]
        public async void GetDocumentsAsyncWithEmptyResult()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);
            CreateTestStore(out var storeConfig, out var storeMapping);

            var logic = new VersionedDocumentStoreClient(dbAccess, storeConfig);

            var result = await logic.GetDocumentsAsync(storeMapping, null);

            Assert.Empty(result);
        }

        [Fact]
        public async void GetDocumentsAsyncWithMixedResult()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);
            CreateTestStore(out var storeConfig, out var storeMapping);

            var createdTime = GetUtcNowSeconds();

            var doc1 = CreateTestDoc1();
            var doc2 = CreateTestDoc1("20");

            var storeDoc1Version1 = CreateDoc(doc1.Id);
            SetDocContent(storeDoc1Version1, doc1, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc1Version1, createdTime);

            var storeDoc1Version2 = CreateDoc(doc1.Id, 2, deleted: false);
            SetDocContent(storeDoc1Version2, doc1, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc1Version2, createdTime);

            var storeDoc2Version1 = CreateDoc(doc2.Id);
            SetDocContent(storeDoc2Version1, doc2, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc2Version1, createdTime);

            var storeDoc2Version2 = CreateDoc(doc2.Id, 2, deleted: true);
            SetDocContent(storeDoc2Version2, doc2, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc2Version2, createdTime);

            client.CreateDocumentQuery<VersionedDocumentStoreClient.VersionedDbDocument>(Arg.Any<Uri>(), Arg.Any<SqlQuerySpec>(), Arg.Any<FeedOptions>())
                .Returns(CreateQueryResult(new[] { storeDoc1Version1, storeDoc1Version2, storeDoc2Version1, storeDoc2Version2 }));

            var logic = new VersionedDocumentStoreClient(dbAccess, storeConfig);

            var options = new VersionedDocumentReadOptions { IncludeDeleted = true };
            var results = await logic.GetDocumentsAsync(storeMapping, options);

            Assert.Equal(2, results.Count);

            var result1 = results.SingleOrDefault(x => x.Document.Id == doc1.Id);
            var result2 = results.SingleOrDefault(x => x.Document.Id == doc2.Id);

            Assert.NotNull(result1);
            Assert.Equal(doc1.Data, result1.Document.Data);
            Assert.Equal(doc1.Id, result1.Document.Id);

            Assert.Equal(2, result1.Metadata.Version);
            Assert.False(result1.Metadata.IsDeleted);
            Assert.Equal(createdTime, result1.Metadata.CreatedTime);
            Assert.Equal(createdTime, result1.Metadata.ModifiedTime);

            Assert.NotNull(result2);
            Assert.Equal(doc2.Data, result2.Document.Data);
            Assert.Equal(doc2.Id, result2.Document.Id);

            Assert.Equal(2, result2.Metadata.Version);
            Assert.True(result2.Metadata.IsDeleted);
            Assert.Equal(createdTime, result2.Metadata.CreatedTime);
            Assert.Equal(createdTime, result2.Metadata.ModifiedTime);
        }

        [Fact]
        public async void GetDocumentsAsyncWithMixedResultExcludingDeleted()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);
            CreateTestStore(out var storeConfig, out var storeMapping);

            var createdTime = GetUtcNowSeconds();

            var doc1 = CreateTestDoc1();
            var doc2 = CreateTestDoc1("20");

            var storeDoc1Version1 = CreateDoc(doc1.Id);
            SetDocContent(storeDoc1Version1, doc1, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc1Version1, createdTime);

            var storeDoc1Version2 = CreateDoc(doc1.Id, 2, deleted: false);
            SetDocContent(storeDoc1Version2, doc1, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc1Version2, createdTime);

            var storeDoc2Version1 = CreateDoc(doc2.Id);
            SetDocContent(storeDoc2Version1, doc2, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc2Version1, createdTime);

            var storeDoc2Version2 = CreateDoc(doc2.Id, 2, deleted: true);
            SetDocContent(storeDoc2Version2, doc2, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc2Version2, createdTime);

            client.CreateDocumentQuery<VersionedDocumentStoreClient.VersionedDbDocument>(Arg.Any<Uri>(), Arg.Any<SqlQuerySpec>(), Arg.Any<FeedOptions>())
                .Returns(CreateQueryResult(new[] { storeDoc1Version1, storeDoc1Version2, storeDoc2Version1, storeDoc2Version2 }));

            var logic = new VersionedDocumentStoreClient(dbAccess, storeConfig);

            var options = new VersionedDocumentReadOptions();
            options.IncludeDeleted = false;

            var results = await logic.GetDocumentsAsync(storeMapping, options);

            Assert.Equal(1, results.Count);

            var result1 = results.SingleOrDefault(x => x.Document.Id == doc1.Id);

            Assert.NotNull(result1);
            Assert.Equal(doc1.Data, result1.Document.Data);
            Assert.Equal(doc1.Id, result1.Document.Id);

            Assert.Equal(2, result1.Metadata.Version);
            Assert.False(result1.Metadata.IsDeleted);
            Assert.Equal(createdTime, result1.Metadata.CreatedTime);
            Assert.Equal(createdTime, result1.Metadata.ModifiedTime);
        }

        [Fact]
        public async void GetDocumentsAsyncByQueryWithEmptyResult()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);
            CreateTestStore(out var storeConfig, out var storeMapping);

            var logic = new VersionedDocumentStoreClient(dbAccess, storeConfig);

            var result = await logic.GetDocumentsAsync("FALSE", storeMapping, null);

            Assert.Empty(result.Loaded);
            Assert.Empty(result.Failed);
        }

        [Fact]
        public async void GetDocumentsAsyncByQueryWithMixedResult()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);
            CreateTestStore(out var storeConfig, out var storeMapping);

            var createdTime = GetUtcNowSeconds();

            var doc1 = CreateTestDoc1();
            var doc2 = CreateTestDoc1("20");

            var storeDoc1Version1 = CreateDoc(doc1.Id);
            SetDocContent(storeDoc1Version1, doc1, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc1Version1, createdTime);

            var storeDoc1Version2 = CreateDoc(doc1.Id, 2, deleted: false);
            SetDocContent(storeDoc1Version2, doc1, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc1Version2, createdTime);

            var storeDoc2Version1 = CreateDoc(doc2.Id);
            SetDocContent(storeDoc2Version1, doc2, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc2Version1, createdTime);

            var storeDoc2Version2 = CreateDoc(doc2.Id, 2, deleted: true);
            SetDocContent(storeDoc2Version2, doc2, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc2Version2, createdTime);

            client.CreateDocumentQuery<VersionedDocumentStoreClient.VersionedDbDocument>(Arg.Any<Uri>(), Arg.Any<SqlQuerySpec>(), Arg.Any<FeedOptions>())
                .Returns(CreateQueryResult(new[] { storeDoc1Version1, storeDoc1Version2, storeDoc2Version1, storeDoc2Version2 }));

            var logic = new VersionedDocumentStoreClient(dbAccess, storeConfig);

            var options = new VersionedDocumentReadOptions { IncludeDeleted = true };
            var results = await logic.GetDocumentsAsync("TRUE", storeMapping, options);

            Assert.Equal(2, results.Loaded.Count);
            Assert.Empty(results.Failed);

            var result1 = results.Loaded.SingleOrDefault(x => x.Document.Id == doc1.Id);
            var result2 = results.Loaded.SingleOrDefault(x => x.Document.Id == doc2.Id);

            Assert.NotNull(result1);
            Assert.Equal(doc1.Data, result1.Document.Data);
            Assert.Equal(doc1.Id, result1.Document.Id);

            Assert.Equal(2, result1.Metadata.Version);
            Assert.False(result1.Metadata.IsDeleted);
            Assert.Equal(createdTime, result1.Metadata.CreatedTime);
            Assert.Equal(createdTime, result1.Metadata.ModifiedTime);

            Assert.NotNull(result2);
            Assert.Equal(doc2.Data, result2.Document.Data);
            Assert.Equal(doc2.Id, result2.Document.Id);

            Assert.Equal(2, result2.Metadata.Version);
            Assert.True(result2.Metadata.IsDeleted);
            Assert.Equal(createdTime, result2.Metadata.CreatedTime);
            Assert.Equal(createdTime, result2.Metadata.ModifiedTime);
        }

        [Fact]
        public async void GetDocumentsAsyncByQueryWithMixedResultExcludingDeleted()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);
            CreateTestStore(out var storeConfig, out var storeMapping);

            var createdTime = GetUtcNowSeconds();

            var doc1 = CreateTestDoc1();
            var doc2 = CreateTestDoc1("20");

            var storeDoc1Version1 = CreateDoc(doc1.Id);
            SetDocContent(storeDoc1Version1, doc1, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc1Version1, createdTime);

            var storeDoc1Version2 = CreateDoc(doc1.Id, 2, deleted: false);
            SetDocContent(storeDoc1Version2, doc1, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc1Version2, createdTime);

            var storeDoc2Version1 = CreateDoc(doc2.Id);
            SetDocContent(storeDoc2Version1, doc2, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc2Version1, createdTime);

            var storeDoc2Version2 = CreateDoc(doc2.Id, 2, deleted: true);
            SetDocContent(storeDoc2Version2, doc2, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc2Version2, createdTime);

            client.CreateDocumentQuery<VersionedDocumentStoreClient.VersionedDbDocument>(Arg.Any<Uri>(), Arg.Any<SqlQuerySpec>(), Arg.Any<FeedOptions>())
                .Returns(CreateQueryResult(new[] { storeDoc1Version1, storeDoc1Version2, storeDoc2Version1, storeDoc2Version2 }));

            var logic = new VersionedDocumentStoreClient(dbAccess, storeConfig);

            var options = new VersionedDocumentReadOptions();
            options.IncludeDeleted = false;

            var results = await logic.GetDocumentsAsync("TRUE", storeMapping, options);

            Assert.Equal(1, results.Loaded.Count);
            Assert.Empty(results.Failed);

            var result1 = results.Loaded.SingleOrDefault(x => x.Document.Id == doc1.Id);

            Assert.NotNull(result1);
            Assert.Equal(doc1.Data, result1.Document.Data);
            Assert.Equal(doc1.Id, result1.Document.Id);

            Assert.Equal(2, result1.Metadata.Version);
            Assert.False(result1.Metadata.IsDeleted);
            Assert.Equal(createdTime, result1.Metadata.CreatedTime);
            Assert.Equal(createdTime, result1.Metadata.ModifiedTime);
        }

        [Fact]
        public async void GetDocumentsAsyncByIdsWithEmptyResult()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);
            CreateTestStore(out var storeConfig, out var storeMapping);

            var logic = new VersionedDocumentStoreClient(dbAccess, storeConfig);

            var result = await logic.GetDocumentsAsync(new[] { "1", "2" }, storeMapping, null);

            Assert.Equal(new[] { "1", "2" }, result.Missing);

            Assert.Empty(result.Loaded);
            Assert.Empty(result.Failed);
        }

        [Fact]
        public async void GetDocumentsAsyncByIdsWithMixedResult()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);
            CreateTestStore(out var storeConfig, out var storeMapping);

            var createdTime = GetUtcNowSeconds();

            var doc1 = CreateTestDoc1();
            var doc2 = CreateTestDoc1("20");
            var doc3MissingId = "30";

            var storeDoc1Version1 = CreateDoc(doc1.Id);
            SetDocContent(storeDoc1Version1, doc1, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc1Version1, createdTime);

            var storeDoc1Version2 = CreateDoc(doc1.Id, 2, deleted: false);
            SetDocContent(storeDoc1Version2, doc1, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc1Version2, createdTime);

            var storeDoc2Version1 = CreateDoc(doc2.Id);
            SetDocContent(storeDoc2Version1, doc2, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc2Version1, createdTime);

            var storeDoc2Version2 = CreateDoc(doc2.Id, 2, deleted: true);
            SetDocContent(storeDoc2Version2, doc2, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc2Version2, createdTime);

            client.CreateDocumentQuery<VersionedDocumentStoreClient.VersionedDbDocument>(Arg.Any<Uri>(), Arg.Any<SqlQuerySpec>(), Arg.Any<FeedOptions>())
                .Returns(CreateQueryResult(new[] { storeDoc1Version1, storeDoc1Version2, storeDoc2Version1, storeDoc2Version2 }));

            var logic = new VersionedDocumentStoreClient(dbAccess, storeConfig);

            var options = new VersionedDocumentReadOptions { IncludeDeleted = true };
            var results = await logic.GetDocumentsAsync(new[] { doc1.Id, doc2.Id, doc3MissingId }, storeMapping, options);

            Assert.Equal(2, results.Loaded.Count);
            Assert.Empty(results.Failed);
            Assert.Equal(1, results.Missing.Count);

            var result1 = results.Loaded.SingleOrDefault(x => x.Document.Id == doc1.Id);
            var result2 = results.Loaded.SingleOrDefault(x => x.Document.Id == doc2.Id);

            Assert.NotNull(result1);
            Assert.Equal(doc1.Data, result1.Document.Data);
            Assert.Equal(doc1.Id, result1.Document.Id);
            Assert.Equal(2, result1.Metadata.Version);
            Assert.False(result1.Metadata.IsDeleted);
            Assert.Equal(createdTime, result1.Metadata.CreatedTime);
            Assert.Equal(createdTime, result1.Metadata.ModifiedTime);

            Assert.NotNull(result2);
            Assert.Equal(doc2.Data, result2.Document.Data);
            Assert.Equal(doc2.Id, result2.Document.Id);
            Assert.Equal(2, result2.Metadata.Version);
            Assert.True(result2.Metadata.IsDeleted);
            Assert.Equal(createdTime, result2.Metadata.CreatedTime);
            Assert.Equal(createdTime, result2.Metadata.ModifiedTime);

            Assert.Equal(doc3MissingId, results.Missing[0]);
        }

        [Fact]
        public async void GetDocumentsAsyncByIdsWithMixedResultExcludingDeleted()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);
            CreateTestStore(out var storeConfig, out var storeMapping);

            var createdTime = GetUtcNowSeconds();

            var doc1 = CreateTestDoc1();
            var doc2 = CreateTestDoc1("20");
            var doc3MissingId = "30";

            var storeDoc1Version1 = CreateDoc(doc1.Id);
            SetDocContent(storeDoc1Version1, doc1, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc1Version1, createdTime);

            var storeDoc1Version2 = CreateDoc(doc1.Id, 2, deleted: false);
            SetDocContent(storeDoc1Version2, doc1, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc1Version2, createdTime);

            var storeDoc2Version1 = CreateDoc(doc2.Id);
            SetDocContent(storeDoc2Version1, doc2, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc2Version1, createdTime);

            var storeDoc2Version2 = CreateDoc(doc2.Id, 2, deleted: true);
            SetDocContent(storeDoc2Version2, doc2, storeConfig, dbAccess, storeMapping);
            SetDocTimestamp(storeDoc2Version2, createdTime);

            client.CreateDocumentQuery<VersionedDocumentStoreClient.VersionedDbDocument>(Arg.Any<Uri>(), Arg.Any<SqlQuerySpec>(), Arg.Any<FeedOptions>())
                .Returns(CreateQueryResult(new[] { storeDoc1Version1, storeDoc1Version2, storeDoc2Version1, storeDoc2Version2 }));

            var logic = new VersionedDocumentStoreClient(dbAccess, storeConfig);

            var options = new VersionedDocumentReadOptions();
            options.IncludeDeleted = false;

            var results = await logic.GetDocumentsAsync(new[] { doc1.Id, doc2.Id, doc3MissingId }, storeMapping, options);

            Assert.Equal(1, results.Loaded.Count);
            Assert.Empty(results.Failed);
            Assert.Equal(2, results.Missing.Count);

            var result1 = results.Loaded.SingleOrDefault(x => x.Document.Id == doc1.Id);

            Assert.NotNull(result1);
            Assert.Equal(doc1.Data, result1.Document.Data);
            Assert.Equal(doc1.Id, result1.Document.Id);
            Assert.Equal(2, result1.Metadata.Version);
            Assert.False(result1.Metadata.IsDeleted);
            Assert.Equal(createdTime, result1.Metadata.CreatedTime);
            Assert.Equal(createdTime, result1.Metadata.ModifiedTime);

            Assert.Equal(new[] { doc2.Id, doc3MissingId }, results.Missing);
        }

        [Fact]
        public async void GetAttachmentAsyncWithExistingAttachment()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);
            CreateTestStoreWithAttachment(out var storeConfig, out var storeMapping, out var attachmentMapping);

            var testDoc = CreateTestDoc1();

            var attachmentResource = new Attachment { MediaLink = "foo/bar" };

            var partitionKey = storeMapping.PartitionKeyMapper(testDoc);

            client.ReadAttachmentAsync(
                    Arg.Is<Uri>(uri => uri.OriginalString.StartsWith("dbs/Db/colls/Col/docs/")),
                    Arg.Is<RequestOptions>(o => o.PartitionKey.Equals(new PartitionKey(partitionKey))))
                .Returns(new ResourceResponse<Attachment>(attachmentResource));

            client.ReadMediaAsync(Arg.Is(attachmentResource.MediaLink))
                .Returns(new MediaResponse());

            var logic = new VersionedDocumentStoreClient(dbAccess, storeConfig);

            // This _success_ test cannot be completed in entirety due to the limits in the testability of the
            // MediaResponse class. The setter on the data stream is internal. Until the SDK is improved, this
            // check is for failing deserialisation of the attachment due to missing stream data.

            var exception = await Assert.ThrowsAsync<NebulaStoreException>(() => logic.GetAttachmentAsync(testDoc, 1, attachmentMapping));
            Assert.Equal("Failed to deserialise document attachment", exception.Message);
        }

        [Fact]
        public async void GetAttachmentAsyncWithDocumentReadFailure()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);
            CreateTestStoreWithAttachment(out var storeConfig, out var storeMapping, out var attachmentMapping);

            var testDoc = CreateTestDoc1();

            var partitionKey = storeMapping.PartitionKeyMapper(testDoc);

            client.ReadAttachmentAsync(
                    Arg.Is<Uri>(uri => uri.OriginalString.StartsWith("dbs/Db/colls/Col/docs/")),
                    Arg.Is<RequestOptions>(o => o.PartitionKey.Equals(new PartitionKey(partitionKey))))
                .Throws(new Exception("error"));

            var logic = new VersionedDocumentStoreClient(dbAccess, storeConfig);

            var exception = await Assert.ThrowsAsync<NebulaStoreException>(() => logic.GetAttachmentAsync(testDoc, 1, attachmentMapping));
            Assert.Contains("Failed to read document attachment", exception.Message);
        }

        [Fact]
        public async void GetAttachmentAsyncWithMediaReadFailure()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);
            CreateTestStoreWithAttachment(out var storeConfig, out var storeMapping, out var attachmentMapping);

            var testDoc = CreateTestDoc1();

            var attachmentResource = new Attachment { MediaLink = "foo/bar" };

            var partitionKey = storeMapping.PartitionKeyMapper(testDoc);

            client.ReadAttachmentAsync(
                    Arg.Is<Uri>(uri => uri.OriginalString.StartsWith("dbs/Db/colls/Col/docs/")),
                    Arg.Is<RequestOptions>(o => o.PartitionKey.Equals(new PartitionKey(partitionKey))))
                .Returns(new ResourceResponse<Attachment>(attachmentResource));

            client.ReadMediaAsync(Arg.Is(attachmentResource.MediaLink))
                .Throws(new Exception("error"));

            var logic = new VersionedDocumentStoreClient(dbAccess, storeConfig);

            var exception = await Assert.ThrowsAsync<NebulaStoreException>(() => logic.GetAttachmentAsync(testDoc, 1, attachmentMapping));
            Assert.Contains("Failed to read document attachment media", exception.Message);
        }

        [Fact]
        public async void GetAttachmentAsyncWithAttachmentDeserialisationFailure()
        {
            var client = Substitute.For<IDocumentClient>();
            var dbAccess = await CreateDbAccess(client);
            CreateTestStoreWithAttachment(out var storeConfig, out var storeMapping, out var attachmentMapping);

            var testDoc = CreateTestDoc1();

            var attachmentResource = new Attachment { MediaLink = "foo/bar" };

            var partitionKey = storeMapping.PartitionKeyMapper(testDoc);

            client.ReadAttachmentAsync(
                    Arg.Is<Uri>(uri => uri.OriginalString.StartsWith("dbs/Db/colls/Col/docs/")),
                    Arg.Is<RequestOptions>(o => o.PartitionKey.Equals(new PartitionKey(partitionKey))))
                .Returns(new ResourceResponse<Attachment>(attachmentResource));

            client.ReadMediaAsync(Arg.Is(attachmentResource.MediaLink))
                .Returns(new MediaResponse());

            var logic = new VersionedDocumentStoreClient(dbAccess, storeConfig);

            var exception = await Assert.ThrowsAsync<NebulaStoreException>(() => logic.GetAttachmentAsync(testDoc, 1, attachmentMapping));
            Assert.Contains("Failed to deserialise document attachment", exception.Message);
        }

        private VersionedDocumentStoreClient.VersionedDbDocument ExtractDoc(object obj)
        {
            // Round trip the obj JSON to ensure that properties reflect the underlying meta data properties.
            var doc = obj as VersionedDocumentStoreClient.VersionedDbDocument;
            var json = JsonConvert.SerializeObject(doc);
            return JsonConvert.DeserializeObject<VersionedDocumentStoreClient.VersionedDbDocument>(json);
        }

        private string CreateContentKey<TDocument>(DocumentStoreConfig config, DocumentDbAccess dbAccess, DocumentTypeMapping<TDocument> mapping)
        {
            return dbAccess.ConfigManager.CreateDocumentContentKey(config.StoreName, mapping.DocumentName);
        }

        private async Task<DocumentDbAccess> CreateDbAccess(IDocumentClient documentClient, IDocumentQueryPolicy queryPolicy = null)
        {
            var signatureGenerator = Substitute.For<IServiceConfigSignatureGenerator>();

            if (queryPolicy == null)
            {
                queryPolicy = CreateAllowAllQueryPolicy();
            }

            var configManager = new ServiceDbConfigManager("Test", signatureGenerator);

            var dbService = Substitute.For<IDocumentDbService>();

            var documentDbAccess = new DocumentDbAccess(CreateDbConfig(), configManager, documentClient, dbService, queryPolicy);

            await documentDbAccess.Open();

            return documentDbAccess;
        }

        private DocumentDbConfig CreateDbConfig()
        {
            return new DocumentDbConfig("https://test", "key", "Db", "Col");
        }

        private IDocumentQueryPolicy CreateAllowAllQueryPolicy()
        {
            var queryPolicy = Substitute.For<IDocumentQueryPolicy>();
            queryPolicy.GetIdSearchLimit(Arg.Any<ICollection<string>>()).Returns(1000);
            queryPolicy.IsQueryValid(Arg.Any<string>()).Returns(true);
            return queryPolicy;
        }

        private void CreateTestStore(out DocumentStoreConfig storeConfig, out DocumentTypeMapping<TestDoc> storeMapping)
        {
            var storeBuilder = new DocumentStoreConfigBuilder("StoreA");
            storeBuilder.AddDocument("TestDoc");

            storeConfig = storeBuilder.Finish();

            storeMapping = storeBuilder.AddDocumentMapping<TestDoc>("TestDoc")
                .SetIdMapper(x => x.Id)
                .SetPartitionMapper(x => x.Id)
                .Finish();
        }

        private void CreateTestStoreWithAttachment(
            out DocumentStoreConfig storeConfig,
            out DocumentTypeMapping<TestDoc> storeMapping,
            out AttachmentTypeMapping<TestDoc, TestAttachment> attachmentMapping)
        {
            var storeBuilder = new DocumentStoreConfigBuilder("StoreA");

            var document = storeBuilder.AddDocument("TestDoc");
            var attachment = document.AddAttachment("TestAttachment");

            storeMapping = storeBuilder.AddDocumentMapping<TestDoc>(document.Name)
                .SetIdMapper(x => x.Id)
                .SetPartitionMapper(x => x.Id)
                .Finish();

            attachmentMapping = storeMapping.AddAttachmentMapping<TestAttachment>(attachment.Name)
                .Finish();

            storeConfig = storeBuilder.Finish();
        }

        private void CreateTestFailureStore(out DocumentStoreConfig storeConfig, out DocumentTypeMapping<TestFailureDoc> storeMapping)
        {
            var storeBuilder = new DocumentStoreConfigBuilder("StoreFail");
            storeBuilder.AddDocument("TestDoc");

            storeConfig = storeBuilder.Finish();

            storeMapping = storeBuilder.AddDocumentMapping<TestFailureDoc>("TestDoc")
                .SetIdMapper(x => x.Id)
                .SetPartitionMapper(x => x.Id)
                .Finish();
        }

        private IQueryable<VersionedDocumentStoreClient.VersionedDbDocument> CreateQueryResult(IEnumerable<VersionedDocumentStoreClient.VersionedDbDocument> docs)
        {
            return new EnumerableQuery<VersionedDocumentStoreClient.VersionedDbDocument>(docs);
        }

        private IEnumerable<VersionedDocumentStoreClient.VersionedDbDocument> CreateFailingQueryEnumerable()
        {
            string error = null;

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed - Intentionally creating an error in an enumerable here.
            // ReSharper disable once PossibleNullReferenceException
            error.ToString();

            yield break;
        }

        private void SetDocContent(
            VersionedDocumentStoreClient.VersionedDbDocument storeDoc,
            TestDoc content,
            DocumentStoreConfig config,
            DocumentDbAccess dbAccess,
            DocumentTypeMapping<TestDoc> mapping)
        {
            storeDoc.SetPropertyValue(CreateContentKey(config, dbAccess, mapping), content);
        }

        private void SetDocTimestamp(VersionedDocumentStoreClient.VersionedDbDocument storeDoc, DateTime time)
        {
            var unixStartTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

            var val = (ulong)(time - unixStartTime).TotalSeconds;
            storeDoc.SetPropertyValue("_ts", val);
        }

        private DateTime GetUtcNowSeconds()
        {
            var now = DateTime.UtcNow;
            return new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second, DateTimeKind.Utc);
        }

        private VersionedDocumentStoreClient.VersionedDbDocument CreateDoc()
        {
            return new VersionedDocumentStoreClient.VersionedDbDocument
            {
                Id = Guid.NewGuid().ToString(),
                DocumentId = "1",
                Service = "Test",
                Version = 1,
                Deleted = false
            };
        }

        private VersionedDocumentStoreClient.VersionedDbDocument CreateDoc(string docId, int version = 1, bool deleted = false)
        {
            return new VersionedDocumentStoreClient.VersionedDbDocument
            {
                Id = Guid.NewGuid().ToString(),
                DocumentId = docId,
                Service = "Test",
                Version = version,
                Deleted = deleted
            };
        }

        private async void DoUpsertTest(
            IDocumentClient client,
            DocumentDbAccess dbAccess,
            IList<VersionedDocumentStoreClient.VersionedDbDocument> existingQueryResult,
            TestDoc expectedDoc,
            int version,
            int? checkVersion,
            bool isDeleted)
        {
            CreateTestStore(out var storeConfig, out var storeMapping);

            if (existingQueryResult != null)
            {
                client.CreateDocumentQuery<VersionedDocumentStoreClient.VersionedDbDocument>(Arg.Any<Uri>(), Arg.Any<SqlQuerySpec>(), Arg.Any<FeedOptions>())
                    .Returns(CreateQueryResult(existingQueryResult));
            }

            object savedObj = null;
            await client.CreateDocumentAsync(
                Arg.Any<Uri>(),
                Arg.Do<DocumentStoreClient<VersionedDocumentStoreClient.VersionedDbDocument>.DbDocument>(x => savedObj = x));

            var logic = new VersionedDocumentStoreClient(dbAccess, storeConfig);

            await logic.UpsertDocumentAsync(expectedDoc, storeMapping, new OperationOptions{ CheckVersion = checkVersion });

            Assert.NotNull(savedObj);
            var savedDoc = ExtractDoc(savedObj);

            client.CreateDocumentQuery<VersionedDocumentStoreClient.VersionedDbDocument>(Arg.Any<Uri>(), Arg.Any<SqlQuerySpec>(), Arg.Any<FeedOptions>())
                .Returns(CreateQueryResult(new[] { savedDoc }));

            var result = await logic.GetDocumentAsync(expectedDoc.Id, storeMapping, null);

            Assert.Equal(DocumentReadResultType.Loaded, result.ResultType);
            Assert.Equal(expectedDoc.Data, result.Document.Data);
            Assert.Equal(expectedDoc.Id, result.Document.Id);
            Assert.Null(result.FailureDetails);

            Assert.Equal(version, result.Metadata.Version);
            Assert.Equal(isDeleted, result.Metadata.IsDeleted);
        }

        private async void DoUpsertFailureTest<T>(
            IDocumentClient client,
            DocumentDbAccess dbAccess,
            IList<VersionedDocumentStoreClient.VersionedDbDocument> existingQueryResult,
            TestDoc storeDoc,
            int checkVersion,
            string expectedErrorMessage) where T : NebulaException
        {
            CreateTestStore(out _, out var storeMapping);
            await DoFailureTest<T>(client, dbAccess, existingQueryResult, async x => await x.UpsertDocumentAsync(storeDoc, storeMapping, 
                new OperationOptions{ CheckVersion = checkVersion }), expectedErrorMessage);
        }

        private async void DoDeleteTest(
            IDocumentClient client,
            DocumentDbAccess dbAccess,
            IList<VersionedDocumentStoreClient.VersionedDbDocument> existingQueryResult,
            TestDoc expectedDoc,
            int version,
            int? checkVersion,
            bool isDeleted)
        {
            CreateTestStore(out var storeConfig, out var storeMapping);

            if (existingQueryResult != null)
            {
                client.CreateDocumentQuery<VersionedDocumentStoreClient.VersionedDbDocument>(Arg.Any<Uri>(), Arg.Any<SqlQuerySpec>(), Arg.Any<FeedOptions>())
                    .Returns(CreateQueryResult(existingQueryResult));
            }

            object savedObj = null;
            await client.CreateDocumentAsync(
                Arg.Any<Uri>(),
                Arg.Do<DocumentStoreClient<VersionedDocumentStoreClient.VersionedDbDocument>.DbDocument>(x => savedObj = x));

            var logic = new VersionedDocumentStoreClient(dbAccess, storeConfig);

            await logic.DeleteDocumentAsync(expectedDoc.Id, storeMapping, new OperationOptions{ CheckVersion = checkVersion });

            Assert.NotNull(savedObj);
            var savedDoc = ExtractDoc(savedObj);

            client.CreateDocumentQuery<VersionedDocumentStoreClient.VersionedDbDocument>(Arg.Any<Uri>(), Arg.Any<SqlQuerySpec>(), Arg.Any<FeedOptions>())
                .Returns(CreateQueryResult(new[] { savedDoc }));

            var options = new VersionedDocumentReadOptions { IncludeDeleted = true };
            var result = await logic.GetDocumentAsync(expectedDoc.Id, storeMapping, options);

            Assert.Equal(DocumentReadResultType.Loaded, result.ResultType);
            Assert.Equal(expectedDoc.Data, result.Document.Data);
            Assert.Equal(expectedDoc.Id, result.Document.Id);
            Assert.Null(result.FailureDetails);

            Assert.Equal(version, result.Metadata.Version);
            Assert.Equal(isDeleted, result.Metadata.IsDeleted);
        }

        private async void DoDeleteNoChangeTest(
            IDocumentClient client,
            DocumentDbAccess dbAccess,
            IList<VersionedDocumentStoreClient.VersionedDbDocument> existingQueryResult,
            string docId)
        {
            CreateTestStore(out var storeConfig, out var storeMapping);

            if (existingQueryResult != null)
            {
                client.CreateDocumentQuery<VersionedDocumentStoreClient.VersionedDbDocument>(Arg.Any<Uri>(), Arg.Any<SqlQuerySpec>(), Arg.Any<FeedOptions>())
                    .Returns(CreateQueryResult(existingQueryResult));
            }

            var logic = new VersionedDocumentStoreClient(dbAccess, storeConfig);

            await logic.DeleteDocumentAsync(docId, storeMapping, new OperationOptions());

            await client.DidNotReceive().CreateDocumentAsync(
                Arg.Any<Uri>(),
                Arg.Any<DocumentStoreClient<VersionedDocumentStoreClient.VersionedDbDocument>.DbDocument>());
        }

        private async void DoDeleteFailureTest<T>(
            IDocumentClient client,
            DocumentDbAccess dbAccess,
            IList<VersionedDocumentStoreClient.VersionedDbDocument> existingQueryResult,
            string docId,
            int checkVersion,
            string expectedErrorMessage) where T : NebulaException
        {
            CreateTestStore(out _, out var storeMapping);
            await DoFailureTest<T>(client, dbAccess, existingQueryResult, async x => await x.DeleteDocumentAsync(docId, storeMapping, 
                new OperationOptions{ CheckVersion = checkVersion }), expectedErrorMessage);
        }

        private async Task DoFailureTest<T>(
            IDocumentClient client,
            DocumentDbAccess dbAccess,
            IList<VersionedDocumentStoreClient.VersionedDbDocument> existingQueryResult,
            Func<VersionedDocumentStoreClient, Task> call,
            string expectedErrorMessage) where T : NebulaException
        {
            CreateTestStore(out var storeConfig, out _);

            if (existingQueryResult != null)
            {
                client.CreateDocumentQuery<VersionedDocumentStoreClient.VersionedDbDocument>(Arg.Any<Uri>(), Arg.Any<SqlQuerySpec>(), Arg.Any<FeedOptions>())
                    .Returns(CreateQueryResult(existingQueryResult));
            }

            var logic = new VersionedDocumentStoreClient(dbAccess, storeConfig);

            var ex = await Assert.ThrowsAsync<T>(async () => await call(logic));

            Assert.Contains(expectedErrorMessage, ex.Message);
        }

        private TestDoc CreateTestDoc1(string id = "10")
        {
            return new TestDoc
            {
                Id = id,
                Data = "test"
            };
        }

        private class TestDoc
        {
            [JsonProperty("id")]
            public string Id { get; set; }

            public string Data { get; set; }
        }

        private class TestFailureDoc : TestDoc
        {
            [JsonProperty(Required = Required.Always)]
            public string Error { get; set; }
        }

        private class TestAttachment
        {
            public string Data { get; set; }
        }
    }
}
