using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Nebula.Config;
using Nebula.Versioned;

namespace Nebula.Tests.Versioned
{
    public class TestDocumentStore : VersionedDocumentStore
    {
        private readonly DocumentTypeMapping<TestDocument> _mapping;

        private readonly DocumentStoreConfig _config;
        private readonly IVersionedDocumentStoreClient _client;

        public TestDocumentStore(IDocumentDbAccessProvider dbAccessProvider) : base(dbAccessProvider, false)
        {
            var config = new DocumentStoreConfigBuilder("Documents");

            var documentType = config.AddDocument("Document").Finish();

            _mapping = config.AddDocumentMapping<TestDocument>(documentType.DocumentName)
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

        public async Task<VersionedDocumentReadResult<TestDocument>> GetById(Guid id, int version)
        {
            var result = await StoreClient.GetDocumentAsync(id.ToString(), version, _mapping);

            return result;
        }

        public async Task<VersionedDocumentReadResult<TestDocument>> GetById(Guid id)
        {
            var result = await StoreClient.GetDocumentAsync(id.ToString(), _mapping, null);

            return result;
        }

        public async Task<VersionedDocumentReadResult<TestDocument>> GetByIdIncludingDeleted(Guid id)
        {
            var result = await StoreClient.GetDocumentAsync(
                id.ToString(),
                _mapping,
                new VersionedDocumentReadOptions { IncludeDeleted = true });

            return result;
        }

        public async Task<VersionedDocumentBatchReadResult<TestDocument>> GetByIds(IEnumerable<Guid> ids)
        {
            var result = await StoreClient.GetDocumentsAsync(ids.Select(id => id.ToString()), _mapping, null);

            return result;
        }

        public async Task<VersionedDocumentBatchReadResult<TestDocument>> GetByIdsIncludingDeleted(IList<Guid> ids)
        {
            var result = await StoreClient.GetDocumentsAsync(
                ids.Select(id => id.ToString()),
                _mapping,
                new VersionedDocumentReadOptions { IncludeDeleted = true });

            return result;
        }

        public async Task<VersionedDocumentMetadataReadResult> GetMetadata(Guid id)
        {
            var result = await StoreClient.GetDocumentMetadataAsync(id.ToString(), _mapping);

            return result;
        }

        public async Task<VersionedDocumentQueryResult<TestDocument>> QueryLatest(string query)
        {
            var result = await StoreClient.GetDocumentsAsync(query, _mapping, null);

            return result;
        }

        public async Task<VersionedDocumentQueryResult<TestDocument>> QueryLatestIncludingDeleted(string query)
        {
            var result = await StoreClient.GetDocumentsAsync(
                query,
                _mapping,
                new VersionedDocumentReadOptions { IncludeDeleted = true });

            return result;
        }

        public async Task<IList<VersionedDocumentWithMetadata<TestDocument>>> QueryAll()
        {
            var result = await StoreClient.GetDocumentsAsync(_mapping, null);

            return result;
        }

        public async Task<IList<VersionedDocumentWithMetadata<TestDocument>>> QueryAllIncludingDeleted()
        {
            var result = await StoreClient.GetDocumentsAsync(
                _mapping,
                new VersionedDocumentReadOptions { IncludeDeleted = true });

            return result;
        }

        public async Task Upsert(TestDocument doc)
        {
            await StoreClient.UpsertDocumentAsync(doc, _mapping, new OperationOptions());
        }

        public async Task Delete(Guid id)
        {
            await StoreClient.DeleteDocumentAsync(id.ToString(), _mapping, new OperationOptions());
        }
    }
}