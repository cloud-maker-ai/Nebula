using System;
using System.Threading.Tasks;
using Nebula.Config;
using Nebula.Versioned;

namespace Nebula.AspNetCore.Tests.Store
{
    public class FlowerStore : VersionedDocumentStore
    {
        private readonly DocumentTypeMapping<Daisy> _mapping;

        private readonly DocumentStoreConfig _config;
        private readonly IVersionedDocumentStoreClient _client;

        public FlowerStore(
            IDocumentDbAccessProvider dbAccessProvider,
            IDocumentMetadataSource metadataSource) : base(dbAccessProvider, false)
        {
            var config = new DocumentStoreConfigBuilder("Flowers");

            var documentType = config.AddDocument("Daisy").Finish();

            _mapping = config.AddDocumentMapping<Daisy>(documentType.DocumentName)
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
}