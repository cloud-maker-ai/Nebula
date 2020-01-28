using System;
using System.Linq;
using System.Threading.Tasks;
using Nebula.Config;

namespace Nebula.Versioned
{
    /// <summary>
    /// A store client for purge operations.
    /// </summary>
    internal class VersionedDocumentStorePurgeClient : DocumentStoreClient<VersionedDocumentStoreClient.VersionedDbDocument>, IVersionedDocumentStorePurgeClient
    {
        private readonly VersionedDocumentQueryClient _queryClient;

        /// <summary>
        /// Initialises a new instance of the <see cref="VersionedDocumentStorePurgeClient"/> class.
        /// </summary>
        /// <param name="dbAccess">The db access interface.</param>
        /// <param name="config">The store config.</param>
        /// <param name="queryClient">The query client.</param>
        public VersionedDocumentStorePurgeClient(
            DocumentDbAccess dbAccess,
            DocumentStoreConfig config,
            VersionedDocumentQueryClient queryClient)
            : base(dbAccess, config, new [] { typeof(PurgeDocumentStoredProcedure) })
        {
            if (queryClient == null)
                throw new ArgumentNullException(nameof(queryClient));

            _queryClient = queryClient;
        }

        /// <inheritdoc />
        public async Task PurgeDocumentAsync<TDocument>(string id, DocumentTypeMapping<TDocument> mapping)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            var query = _queryClient.CreateQueryById(id, 1, mapping);
            var documents = await ExecuteQueryAsync(query);

            var document = documents.FirstOrDefault();

            if (document != null)
            {
                await PurgeDocument(document, mapping);
            }
        }

        /// <inheritdoc />
        public async Task PurgeDocumentsAsync<TDocument>(DocumentTypeMapping<TDocument> mapping)
        {
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));
            
            var query = _queryClient.CreateQueryAll(mapping);
            var documents = await ExecuteQueryAsync(query);

            foreach (var document in documents.GroupBy(x => x.DocumentId))
            {
                await PurgeDocument(document.First(), mapping);
            }
        }

        private async Task PurgeDocument<TDocument>(
            VersionedDocumentStoreClient.VersionedDbDocument document,
            DocumentTypeMapping<TDocument> mapping)
        {
            var contentKey = CreateContentKey(mapping);

            await ExecuteStoredProcedureAsync<PurgeDocumentStoredProcedure>(
                document.PartitionKey,
                contentKey, document.DocumentId);
        }
    }
}