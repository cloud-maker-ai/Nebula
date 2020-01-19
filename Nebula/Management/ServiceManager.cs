using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;

namespace Nebula.Management
{
    /// <inheritdoc />
    public class ServiceManager : IServiceManager
    {
        private readonly DocumentDbAccess _dbAccess;
        private readonly Uri _collectionUri;

        /// <summary>
        /// Initialises a new instance of the <see cref="ServiceManager"/> class.
        /// </summary>
        /// <param name="dbAccess">The db access interface.</param>
        public ServiceManager(DocumentDbAccess dbAccess)
        {
            if (dbAccess == null)
                throw new ArgumentNullException(nameof(dbAccess));

            _dbAccess = dbAccess;
            _collectionUri = UriFactory.CreateDocumentCollectionUri(dbAccess.DbConfig.DatabaseId, dbAccess.DbConfig.CollectionName);
        }

        /// <inheritdoc />
        public async Task PurgeDocumentsAsync()
        {
            var documents = await GetAllDocumentsAsync();

            foreach (var document in documents)
            {
                var requestOptions = new RequestOptions { PartitionKey = new PartitionKey(document.PartitionKey) };

                await _dbAccess.DbClient.DeleteDocumentAsync(
                    UriFactory.CreateDocumentUri(_dbAccess.DbConfig.DatabaseId, _dbAccess.DbConfig.CollectionName, document.Id),
                    requestOptions);
            }
        }

        private async Task<IList<DocumentMetadata>> GetAllDocumentsAsync()
        {
            FeedOptions queryOptions = new FeedOptions
            {
                MaxItemCount = -1,
                EnableCrossPartitionQuery = true
            };

            string selectStatement = $@"
                SELECT c.id, c['_partitionKey'] FROM {_dbAccess.DbConfig.CollectionName} as c
                WHERE is_defined(c['@documentId'])
                    AND c['@service'] = '{_dbAccess.ConfigManager.ServiceName}'
                    AND NOT IS_NULL(c['_partitionKey'])";

            var query = _dbAccess.DbClient.CreateDocumentQuery<DocumentMetadata>(_collectionUri, selectStatement, queryOptions);

            return await ExecuteQueryAsync(query);
        }

        private async Task<IList<DocumentMetadata>> ExecuteQueryAsync(IQueryable<DocumentMetadata> query)
        {
            return await MakeClientCall(async () => await Task.Factory.StartNew(query.ToList), "Failed to execute query");
        }

        private async Task<TReturn> MakeClientCall<TReturn>(Func<Task<TReturn>> call, string onErrorReason)
        {
            try
            {
                return await call();
            }
            catch (Exception e)
            {
                HandleClientError(e, onErrorReason);
                return default(TReturn);
            }
        }

        private void HandleClientError(Exception e, string onErrorReason)
        {
            if (e is DocumentClientException)
            {
                throw new NebulaStoreException(onErrorReason, e);
            }

            throw new NebulaStoreException("Unknown client failure - " + onErrorReason, e);
        }

        internal class DocumentMetadata
        {
            [JsonProperty]
            public string Id { get; set; }

            [JsonProperty("_partitionKey")]
            public string PartitionKey { get; set; }
        }
    }
}