using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Nebula.Config;
using Nebula.Versioned;
using Newtonsoft.Json;

namespace Nebula.Unversioned
{
    /// <inheritdoc cref="IVersionedDocumentStoreClient"/>
    internal class UnversionedDocumentStoreClient : DocumentStoreClient<UnversionedDocumentStoreClient.UnversionedDbDocument>, IUnversionedDocumentStoreClient
    {
        /// <summary>
        /// Initialises a new instance of the <see cref="UnversionedDocumentStoreClient"/> class.
        /// </summary>
        /// <param name="dbAccess">The db access interface.</param>
        /// <param name="config">The store config.</param>
        public UnversionedDocumentStoreClient(DocumentDbAccess dbAccess, DocumentStoreConfig config)
            : this(dbAccess, config, null)
        {
        }

        /// <summary>
        /// Initialises a new instance of the <see cref="VersionedDocumentStoreClient"/> class.
        /// </summary>
        /// <param name="dbAccess">The db access interface.</param>
        /// <param name="config">The store config.</param>
        /// <param name="metadataSource">The document metadata source.</param>
        public UnversionedDocumentStoreClient(
            DocumentDbAccess dbAccess,
            DocumentStoreConfig config,
            IDocumentMetadataSource metadataSource)
            : base(dbAccess, config, new[] { typeof(CreateDocumentStoredProcedure) })
        {
            MetadataSource = metadataSource ?? new NullDocumentMetadataSource();
            QueryClient = new UnversionedDocumentQueryClient(dbAccess, config);
        }

        private IDocumentMetadataSource MetadataSource { get; }

        private UnversionedDocumentQueryClient QueryClient { get; }

        /// <inheritdoc />
        public async Task CreateDocumentAsync<TDocument>(
            TDocument document,
            DocumentTypeMapping<TDocument> mapping,
            OperationOptions options)
        {
            if (document == null)
                throw new ArgumentNullException(nameof(document));
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            options = options ?? new OperationOptions();

            var documentId = mapping.IdMapper(document);
            var modifiedTime = DateTime.UtcNow;

            var dbRecord = CreateDbDocument(document, documentId, mapping, options, modifiedTime, modifiedTime);

            await CreateDocumentAsync(dbRecord);
        }

        /// <inheritdoc />
        public async Task UpdateDocumentAsync<TDocument>(
            TDocument document,
            DocumentTypeMapping<TDocument> mapping,
            OperationOptions options)
        {
            if (document == null)
                throw new ArgumentNullException(nameof(document));
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            options = options ?? new OperationOptions();

            var documentId = mapping.IdMapper(document);
            var existingDocument = await GetDocumentByIdAsync(documentId, mapping);

            if (existingDocument == null)
            {
                throw new NebulaStoreException($"Document '{documentId}' for update does not exist");
            }

            var createdTime = existingDocument.CreatedTimestamp;
            var modifiedTime = DateTime.UtcNow;

            var dbRecord = CreateDbDocument(document, documentId, mapping, options, createdTime, modifiedTime);

            await UpdateDocumentAsync(dbRecord, existingDocument);
        }

        /// <inheritdoc />
        public async Task UpsertDocumentAsync<TDocument>(
            TDocument document,
            DocumentTypeMapping<TDocument> mapping,
            OperationOptions options)
        {
            if (document == null)
                throw new ArgumentNullException(nameof(document));
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            options = options ?? new OperationOptions();

            var documentId = mapping.IdMapper(document);
            var existingDocument = await GetDocumentByIdAsync(documentId, mapping);

            var modifiedTime = DateTime.UtcNow;
            var createdTime = existingDocument?.CreatedTimestamp ?? modifiedTime;

            var dbRecord = CreateDbDocument(document, documentId, mapping, options, createdTime, modifiedTime);

            await UpsertDocumentAsync(dbRecord);
        }

        /// <inheritdoc />
        public async Task DeleteDocumentAsync<TDocument>(
            string id,
            DocumentTypeMapping<TDocument> mapping)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            var query = QueryClient.CreateQueryById(id, mapping);
            var documents = await ExecuteQueryAsync(query);

            var document = documents.FirstOrDefault();

            if (document != null)
            {
                await PurgeDocumentAsync(document);
            }
        }

        /// <inheritdoc />
        public async Task<DocumentReadResult<TDocument>> GetDocumentAsync<TDocument>(
            string id,
            DocumentTypeMapping<TDocument> mapping)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            var query = QueryClient.CreateQueryById(id, mapping);
            var documents = await ExecuteQueryAsync(query);

            var document = documents.FirstOrDefault();

            if (document == null)
            {
                return null;
            }

            return ReadDocument(mapping, document);
        }

        /// <inheritdoc />
        public async Task<DocumentBatchReadResult<TDocument>> GetDocumentsAsync<TDocument>(
            IEnumerable<string> ids,
            DocumentTypeMapping<TDocument> mapping)
        {
            if (ids == null)
                throw new ArgumentNullException(nameof(ids));
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            var idSet = new HashSet<string>(ids);

            if (idSet.Count == 0)
            {
                return DocumentBatchReadResult<TDocument>.Empty;
            }

            var query = QueryClient.CreateQueryByIds(idSet, mapping);
            var documents = await ExecuteQueriesAsync(query);

            var loaded = new List<DocumentReadResult<TDocument>>();
            var failed = new List<DocumentReadResult<TDocument>>();

            foreach (var document in documents)
            {
                var readResult = ReadDocument(mapping, document);

                if (readResult.ResultType == DocumentReadResultType.Loaded)
                {
                    loaded.Add(readResult);
                }
                else
                {
                    failed.Add(readResult);
                }

                idSet.Remove(document.DocumentId);
            }

            var missing = ImmutableList.CreateRange(idSet);

            return new DocumentBatchReadResult<TDocument>(
                loaded.ToImmutableList(),
                missing,
                failed.ToImmutableList());
        }

        /// <inheritdoc />
        public async Task<DocumentQueryResult<TDocument>> GetDocumentsAsync<TDocument>(
            string query,
            DocumentTypeMapping<TDocument> mapping)
        {
            if (query == null)
                throw new ArgumentNullException(nameof(query));
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            return await GetDocumentsAsync(query, null, mapping);
        }

        /// <inheritdoc />
        public async Task<DocumentQueryResult<TDocument>> GetDocumentsAsync<TDocument>(
            string query,
            IEnumerable<DbParameter> parameters,
            DocumentTypeMapping<TDocument> mapping)
        {
            if (query == null)
                throw new ArgumentNullException(nameof(query));
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            parameters = parameters ?? new DbParameter[0];

            var builtQuery = QueryClient.CreateQuery(mapping, query, parameters);
            var documents = await ExecuteQueryAsync(builtQuery);

            if (documents.Count == 0)
            {
                return DocumentQueryResult<TDocument>.Empty;
            }

            var loaded = new List<DocumentReadResult<TDocument>>();
            var failed = new List<DocumentReadResult<TDocument>>();

            foreach (var document in documents)
            {
                var readResult = ReadDocument(mapping, document);

                if (readResult.ResultType == DocumentReadResultType.Loaded)
                {
                    loaded.Add(readResult);
                }
                else
                {
                    failed.Add(readResult);
                }
            }

            return new DocumentQueryResult<TDocument>(loaded.ToImmutableList(), failed.ToImmutableList());
        }

        /// <inheritdoc />
        public async Task<IList<DocumentWithMetadata<TDocument>>> GetDocumentsAsync<TDocument>(
            DocumentTypeMapping<TDocument> mapping)
        {
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            var query = QueryClient.CreateQueryAll(mapping);
            var documents = await ExecuteQueryAsync(query);

            var result = new List<DocumentWithMetadata<TDocument>>();

            foreach (var document in documents)
            {
                var docWithMetaData = ReadDocumentWithMetadata(mapping, document);

                if (docWithMetaData != null)
                {
                    result.Add(docWithMetaData);
                }
            }

            return result;
        }

        private DocumentReadResult<TDocument> ReadDocument<TDocument>(
            DocumentTypeMapping<TDocument> mapping,
            UnversionedDbDocument document)
        {
            var metadata = new DocumentMetadata(document.CreatedTimestamp, document.Timestamp, document.Actor);

            TDocument content;
            DocumentReadFailureDetails failure;

            if (!TryGetDocumentContent(document, mapping, out content, out failure))
            {
                return DocumentReadResult<TDocument>.CreateFailure(document.DocumentId, metadata, failure);
            }

            return DocumentReadResult<TDocument>.CreateOkay(document.DocumentId, metadata, content);
        }

        private DocumentWithMetadata<TDocument> ReadDocumentWithMetadata<TDocument>(
            DocumentTypeMapping<TDocument> mapping,
            UnversionedDbDocument document)
        {
            var metadata = new DocumentMetadata(document.CreatedTimestamp, document.Timestamp, document.Actor);

            TDocument content;

            if (!TryGetDocumentContent(document, mapping, out content, out _))
            {
                return null;
            }

            return new DocumentWithMetadata<TDocument>(metadata, content);
        }

        private async Task<UnversionedDbDocument> GetDocumentByIdAsync<TDocument>(
            string documentId,
            DocumentTypeMapping<TDocument> mapping)
        {
            var query = QueryClient.CreateQueryById(documentId, mapping);
            var existingDocuments = await ExecuteQueryAsync(query);

            return existingDocuments.FirstOrDefault();
        }

        private UnversionedDbDocument CreateDbDocument<TDocument>(
            TDocument document,
            string documentId,
            DocumentTypeMapping<TDocument> mapping,
            OperationOptions operationOptions,
            DateTime createdTime,
            DateTime modifiedTime)
        {
            var dbRecord = new UnversionedDbDocument();
            dbRecord.Id = CreateDbRecordId(mapping, documentId);
            dbRecord.DocumentId = documentId;
            dbRecord.Service = DbAccess.ConfigManager.ServiceName;
            dbRecord.PartitionKey = mapping.PartitionKeyMapper(document);
            dbRecord.Actor = GetActorId();

            dbRecord.CreatedTimestamp = createdTime;
            dbRecord.Timestamp = modifiedTime;

            if (operationOptions.Ttl.HasValue)
            {
                dbRecord.TimeToLive = operationOptions.Ttl.Value;
            }

            SetDocumentContent(dbRecord, document, mapping);

            return dbRecord;
        }

        private async Task PurgeDocumentAsync(DbDocument existingDbRecord)
        {
            var requestOptions = new RequestOptions
            {
                PartitionKey = new PartitionKey(existingDbRecord.PartitionKey)
            };

            await MakeClientCall(
                async () => await DbClient.DeleteDocumentAsync(existingDbRecord.SelfLink, requestOptions),
                "Failed to purge document");
        }

        private async Task UpdateDocumentAsync(DbDocument dbRecord, DbDocument existingDbRecord)
        {
            var requestOptions = new RequestOptions
            {
                AccessCondition = new AccessCondition
                {
                    Type = AccessConditionType.IfMatch,
                    Condition = existingDbRecord.ETag
                },
                PartitionKey = new PartitionKey(dbRecord.PartitionKey)
            };

            await MakeClientCall(
                async () => await DbClient.UpsertDocumentAsync(CollectionUri, dbRecord, requestOptions),
                "Failed to update document");
        }

        private async Task UpsertDocumentAsync(DbDocument dbRecord)
        {
            var requestOptions = new RequestOptions {
                PartitionKey = new PartitionKey(dbRecord.PartitionKey)
            };

            await MakeClientCall(
                async () => await DbClient.UpsertDocumentAsync(CollectionUri, dbRecord, requestOptions),
                "Failed to upsert document");
        }

        private string GetActorId()
        {
            try
            {
                return MetadataSource.GetActorId();
            }
            catch (Exception e)
            {
                throw new NebulaStoreException("Document metadata source threw an exception while attempting to retrieve the actor id", e);
            }
        }

        internal class UnversionedDbDocument : DbDocument
        {
            [JsonProperty("@createdTimestamp")]
            [JsonConverter(typeof(UnixDateTimeConverter))]
            public DateTime CreatedTimestamp { get; set; }
        }
    }
}