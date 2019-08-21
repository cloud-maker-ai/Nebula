using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Nebula.Config;
using Nebula.Utils;
using Newtonsoft.Json;

namespace Nebula.Versioned
{
    /// <inheritdoc cref="IVersionedDocumentStoreClient"/>
    internal class VersionedDocumentStoreClient : DocumentStoreClient<VersionedDocumentStoreClient.VersionedDbDocument>, IVersionedDocumentStoreClient
    {
        /// <summary>
        /// Initialises a new instance of the <see cref="VersionedDocumentStoreClient"/> class.
        /// </summary>
        /// <param name="dbAccess">The db access interface.</param>
        /// <param name="config">The store config.</param>
        public VersionedDocumentStoreClient(DocumentDbAccess dbAccess, DocumentStoreConfig config)
            : this(dbAccess, config, null)
        {
        }

        /// <summary>
        /// Initialises a new instance of the <see cref="VersionedDocumentStoreClient"/> class.
        /// </summary>
        /// <param name="dbAccess">The db access interface.</param>
        /// <param name="config">The store config.</param>
        /// <param name="metadataSource">The document metadata source.</param>
        public VersionedDocumentStoreClient(
            DocumentDbAccess dbAccess,
            DocumentStoreConfig config,
            IDocumentMetadataSource metadataSource)
            : base(dbAccess, config, new[] { typeof(CreateDocumentStoredProcedure) })
        {
            MetadataSource = metadataSource ?? new NullDocumentMetadataSource();
        }

        private IDocumentMetadataSource MetadataSource { get; }

        /// <inheritdoc />
        public async Task<VersionedDocumentUpsertResult<TDocument>> UpsertDocumentAsync<TDocument>(
            TDocument document,
            DocumentTypeMapping<TDocument> mapping,
            OperationOptions operationOptions)
        {
            if (document == null)
                throw new ArgumentNullException(nameof(document));
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            operationOptions = operationOptions ?? new OperationOptions();

            var documentId = mapping.IdMapper(document);

            // Get the newest document version if one exists.
            var existingDocument = await GetDocumentIncludingDeleted(documentId, mapping);

            if (existingDocument != null && operationOptions.CheckVersion.HasValue && operationOptions.CheckVersion != existingDocument.Version)
            {
                throw new NebulaStoreConcurrencyException("Existing document version does not match the specified check version.");
            }

            var version = CalculateNextVersion(existingDocument);

            var dbRecord = new VersionedDbDocument();
            dbRecord.Id = CreateRecordId(documentId, version, mapping);
            dbRecord.DocumentId = documentId;
            dbRecord.Service = DbAccess.ConfigManager.ServiceName;
            dbRecord.PartitionKey = mapping.PartitionKeyMapper(document);
            dbRecord.Version = version;
            dbRecord.Actor = GetActorId();

            SetDocumentContent(dbRecord, document, mapping);

            await CreateDocumentAsync(dbRecord, existingDocument);

            var updatedDocument = await GetDocumentAsync(documentId, version, mapping);

            return new VersionedDocumentUpsertResult<TDocument>(documentId, updatedDocument.Metadata, updatedDocument.Document);
        }

        /// <inheritdoc />
        public async Task DeleteDocumentAsync<TDocument>(string id, DocumentTypeMapping<TDocument> mapping, OperationOptions operationOptions)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            operationOptions = operationOptions ?? new OperationOptions();

            var query = CreateQueryById(id, mapping);
            var documents = await ExecuteQueryAsync(query);

            var existingDocument = FindLatestDocumentIncludingDeleted(documents);

            if (existingDocument == null)
            {
                // Document not found. Treated similarly to already deleted.
                return;
            }

            if (operationOptions.CheckVersion.HasValue && operationOptions.CheckVersion != existingDocument.Version)
            {
                throw new NebulaStoreConcurrencyException("Existing document version does not match the specified check version");
            }

            // Only perform removal if it is not already deleted.
            if (existingDocument.Deleted)
            {
                // Document already deleted.
                return;
            }

            // Document not deleted. Create deletion record.

            var version = CalculateNextVersion(existingDocument);

            var dbRecord = new VersionedDbDocument();
            dbRecord.Id = CreateRecordId(id, version, mapping);
            dbRecord.DocumentId = existingDocument.DocumentId;
            dbRecord.Service = DbAccess.ConfigManager.ServiceName;
            dbRecord.PartitionKey = existingDocument.PartitionKey;
            dbRecord.Version = version;
            dbRecord.Deleted = true;
            dbRecord.Actor = GetActorId();

            SetDocumentContentFromExisting(dbRecord, existingDocument, mapping);

            await CreateDocumentAsync(dbRecord, existingDocument);
        }

        /// <inheritdoc />
        public async Task<VersionedDocumentReadResult<TDocument>> GetDocumentAsync<TDocument>(
            string id,
            DocumentTypeMapping<TDocument> mapping,
            VersionedDocumentReadOptions options)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            options = options ?? new VersionedDocumentReadOptions();

            var query = CreateQueryById(id, mapping);
            var documents = await ExecuteQueryAsync(query);

            return ReadDocumentAsync(id, mapping, documents, options);
        }

        /// <inheritdoc />
        public async Task<VersionedDocumentReadResult<TDocument>> GetDocumentAsync<TDocument>(
            string id,
            int version,
            DocumentTypeMapping<TDocument> mapping)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            var query = CreateQueryById(id, version, mapping);
            var documents = await ExecuteQueryAsync(query);

            var readOptions = new VersionedDocumentReadOptions { IncludeDeleted = true };

            return ReadDocumentAsync(id, mapping, documents, readOptions);
        }

        /// <inheritdoc />
        public async Task<VersionedDocumentMetadataReadResult> GetDocumentMetadataAsync<TDocument>(
            string id,
            DocumentTypeMapping<TDocument> mapping)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            var query = CreateQueryAllById(id, mapping);
            var documents = await ExecuteQueryAsync(query);

            var ordered = documents.OrderBy(d => d.Version).ToArray();

            if (ordered.Length == 0)
            {
                // Document not found.
                return null;
            }

            var records = new List<VersionedDocumentMetadata>();

            var createdTime = ordered[0].Timestamp;

            foreach (var document in ordered)
            {
                var metadata = new VersionedDocumentMetadata(
                    document.Version, document.Deleted, createdTime, document.Timestamp, document.Actor);

                records.Add(metadata);
            }

            return new VersionedDocumentMetadataReadResult(id, ImmutableList.CreateRange(records));
        }

        /// <inheritdoc />
        public async Task<VersionedDocumentBatchReadResult<TDocument>> GetDocumentsAsync<TDocument>(
            IEnumerable<string> ids,
            DocumentTypeMapping<TDocument> mapping,
            VersionedDocumentReadOptions options)
        {
            if (ids == null)
                throw new ArgumentNullException(nameof(ids));
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            var idSet = new HashSet<string>(ids);

            if (idSet.Count == 0)
            {
                return VersionedDocumentBatchReadResult<TDocument>.Empty;
            }

            options = options ?? new VersionedDocumentReadOptions();

            var query = CreateQueryByIds(idSet, mapping);
            var documents = await ExecuteQueriesAsync(query);

            List<VersionedDocumentReadResult<TDocument>> loaded = new List<VersionedDocumentReadResult<TDocument>>();
            List<VersionedDocumentReadResult<TDocument>> failed = new List<VersionedDocumentReadResult<TDocument>>();

            foreach (var docs in documents.GroupBy(x => x.DocumentId))
            {
                VersionedDbDocument doc;
                DateTime createdTime;
                DateTime modifiedTime;

                if (!TryGetLatestDocument(docs, options, out doc, out createdTime, out modifiedTime))
                {
                    // Document exists but the latest version is deleted.
                    continue;
                }

                var metadata = new VersionedDocumentMetadata(doc.Version, doc.Deleted, createdTime, modifiedTime, doc.Actor);

                TDocument content;
                DocumentReadFailureDetails failure;

                if (TryGetDocumentContent(doc, mapping, out content, out failure))
                {
                    loaded.Add(VersionedDocumentReadResult<TDocument>.CreateOkay(doc.DocumentId, metadata, content));
                }
                else
                {
                    failed.Add(VersionedDocumentReadResult<TDocument>.CreateFailure(doc.DocumentId, metadata, failure));
                }

                idSet.Remove(doc.DocumentId);
            }

            var missing = ImmutableList.CreateRange(idSet);

            return new VersionedDocumentBatchReadResult<TDocument>(ImmutableList.CreateRange(loaded), missing, ImmutableList.CreateRange(failed));
        }

        /// <inheritdoc />
        public async Task<VersionedDocumentQueryResult<TDocument>> GetDocumentsAsync<TDocument>(
            string query,
            DocumentTypeMapping<TDocument> mapping,
            VersionedDocumentReadOptions options)
        {
            if (query == null)
                throw new ArgumentNullException(nameof(query));
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            return await GetDocumentsAsync(query, new DbParameter[0], mapping, options);
        }

        /// <inheritdoc />
        public async Task<VersionedDocumentQueryResult<TDocument>> GetDocumentsAsync<TDocument>(
            string query,
            IEnumerable<DbParameter> parameters,
            DocumentTypeMapping<TDocument> mapping)
        {
            if (query == null)
                throw new ArgumentNullException(nameof(query));
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            return await GetDocumentsAsync(query, parameters, mapping, null);
        }

        /// <inheritdoc />
        public async Task<VersionedDocumentQueryResult<TDocument>> GetDocumentsAsync<TDocument>(
            string query,
            IEnumerable<DbParameter> parameters,
            DocumentTypeMapping<TDocument> mapping,
            VersionedDocumentReadOptions options)
        {
            if (query == null)
                throw new ArgumentNullException(nameof(query));
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            parameters = parameters ?? new DbParameter[0];

            var builtQuery = CreateQuery(mapping, query, parameters);
            var documents = await ExecuteQueryAsync(builtQuery);

            if (documents.Count == 0)
            {
                return VersionedDocumentQueryResult<TDocument>.Empty;
            }

            options = options ?? new VersionedDocumentReadOptions();

            List<VersionedDocumentReadResult<TDocument>> loaded = new List<VersionedDocumentReadResult<TDocument>>();
            List<VersionedDocumentReadResult<TDocument>> failed = new List<VersionedDocumentReadResult<TDocument>>();

            foreach (var docs in documents.GroupBy(x => x.DocumentId))
            {
                VersionedDbDocument doc;
                DateTime createdTime;
                DateTime modifiedTime;

                if (!TryGetLatestDocument(docs, options, out doc, out createdTime, out modifiedTime))
                {
                    // Document exists but the latest version is deleted.
                    continue;
                }

                var metadata = new VersionedDocumentMetadata(doc.Version, doc.Deleted, createdTime, modifiedTime, doc.Actor);

                TDocument content;
                DocumentReadFailureDetails failure;

                if (TryGetDocumentContent(doc, mapping, out content, out failure))
                {
                    loaded.Add(VersionedDocumentReadResult<TDocument>.CreateOkay(doc.DocumentId, metadata, content));
                }
                else
                {
                    failed.Add(VersionedDocumentReadResult<TDocument>.CreateFailure(doc.DocumentId, metadata, failure));
                }
            }

            return new VersionedDocumentQueryResult<TDocument>(ImmutableList.CreateRange(loaded), ImmutableList.CreateRange(failed));
        }

        public async Task<IList<VersionedDocumentWithMetadata<TDocument>>> GetDocumentsAsync<TDocument>(
            DocumentTypeMapping<TDocument> mapping,
            VersionedDocumentReadOptions options)
        {
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            options = options ?? new VersionedDocumentReadOptions();

            var query = CreateQueryAll(mapping);
            var documents = await ExecuteQueryAsync(query);

            List<VersionedDocumentWithMetadata<TDocument>> result = new List<VersionedDocumentWithMetadata<TDocument>>();

            foreach (var docs in documents.GroupBy(x => x.DocumentId))
            {
                VersionedDbDocument doc;
                DateTime createdTime;
                DateTime modifiedTime;

                if (!TryGetLatestDocument(docs, options, out doc, out createdTime, out modifiedTime))
                {
                    // Document exists but the latest version is deleted.
                    continue;
                }

                TDocument content;
                if (TryGetDocumentContent(doc, mapping, out content, out _))
                {
                    var metadata = new VersionedDocumentMetadata(doc.Version, doc.Deleted, createdTime, modifiedTime, doc.Actor);
                    result.Add(new VersionedDocumentWithMetadata<TDocument>(metadata, content));
                }
            }

            return result;
        }

        public Task<TAttachment> GetAttachmentAsync<TDocument, TAttachment>(
            TDocument document,
            int documentVersion,
            AttachmentTypeMapping<TDocument, TAttachment> attachmentMapping)
        {
            if (document == null)
                throw new ArgumentNullException(nameof(document));
            if (attachmentMapping == null)
                throw new ArgumentNullException(nameof(attachmentMapping));

            var documentMapping = attachmentMapping.DocumentMapping;

            var partitionKey = documentMapping.PartitionKeyMapper(document);
            var documentId = documentMapping.IdMapper(document);
            var documentRecordId = CreateRecordId(documentId, documentVersion, documentMapping);

            return GetDocumentAttachmentAsync(documentRecordId, partitionKey, attachmentMapping);
        }

        public Task CreateAttachmentAsync<TDocument, TAttachment>(
            TDocument document,
            int documentVersion,
            AttachmentTypeMapping<TDocument, TAttachment> attachmentMapping,
            TAttachment attachment)
        {
            if (document == null)
                throw new ArgumentNullException(nameof(document));
            if (attachmentMapping == null)
                throw new ArgumentNullException(nameof(attachmentMapping));
            if (attachment == null)
                throw new ArgumentNullException(nameof(attachment));

            var documentMapping = attachmentMapping.DocumentMapping;

            var documentId = documentMapping.IdMapper(document);
            var documentRecordId = CreateRecordId(documentId, documentVersion, documentMapping);
            var partitionKey = documentMapping.PartitionKeyMapper(document);

            return CreateDocumentAttachmentAsync(documentRecordId, partitionKey, attachmentMapping, attachment);
        }

        private VersionedDocumentReadResult<TDocument> ReadDocumentAsync<TDocument>(
            string id,
            DocumentTypeMapping<TDocument> mapping,
            IList<VersionedDbDocument> documents,
            VersionedDocumentReadOptions readOptions)
        {
            VersionedDbDocument document;
            DateTime createdTime;
            DateTime modifiedTime;

            if (!TryGetLatestDocument(documents, readOptions, out document, out createdTime, out modifiedTime))
            {
                return null;
            }

            var metadata = new VersionedDocumentMetadata(document.Version, document.Deleted, createdTime, modifiedTime, document.Actor);

            TDocument content;
            DocumentReadFailureDetails failure;

            if (!TryGetDocumentContent(document, mapping, out content, out failure))
            {
                return VersionedDocumentReadResult<TDocument>.CreateFailure(id, metadata, failure);
            }

            return VersionedDocumentReadResult<TDocument>.CreateOkay(id, metadata, content);
        }

        private async Task<VersionedDbDocument> GetDocumentIncludingDeleted<TDocument>(string id, DocumentTypeMapping<TDocument> mapping)
        {
            var query = CreateQueryById(id, mapping);
            var documents = await ExecuteQueryAsync(query);

            return FindLatestDocumentIncludingDeleted(documents);
        }

        private string CreateRecordId<TDocument>(string id, long version, DocumentTypeMapping<TDocument> mapping)
        {
            return CreateDbRecordId(mapping, id, version.ToString());
        }

        private int CalculateNextVersion(VersionedDbDocument document)
        {
            int version = 1;

            if (document != null)
            {
                version = document.Version + 1;
            }

            return version;
        }

        private VersionedDbDocument FindLatestDocumentIncludingDeleted(IEnumerable<VersionedDbDocument> documents)
        {
            VersionedDbDocument document;

            var options = new VersionedDocumentReadOptions();
            options.IncludeDeleted = true;

            if (TryGetLatestDocument(documents, options, out document, out _, out _))
            {
                return document;
            }

            return null;
        }

        private bool TryGetLatestDocument(
            IEnumerable<VersionedDbDocument> documents,
            VersionedDocumentReadOptions options,
            out VersionedDbDocument document,
            out DateTime createdTime,
            out DateTime modifiedTime)
        {
            var ordered = documents.OrderBy(x => x.Version).ToArray();

            if (ordered.Length == 0)
            {
                document = null;
                createdTime = default(DateTime);
                modifiedTime = default(DateTime);
                return false;
            }

            var first = ordered[0];
            var last = ordered[ordered.Length - 1];

            if (last.Deleted && !options.IncludeDeleted)
            {
                document = null;
                createdTime = default(DateTime);
                modifiedTime = default(DateTime);
                return false;
            }

            createdTime = first.Timestamp;
            modifiedTime = last.Timestamp;
            document = last;
            return true;
        }

        private async Task CreateDocumentAsync(VersionedDbDocument newRecord, VersionedDbDocument existingRecord)
        {
            await ExecuteStoredProcedureAsync<CreateDocumentStoredProcedure>(newRecord.PartitionKey, newRecord, existingRecord);
        }

        private IQueryable<VersionedDbDocument> CreateQueryById<TDocument>(string id, DocumentTypeMapping<TDocument> mapping)
        {
            var idParameter = new DbParameter("id", id);

            // The first version is always fetched to get the creation time.
            var query = $"[x].{mapping.IdPropertyName} = @id AND (c['@latest'] = true OR c['@version'] = 1)";

            return CreateQuery(mapping, query, new[] { idParameter });
        }

        private IQueryable<VersionedDbDocument> CreateQueryById<TDocument>(string id, int version, DocumentTypeMapping<TDocument> mapping)
        {
            var idParameter = new DbParameter("id", id);

            // The first version is always fetched to get the creation time.
            var query = $"[x].{mapping.IdPropertyName} = @id AND (c['@version'] = {version} OR c['@version'] = 1)";

            return CreateQuery(mapping, query, new[] { idParameter });
        }

        private IQueryable<VersionedDbDocument> CreateQueryAllById<TDocument>(string id, DocumentTypeMapping<TDocument> mapping)
        {
            var idParameter = new DbParameter("id", id);
            var query = $"[x].{mapping.IdPropertyName} = @id";

            return CreateQuery(mapping, query, new[] { idParameter });
        }

        private List<IQueryable<VersionedDbDocument>> CreateQueryByIds<TDocument>(ICollection<string> ids, DocumentTypeMapping<TDocument> mapping)
        {
            var batchSize = DbAccess.QueryPolicy.GetIdSearchLimit(ids);
            var batched = ids.Batch(batchSize);

            var result = new List<IQueryable<VersionedDbDocument>>();

            foreach (var batch in batched)
            {
                var query = CreateQueryByIdsImpl(batch.ToArray(), mapping);
                result.Add(query);
            }

            return result;
        }

        private IQueryable<VersionedDbDocument> CreateQueryAll<TDocument>(DocumentTypeMapping<TDocument> mapping)
        {
            return CreateQuery(mapping, null);
        }

        private IQueryable<VersionedDbDocument> CreateQuery<TDocument>(
            DocumentTypeMapping<TDocument> mapping,
            string query,
            IEnumerable<DbParameter> parameters = null)
        {
            FeedOptions queryOptions = new FeedOptions
            {
                MaxItemCount = -1,
                EnableCrossPartitionQuery = true
            };

            var contentKey = CreateContentKey(mapping);

            string selectStatement = $"SELECT * FROM {DbAccess.DbConfig.CollectionName} as c WHERE is_defined(c.{contentKey})";

            if (query != null)
            {
                // Perform substitution on references to the internal document. '[x].' is replaced while excluding
                // occurrences in a string.
                string substitutedClause = Regex.Replace(query, "\\[x\\]\\.(?=[^']*(?:'[^']*'[^']*)*$)", $"c.{contentKey}.");

                selectStatement = $"{selectStatement} AND {substitutedClause}";
            }

            if (!DbAccess.QueryPolicy.IsQueryValid(selectStatement))
            {
                throw new NebulaStoreException("Failed to create document query");
            }

            var querySpec = CreateQuerySpec(selectStatement, parameters);

            var client = DbAccess.GetClient();

            return MakeClientCall(
                () => client.CreateDocumentQuery<VersionedDbDocument>(CollectionUri, querySpec, queryOptions),
                "Failed to create document query");
        }

        private IQueryable<VersionedDbDocument> CreateQueryByIdsImpl<TDocument>(ICollection<string> ids, DocumentTypeMapping<TDocument> mapping)
        {
            // Optimise queries for a single id to use EQUALS instead of IN.
            if (ids.Count == 1)
            {
                return CreateQueryById(ids.First(), mapping);
            }

            var inIds = "'" + string.Join("','", ids) + "'";

            var query = $"[x].{mapping.IdPropertyName} IN ({inIds})";

            return CreateQuery(mapping, query);
        }

        private SqlQuerySpec CreateQuerySpec(string queryText, IEnumerable<DbParameter> parameters)
        {
            var querySpec = new SqlQuerySpec { QueryText = queryText };

            if (parameters != null)
            {
                querySpec.Parameters = new SqlParameterCollection(
                    parameters.Select(p => new SqlParameter(p.Name, p.Value)));
            }

            return querySpec;
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

        internal class VersionedDbDocument : DbDocument
        {
            [JsonProperty("@version")]
            public int Version { get; set; }

            [JsonProperty("@deleted")]
            public bool Deleted { get; set; }

            [JsonProperty("@latest", DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
            public bool Latest { get; set; }
        }
    }
}