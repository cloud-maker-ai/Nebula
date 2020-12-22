using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Nebula.Config;
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
            QueryClient = new VersionedDocumentQueryClient(dbAccess, config);
        }

        private IDocumentMetadataSource MetadataSource { get; }

        private VersionedDocumentQueryClient QueryClient { get; }

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
            dbRecord.Timestamp = DateTime.UtcNow;

            SetDocumentContent(dbRecord, document, mapping);

            await CreateDocumentAsync(dbRecord, existingDocument);

            var updatedDocument = await GetDocumentAsync(documentId, version, mapping);

            if (updatedDocument == null)
            {
                throw new NebulaStoreException("Failed to retrieve document after successful upsert");
            }

            if (updatedDocument.ResultType == DocumentReadResultType.Failed)
            {
                throw new NebulaStoreException($"Failed to retrieve document: {updatedDocument.FailureDetails.Message}");
            }

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

            var query = QueryClient.CreateQueryById(id, mapping);
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
            dbRecord.Timestamp = DateTime.UtcNow;

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

            var query = QueryClient.CreateQueryById(id, mapping);
            var documents = await ExecuteQueryAsync(query);

            return ReadLatestDocumentAsync(id, mapping, documents, options);
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
            if (version < 1)
                throw new ArgumentOutOfRangeException(nameof(version), "Version must be greater than zero");

            var query = QueryClient.CreateQueryById(id, version, mapping);
            var documents = await ExecuteQueryAsync(query);

            return ReadDocumentVersionAsync(id, mapping, documents);
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

            var query = QueryClient.CreateQueryAllVersionsById(id, mapping);
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

            var query = QueryClient.CreateQueryByIds(idSet, mapping);
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

            var builtQuery = QueryClient.CreateQueryByLatest(mapping, query, parameters);
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

            var query = QueryClient.CreateQueryByLatest(mapping, null);
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

        private VersionedDocumentReadResult<TDocument> ReadDocumentVersionAsync<TDocument>(
            string id,
            DocumentTypeMapping<TDocument> mapping,
            IList<VersionedDbDocument> documents)
        {
            var ordered = documents.OrderBy(x => x.Version).ToArray();

            if (ordered.Length == 0 || ordered.Length > 2)
            {
                return null;
            }

            var firstVersion = ordered[0];
            var queriedVersion = ordered[ordered.Length - 1];

            return CreateReadResult(id, queriedVersion, mapping, firstVersion.Timestamp, queriedVersion.Timestamp);
        }

        private VersionedDocumentReadResult<TDocument> ReadLatestDocumentAsync<TDocument>(
            string id,
            DocumentTypeMapping<TDocument> mapping,
            IList<VersionedDbDocument> documents,
            VersionedDocumentReadOptions readOptions)
        {
            if (!TryGetLatestDocument(documents, readOptions, out var document, out var createdTime, out var modifiedTime))
            {
                return null;
            }

            return CreateReadResult(id, document, mapping, createdTime, modifiedTime);
        }

        private VersionedDocumentReadResult<TDocument> CreateReadResult<TDocument>(
            string id,
            VersionedDbDocument document,
            DocumentTypeMapping<TDocument> mapping,
            DateTime createdTime,
            DateTime modifiedTime)
        {
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
            var query = QueryClient.CreateQueryById(id, mapping);
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

            if (ordered.Length == 1 && !last.Latest)
            {
                // There is only one document returned by the query and that document is not the
                // latest. That means the query did not match the latest version of the document
                // because the first document version is always included in latest queries.
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